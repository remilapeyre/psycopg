"""
psycopg connection objects
"""

# Copyright (C) 2020 The Psycopg Team

import time
import struct
from typing import Optional, Iterator
from contextlib import contextmanager, asynccontextmanager

from . import pq
from .rows import Row
from .cursor import Cursor
from .cursor_async import AsyncCursor
from . import errors as e

from typing import Literal, TypeAlias

ReplicationType: TypeAlias = Literal["physical", "logical"]

_POSTGRES_EPOCH_JDATE = 2451545
_UNIX_EPOCH_JDATE = 2440588
_SECS_PER_DAY = 86400
_USECS_PER_SEC = 1000000


def _now() -> int:
    t = time.time()
    result: int = (
        int(t) - ((_POSTGRES_EPOCH_JDATE - _UNIX_EPOCH_JDATE) * _SECS_PER_DAY)
    ) * _USECS_PER_SEC
    return result + int((t % 1) * _USECS_PER_SEC)


class ReplicationCursor(Cursor[Row]):
    _write_lsn = 0
    _flush_lsn = 0
    _apply_lsn = 0

    def fileno(self) -> int:
        """Return the file descriptor of the connection.

        This function allows to use the connection as file-like object in
        functions waiting for readiness, such as the ones defined in the
        `selectors` module.
        """
        return self._conn.pgconn.socket

    def create_replication_slot(
        self,
        name: str,
        *,
        temporary: bool = False,
        type: ReplicationType,
        output_plugin: Optional[str] = None,
        two_phase: bool = False,
        reserve_wal: bool = False,
        snapshot: Optional[str] = None
    ) -> None:
        try:
            with self._conn.lock:
                self._conn.wait(
                    self._create_replication_slot_gen(
                        name,
                        temporary=temporary,
                        type=type,
                        output_plugin=output_plugin,
                        two_phase=two_phase,
                        reserve_wal=reserve_wal,
                        snapshot=snapshot,
                    )
                )
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    def drop_replication_slot(self, name: str, wait: bool = False) -> None:
        try:
            with self._conn.lock:
                self._conn.wait(self._drop_replication_slot_gen(name, wait))
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    @contextmanager
    def start_replication(
        self, name: str, type: ReplicationType, start_lsn: str
    ) -> Iterator[None]:
        try:
            with self._conn.lock:
                self._conn.wait(self._start_replication_gen(name, type, start_lsn))

            yield
            with self._conn.lock:
                self._stop_replication()

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    def _stop_replication(self) -> None:
        try:
            self._conn.pgconn.put_copy_end()
            self._conn.pgconn.flush()

            res = self._conn.pgconn.get_result()
            if res is not None and res.status == pq.ExecStatus.COPY_OUT:
                # We're doing a client-initiated clean exit and have sent CopyDone to
                # the server. Drain any messages, so we don't miss a last-minute
                # ErrorResponse. The walsender stops generating XLogData records once
                # it sees CopyDone, so expect this to finish quickly. After CopyDone,
                # it's too late for sendFeedback(), even if this were to take a long
                # time. Hence, use synchronous-mode PQgetCopyData().
                while True:
                    r, _ = self._conn.pgconn.get_copy_data(0)
                    if r == -1:
                        break

                res = self._conn.pgconn.get_result()

            while res is not None:
                if res.status != pq.ExecStatus.COMMAND_OK:
                    raise e.error_from_result(res)
                res = self._conn.pgconn.get_result()

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    def send_feedback(
        self,
        write_lsn: int = 0,
        flush_lsn: int = 0,
        apply_lsn: int = 0,
        reply: bool = False,
        force: bool = False,
    ) -> None:
        if write_lsn > self._write_lsn:
            self._write_lsn = write_lsn
        if flush_lsn > self._flush_lsn:
            self._flush_lsn = flush_lsn
        if apply_lsn > self._apply_lsn:
            self._apply_lsn = apply_lsn

        buffer = _pack_feedback(
            b"r",
            self._write_lsn,
            self._flush_lsn,
            self._apply_lsn,
            _now(),
            reply,
        )

        self._conn.pgconn.put_copy_data(buffer)
        self._conn.pgconn.flush()

    def read_message(self) -> Optional[bytes]:
        length, buffer = self._conn.pgconn.get_copy_data(1)

        consumed = False
        while not consumed:
            if length == 0:
                # We should only try reading more data when there is nothing
                # available at the moment.  Otherwise, with a really highly loaded
                # server we might be reading a number of messages for every single
                # one we process, thus overgrowing the internal buffer until the
                # client system runs out of memory.
                self._conn.pgconn.consume_input()

                # But PQconsumeInput() doesn't tell us if it has actually read
                # anything into the internal buffer and there is no (supported) way
                # to ask libpq about this directly.  The way we check is setting the
                # flag and re-trying PQgetCopyData(): if that returns 0 again,
                # there's no more data available in the buffer, so we return None.
                length, buffer = self._conn.pgconn.get_copy_data(1)
                if length == 0:
                    return None

            if length == -1:
                self.pgresult = self._conn.pgconn.get_result()

                if (
                    self.pgresult is not None
                    and self.pgresult.status != pq.ExecStatus.COMMAND_OK
                ):
                    raise e.error_from_result(self.pgresult)

                return None

            # It also makes sense to set this flag here to make us return early in
            # case of retry due to keepalive message.  Any pending data on the socket
            # will trigger read condition in select() in the calling code anyway.
            consumed = True

            if chr(buffer[0]) == "w":
                hdr = 1 + 8 + 8 + 8
                if len(buffer) < 1 + 8 + 8 + 8 + 1:
                    raise e.OperationalError("data message header too small")

                _, data_start, wal_end, send_time = _unpack_data(buffer[:hdr])
                return bytes(buffer[hdr:])

            elif chr(buffer[0]) == "k":
                if len(buffer) < 1 + 8 + 8 + 1:
                    raise e.OperationalError("keepalive message header too small")
                _, wal_end, send_time, reply = _unpack_keepalive(buffer[:])
                if reply:
                    self.send_feedback(
                        self._write_lsn, self._flush_lsn, self._apply_lsn
                    )
            else:
                raise e.OperationalError("unrecognized replication message type")

        return None


class AsyncReplicationCursor(AsyncCursor[Row]):
    _write_lsn = 0
    _flush_lsn = 0
    _apply_lsn = 0

    def fileno(self) -> int:
        """Return the file descriptor of the connection.

        This function allows to use the connection as file-like object in
        functions waiting for readiness, such as the ones defined in the
        `selectors` module.
        """
        return self._conn.pgconn.socket

    async def create_replication_slot(
        self,
        name: str,
        *,
        temporary: bool = False,
        type: ReplicationType,
        output_plugin: Optional[str] = None,
        two_phase: bool = False,
        reserve_wal: bool = False,
        snapshot: Optional[str] = None
    ) -> None:
        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._create_replication_slot_gen(
                        name,
                        temporary=temporary,
                        type=type,
                        output_plugin=output_plugin,
                        two_phase=two_phase,
                        reserve_wal=reserve_wal,
                        snapshot=snapshot,
                    )
                )
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    async def drop_replication_slot(self, name: str, wait: bool = False) -> None:
        try:
            async with self._conn.lock:
                await self._conn.wait(self._drop_replication_slot_gen(name, wait))
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    @asynccontextmanager
    async def start_replication(
        self, name: str, type: ReplicationType, start_lsn: str
    ) -> Iterator[None]:
        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._start_replication_gen(name, type, start_lsn)
                )

            yield
            async with self._conn.lock:
                self._stop_replication()

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    def _stop_replication(self) -> None:
        try:
            self._conn.pgconn.put_copy_end()
            self._conn.pgconn.flush()

            res = self._conn.pgconn.get_result()
            if res is not None and res.status == pq.ExecStatus.COPY_OUT:
                # We're doing a client-initiated clean exit and have sent CopyDone to
                # the server. Drain any messages, so we don't miss a last-minute
                # ErrorResponse. The walsender stops generating XLogData records once
                # it sees CopyDone, so expect this to finish quickly. After CopyDone,
                # it's too late for sendFeedback(), even if this were to take a long
                # time. Hence, use synchronous-mode PQgetCopyData().
                while True:
                    r, _ = self._conn.pgconn.get_copy_data(0)
                    if r == -1:
                        break

                res = self._conn.pgconn.get_result()

            while res is not None:
                if res.status != pq.ExecStatus.COMMAND_OK:
                    raise e.error_from_result(res)
                res = self._conn.pgconn.get_result()

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    async def send_feedback(
        self,
        write_lsn: int = 0,
        flush_lsn: int = 0,
        apply_lsn: int = 0,
        reply: bool = False,
        force: bool = False,
    ) -> None:
        if write_lsn > self._write_lsn:
            self._write_lsn = write_lsn
        if flush_lsn > self._flush_lsn:
            self._flush_lsn = flush_lsn
        if apply_lsn > self._apply_lsn:
            self._apply_lsn = apply_lsn

        buffer = _pack_feedback(
            b"r",
            self._write_lsn,
            self._flush_lsn,
            self._apply_lsn,
            _now(),
            reply,
        )

        self._conn.pgconn.put_copy_data(buffer)
        self._conn.pgconn.flush()


_pack_feedback = struct.Struct("!cqqqq?").pack
_unpack_data = struct.Struct("!cqqq").unpack
_unpack_keepalive = struct.Struct("!cqq?").unpack
