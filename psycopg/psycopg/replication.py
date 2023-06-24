"""
psycopg connection objects
"""

# Copyright (C) 2020 The Psycopg Team

from contextlib import contextmanager, asynccontextmanager
from typing import Any, Iterator, AsyncIterator
from typing import Optional, Type, Union, cast
from typing import overload
from dataclasses import dataclass

from . import errors as e
from .abc import AdaptContext
from .rows import Row, RowFactory, AsyncRowFactory, TupleRow
from .cursor import Cursor, ReplicationType
from .connection import Connection as _Connection
from .cursor_async import AsyncCursor
from .connection_async import AsyncConnection as _AsyncConnection


@dataclass
class ReplicationMessage:
    __slots__ = ["payload", "data_size", "data_start", "wal_end", "send_time"]

    payload: bytes
    data_size: int
    data_start: int
    wal_end: int
    send_time: int


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
        snapshot: Optional[str] = None,
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
                self._conn.wait(self._stop_replication_gen())

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

        try:
            with self._conn.lock:
                self._conn.wait(
                    self._send_feedback_gen(
                        self._write_lsn,
                        self._flush_lsn,
                        self._apply_lsn,
                        reply,
                        force,
                    )
                )

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    def read_message(self) -> Optional[ReplicationMessage]:
        try:
            with self._conn.lock:
                res = self._conn.wait(self._read_message_gen())
                if res is None:
                    return None
                else:
                    return ReplicationMessage(*res)

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)


class Connection(_Connection[Row]):
    @overload  # type: ignore[override]
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        row_factory: RowFactory[Row],
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[ReplicationCursor[Row]]] = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "Connection[Row]":
        # TODO: returned type should be _Self. See #308.
        ...

    @overload
    @classmethod
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[ReplicationCursor[Any]]] = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "Connection[TupleRow]":
        ...

    @classmethod  # type: ignore[misc] # https://github.com/python/mypy/issues/11004
    def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        row_factory: Optional[RowFactory[Row]] = None,
        cursor_factory: Optional[Type[ReplicationCursor[Row]]] = cast(
            Type[ReplicationCursor[Row]], ReplicationCursor
        ),
        context: Optional[AdaptContext] = None,
        **kwargs: Any,
    ) -> "Connection[Any]":
        rv = super().connect(
            conninfo,
            autocommit=autocommit,
            prepare_threshold=prepare_threshold,
            row_factory=row_factory,
            cursor_factory=cursor_factory,
            context=context,
            **kwargs,
        )
        return cast(Connection[Any], rv)


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
        snapshot: Optional[str] = None,
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
    ) -> AsyncIterator[None]:
        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._start_replication_gen(name, type, start_lsn)
                )

            yield
            async with self._conn.lock:
                await self._conn.wait(self._stop_replication_gen())

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

        try:
            async with self._conn.lock:
                await self._conn.wait(
                    self._send_feedback_gen(
                        self._write_lsn,
                        self._flush_lsn,
                        self._apply_lsn,
                        reply,
                        force,
                    )
                )

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    async def read_message(self) -> Optional[ReplicationMessage]:
        try:
            async with self._conn.lock:
                res = await self._conn.wait(self._read_message_gen())
                if res is None:
                    return None
                else:
                    return ReplicationMessage(*res)

        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)


class AsyncConnection(_AsyncConnection[Row]):
    @overload  # type: ignore[override]
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        row_factory: AsyncRowFactory[Row],
        cursor_factory: Optional[Type[AsyncReplicationCursor[Row]]] = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AsyncConnection[Row]":
        # TODO: returned type should be _Self. See #308.
        ...

    @overload
    @classmethod
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        cursor_factory: Optional[Type[AsyncReplicationCursor[Any]]] = None,
        context: Optional[AdaptContext] = None,
        **kwargs: Union[None, int, str],
    ) -> "AsyncConnection[TupleRow]":
        ...

    @classmethod  # type: ignore[misc] # https://github.com/python/mypy/issues/11004
    async def connect(
        cls,
        conninfo: str = "",
        *,
        autocommit: bool = False,
        prepare_threshold: Optional[int] = 5,
        context: Optional[AdaptContext] = None,
        row_factory: Optional[AsyncRowFactory[Row]] = None,
        cursor_factory: Optional[Type[AsyncReplicationCursor[Row]]] = cast(
            Type[AsyncReplicationCursor[Row]], AsyncReplicationCursor
        ),
        **kwargs: Any,
    ) -> "AsyncConnection[Any]":
        rv = await super().connect(
            conninfo,
            autocommit=autocommit,
            prepare_threshold=prepare_threshold,
            row_factory=row_factory,
            cursor_factory=cursor_factory,
            context=context,
            **kwargs,
        )
        return cast(AsyncConnection[Any], rv)
