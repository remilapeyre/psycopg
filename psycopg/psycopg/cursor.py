"""
psycopg cursor objects
"""

# Copyright (C) 2020 The Psycopg Team

from functools import partial
from types import TracebackType
from typing import Any, Generic, Iterable, Iterator, List
from typing import (
    Optional,
    NoReturn,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)
from typing import overload, TYPE_CHECKING
from contextlib import contextmanager
from typing_extensions import TypeAlias, Literal
import struct
import time

from . import pq
from . import adapt
from . import errors as e
from .abc import ConnectionType, Query, Params, PQGen
from .sql import Composable, Composed, SQL, Identifier
from .copy import Copy, Writer as CopyWriter
from .rows import Row, RowMaker, RowFactory
from .waiting import Wait
from ._column import Column
from ._queries import PostgresQuery, PostgresClientQuery
from ._pipeline import Pipeline
from ._encodings import pgconn_encoding
from ._preparing import Prepare
from .generators import execute, fetch, send, copy_to, copy_from, copy_end

if TYPE_CHECKING:
    from .abc import Transformer
    from .pq.abc import PGconn, PGresult
    from .connection import Connection

TEXT = pq.Format.TEXT
BINARY = pq.Format.BINARY

EMPTY_QUERY = pq.ExecStatus.EMPTY_QUERY
COMMAND_OK = pq.ExecStatus.COMMAND_OK
TUPLES_OK = pq.ExecStatus.TUPLES_OK
COPY_OUT = pq.ExecStatus.COPY_OUT
COPY_IN = pq.ExecStatus.COPY_IN
COPY_BOTH = pq.ExecStatus.COPY_BOTH
FATAL_ERROR = pq.ExecStatus.FATAL_ERROR
SINGLE_TUPLE = pq.ExecStatus.SINGLE_TUPLE
PIPELINE_ABORTED = pq.ExecStatus.PIPELINE_ABORTED

ACTIVE = pq.TransactionStatus.ACTIVE


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


class BaseCursor(Generic[ConnectionType, Row]):
    __slots__ = """
        _conn format _adapters arraysize _closed _results pgresult _pos
        _iresult _rowcount _query _tx _last_query _row_factory _make_row
        _pgconn _execmany_returning
        __weakref__
        """.split()

    ExecStatus = pq.ExecStatus

    _tx: "Transformer"
    _make_row: RowMaker[Row]
    _pgconn: "PGconn"
    _write_lsn: int
    _flush_lsn: int
    _apply_lsn: int

    def __init__(self, connection: ConnectionType):
        self._conn = connection
        self.format = TEXT
        self._pgconn = connection.pgconn
        self._adapters = adapt.AdaptersMap(connection.adapters)
        self.arraysize = 1
        self._closed = False
        self._last_query: Optional[Query] = None
        self._reset()

    def _reset(self, reset_query: bool = True) -> None:
        self._results: List["PGresult"] = []
        self.pgresult: Optional["PGresult"] = None
        self._pos = 0
        self._iresult = 0
        self._rowcount = -1
        self._query: Optional[PostgresQuery]
        # None if executemany() not executing, True/False according to returning state
        self._execmany_returning: Optional[bool] = None
        if reset_query:
            self._query = None

    def __repr__(self) -> str:
        cls = f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        info = pq.misc.connection_summary(self._pgconn)
        if self._closed:
            status = "closed"
        elif self.pgresult:
            status = pq.ExecStatus(self.pgresult.status).name
        else:
            status = "no result"
        return f"<{cls} [{status}] {info} at 0x{id(self):x}>"

    @property
    def connection(self) -> ConnectionType:
        """The connection this cursor is using."""
        return self._conn

    @property
    def adapters(self) -> adapt.AdaptersMap:
        return self._adapters

    @property
    def closed(self) -> bool:
        """`True` if the cursor is closed."""
        return self._closed

    @property
    def description(self) -> Optional[List[Column]]:
        """
        A list of `Column` objects describing the current resultset.

        `!None` if the current resultset didn't return tuples.
        """
        res = self.pgresult

        # We return columns if we have nfields, but also if we don't but
        # the query said we got tuples (mostly to handle the super useful
        # query "SELECT ;"
        if res and (
            res.nfields or res.status == TUPLES_OK or res.status == SINGLE_TUPLE
        ):
            return [Column(self, i) for i in range(res.nfields)]
        else:
            return None

    @property
    def rowcount(self) -> int:
        """Number of records affected by the precedent operation."""
        return self._rowcount

    @property
    def rownumber(self) -> Optional[int]:
        """Index of the next row to fetch in the current result.

        `!None` if there is no result to fetch.
        """
        tuples = self.pgresult and self.pgresult.status == TUPLES_OK
        return self._pos if tuples else None

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        # no-op
        pass

    def setoutputsize(self, size: Any, column: Optional[int] = None) -> None:
        # no-op
        pass

    def nextset(self) -> Optional[bool]:
        """
        Move to the result set of the next query executed through `executemany()`
        or to the next result set if `execute()` returned more than one.

        Return `!True` if a new result is available, which will be the one
        methods `!fetch*()` will operate on.
        """
        if self._iresult < len(self._results) - 1:
            self._select_current_result(self._iresult + 1)
            return True
        else:
            return None

    @property
    def statusmessage(self) -> Optional[str]:
        """
        The command status tag from the last SQL command executed.

        `!None` if the cursor doesn't have a result available.
        """
        msg = self.pgresult.command_status if self.pgresult else None
        return msg.decode() if msg else None

    def _make_row_maker(self) -> RowMaker[Row]:
        raise NotImplementedError

    #
    # Generators for the high level operations on the cursor
    #
    # Like for sync/async connections, these are implemented as generators
    # so that different concurrency strategies (threads,asyncio) can use their
    # own way of waiting (or better, `connection.wait()`).
    #

    def _execute_gen(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> PQGen[None]:
        """Generator implementing `Cursor.execute()`."""
        yield from self._start_query(query)
        pgq = self._convert_query(query, params)
        yield from self._maybe_prepare_gen(pgq, prepare=prepare, binary=binary)
        if self._conn._pipeline:
            yield from self._conn._pipeline._communicate_gen()

        self._last_query = query

        for cmd in self._conn._prepared.get_maintenance_commands():
            yield from self._conn._exec_command(cmd)

    def _executemany_gen_pipeline(
        self, query: Query, params_seq: Iterable[Params], returning: bool
    ) -> PQGen[None]:
        """
        Generator implementing `Cursor.executemany()` with pipelines available.
        """
        pipeline = self._conn._pipeline
        assert pipeline

        yield from self._start_query(query)
        if not returning:
            self._rowcount = 0

        assert self._execmany_returning is None
        self._execmany_returning = returning

        first = True
        for params in params_seq:
            if first:
                pgq = self._convert_query(query, params)
                self._query = pgq
                first = False
            else:
                pgq.dump(params)

            yield from self._maybe_prepare_gen(pgq, prepare=True)
            yield from pipeline._communicate_gen()

        self._last_query = query

        if returning:
            yield from pipeline._fetch_gen(flush=True)

        for cmd in self._conn._prepared.get_maintenance_commands():
            yield from self._conn._exec_command(cmd)

    def _executemany_gen_no_pipeline(
        self, query: Query, params_seq: Iterable[Params], returning: bool
    ) -> PQGen[None]:
        """
        Generator implementing `Cursor.executemany()` with pipelines not available.
        """
        yield from self._start_query(query)
        if not returning:
            self._rowcount = 0

        assert self._execmany_returning is None
        self._execmany_returning = returning

        first = True
        for params in params_seq:
            if first:
                pgq = self._convert_query(query, params)
                self._query = pgq
                first = False
            else:
                pgq.dump(params)

            yield from self._maybe_prepare_gen(pgq, prepare=True)

        self._last_query = query

        for cmd in self._conn._prepared.get_maintenance_commands():
            yield from self._conn._exec_command(cmd)

    def _maybe_prepare_gen(
        self,
        pgq: PostgresQuery,
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> PQGen[None]:
        # Check if the query is prepared or needs preparing
        prep, name = self._get_prepared(pgq, prepare)
        if prep is Prepare.NO:
            # The query must be executed without preparing
            self._execute_send(pgq, binary=binary)
        else:
            # If the query is not already prepared, prepare it.
            if prep is Prepare.SHOULD:
                self._send_prepare(name, pgq)
                if not self._conn._pipeline:
                    (result,) = yield from execute(self._pgconn)
                    if result.status == FATAL_ERROR:
                        raise e.error_from_result(result, encoding=self._encoding)
            # Then execute it.
            self._send_query_prepared(name, pgq, binary=binary)

        # Update the prepare state of the query.
        # If an operation requires to flush our prepared statements cache,
        # it will be added to the maintenance commands to execute later.
        key = self._conn._prepared.maybe_add_to_cache(pgq, prep, name)

        if self._conn._pipeline:
            queued = None
            if key is not None:
                queued = (key, prep, name)
            self._conn._pipeline.result_queue.append((self, queued))
            return

        # run the query
        results = yield from execute(self._pgconn)

        if key is not None:
            self._conn._prepared.validate(key, prep, name, results)

        self._check_results(results)
        self._set_results(results)

    def _get_prepared(
        self, pgq: PostgresQuery, prepare: Optional[bool] = None
    ) -> Tuple[Prepare, bytes]:
        return self._conn._prepared.get(pgq, prepare)

    def _stream_send_gen(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
    ) -> PQGen[None]:
        """Generator to send the query for `Cursor.stream()`."""
        yield from self._start_query(query)
        pgq = self._convert_query(query, params)
        self._execute_send(pgq, binary=binary, force_extended=True)
        self._pgconn.set_single_row_mode()
        self._last_query = query
        yield from send(self._pgconn)

    def _stream_fetchone_gen(self, first: bool) -> PQGen[Optional["PGresult"]]:
        res = yield from fetch(self._pgconn)
        if res is None:
            return None

        status = res.status
        if status == SINGLE_TUPLE:
            self.pgresult = res
            self._tx.set_pgresult(res, set_loaders=first)
            if first:
                self._make_row = self._make_row_maker()
            return res

        elif status == TUPLES_OK or status == COMMAND_OK:
            # End of single row results
            while res:
                res = yield from fetch(self._pgconn)
            if status != TUPLES_OK:
                raise e.ProgrammingError(
                    "the operation in stream() didn't produce a result"
                )
            return None

        else:
            # Errors, unexpected values
            return self._raise_for_result(res)

    def _start_query(self, query: Optional[Query] = None) -> PQGen[None]:
        """Generator to start the processing of a query.

        It is implemented as generator because it may send additional queries,
        such as `begin`.
        """
        if self.closed:
            raise e.InterfaceError("the cursor is closed")

        self._reset()
        if not self._last_query or (self._last_query is not query):
            self._last_query = None
            self._tx = adapt.Transformer(self)
        yield from self._conn._start_query()

    def _start_copy_gen(
        self, statement: Query, params: Optional[Params] = None
    ) -> PQGen[None]:
        """Generator implementing sending a command for `Cursor.copy()."""

        # The connection gets in an unrecoverable state if we attempt COPY in
        # pipeline mode. Forbid it explicitly.
        if self._conn._pipeline:
            raise e.NotSupportedError("COPY cannot be used in pipeline mode")

        yield from self._start_query()

        # Merge the params client-side
        if params:
            pgq = PostgresClientQuery(self._tx)
            pgq.convert(statement, params)
            statement = pgq.query

        query = self._convert_query(statement)

        self._execute_send(query, binary=False)
        results = yield from execute(self._pgconn)
        if len(results) != 1:
            raise e.ProgrammingError("COPY cannot be mixed with other operations")

        self._check_copy_result(results[0])
        self._set_results(results)

    def _execute_send(
        self,
        query: PostgresQuery,
        *,
        force_extended: bool = False,
        binary: Optional[bool] = None,
    ) -> None:
        """
        Implement part of execute() before waiting common to sync and async.

        This is not a generator, but a normal non-blocking function.
        """
        if binary is None:
            fmt = self.format
        else:
            fmt = BINARY if binary else TEXT

        self._query = query

        if self._conn._pipeline:
            # In pipeline mode always use PQsendQueryParams - see #314
            # Multiple statements in the same query are not allowed anyway.
            self._conn._pipeline.command_queue.append(
                partial(
                    self._pgconn.send_query_params,
                    query.query,
                    query.params,
                    param_formats=query.formats,
                    param_types=query.types,
                    result_format=fmt,
                )
            )
        elif force_extended or query.params or fmt == BINARY:
            self._pgconn.send_query_params(
                query.query,
                query.params,
                param_formats=query.formats,
                param_types=query.types,
                result_format=fmt,
            )
        else:
            # If we can, let's use simple query protocol,
            # as it can execute more than one statement in a single query.
            self._pgconn.send_query(query.query)

    def _convert_query(
        self, query: Query, params: Optional[Params] = None
    ) -> PostgresQuery:
        pgq = PostgresQuery(self._tx)
        pgq.convert(query, params)
        return pgq

    def _check_results(self, results: List["PGresult"]) -> None:
        """
        Verify that the results of a query are valid.

        Verify that the query returned at least one result and that they all
        represent a valid result from the database.
        """
        if not results:
            raise e.InternalError("got no result from the query")

        for res in results:
            status = res.status
            if status != TUPLES_OK and status != COMMAND_OK and status != EMPTY_QUERY:
                self._raise_for_result(res)

    def _raise_for_result(self, result: "PGresult") -> NoReturn:
        """
        Raise an appropriate error message for an unexpected database result
        """
        status = result.status
        if status == FATAL_ERROR:
            raise e.error_from_result(result, encoding=self._encoding)
        elif status == PIPELINE_ABORTED:
            raise e.PipelineAborted("pipeline aborted")
        elif status == COPY_IN or status == COPY_OUT or status == COPY_BOTH:
            raise e.ProgrammingError(
                "COPY cannot be used with this method; use copy() instead"
            )
        else:
            raise e.InternalError(
                "unexpected result status from query:" f" {pq.ExecStatus(status).name}"
            )

    def _select_current_result(
        self, i: int, format: Optional[pq.Format] = None
    ) -> None:
        """
        Select one of the results in the cursor as the active one.
        """
        self._iresult = i
        res = self.pgresult = self._results[i]

        # Note: the only reason to override format is to correctly set
        # binary loaders on server-side cursors, because send_describe_portal
        # only returns a text result.
        self._tx.set_pgresult(res, format=format)

        self._pos = 0

        if res.status == TUPLES_OK:
            self._rowcount = self.pgresult.ntuples

        # COPY_OUT has never info about nrows. We need such result for the
        # columns in order to return a `description`, but not overwrite the
        # cursor rowcount (which was set by the Copy object).
        elif res.status != COPY_OUT:
            nrows = self.pgresult.command_tuples
            self._rowcount = nrows if nrows is not None else -1

        self._make_row = self._make_row_maker()

    def _set_results(self, results: List["PGresult"]) -> None:
        first_batch = not self._results

        if self._execmany_returning is None:
            # Received from execute()
            self._results.extend(results)
            if first_batch:
                self._select_current_result(0)

        else:
            # Received from executemany()
            if self._execmany_returning:
                self._results.extend(results)
                if first_batch:
                    self._select_current_result(0)
            else:
                # In non-returning case, set rowcount to the cumulated number of
                # rows of executed queries.
                for res in results:
                    self._rowcount += res.command_tuples or 0

    def _send_prepare(self, name: bytes, query: PostgresQuery) -> None:
        if self._conn._pipeline:
            self._conn._pipeline.command_queue.append(
                partial(
                    self._pgconn.send_prepare,
                    name,
                    query.query,
                    param_types=query.types,
                )
            )
            self._conn._pipeline.result_queue.append(None)
        else:
            self._pgconn.send_prepare(name, query.query, param_types=query.types)

    def _send_query_prepared(
        self, name: bytes, pgq: PostgresQuery, *, binary: Optional[bool] = None
    ) -> None:
        if binary is None:
            fmt = self.format
        else:
            fmt = BINARY if binary else TEXT

        if self._conn._pipeline:
            self._conn._pipeline.command_queue.append(
                partial(
                    self._pgconn.send_query_prepared,
                    name,
                    pgq.params,
                    param_formats=pgq.formats,
                    result_format=fmt,
                )
            )
        else:
            self._pgconn.send_query_prepared(
                name, pgq.params, param_formats=pgq.formats, result_format=fmt
            )

    def _check_result_for_fetch(self) -> None:
        if self.closed:
            raise e.InterfaceError("the cursor is closed")
        res = self.pgresult
        if not res:
            raise e.ProgrammingError("no result available")

        status = res.status
        if status == TUPLES_OK:
            return
        elif status == FATAL_ERROR:
            raise e.error_from_result(res, encoding=self._encoding)
        elif status == PIPELINE_ABORTED:
            raise e.PipelineAborted("pipeline aborted")
        else:
            raise e.ProgrammingError("the last operation didn't produce a result")

    def _check_copy_result(self, result: "PGresult") -> None:
        """
        Check that the value returned in a copy() operation is a legit COPY.
        """
        status = result.status
        if status == COPY_IN or status == COPY_OUT or status == COPY_BOTH:
            return
        elif status == FATAL_ERROR:
            raise e.error_from_result(result, encoding=self._encoding)
        else:
            raise e.ProgrammingError(
                "copy() should be used only with COPY ... TO STDOUT or COPY ..."
                f" FROM STDIN statements, got {pq.ExecStatus(status).name}"
            )

    def _scroll(self, value: int, mode: str) -> None:
        self._check_result_for_fetch()
        assert self.pgresult
        if mode == "relative":
            newpos = self._pos + value
        elif mode == "absolute":
            newpos = value
        else:
            raise ValueError(f"bad mode: {mode}. It should be 'relative' or 'absolute'")
        if not 0 <= newpos < self.pgresult.ntuples:
            raise IndexError("position out of bound")
        self._pos = newpos

    def _close(self) -> None:
        """Non-blocking part of closing. Common to sync/async."""
        # Don't reset the query because it may be useful to investigate after
        # an error.
        self._reset(reset_query=False)
        self._closed = True

    @property
    def _encoding(self) -> str:
        return pgconn_encoding(self._pgconn)

    def _create_replication_slot_gen(
        self,
        name: str,
        *,
        temporary: bool = False,
        type: ReplicationType,
        output_plugin: Optional[str] = None,
        two_phase: bool = False,
        reserve_wal: bool = False,
        snapshot: Optional[str] = None,
    ) -> PQGen[None]:
        if type not in ("logical", "physical"):
            raise ValueError(f"type must be 'physical' or 'logical', not {type!r}")
        elif type == "logical" and output_plugin is None:
            raise ValueError("output_plugin is required for logical replication")
        elif type == "physical" and output_plugin is not None:
            raise ValueError("output_plugin can only be set for logical replication")

        query: Composable = SQL("CREATE_REPLICATION_SLOT ")
        query += Identifier(name)
        if temporary:
            query += SQL("TEMPORARY ")
        query += SQL(f"{type.upper()} ")
        if output_plugin:
            query += Identifier(f"{output_plugin}") + SQL(" ")

        # reserve_wal is only for physical, the other two only for logical
        if reserve_wal:
            query += SQL("RESERVE_WAL ")

        if snapshot is None:
            pass
        elif snapshot == "export":
            query += SQL("EXPORT_SNAPSHOT ")
        elif snapshot == "use":
            query += SQL("USE_SNAPSHOT ")
        elif snapshot == "nothing":
            query += SQL("NOEXPORT_SNAPSHOT ")
        else:
            raise ValueError(f"unexpected value for snapshot: {snapshot!r}")

        # TWO_PHASE is only supported on PG15
        # if two_phase:
        #     query += SQL("TWO_PHASE ")

        return self._execute_gen(query)

    def _start_replication_gen(
        self, name: str, type: ReplicationType, start_lsn: str
    ) -> PQGen[None]:
        query: Composable = SQL("START_REPLICATION ")
        if name is not None:
            query += SQL("SLOT {} ").format(Identifier(name))

        if type == "physical":
            query += SQL("PHYSICAL ")
        else:
            query += SQL("LOGICAL ")

        query += SQL(start_lsn)
        return self._start_copy_gen(query)

    def _stop_replication_gen(self) -> PQGen[None]:
        res: Optional[PGresult]
        res = yield from copy_end(self._pgconn, None)

        if res.status == pq.ExecStatus.COPY_OUT:
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

                yield Wait.W

            res = self._conn.pgconn.get_result()

        while res is not None:
            if res.status != pq.ExecStatus.COMMAND_OK:
                raise e.error_from_result(res)
            res = self._conn.pgconn.get_result()

    def _drop_replication_slot_gen(self, name: str, wait: bool = False) -> PQGen[None]:
        if wait:
            query: Composed = SQL("DROP_REPLICATION_SLOT {} WAIT").format(
                Identifier(name)
            )
        else:
            query = SQL("DROP_REPLICATION_SLOT {}").format(Identifier(name))

        return self._execute_gen(query, None)

    def _send_feedback_gen(
        self,
        write_lsn: int = 0,
        flush_lsn: int = 0,
        apply_lsn: int = 0,
        reply: bool = False,
        force: bool = False,
    ) -> PQGen[None]:
        buffer = _pack_feedback(
            b"r",
            write_lsn,
            flush_lsn,
            apply_lsn,
            _now(),
            reply,
        )
        yield from copy_to(self._pgconn, buffer)
        self._conn.pgconn.flush()

    def _read_message_gen(self) -> PQGen[Optional[tuple[bytes, int, Any, Any, Any]]]:
        data = yield from copy_from(self._pgconn)
        if not isinstance(data, memoryview):
            self.pgresult = data
            if self.pgresult.status != pq.ExecStatus.COMMAND_OK:
                raise e.error_from_result(self.pgresult)
            return None

        if chr(data[0]) == "w":
            hdr = 1 + 8 + 8 + 8
            if len(data) < hdr + 1:
                raise e.OperationalError("data message header too small")

            _, data_start, wal_end, send_time = _unpack_data(data[:hdr])
            return (
                bytes(data[hdr:]),
                len(data) - hdr,
                data_start,
                wal_end,
                send_time,
            )

        elif chr(data[0]) == "k":
            if len(data) < 1 + 8 + 8 + 1:
                raise e.OperationalError("keepalive message header too small")
            _, wal_end, send_time, reply = _unpack_keepalive(data[:])
            if reply:
                yield from self._send_feedback_gen(
                    self._write_lsn, self._flush_lsn, self._apply_lsn
                )
            return None

        raise e.OperationalError("unrecognized replication message type")


class Cursor(BaseCursor["Connection[Any]", Row]):
    __module__ = "psycopg"
    __slots__ = ()
    _Self = TypeVar("_Self", bound="Cursor[Any]")

    @overload
    def __init__(self: "Cursor[Row]", connection: "Connection[Row]"):
        ...

    @overload
    def __init__(
        self: "Cursor[Row]",
        connection: "Connection[Any]",
        *,
        row_factory: RowFactory[Row],
    ):
        ...

    def __init__(
        self,
        connection: "Connection[Any]",
        *,
        row_factory: Optional[RowFactory[Row]] = None,
    ):
        super().__init__(connection)
        self._row_factory = row_factory or connection.row_factory

    def __enter__(self: _Self) -> _Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Close the current cursor and free associated resources.
        """
        self._close()

    @property
    def row_factory(self) -> RowFactory[Row]:
        """Writable attribute to control how result rows are formed."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, row_factory: RowFactory[Row]) -> None:
        self._row_factory = row_factory
        if self.pgresult:
            self._make_row = row_factory(self)

    def _make_row_maker(self) -> RowMaker[Row]:
        return self._row_factory(self)

    def execute(
        self: _Self,
        query: Query,
        params: Optional[Params] = None,
        *,
        prepare: Optional[bool] = None,
        binary: Optional[bool] = None,
    ) -> _Self:
        """
        Execute a query or command to the database.
        """
        try:
            with self._conn.lock:
                self._conn.wait(
                    self._execute_gen(query, params, prepare=prepare, binary=binary)
                )
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)
        return self

    def executemany(
        self,
        query: Query,
        params_seq: Iterable[Params],
        *,
        returning: bool = False,
    ) -> None:
        """
        Execute the same command with a sequence of input data.
        """
        try:
            if Pipeline.is_supported():
                # If there is already a pipeline, ride it, in order to avoid
                # sending unnecessary Sync.
                with self._conn.lock:
                    p = self._conn._pipeline
                    if p:
                        self._conn.wait(
                            self._executemany_gen_pipeline(query, params_seq, returning)
                        )
                # Otherwise, make a new one
                if not p:
                    with self._conn.pipeline(), self._conn.lock:
                        self._conn.wait(
                            self._executemany_gen_pipeline(query, params_seq, returning)
                        )
            else:
                with self._conn.lock:
                    self._conn.wait(
                        self._executemany_gen_no_pipeline(query, params_seq, returning)
                    )
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

    def stream(
        self,
        query: Query,
        params: Optional[Params] = None,
        *,
        binary: Optional[bool] = None,
    ) -> Iterator[Row]:
        """
        Iterate row-by-row on a result from the database.
        """
        if self._pgconn.pipeline_status:
            raise e.ProgrammingError("stream() cannot be used in pipeline mode")

        with self._conn.lock:
            try:
                self._conn.wait(self._stream_send_gen(query, params, binary=binary))
                first = True
                while self._conn.wait(self._stream_fetchone_gen(first)):
                    # We know that, if we got a result, it has a single row.
                    rec: Row = self._tx.load_row(0, self._make_row)  # type: ignore
                    yield rec
                    first = False

            except e._NO_TRACEBACK as ex:
                raise ex.with_traceback(None)

            finally:
                if self._pgconn.transaction_status == ACTIVE:
                    # Try to cancel the query, then consume the results
                    # already received.
                    self._conn.cancel()
                    try:
                        while self._conn.wait(self._stream_fetchone_gen(first=False)):
                            pass
                    except Exception:
                        pass

                    # Try to get out of ACTIVE state. Just do a single attempt, which
                    # should work to recover from an error or query cancelled.
                    try:
                        self._conn.wait(self._stream_fetchone_gen(first=False))
                    except Exception:
                        pass

    def fetchone(self) -> Optional[Row]:
        """
        Return the next record from the current recordset.

        Return `!None` the recordset is finished.

        :rtype: Optional[Row], with Row defined by `row_factory`
        """
        self._fetch_pipeline()
        self._check_result_for_fetch()
        record = self._tx.load_row(self._pos, self._make_row)
        if record is not None:
            self._pos += 1
        return record

    def fetchmany(self, size: int = 0) -> List[Row]:
        """
        Return the next `!size` records from the current recordset.

        `!size` default to `!self.arraysize` if not specified.

        :rtype: Sequence[Row], with Row defined by `row_factory`
        """
        self._fetch_pipeline()
        self._check_result_for_fetch()
        assert self.pgresult

        if not size:
            size = self.arraysize
        records = self._tx.load_rows(
            self._pos,
            min(self._pos + size, self.pgresult.ntuples),
            self._make_row,
        )
        self._pos += len(records)
        return records

    def fetchall(self) -> List[Row]:
        """
        Return all the remaining records from the current recordset.

        :rtype: Sequence[Row], with Row defined by `row_factory`
        """
        self._fetch_pipeline()
        self._check_result_for_fetch()
        assert self.pgresult
        records = self._tx.load_rows(self._pos, self.pgresult.ntuples, self._make_row)
        self._pos = self.pgresult.ntuples
        return records

    def __iter__(self) -> Iterator[Row]:
        self._fetch_pipeline()
        self._check_result_for_fetch()

        def load(pos: int) -> Optional[Row]:
            return self._tx.load_row(pos, self._make_row)

        while True:
            row = load(self._pos)
            if row is None:
                break
            self._pos += 1
            yield row

    def scroll(self, value: int, mode: str = "relative") -> None:
        """
        Move the cursor in the result set to a new position according to mode.

        If `!mode` is ``'relative'`` (default), `!value` is taken as offset to
        the current position in the result set; if set to ``'absolute'``,
        `!value` states an absolute target position.

        Raise `!IndexError` in case a scroll operation would leave the result
        set. In this case the position will not change.
        """
        self._fetch_pipeline()
        self._scroll(value, mode)

    @contextmanager
    def copy(
        self,
        statement: Query,
        params: Optional[Params] = None,
        *,
        writer: Optional[CopyWriter] = None,
    ) -> Iterator[Copy]:
        """
        Initiate a :sql:`COPY` operation and return an object to manage it.

        :rtype: Copy
        """
        try:
            with self._conn.lock:
                self._conn.wait(self._start_copy_gen(statement, params))

            with Copy(self, writer=writer) as copy:
                yield copy
        except e._NO_TRACEBACK as ex:
            raise ex.with_traceback(None)

        # If a fresher result has been set on the cursor by the Copy object,
        # read its properties (especially rowcount).
        self._select_current_result(0)

    def _fetch_pipeline(self) -> None:
        if (
            self._execmany_returning is not False
            and not self.pgresult
            and self._conn._pipeline
        ):
            with self._conn.lock:
                self._conn.wait(self._conn._pipeline._fetch_gen(flush=True))


_pack_feedback = struct.Struct("!cqqqq?").pack
_unpack_data = struct.Struct("!cqqq").unpack
_unpack_keepalive = struct.Struct("!cqq?").unpack
