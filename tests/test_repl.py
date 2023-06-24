import pytest
import psycopg
import time
from psycopg.repl import ReplicationCursor, AsyncReplicationCursor
from psycopg.rows import Row, dict_row
from select import select


class ReplicationSlotManager:
    def __init__(self):
        self._slots: list[str] = []

    def create_replication_slot(self, cur, name, **kwargs):
        cur.create_replication_slot(name, **kwargs)
        self._slots.append(name)

    async def create_async_replication_slot(self, acur, name, **kwargs):
        await acur.create_replication_slot(name, **kwargs)
        self._slots.append(name)

    def drop_replication_slot(self, cur, name):
        cur.drop_replication_slot(name)
        self._slots.remove(name)


@pytest.fixture
def slots(conn):
    """slots keep track of the replication slot created during the tests and
    drop them at the end of the each test if they are still present"""
    manager = ReplicationSlotManager()
    yield manager

    if manager._slots:
        time.sleep(0.025)

        with conn:
            for slot in manager._slots:
                try:
                    conn.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                except psycopg.errors.UndefinedObject:
                    pass


class BaseTest:
    def make_replication_events(self, dsn: str) -> None:
        with psycopg.connect(dsn) as conn:
            try:
                conn.execute("DROP TABLE dummy1")
            except psycopg.ProgrammingError:
                conn.rollback()
            conn.execute(
                "CREATE TABLE dummy1 AS SELECT * FROM generate_series(1, 5) AS id"
            )

    def expect_messages(self, cur: ReplicationCursor[Row], n: int) -> list[bytes]:
        res: list[bytes] = []

        for _ in range(n + 100):
            msg = cur.read_message()
            if msg is not None:
                res.append(msg)
                if len(res) == n:
                    return res
            else:
                try:
                    select([cur], [], [], 0.01)
                except InterruptedError:
                    pass

        return res

    def _test_extended_protocol(self, conn):
        def _pipeline():
            with conn.pipeline() as p:
                conn.execute("SELECT 1")
                p.sync()

        # The extended protocol is not supported in replication mode so
        # neither params nor the prepared statements are accepted
        pytest.raises(
            psycopg.errors.ProtocolViolation, conn.execute, "SELECT %s", params=(1,)
        )
        pytest.raises(
            psycopg.errors.ProtocolViolation, conn.execute, "SELECT 1", prepare=True
        )
        pytest.raises(
            psycopg.errors.ProtocolViolation,
            conn.execute,
            "SELECT 1",
            prepare=True,
            binary=True,
        )
        pytest.raises(psycopg.errors.ProtocolViolation, _pipeline)

    async def _test_async_extended_protocol(self, aconn):
        async def _pipeline():
            async with aconn.pipeline() as p:
                await aconn.execute("SELECT 1")
                await p.sync()

        # The extended protocol is not supported in replication protocol so
        # neither params nor the prepared statements are accepted
        with pytest.raises(psycopg.errors.ProtocolViolation):
            await aconn.execute("SELECT %s", params=(1,))
        with pytest.raises(psycopg.errors.ProtocolViolation):
            await aconn.execute("SELECT 1", prepare=True)
        with pytest.raises(psycopg.errors.ProtocolViolation):
            await aconn.execute("SELECT 1", prepare=True, binary=True)
        with pytest.raises(psycopg.errors.ProtocolViolation):
            await _pipeline()

    def _test_replication_slot(
        self, conn, expected_name, expected_type, expected_plugin
    ):
        ((slot_name, slot_type, plugin),) = conn.execute(
            "select slot_name, slot_type, plugin from pg_replication_slots"
        )
        assert slot_name == expected_name
        assert slot_type == expected_type
        assert plugin == expected_plugin


class TestReplication(BaseTest):
    @pytest.mark.parametrize("autocommit", (False, True))
    def test_logical_replication_connection(self, dsn, autocommit):
        with psycopg.connect(
            dsn, replication="database", autocommit=autocommit
        ) as conn:
            # The simple protocol is supported...
            conn.execute("SELECT 1")
            # but not the extended protocol
            self._test_extended_protocol(conn)

    def test_physical_replication_connection(self, dsn):
        with psycopg.connect(dsn, replication="true", autocommit=True) as conn:
            # Neither the simple nor extended protocol are supported
            pytest.raises(psycopg.errors.FeatureNotSupported, conn.execute, "SELECT 1")
            self._test_extended_protocol(conn)

    @pytest.mark.parametrize(
        "conn_type,kwargs",
        [
            ("database", dict(type="logical", output_plugin="test_decoding")),
            ("database", dict(type="physical")),
            ("true", dict(type="physical")),
        ],
    )
    def test_create_replication_slot(self, conn, slots, dsn, kwargs, conn_type):
        with (
            psycopg.connect(
                dsn,
                replication=conn_type,
                autocommit=True,
                cursor_factory=ReplicationCursor,
            ) as rconn,
            rconn.cursor() as cur,
        ):
            slots.create_replication_slot(cur, "test_slot", **kwargs)
            self._test_replication_slot(
                conn, "test_slot", kwargs["type"], kwargs.get("output_plugin")
            )

    def test_create_replication_slot_fail(self, slots, dsn):
        with (
            psycopg.connect(
                dsn,
                replication="true",
                autocommit=True,
                cursor_factory=ReplicationCursor,
            ) as rconn,
            rconn.cursor() as cur,
        ):
            pytest.raises(
                psycopg.errors.ObjectNotInPrerequisiteState,
                slots.create_replication_slot,
                cur,
                "test_slot",
                type="logical",
                output_plugin="test_decoding",
            )

    @pytest.mark.parametrize(
        "conn_type,kwargs",
        [
            ("database", dict(type="logical", output_plugin="test_decoding")),
            ("database", dict(type="physical")),
            ("true", dict(type="physical")),
        ],
    )
    @pytest.mark.parametrize("wait", (True, False))
    def test_drop_replication_slot(self, conn, slots, dsn, wait, kwargs, conn_type):
        with (
            psycopg.connect(
                dsn,
                replication=conn_type,
                autocommit=True,
                cursor_factory=ReplicationCursor,
            ) as rconn,
            rconn.cursor() as cur,
        ):
            pytest.raises(
                psycopg.errors.UndefinedObject, cur.drop_replication_slot, "test_slot"
            )

            slots.create_replication_slot(cur, "test_slot", **kwargs)
            cur.drop_replication_slot("test_slot", wait=wait)
            res = conn.execute("select count(*) from pg_replication_slots")
            assert res.fetchone() == (0,)

    def test_start_replication(self, dsn, slots):
        with (
            psycopg.connect(
                dsn,
                replication="database",
                autocommit=True,
                cursor_factory=ReplicationCursor,
            ) as conn,
            conn.cursor() as cur,
        ):
            with pytest.raises(psycopg.errors.UndefinedObject), cur.start_replication(
                "test_slot", "logical", "000/000"
            ):
                pass

            slots.create_replication_slot(
                cur, "test_slot", type="logical", output_plugin="test_decoding"
            )
            with cur.start_replication("test_slot", "logical", "000/000"):
                pass

    def test_send_feedback(self, dsn, slots):
        with (
            psycopg.connect(
                dsn,
                replication="database",
                autocommit=True,
                cursor_factory=ReplicationCursor,
            ) as conn,
            conn.cursor() as cur,
        ):
            pytest.raises(psycopg.errors.OperationalError, cur.send_feedback)

            slots.create_replication_slot(
                cur, "test_slot", type="logical", output_plugin="test_decoding"
            )
            with cur.start_replication("test_slot", "logical", "000/000"):
                cur.send_feedback()

    def test_replication_cursor(self, dsn, slots):
        with (
            psycopg.connect(
                dsn,
                replication="database",
                autocommit=True,
                cursor_factory=ReplicationCursor,
                row_factory=dict_row,
            ) as conn,
            conn.cursor() as cur,
        ):
            assert isinstance(cur, ReplicationCursor)
            slots.create_replication_slot(
                cur, "test_slot", type="logical", output_plugin="test_decoding"
            )

            with cur.start_replication("test_slot", "logical", "000/000"):
                self.make_replication_events(dsn)
                res = self.expect_messages(cur, 7)
                # We ignore the first and last message which contain the ID of
                # the transaction
                assert res[1:6] == [
                    b"table public.dummy1: INSERT: id[integer]:1",
                    b"table public.dummy1: INSERT: id[integer]:2",
                    b"table public.dummy1: INSERT: id[integer]:3",
                    b"table public.dummy1: INSERT: id[integer]:4",
                    b"table public.dummy1: INSERT: id[integer]:5",
                ]


@pytest.mark.anyio
class TestAsyncReplication(BaseTest):
    @pytest.mark.parametrize("autocommit", (False, True))
    async def test_logical_replication_connection(self, dsn, autocommit):
        async with await psycopg.AsyncConnection.connect(
            dsn, replication="database", autocommit=autocommit
        ) as aconn:
            await aconn.execute("SELECT 1")
            await self._test_async_extended_protocol(aconn)

    async def test_physical_replication_connection(self, dsn):
        async with await psycopg.AsyncConnection.connect(
            dsn, replication="true", autocommit=True
        ) as aconn:
            with pytest.raises(psycopg.errors.FeatureNotSupported):
                await aconn.execute("SELECT 1")
            await self._test_async_extended_protocol(aconn)

    @pytest.mark.parametrize(
        "conn_type,kwargs",
        [
            ("database", dict(type="logical", output_plugin="test_decoding")),
            ("database", dict(type="physical")),
            ("true", dict(type="physical")),
        ],
    )
    async def test_create_replication_slot(self, conn, slots, dsn, kwargs, conn_type):
        async with (
            await psycopg.AsyncConnection.connect(
                dsn,
                replication=conn_type,
                autocommit=True,
                cursor_factory=AsyncReplicationCursor,
            ) as aconn,
            aconn.cursor() as acur,
        ):
            await slots.create_async_replication_slot(acur, "test_slot", **kwargs)
            self._test_replication_slot(
                conn, "test_slot", kwargs["type"], kwargs.get("output_plugin")
            )

    async def test_create_replication_slot_fail(self, slots, dsn):
        async with (
            await psycopg.AsyncConnection.connect(
                dsn,
                replication="true",
                autocommit=True,
                cursor_factory=AsyncReplicationCursor,
            ) as aconn,
            aconn.cursor() as acur,
        ):
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                await slots.create_async_replication_slot(
                    acur, "test_slot", type="logical", output_plugin="test_decoding"
                )

    @pytest.mark.parametrize(
        "conn_type,kwargs",
        [
            ("database", dict(type="logical", output_plugin="test_decoding")),
            ("database", dict(type="physical")),
            ("true", dict(type="physical")),
        ],
    )
    @pytest.mark.parametrize("wait", (True, False))
    async def test_drop_replication_slot(
        self, conn, slots, dsn, wait, kwargs, conn_type
    ):
        async with (
            await psycopg.AsyncConnection.connect(
                dsn,
                replication=conn_type,
                autocommit=True,
                cursor_factory=AsyncReplicationCursor,
            ) as aconn,
            aconn.cursor() as acur,
        ):
            with pytest.raises(psycopg.errors.UndefinedObject):
                await acur.drop_replication_slot("test_slot", wait=wait)

            await slots.create_async_replication_slot(acur, "test_slot", **kwargs)
            await acur.drop_replication_slot("test_slot", wait=wait)
            res = conn.execute("select count(*) from pg_replication_slots")
            assert res.fetchone() == (0,)

    async def test_start_replication(self, dsn, slots):
        async with (
            await psycopg.AsyncConnection.connect(
                dsn,
                replication="database",
                autocommit=True,
                cursor_factory=AsyncReplicationCursor,
            ) as aconn,
            aconn.cursor() as acur,
        ):
            with pytest.raises(psycopg.errors.UndefinedObject):
                async with acur.start_replication("test_slot", "logical", "0/000"):
                    pass

            await slots.create_async_replication_slot(
                acur, "test_slot", type="logical", output_plugin="test_decoding"
            )
            async with acur.start_replication("test_slot", "logical", "0/000"):
                pass

    async def test_send_feedback(self, dsn, slots):
        async with (
            await psycopg.AsyncConnection.connect(
                dsn,
                replication="database",
                autocommit=True,
                cursor_factory=AsyncReplicationCursor,
            ) as aconn,
            aconn.cursor() as acur,
        ):
            with pytest.raises(psycopg.errors.OperationalError):
                await acur.send_feedback()

            await slots.create_async_replication_slot(
                acur, "test_slot", type="logical", output_plugin="test_decoding"
            )
            async with acur.start_replication("test_slot", "logical", "0/000"):
                await acur.send_feedback()

    async def test_async_replication_cursor(self, dsn, slots):
        async with (
            await psycopg.AsyncConnection.connect(
                dsn,
                replication="database",
                autocommit=True,
                cursor_factory=AsyncReplicationCursor,
                row_factory=dict_row,
            ) as conn,
            conn.cursor() as acur,
        ):
            assert isinstance(acur, AsyncReplicationCursor)
            await acur.execute("IDENTIFY_SYSTEM")
            res = await acur.fetchone()
            assert res is not None
            assert list(res) == ["systemid", "timeline", "xlogpos", "dbname"]

            with pytest.raises(psycopg.errors.UndefinedObject):
                await acur.drop_replication_slot("test_slot")

            await slots.create_async_replication_slot(
                acur, "test_slot", type="logical", output_plugin="test_decoding"
            )
            await acur.drop_replication_slot("test_slot")
