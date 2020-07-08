import asyncio

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from .async_case import IsolatedAsyncioTestCase

from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extensions import (
    TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS,
    ISOLATION_LEVEL_READ_UNCOMMITTED, ISOLATION_LEVEL_READ_COMMITTED,
    ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SERIALIZABLE,
    ISOLATION_LEVEL_DEFAULT)
from psycopg2.extras import DictCursor

from psycaio import connect as psycaio_connect, AioCursorMixin
from psycaio.conn_proactor_connect import connect as proactor_connect

from .loops import loop_classes

connect = psycaio_connect


class ExecTestCase(IsolatedAsyncioTestCase):

    def setUp(self):
        cls = type(self)
        if cls.__name__.startswith("ForcedProactor"):
            global connect
            connect = proactor_connect
        super().setUp()

    def tearDown(self):
        global connect
        connect = psycaio_connect
        super().tearDown()

    async def asyncSetUp(self):
        self.cn = await connect(dbname="postgres")
        self.cr = self.cn.cursor()

    async def asyncTearDown(self):
        self.cn.close()

    async def test_simple(self):
        await self.cr.execute("SELECT 42")
        self.assertEqual(self.cr.fetchone()[0], 42)

    async def test_autocommit(self):
        self.assertFalse(self.cn.autocommit)
        await self.cr.execute("SELECT 42")
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_INTRANS)
        await self.cn.commit()
        self.cn.autocommit = True
        await self.cr.execute("SELECT 42")
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_IDLE)

    async def _test_iso_level_numeric(self, iso_level, iso_text):
        self.cn.isolation_level = iso_level
        await self.cr.execute("SHOW TRANSACTION ISOLATION LEVEL")
        self.assertEqual(self.cr.fetchone()[0].upper(), iso_text)
        await self.cn.rollback()

    async def _test_iso_level_text(self, iso_text):
        self.cn.isolation_level = iso_text
        await self.cr.execute("SHOW TRANSACTION ISOLATION LEVEL")
        self.assertEqual(self.cr.fetchone()[0].upper(), iso_text)
        await self.cn.rollback()

    async def test_isolation_level(self):
        await self.cr.execute("SHOW TRANSACTION ISOLATION LEVEL")
        default_level = self.cr.fetchone()[0]
        await self.cn.rollback()

        for iso_level, iso_text in [
                (ISOLATION_LEVEL_READ_COMMITTED, "READ COMMITTED"),
                (ISOLATION_LEVEL_REPEATABLE_READ, "REPEATABLE READ"),
                (ISOLATION_LEVEL_SERIALIZABLE, "SERIALIZABLE"),
                (ISOLATION_LEVEL_READ_UNCOMMITTED, "READ UNCOMMITTED"),
                ]:
            await self._test_iso_level_numeric(iso_level, iso_text)
            await self._test_iso_level_text(iso_text)

        self.cn.isolation_level = b"read committed"
        await self.cr.execute("SHOW TRANSACTION ISOLATION LEVEL")
        self.assertEqual(self.cr.fetchone()[0].upper(), "READ COMMITTED")
        await self.cn.rollback()

        self.cn.isolation_level = ISOLATION_LEVEL_DEFAULT
        await self.cr.execute("SHOW TRANSACTION ISOLATION LEVEL")
        self.assertEqual(self.cr.fetchone()[0], default_level)

        with self.assertRaises(ProgrammingError):
            self.cn.isolation_level = "DEFAULT"

        await self.cn.rollback()
        with self.assertRaises(ValueError):
            self.cn.isolation_level = 6

        with self.assertRaises(ValueError):
            self.cn.isolation_level = "nonsense"

        with self.assertRaises(TypeError):
            self.cn.isolation_level = object()

    async def test_long_result(self):
        # to create the same event (POLL_READ) to test the shortcut
        await self.cr.execute("SELECT  * FROM generate_series(1,10000) i;")
        self.assertEqual(self.cr.rowcount, 10000)

    async def test_cancel(self):
        self.cn.autocommit = True
        task = asyncio.ensure_future(self.cr.execute("SELECT pg_sleep(5)"))
        await asyncio.sleep(0.1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # check if statement is cancelled server side as well
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_IDLE)

    async def test_bad_cursor(self):

        with self.assertRaises(OperationalError):
            self.cn.cursor(cursor_factory=DictCursor)

    async def test_dict_cursor(self):

        class AioDictCursor(AioCursorMixin, DictCursor):
            pass

        cr = self.cn.cursor(cursor_factory=AioDictCursor)
        await cr.execute("SELECT 48 as value")
        self.assertEqual(cr.fetchone()['value'], 48)

        cr = (await connect(dbname="postgres", cursor_factory=AioDictCursor)).cursor()
        await cr.execute("SELECT 48 as value")
        self.assertEqual(cr.fetchone()['value'], 48)

    async def test_named_cursor(self):
        with self.assertRaises(ProgrammingError):
            self.cn.cursor("hello")


globals().update(**{cls.__name__: cls for cls in loop_classes(ExecTestCase)})
del ExecTestCase
