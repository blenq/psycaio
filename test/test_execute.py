import asyncio

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from .async_case import IsolatedAsyncioTestCase

from psycopg2 import OperationalError, ProgrammingError, InterfaceError
from psycopg2.extensions import (
    TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INTRANS,
    ISOLATION_LEVEL_READ_UNCOMMITTED, ISOLATION_LEVEL_READ_COMMITTED,
    ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SERIALIZABLE,
    ISOLATION_LEVEL_DEFAULT)
from psycopg2.extras import DictCursor

from psycaio import connect, AioCursorMixin

from .loops import loop_classes


class ExecTestCase(IsolatedAsyncioTestCase):

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
        with self.assertRaises(ProgrammingError):
            self.cn.autocommit = True

        await self.cr.execute("SELECT 42")
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_INTRANS)
        await self.cn.commit()
        self.cn.autocommit = True
        await self.cr.execute("SELECT 42")
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_IDLE)

        self.cn.close()
        with self.assertRaises(InterfaceError):
            self.cn.autocommit = True

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
        cn = await connect(dbname="postgres")
        cn.close()
        with self.assertRaises(InterfaceError):
            cn.isolation_level = ISOLATION_LEVEL_REPEATABLE_READ

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

        await self.cn.rollback()
        self.cn.isolation_level = "DEFAULT"
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

    async def test_readonly(self):
        cn = await connect(dbname="postgres")
        cn.close()
        with self.assertRaises(InterfaceError):
            cn.readonly = True

        await self.cr.execute("SELECT 8")
        with self.assertRaises(ProgrammingError):
            self.cn.readonly = True

        await self.cn.rollback()
        await self.cr.execute("SHOW transaction_read_only")
        default_ro = self.cr.fetchone()[0]

        await self.cn.rollback()
        self.cn.readonly = True
        await self.cr.execute("SHOW transaction_read_only")
        self.assertEqual(self.cr.fetchone()[0], 'on')

        await self.cn.rollback()
        self.cn.readonly = False
        await self.cr.execute("SHOW transaction_read_only")
        self.assertEqual(self.cr.fetchone()[0], 'off')

        await self.cn.rollback()
        self.cn.readonly = None
        await self.cr.execute("SHOW transaction_read_only")
        self.assertEqual(self.cr.fetchone()[0], default_ro)

        await self.cn.rollback()
        self.cn.readonly = "default"
        await self.cr.execute("SHOW transaction_read_only")
        self.assertEqual(self.cr.fetchone()[0], default_ro)

        await self.cn.rollback()
        self.cn.readonly = b"default"
        await self.cr.execute("SHOW transaction_read_only")
        self.assertEqual(self.cr.fetchone()[0], default_ro)

        await self.cn.rollback()
        with self.assertRaises(ValueError):
            self.cn.readonly = "nonsense"

    async def test_deferrable(self):
        cn = await connect(dbname="postgres")
        cn.close()
        with self.assertRaises(InterfaceError):
            cn.deferrable = True

        await self.cr.execute("SELECT 8")
        with self.assertRaises(ProgrammingError):
            self.cn.deferrable = True

        await self.cn.rollback()
        await self.cr.execute("SHOW transaction_deferrable")
        default_ro = self.cr.fetchone()[0]

        await self.cn.rollback()
        self.cn.deferrable = True
        await self.cr.execute("SHOW transaction_deferrable")
        self.assertEqual(self.cr.fetchone()[0], 'on')

        await self.cn.rollback()
        self.cn.deferrable = False
        await self.cr.execute("SHOW transaction_deferrable")
        self.assertEqual(self.cr.fetchone()[0], 'off')

        await self.cn.rollback()
        self.cn.deferrable = None
        await self.cr.execute("SHOW transaction_deferrable")
        self.assertEqual(self.cr.fetchone()[0], default_ro)

        await self.cn.rollback()
        self.cn.deferrable = "default"
        await self.cr.execute("SHOW transaction_deferrable")
        self.assertEqual(self.cr.fetchone()[0], default_ro)

        await self.cn.rollback()
        self.cn.deferrable = b"default"
        await self.cr.execute("SHOW transaction_deferrable")
        self.assertEqual(self.cr.fetchone()[0], default_ro)

        await self.cn.rollback()
        with self.assertRaises(ValueError):
            self.cn.deferrable = "nonsense"

    async def test_all_trans_options(self):
        await self.cn.rollback()
        self.cn.isolation_level = ISOLATION_LEVEL_SERIALIZABLE
        self.cn.readonly = True
        self.cn.deferrable = True
        await self.cr.execute("SHOW TRANSACTION ISOLATION LEVEL")
        self.assertEqual(self.cr.fetchone()[0].upper(), "SERIALIZABLE")
        await self.cr.execute("SHOW transaction_read_only")
        self.assertEqual(self.cr.fetchone()[0], 'on')
        await self.cr.execute("SHOW transaction_deferrable")
        self.assertEqual(self.cr.fetchone()[0], 'on')

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

    async def test_callproc(self):
        await self.cr.callproc("generate_series", (1, 1))
        self.assertEqual(self.cr.fetchone()[0], 1)


globals().update(**{cls.__name__: cls for cls in loop_classes(ExecTestCase)})
del ExecTestCase
