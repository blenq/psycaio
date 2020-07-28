import asyncio
import sys

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from .async_case import IsolatedAsyncioTestCase

from psycopg2 import ProgrammingError, InterfaceError
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, cursor
from psycopg2.extras import DictCursor

from psycaio import connect, AioCursorMixin

from .loops import loop_classes


class ExecTestCase(IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.cn = await connect(dbname="postgres")
        self.cr = self.cn.cursor()

    async def asyncTearDown(self):
        self.cn.close()

    async def test_ref(self):
        cn = await connect(dbname="postgres")
        cr = cn.cursor()
        await cr.execute("SELECT 42")
        self.assertEqual(cr.fetchone()[0], 42)
        cr.close()
        del cr
        self.assertEqual(sys.getrefcount(cn), 2)

    async def test_simple(self):
        await self.cr.execute("SELECT 42")
        self.assertEqual(self.cr.fetchone()[0], 42)

        await self.cr.execute("SELECT %s", (42,))
        self.assertEqual(self.cr.fetchone(), (42,))

    async def test_autocommit(self):
        self.assertTrue(self.cn.autocommit)
        with self.assertRaises(ProgrammingError):
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

        with self.assertRaises(ProgrammingError):
            self.cn.isolation_level = "DEFAULT"

    async def test_readonly(self):

        with self.assertRaises(ProgrammingError):
            self.cn.readonly = True

    async def test_deferrable(self):

        with self.assertRaises(ProgrammingError):
            self.cn.deferrable = True

    async def test_long_result(self):
        # to create the same event (POLL_READ) to test the shortcut
        await self.cr.execute("SELECT  * FROM generate_series(1,10000) i;")
        self.assertEqual(self.cr.rowcount, 10000)

    async def test_cancel(self):
        await self.cr.execute("ROLLBACK")
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_IDLE)

        task = asyncio.ensure_future(self.cr.execute("SELECT pg_sleep(5)"))

        await asyncio.sleep(0.1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        # asyncio Task is cancelled, but the underlying future is shielded to
        # try to cancel the statement server side. At this moment that hasn't
        # happened yet, so we need to wait a bit.
        await asyncio.sleep(0.1)
        # check if statement is cancelled server side as well
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_IDLE)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.cr.execute("SELECT pg_sleep(5)"), 0.1)

        await asyncio.sleep(0.1)
        self.assertEqual(
            self.cn.info.transaction_status, TRANSACTION_STATUS_IDLE)

    async def test_bad_cursor(self):

        class BadCursor:

            def __init__(self, *args, **kwargs):
                pass

        with self.assertRaises(TypeError):
            self.cn.cursor(cursor_factory=BadCursor)

        with self.assertRaises(TypeError):
            self.cn.cursor(cursor_factory=DictCursor)

    async def test_reverse_cursor(self):

        class BadCursor(cursor, AioCursorMixin):
            pass

        with self.assertRaises(TypeError):
            self.cn.cursor(cursor_factory=BadCursor)

    async def test_dict_cursor(self):

        class AioDictCursor(AioCursorMixin, DictCursor):
            pass

        cr = self.cn.cursor(cursor_factory=AioDictCursor)
        await cr.execute("SELECT 48 as value")
        self.assertEqual(cr.fetchone()['value'], 48)

        cr = (await connect(
            dbname="postgres", cursor_factory=AioDictCursor)).cursor()
        await cr.execute("SELECT 48 as value")
        self.assertEqual(cr.fetchone()['value'], 48)

    async def test_named_cursor(self):
        with self.assertRaises(ProgrammingError):
            self.cn.cursor("hello")

    async def test_callproc(self):
        await self.cr.callproc("generate_series", (1, 1))
        self.assertEqual(self.cr.fetchone()[0], 1)

    async def test_notify(self):
        await self.cr.execute("LISTEN queue")
        await self.cr.execute("NOTIFY queue, 'hi'")
        notify = await self.cn.get_notify()
        self.assertEqual(notify.payload, 'hi')

        await self.cr.execute("LISTEN queue")
        task = asyncio.ensure_future(self.cn.get_notify())
        await asyncio.sleep(0.1)
        await self.cr.execute("NOTIFY queue, 'hello'")
        await task
        notify = task.result()
        self.assertEqual(notify.payload, 'hello')

    async def test_notify_nowait(self):
        await self.cr.execute("LISTEN queue")
        await self.cr.execute("NOTIFY queue, 'hi'")
        await asyncio.sleep(0.1)
        notify = self.cn.get_notify_nowait()
        self.assertEqual(notify.payload, 'hi')
        with self.assertRaises(asyncio.QueueEmpty):
            self.cn.get_notify_nowait()

    async def test_notify_already_closed(self):
        self.cn.close()
        with self.assertRaises(InterfaceError):
            await self.cn.get_notify()

    async def test_notify_closed(self):
        task = asyncio.ensure_future(self.cn.get_notify())
        await asyncio.sleep(0)
        self.cn.close()
        with self.assertRaises(InterfaceError):
            await task

    async def test_executemany(self):
        await self.cr.execute("BEGIN")
        await self.cr.execute("CREATE TEMP TABLE test (val int)")
        await self.cr.executemany(
            "INSERT INTO test (val) VALUES (%s)", [(1,), (2,)])
        await self.cr.execute("SELECT SUM(val) FROM test")
        self.assertEqual(self.cr.fetchone()[0], 3)
        await self.cr.execute("DROP TABLE test")
        await self.cr.execute("ROLLBACK")


globals().update(**{cls.__name__: cls for cls in loop_classes(ExecTestCase)})
del ExecTestCase
