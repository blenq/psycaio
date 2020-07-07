from asyncio import TimeoutError, wait_for
import os
import tempfile

try:
    from unittest import IsolatedAsyncioTestCase
except ImportError:
    from .async_case import IsolatedAsyncioTestCase

from psycopg2 import OperationalError

from psycaio import connect, AioConnection

# from .loops import create_loop_testcases
from .loops import loop_classes


class ConnTestCase(IsolatedAsyncioTestCase):

    async def test_connect(self):
        cn = await connect(dbname='postgres')
        self.assertIsInstance(cn, AioConnection)

    async def test_connect_dsn(self):
        cn = await connect('dbname=postgres')
        self.assertIsInstance(cn, AioConnection)

    async def test_connect_timeout(self):
        cn = await connect(dbname='postgres', connect_timeout="10")
        self.assertIsInstance(cn, AioConnection)
        self.assertEqual(cn.get_dsn_parameters()["connect_timeout"], "10")

        cn = await connect(dbname='postgres', connect_timeout="1")
        self.assertEqual(cn.get_dsn_parameters()["connect_timeout"], "1")

        cn = await connect(dbname='postgres', connect_timeout=-1)
        self.assertEqual(cn.get_dsn_parameters()["connect_timeout"], "-1")

    async def test_wrong_number_of_hosts(self):
        with self.assertRaises(OperationalError):
            await connect(host="db1.com,db2.com", hostaddr="127.0.0.1")

    async def test_wrong_number_of_ports(self):
        with self.assertRaises(OperationalError):
            await connect(port="5432,5432")

    async def test_one_port(self):
        cn = await connect(dbname='postgres', host=",", port="5432")
        self.assertIsInstance(cn, AioConnection)

    async def test_wrong_port(self):
        with self.assertRaises(OperationalError):
            await connect(dbname='postgres', host="localhost", port="2345")

    async def test_wrong_port_hostaddr(self):
        with self.assertRaises(OperationalError):
            await connect(dbname='postgres', hostaddr="127.0.0.1", port="2345")

    async def test_service_file(self):
        service_file = tempfile.NamedTemporaryFile('w', delete=False)
        service_file.write("[test]\ndbname=postgres\n")
        service_file.close()
        os.environ["PGSERVICEFILE"] = service_file.name
        cn = await connect(service="test")
        self.assertIsInstance(cn, AioConnection)
        del os.environ["PGSERVICEFILE"]
        os.unlink(service_file.name)

    async def test_environ_port(self):
        os.environ["PGPORT"] = "5432"
        cn = await connect(dbname="postgres")
        self.assertIsInstance(cn, AioConnection)
        os.environ["PGPORT"] = "2345"
        with self.assertRaises(OperationalError):
            cn = await connect(dbname="postgres")
        del os.environ["PGPORT"]

    async def test_invalid_conn(self):

        class BadConn:

            def __init__(self, *args, **kwargs):
                pass

        with self.assertRaises(OperationalError):
            await connect(dbname='postgres', connection_factory=BadConn)

    async def test_cancellation(self):

        with self.assertRaises(TimeoutError):
            await wait_for(
                connect(dbname='postgres', host='www.example.com'), 0.1)

    async def test_unexpected_poll(self):
        cn = await connect(dbname="postgres")
        cn._try_poll = lambda: 5
        with self.assertRaises(OperationalError):
            await cn.cursor().execute("SELECT 42")


globals().update(**{cls.__name__: cls for cls in loop_classes(ConnTestCase)})
del ConnTestCase
