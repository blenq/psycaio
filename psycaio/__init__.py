import socket

from asyncio import TimeoutError

try:
    from asyncio import get_running_loop, wait_for
except ImportError:
    from asyncio import get_event_loop as get_running_loop, wait_for

from psycopg2 import connect as pg_connect, OperationalError
from psycopg2.extensions import (
    cursor, connection, parse_dsn, POLL_OK, POLL_READ, POLL_WRITE,
    TRANSACTION_STATUS_IDLE)

__all__ = ['connect', 'AioConnection', 'AioCursorMixin', 'AioCursor']


class AioCursorMixin():

    async def execute(self, *args, **kwargs):
        cn = self.connection
        if (not cn.autocommit and
                cn.info.transaction_status == TRANSACTION_STATUS_IDLE):
            # restore dbapi transaction behaviour, which is not available
            # on a psycopg2 asynchronous connection
            await self._execute("BEGIN")
        await self._execute(*args, **kwargs)

    async def _execute(self, *args, **kwargs):
        super().execute(*args, **kwargs)
        await self.connection._perform_io()


class AioCursor(AioCursorMixin, cursor):
    pass


class AioConnection(connection):
    """ override to restore dbapi transaction behaviour and add asyncio
    behaviour

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._autocommit = False
        self._fut = None

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = value

    def cursor(self, *args, **kwargs):
        cr = super().cursor(*args, **kwargs)
        if not isinstance(cr, AioCursorMixin):
            raise OperationalError(
                "cursor_factory must return an instance of AioCursorMixin")
        return cr

    async def commit(self):
        await self.cursor().execute("COMMIT")

    async def rollback(self):
        await self.cursor().execute("ROLLBACK")

    async def cancel(self):
        # connection.cancel is a blocking method, run it in executor
        await get_running_loop().run_in_executor(None, self.cancel())

    async def _perform_io(self, timeout=None):
        if self._fut is not None and not self._fut.done():
            raise OperationalError("IO still in progress")

        loop = get_running_loop()
        self._fut = loop.create_future()
        self._poll()
        await wait_for(self._fut, timeout)

    def _poll(self):
        try:
            state = self.poll()
        except Exception as ex:
            self._fut.set_exception(ex)
            return

        if state == POLL_OK:
            self._fut.set_result(True)
            return

        loop = get_running_loop()
        fd = self.fileno()
        if state == POLL_READ:
            self._remove_fileno = loop.remove_reader
            loop.add_reader(fd, self._io_ready)
        elif state == POLL_WRITE:
            self._remove_fileno = loop.remove_writer
            loop.add_writer(fd, self._io_ready)
        else:
            self._fut.set_exception(
                OperationalError(
                    "Unexpected result from poll: {}".format(state)))

    def _io_ready(self):
        self._remove_fileno(self.fileno())
        self._poll()


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None,
        lookup_host=True, **kwargs):

    if connection_factory is None:
        connection_factory = AioConnection
    if cursor_factory is None:
        cursor_factory = AioCursor

    kwargs.pop('async_', None)

    timeout = kwargs.pop('connect_timeout', None)
    if timeout is not None:
        timeout = int(timeout)
        # mimic libpq behavior
        if timeout == 1:
            timeout = 2
        elif timeout <= 0:
            timeout = None

    if dsn:
        conn_kwargs = parse_dsn(dsn)
    else:
        conn_kwargs = {}

    conn_kwargs.update(kwargs)
    conn_kwargs['async'] = True

    # libpq and therefore psycopg2 do not respect connect_timeout in non
    # blocking mode.
    # DNS lookups by libpq are blocking even in non blocking
    # mode.
    # Here we try to solve those two issues. If host(s) are provided, and
    # hostaddres(ses) are not, do the DNS lookup here using the asyncio version
    # of getaddrinfo.
    # Also split the hosts or recognize that a single host name might have
    # multiple addresses, for example IPv4 en IPv6, so later we can apply
    # the timeout per address. Just like libpq is doing in synchronous mode
    #
    # Note: hostname(s) can be set in other ways. For example, using a service
    # file or specific environment variables. These are not recognized here and
    # the issues mentioned above are not solved in that case.
    host = conn_kwargs.get("host")
    hostaddr = conn_kwargs.get("hostaddr")
    if hostaddr is None and host and lookup_host:
        hosts = []
        loop = get_running_loop()
        for host in host.split(','):
            if host == '' or host.startswith('/'):
                hosts.append((host, None))
            else:
                for addrinfo in await loop.getaddrinfo(
                        host, None, proto=socket.IPPROTO_TCP):
                    hosts.append((host, addrinfo[4][0]))
        if not hosts:
            raise OperationalError("Host lookup failed")
    else:
        hosts = [(host, hostaddr)]

    exceptions = []
    for host, hostaddr in hosts:
        conn_kwargs.update(host=host, hostaddr=hostaddr)
        cn = pg_connect(connection_factory=connection_factory,
                        cursor_factory=cursor_factory, **conn_kwargs)
        if not isinstance(cn, AioConnection):
            raise OperationalError(
                "connection_factory must return an instance of AioConnection")
        try:
            await cn._perform_io(timeout=timeout)
            return cn
        except TimeoutError as ex:
            exceptions.append(ex)
    raise OperationalError(exceptions)
