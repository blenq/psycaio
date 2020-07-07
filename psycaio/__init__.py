from collections import defaultdict
from functools import partial
import os
import socket

from asyncio import CancelledError, shield, Lock, wait_for, SelectorEventLoop

from asyncio.proactor_events import BaseProactorEventLoop

try:
    from asyncio import get_running_loop
except ImportError:
    from asyncio import get_event_loop as get_running_loop

from psycopg2 import connect as pg_connect, OperationalError
from psycopg2.extensions import (
    cursor, connection, parse_dsn, POLL_OK, POLL_READ, POLL_WRITE,
    TRANSACTION_STATUS_IDLE, ISOLATION_LEVEL_READ_UNCOMMITTED,
    ISOLATION_LEVEL_READ_COMMITTED, ISOLATION_LEVEL_REPEATABLE_READ,
    ISOLATION_LEVEL_SERIALIZABLE, ISOLATION_LEVEL_DEFAULT
)

__all__ = ['connect', 'AioConnection', 'AioCursorMixin', 'AioCursor']


class SelectorAioCursorMixin:

    async def execute(self, *args, **kwargs):
        async with self.connection._execute_lock:
            transaction_cmd = self.connection._transaction_command()
            if transaction_cmd is not None:
                await self._execute(transaction_cmd)
            await self._execute(*args, **kwargs)

    async def _execute(self, *args, **kwargs):
        super().execute(*args, **kwargs)
        fut = self.connection._start_exec_poll()
        try:
            # Shield the future so we can still wait for it when we cancel the
            # operation server side as well.
            await shield(fut)
        except CancelledError:
            if not fut.done():
                # This routine got cancelled, but the server is still busy
                # with our statement. Try to cancel the current server
                # operation as well.
                try:
                    await self.connection.cancel()
                    await fut
                except Exception:
                    # Don't bother with this exception.
                    pass

            # And reraise. We got cancelled after all.
            raise
        finally:
            # Make sure future is done. This is a no-op when fut is
            # already done.
            fut.cancel()


class ProactorAioCursorMixin:

    async def execute(self, *args, **kwargs):
        async with self.connection._execute_lock:
            func = partial(super().execute, *args, **kwargs)
            try:
                await self.connection._loop.run_in_executor(None, func)
            except CancelledError:
                try:
                    await self.connection.cancel()
                except Exception:
                    pass
                raise


class TypeCache(defaultdict):
    """ Cache object for types based on mixin """

    def __init__(self, prefix, mixin):
        self.prefix = prefix
        self.mixin = mixin

    def __missing__(self, cls):
        # create a new class that inherits from the mixin
        self[cls] = new_cls = type(
            self.prefix + cls.__name__, (self.mixin, cls), {})
        return new_cls


# Cache for dynamically created types. We need both. In a Windows
# world it can theoretically happen that both a SelectorEventLoop and a
# ProactorEventLoop are instantiated, not necessarily at the same time.
_selector_cursor_types = TypeCache("Selector", SelectorAioCursorMixin)
_proactor_cursor_types = TypeCache("Proactor", ProactorAioCursorMixin)


class AioCursorMixin:

    def __new__(cls, *args, **kwargs):
        """ Instantiate the proper class instance based on the type of loop """

        loop = get_running_loop()

        # Assumption: All loop implementations besides the ProactorEventLoop
        # support add_reader and add_writer.
        # This is true for the existing loops in the Python stdlib and the
        # uvloop, but it might not be true for a yet unknown loop.
        if isinstance(loop, BaseProactorEventLoop):
            _types = _proactor_cursor_types
        else:
            _types = _selector_cursor_types

        return super().__new__(_types[cls], *args, **kwargs)


class AioCursor(AioCursorMixin, cursor):
    pass


class AioConnection(connection):
    """ override to restore dbapi transaction behaviour and add asyncio
    behaviour

    """
    def __init__(self, *args, **kwargs):
        self._loop = get_running_loop()
        self._poll_state = None

        super().__init__(*args, **kwargs)
        self._autocommit = False
        self._isolation_level = ISOLATION_LEVEL_DEFAULT
        self._fd = self.fileno()
        self._execute_lock = Lock()

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = bool(value)

    @property
    def isolation_level(self):
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, level):
        if level is None:
            self._isolation_level = ISOLATION_LEVEL_DEFAULT
            return

        if isinstance(level, int):
            if level not in range(1, 5):
                raise ValueError("isolation_level must be between 1 and 4")
            self._isolation_level = level
            return

        if isinstance(level, bytes):
            level = level.decode()
        if isinstance(level, str):
            for level_text, level_value in [
                    ("READ COMMITTED", ISOLATION_LEVEL_READ_COMMITTED),
                    ("REPEATABLE READ", ISOLATION_LEVEL_REPEATABLE_READ),
                    ("SERIALIZABLE", ISOLATION_LEVEL_SERIALIZABLE),
                    ("READ UNCOMMITTED", ISOLATION_LEVEL_READ_UNCOMMITTED),
                    ("DEFAULT", ISOLATION_LEVEL_DEFAULT),
                    ]:
                if level.upper() == level_text:
                    self._isolation_level = level_value
                    return

        raise ValueError(f"bad value for isolation_level: '{level}'")

    def _transaction_command(self):
        if (self.autocommit or
                self.info.transaction_status != TRANSACTION_STATUS_IDLE):
            return None
        cmd = ["BEGIN TRANSACTION"]
        if self.isolation_level:
            cmd.append("ISOLATION LEVEL")
            cmd.append({
                ISOLATION_LEVEL_READ_COMMITTED: "READ COMMITTED",
                ISOLATION_LEVEL_READ_UNCOMMITTED: "READ UNCOMMITTED",
                ISOLATION_LEVEL_REPEATABLE_READ: "REPEATABLE READ",
                ISOLATION_LEVEL_SERIALIZABLE: "SERIALIZABLE",
            }[self.isolation_level])
        return ' '.join(cmd)

    def cursor(self, *args, **kwargs):
        """ Override to add type check """
        cr = super().cursor(*args, **kwargs)
        if not isinstance(cr, SelectorAioCursorMixin):
            raise OperationalError(
                "cursor_factory must return an instance of AioCursorMixin")
        return cr

    async def commit(self):
        async with self._execute_lock:
            await self.cursor()._execute("COMMIT")

    async def rollback(self):
        async with self._execute_lock:
            await self.cursor()._execute("ROLLBACK")

    async def cancel(self):
        await self._loop.run_in_executor(None, super().cancel)

    def _start_connect_poll(self):
        """ Starts polling after connect """

        fut = self._fut = self._loop.create_future()
        self._connect_poll()
        return fut

    def _start_exec_poll(self):
        """ Starts polling after execute """

        fut = self._fut = self._loop.create_future()
        self._exec_poll()
        return fut

    def _try_poll(self):
        """ exception safe version of poll """
        try:
            return self.poll()
        except Exception as ex:
            # done with error, cleanup and notify waiter
            self._reset_io()
            if not self._fut.done():
                self._fut.set_exception(ex)

    def _handle_poll_ok(self):
        fut = self._fut
        if not fut.done():
            fut.set_result(True)

    def _handle_poll_read(self, callback):
        self._poll_state = POLL_READ
        self._loop.add_reader(self._fd, callback)

    def _handle_poll_write(self, callback):
        self._poll_state = POLL_WRITE
        self._loop.add_writer(self._fd, callback)

    def _handle_poll_state(self, state, callback):
        if state == POLL_READ:
            self._handle_poll_read(callback)
        elif state == POLL_OK:
            self._handle_poll_ok()
        elif state == POLL_WRITE:
            self._handle_poll_write(callback)
        else:
            # should not happen
            if not self._fut.done():
                self._fut.set_exception(
                    OperationalError(
                        "Unexpected result from poll: {}".format(state)))

    def _connect_poll(self):
        """ Poll method for connecting

        This resets the io notifications after each event, because file
        descriptor (or underlying socket) might change.

        """
        state = self._try_poll()
        if state is None:
            # error occurred
            return

        self._reset_io()
        self._fd = self.fileno()
        self._handle_poll_state(state, self._connect_poll)

    def _exec_poll(self):
        """Poll method for executing a command

        Slightly more efficient that the connecting version. It reuses existing
        io notifications and does not set the file descriptor after every
        event.

        """
        state = self._try_poll()
        if state == self._poll_state or state is None:
            # just need more of the same or an error occurred
            return

        self._reset_io()
        self._handle_poll_state(state, self._exec_poll)

    def _reset_io(self):
        """ Resets status and io handlers """

        poll_state = self._poll_state
        if poll_state is None:
            # already reset
            return
        self._poll_state = None
        if poll_state == POLL_WRITE:
            self._loop.remove_writer(self._fd)
        elif poll_state == POLL_READ:
            self._loop.remove_reader(self._fd)

    def close(self):
        self._reset_io()
        super().close()

    def __del__(self):
        if not self._loop.is_closed():
            self._reset_io()


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None, **kwargs):

    if connection_factory is None:
        connection_factory = AioConnection
    if cursor_factory is None:
        cursor_factory = AioCursor

    if dsn:
        conn_kwargs = parse_dsn(dsn)
    else:
        conn_kwargs = {}

    conn_kwargs.update(kwargs)
    conn_kwargs.update({
        'async': True, 'async_': True, 'client_encoding': 'UTF8'})

    # Two issues with non-blocking libpq:
    # * libpq and therefore psycopg2 do not respect connect_timeout in non
    #   blocking mode
    # * DNS lookups by libpq are blocking even in non blocking mode.
    #
    # Here we try to solve those two issues. If host(s) are provided, and
    # hostaddres(ses) are not, do the DNS lookup here using the asyncio version
    # of getaddrinfo.
    #
    # Also split the hosts or recognize that a single host name might have
    # multiple addresses, for example IPv4 and IPv6, so later we can apply
    # the timeout per address. Just like libpq is doing in synchronous mode.
    # This solves the issue where the first host drops the traffic (client will
    # not notice) and a second connection attempt will never be undertaken
    # because the first attempt uses up the entire timeout.
    #
    # Note: hostname(s) can be set using a service file. These are not
    # recognized here and the issues mentioned above are not solved in that
    # case.

    # first get the timeout
    timeout = conn_kwargs.get('connect_timeout')
    if timeout is not None:
        timeout = int(timeout)
        # mimic libpq behavior
        if timeout == 1:
            timeout = 2
        if timeout <= 0:
            timeout = None

    if not conn_kwargs.get("service"):

        def parse_multi(param_name):
            param = (conn_kwargs.get(param_name) or
                     os.environ.get(f"PG{param_name.upper()}"))
            return str(param).split(',') if param else []

        hostaddrs = parse_multi("hostaddr")
        hosts = parse_multi("host")
        ports = parse_multi("port")

        # same logic as in libpq
        num_host_entries = len(hostaddrs) or len(hosts) or 1

        # Build up three lists for hosts, hostaddrs and ports of equal length.
        # Lists can contain None for any value
        if not hostaddrs:
            hostaddrs = [None] * num_host_entries

        if hosts:
            # number of hosts must be the same as number of hostaddrs
            if len(hosts) != num_host_entries:
                raise OperationalError(
                    f"could not match {len(hosts)} host names to "
                    f"{num_host_entries} hostaddr values")
        else:
            hosts = [None] * num_host_entries

        if ports:
            num_ports = len(ports)
            # number of ports must be the same as number of host(addr)s or 1
            if num_ports != num_host_entries:
                if num_ports != 1:
                    raise OperationalError(
                        f"could not match {num_ports} port numbers to "
                        f"{num_host_entries} hosts")
                # Multiple host(addr) values, but just one port. That is ok.
                # Stretch the ports list to equal length
                ports *= num_host_entries
        else:
            ports = [None] * num_host_entries

        # Now we got three lists of equal length. Loop through them and add
        # a tuple for each host entry that we find
        loop = get_running_loop()
        host_entries = []
        for host, hostaddr, port in zip(hosts, hostaddrs, ports):
            if hostaddr or not host or host.startswith('/'):
                # host address is already provided, host is empty or is a unix
                # socket address. Just add it to the list
                host_entries.append((host, hostaddr, port))
            else:
                # perform async DNS lookup
                for addrinfo in await loop.getaddrinfo(
                        host, None, proto=socket.IPPROTO_TCP):
                    host_entries.append((host, addrinfo[4][0], port))
    else:
        # A service name is used. Just let libpq handle it.
        host_entries = [(
            conn_kwargs.get("host"),
            conn_kwargs.get("hostaddr"),
            conn_kwargs.get("port"),
        )]

    exceptions = []
    for host, hostaddr, port in host_entries:
        # Try to connect for each host entry. The timeout applies
        # to each attempt separately
        conn_kwargs.update(host=host, hostaddr=hostaddr, port=port)
        cn = pg_connect(connection_factory=connection_factory,
                        cursor_factory=cursor_factory, **conn_kwargs)
        if not isinstance(cn, AioConnection):
            raise OperationalError(
                "connection_factory must return an instance of AioConnection")
        try:
            await wait_for(cn._start_connect_poll(), timeout)
            return cn
        except CancelledError:
            # we got cancelled, do not try next entry
            raise
        except Exception as ex:
            exceptions.append(ex)
    if len(exceptions) == 1:
        raise exceptions[0]
    raise OperationalError(exceptions)
