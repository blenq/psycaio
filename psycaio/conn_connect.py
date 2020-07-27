from asyncio import wait_for, CancelledError
import os
import socket

from psycopg2 import OperationalError, connect as pg_connect
from psycopg2.extensions import parse_dsn, connection

from .conn import AioConnMixin, AioConnection
from .cursor import AioCursor
from .utils import get_running_loop


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None, **kwargs):
    """Open a connection to the database server and return a `connection`
    object.

    The parameters are the same as for the :py:func:`psycopg2.connect` function
    with a few exceptions:

    * The  *async* or *async\_* argument will have no effect. A connection will
      always be in asynchronous mode.

    * If set, the *connection_factory* must return an instance of both an
      :class:`AioConnMixin <psycaio.AioConnMixin>` and a
      psycopg2 :py:class:`connection <psycopg2.extensions.connection>`.
      The default is the :class:`AioConnection <psycaio.AioConnection>`
      class which just inherits from both. The
      :class:`AioConnMixin <psycaio.AioConnMixin>` type must be located
      before the psycopg2
      :py:class:`connection <psycopg2.extensions.connection>` type in the class
      hierarchy when performing a method lookup.

    * If set, the *cursor_factory* must return an instance of both an
      :class:`AioCursorMixin <psycaio.AioCursorMixin>` and a psycopg2
      :py:class:`cursor <psycopg2.extensions.cursor>`.
      The default is the :class:`AioCursor <psycaio.AioCursor>`
      class which just inherits from both. The
      :class:`AioConnMixin <psycaio.AioCursorMixin>` type must be
      located before the psycopg2
      :py:class:`cursor <psycopg2.extensions.cursor>` type in the class
      hierarchy when performing a method lookup.

      For example, to create an asynchronous version of the psycopg2
      :py:class:`DictCursor <psycopg2.extras.DictCursor>` type, the following
      code can be used:

      .. code-block:: python

         from psycopg2.extras import DictCursor
         from psycaio import AioCursorMixin

         class AioDictCursor(AioCursorMixin, DictCursor):
            pass

      The *AioDictCursor* can then be used as the *cursor_factory* argument for
      the :py:func:`connect <psycaio.connect>` function or the
      :py:meth:`AioConnMixin.cursor <psycaio.AioConnMixin.cursor>` method.

    * The connect_timeout is ignored in asynchronous mode by psycopg2 (or
      actually libpq).
      Therefore timeout functionality is implemented in this function. When
      multiple hosts are provided, or a host resolves to multiple IP addresses,
      it will apply the timeout per single host, just like libpq in
      synchronous/blocking mode.

    Asynchronous DNS lookups are performed by this function as well, if
    necessary, because that part of the functionality is always blocking in
    libpq.

    """
    if connection_factory is None:
        connection_factory = AioConnection
    if cursor_factory is None:
        cursor_factory = AioCursor

    if dsn:
        conn_kwargs = parse_dsn(dsn)
    else:
        conn_kwargs = {}

    conn_kwargs.update(kwargs)
    conn_kwargs.update({'async_': True, 'client_encoding': 'UTF8'})

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

    loop = get_running_loop()

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

        # Check base type and order. Psycopg2 already checked if it is a valid
        # psycopg2 connection.
        mro = type(cn).__mro__
        try:
            mixin_pos = mro.index(AioConnMixin)
        except ValueError:
            raise OperationalError(
                "connection_factory must return an instance of AioConnMixin")
        if mro.index(connection) < mixin_pos:
            raise OperationalError(
                "AioConnMixin must be present before psycopg2 connection in"
                "method resolution order. Maybe base classes should be "
                "switched.")

        try:
            await wait_for(cn._start_connect_poll(), timeout)
            return cn
        except CancelledError:
            cn.close()
            # we got cancelled, do not try next entry
            raise
        except Exception as ex:
            cn.close()
            exceptions.append(ex)
    if len(exceptions) == 1:
        raise exceptions[0]
    raise OperationalError(exceptions)
