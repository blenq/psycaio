import os
import socket
from asyncio import CancelledError, wait_for

try:
    from asyncio import get_running_loop
except ImportError:
    from asyncio import get_event_loop as get_running_loop

from psycopg2 import connect as pg_connect, OperationalError
from psycopg2.extensions import parse_dsn

from .conn_selector import AioConnection
from .cursor import AioCursor


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
