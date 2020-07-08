from functools import partial

try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop

from psycopg2 import OperationalError, connect as pg_connect

from .conn import AioConnection as GenericConn
from .cursor import AioCursor
from .cursor_proactor import ProactorAioCursorMixin
from .conn_proactor import ProactorAioConnMixin


class AioConnection(ProactorAioConnMixin, GenericConn):
    pass


class AioCursor(ProactorAioCursorMixin, AioCursor):
    pass


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None, **kwargs):

    if connection_factory is None:
        connection_factory = AioConnection
    if cursor_factory is None:
        cursor_factory = AioCursor
    loop = get_running_loop()
    func = partial(
        pg_connect, dsn=dsn, connection_factory=connection_factory,
        cursor_factory=cursor_factory, **kwargs)
    cn = await loop.run_in_executor(None, func)
    if not isinstance(cn, AioConnection):
        raise OperationalError(
            "connection_factory must return an instance of AioConnection")

    return cn
