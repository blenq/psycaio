from functools import partial

from psycopg2 import OperationalError, connect as pg_connect

from .conn import AioConnMixin, AioConnection  # as GenericConn
from .cursor import AioCursor
from .utils import get_running_loop


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None, **kwargs):

    conn_kwargs = {
        **kwargs,
        **{'async_': False, 'client_encoding': 'UTF8'},
    }

    loop = get_running_loop()
    func = partial(
        pg_connect, dsn=dsn, connection_factory=connection_factory,
        cursor_factory=cursor_factory, **conn_kwargs)
    cn = await loop.run_in_executor(None, func)
    if not isinstance(cn, AioConnMixin):
        raise OperationalError(
            "connection_factory must return an instance of AioConnection")
    return cn
