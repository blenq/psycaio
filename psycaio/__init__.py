from asyncio.proactor_events import BaseProactorEventLoop

from .conn import AioConnection
from .cursor import AioCursor, AioCursorMixin
from .conn_selector_connect import connect as selector_connect
from .conn_proactor_connect import connect as proactor_connect
from .utils import get_running_loop

__all__ = ['connect', 'AioConnection', 'AioCursorMixin', 'AioCursor']


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None, **kwargs):

    if connection_factory is None:
        connection_factory = AioConnection
    if cursor_factory is None:
        cursor_factory = AioCursor

    loop = get_running_loop()
    if hasattr(loop, "_proactor"):
        _connect = proactor_connect
    else:
        _connect = selector_connect

    return await _connect(
        dsn=dsn, connection_factory=connection_factory,
        cursor_factory=cursor_factory, **kwargs)
