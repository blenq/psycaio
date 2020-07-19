from asyncio.proactor_events import BaseProactorEventLoop

from .cursor import AioCursor, AioCursorMixin
from .conn import AioConnection
from .conn_connect import connect as _connect

__all__ = ['connect', 'AioConnection', 'AioCursorMixin', 'AioCursor']


async def connect(
        dsn=None, connection_factory=None, cursor_factory=None, **kwargs):

    if connection_factory is None:
        connection_factory = AioConnection
    if cursor_factory is None:
        cursor_factory = AioCursor

    return await _connect(
        dsn=dsn, connection_factory=connection_factory,
        cursor_factory=cursor_factory, **kwargs)
