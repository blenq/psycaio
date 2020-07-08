from asyncio.proactor_events import BaseProactorEventLoop

try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop

from .conn import AioConnection
from .cursor import AioCursor, AioCursorMixin
from .conn_selector_connect import connect as selector_connect
from .conn_proactor_connect import connect as proactor_connect

__all__ = ['connect', 'AioConnection', 'AioCursorMixin', 'AioCursor']


async def connect(*args, **kwargs):
    loop = get_running_loop()
    if isinstance(loop, BaseProactorEventLoop):
        return await proactor_connect(*args, **kwargs)
    else:
        return await selector_connect(*args, **kwargs)
