try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop

from psycopg2 import ProgrammingError

from .cursor_proactor import ProactorAioCursorMixin


class ProactorAioConnMixin:
    _cursor_check_type = ProactorAioCursorMixin
    _loop_type = 'proactor'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def commit(self):
        await get_running_loop().run_in_executor(None, super().commit)

    async def rollback(self):
        await get_running_loop().run_in_executor(None, super().rollback)

    def cursor(self, name=None, *args, **kwargs):
        if name is not None:
            raise ProgrammingError(
                "asynchronous connections cannot produce named cursors")
        return super().cursor(name, *args, **kwargs)
