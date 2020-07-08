try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop
from functools import partial


class ProactorAioCursorMixin:

    async def execute(self, *args, **kwargs):
        func = partial(super().execute, *args, **kwargs)
        fut = get_running_loop().run_in_executor(None, func)
        await self._wait_for_execute(fut)
