from functools import partial

from .utils import get_running_loop


class ProactorAioCursorMixin:

    async def _exec_async(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        fut = get_running_loop().run_in_executor(None, func)
        return await self._wait_for_execute(fut)
