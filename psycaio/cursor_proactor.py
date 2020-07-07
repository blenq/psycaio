from asyncio import CancelledError
from functools import partial


class ProactorAioCursorMixin:

    async def execute(self, *args, **kwargs):
        async with self.connection._execute_lock:
            func = partial(super().execute, *args, **kwargs)
            try:
                await self.connection._loop.run_in_executor(None, func)
            except CancelledError:
                try:
                    await self.connection.cancel()
                except Exception:
                    pass
                raise
