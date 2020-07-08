
class SelectorAioCursorMixin:

    async def execute(self, *args, **kwargs):
        async with self.connection._execute_lock:
            transaction_cmd = self.connection._transaction_command()
            if transaction_cmd is not None:
                await self._execute(transaction_cmd)
            await self._execute(*args, **kwargs)

    async def _execute(self, *args, **kwargs):
        super().execute(*args, **kwargs)
        fut = self.connection._start_exec_poll()
        await self._wait_for_execute(fut)
