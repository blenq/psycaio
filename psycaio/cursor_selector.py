from psycopg2.extensions import cursor


class SelectorAioCursorMixin:

    async def _call_async(self, func, *args, **kwargs):
        ret = func(*args, **kwargs)
        fut = self.connection._start_exec_poll()
        await self._wait_for_execute(fut)
        return ret

    async def _exec_async(self, func, *args, **kwargs):
        async with self.connection._execute_lock:
            transaction_cmd = self.connection._transaction_command()
            if transaction_cmd is not None:
                await self._call_async(cursor.execute, self, transaction_cmd)
            return await self._call_async(func, *args, **kwargs)
