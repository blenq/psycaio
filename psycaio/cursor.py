from psycopg2.extensions import cursor


class AioCursorMixin:

    async def _call_async(self, func, *args, **kwargs):
        async with self.connection._execute_lock:
            ret = func(*args, **kwargs)
            await self.connection._start_poll()
            return ret

    async def callproc(self, *args, **kwargs):
        return await self._call_async(super().callproc, *args, **kwargs)

    async def execute(self, *args, **kwargs):
        return await self._call_async(super().execute, *args, **kwargs)

    async def executemany(self, query, vars_list):
        for variables in vars_list:
            await self.execute(query, variables)


class AioCursor(AioCursorMixin, cursor):
    pass
