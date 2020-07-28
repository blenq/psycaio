from psycopg2.extensions import cursor as PGCursor


class AioCursorMixin:
    """ Mixin class to add asyncio behavior to the psycopg2
    :py:class:`psycopg2:cursor` class.

    This class should be not be instantiated directly. It should be used as a
    base class when implementing a custom cursor.

    When an operation is cancelled by asyncio (:py:meth:`asyncio.Task.cancel`,
    :py:func:`asyncio.wait_for`, ...), i.e. when a
    :py:exc:`asyncio.CancelledError` is raised during the operation, then
    psycaio will try to cancel the operation server side as well.

    The execute and callproc methods can be called concurrently, but psycaio
    will make sure that all operations for one database connection are actually
    executed serially, because the underlying libraries can not handle multiple
    operations concurrently. If you want more concurrency make sure to
    use multiple database connections.

    """
    __module__ = 'psycaio'

    async def _call_async(self, func, *args, **kwargs):
        async with self.connection._execute_lock:
            ret = func(*args, **kwargs)
            await self.connection._start_poll()
            return ret

    async def callproc(self, procname, parameters=None):
        """Calls a PostgreSQL function using SELECT.

        This is the coroutine version of the psycopg2
        :py:meth:`cursor.callproc` method.

        """
        return await self._call_async(super().callproc, procname, parameters)

    async def execute(self, query, vars=None):  # noqa
        """Execute a database query.

        This is the coroutine version of the psycopg2 :py:meth:`cursor.execute`
        method.

        """
        return await self._call_async(super().execute, query, vars=vars)

    async def executemany(self, query, vars_list):
        """Execute a database query against multiple sequences or mappings of
        parameters.

        This is the coroutine version of the psycopg2
        :py:meth:`cursor.executemany` method.

        """
        for variables in vars_list:
            await self.execute(query, variables)


class AioCursor(AioCursorMixin, PGCursor):
    """The default cursor class used by psycaio.

    It just inherits from both :class:`AioCursorMixin <psycaio.AioCursorMixin>`
    for the asyncio behavior and the standard psycopg2 :py:class:`cursor`, and
    contains no additional implementation.

    This class should not be instantiated directly. Use the
    :meth:`AioConnMixin.cursor <psycaio.AioConnMixin.cursor>` method instead.

    """
    __module__ = 'psycaio'
