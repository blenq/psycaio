from asyncio import shield, CancelledError


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
        try:
            # Shield the future so we can still wait for it when we cancel the
            # operation server side as well.
            await shield(fut)
        except CancelledError:
            if not fut.done():
                # This routine got cancelled, but the server is still busy
                # with our statement. Try to cancel the current server
                # operation as well.
                try:
                    await self.connection.cancel()
                    await fut
                except Exception:
                    # Don't bother with this exception.
                    pass

            # And reraise. We got cancelled after all.
            raise
        finally:
            # Make sure future is done. This is a no-op when fut is
            # already done.
            fut.cancel()
