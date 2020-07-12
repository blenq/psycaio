from asyncio import shield, CancelledError
import threading

from psycopg2.extensions import cursor

from .cursor_selector import SelectorAioCursorMixin
from .cursor_proactor import ProactorAioCursorMixin
from .utils import TypeCache

# Cache for dynamically created types. We need both. In a Windows
# world it can happen that both a SelectorEventLoop and a
# ProactorEventLoop are instantiated, not necessarily at the same time.
_selector_cursor_types = TypeCache("Selector", SelectorAioCursorMixin)
_proactor_cursor_types = TypeCache("Proactor", ProactorAioCursorMixin)

# TODO: replace with contextvars when 3.6 is EOL
_local_state = threading.local()


class AioCursorMixin:

    def __new__(cls, *args, **kwargs):
        """ Instantiate the proper class instance based on the type of loop """

        if issubclass(cls, (ProactorAioCursorMixin, SelectorAioCursorMixin)):
            return super().__new__(cls, *args, **kwargs)

        if _local_state.loop_type == 'proactor':
            _types = _proactor_cursor_types
        else:
            _types = _selector_cursor_types

        return super().__new__(_types[cls], *args, **kwargs)

    async def _wait_for_execute(self, fut):
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

    async def execute(self, *args, **kwargs):
        return await self._exec_async(super().execute, *args, **kwargs)

    async def callproc(self, *args, **kwargs):
        return await self._exec_async(super().callproc, *args, **kwargs)


class AioCursor(AioCursorMixin, cursor):
    pass
