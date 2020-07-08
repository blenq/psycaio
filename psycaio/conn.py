from asyncio.proactor_events import BaseProactorEventLoop
try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop

from psycopg2 import OperationalError
from psycopg2.extensions import connection

from .conn_selector import SelectorAioConnMixin
from .conn_proactor import ProactorAioConnMixin
from .cursor import _local_state
from .utils import TypeCache

# Cache for dynamically created types. We need both. In a Windows
# world it can theoretically happen that both a SelectorEventLoop and a
# ProactorEventLoop are instantiated, not necessarily at the same time.
_selector_types = TypeCache("Selector", SelectorAioConnMixin)
_proactor_types = TypeCache("Proactor", ProactorAioConnMixin)


class AioConnMixin:

    def __new__(cls, *args, **kwargs):
        """ Instantiate the proper class instance based on the type of loop """

        if issubclass(cls, (ProactorAioConnMixin, SelectorAioConnMixin)):
            return super().__new__(cls, *args, **kwargs)

        loop = get_running_loop()

        # Assumption: All loop implementations besides the ProactorEventLoop
        # support add_reader and add_writer.
        # This is true for the existing loops in the Python stdlib and the
        # uvloop, but it might not be true for a yet unknown loop.
        if isinstance(loop, BaseProactorEventLoop):
            _types = _proactor_types
        else:
            _types = _selector_types

        return super().__new__(_types[cls], *args, **kwargs)

    def cursor(self, *args, **kwargs):
        """ Override to add type check """
        _local_state.loop_type = self._loop_type
        cr = super().cursor(*args, **kwargs)
        if not isinstance(cr, self._cursor_check_type):
            raise OperationalError(
                "cursor_factory must return an instance of AioCursorMixin")
        return cr

    async def cancel(self):
        await get_running_loop().run_in_executor(None, super().cancel)


class AioConnection(AioConnMixin, connection):
    pass
