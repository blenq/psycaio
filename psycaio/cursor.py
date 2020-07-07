from asyncio.proactor_events import BaseProactorEventLoop
try:
    from asyncio import get_running_loop
except ImportError:
    from asyncio import get_event_loop as get_running_loop

from psycopg2.extensions import cursor

from .cursor_selector import SelectorAioCursorMixin
from .cursor_proactor import ProactorAioCursorMixin
from .utils import TypeCache

# Cache for dynamically created types. We need both. In a Windows
# world it can theoretically happen that both a SelectorEventLoop and a
# ProactorEventLoop are instantiated, not necessarily at the same time.
_selector_cursor_types = TypeCache("Selector", SelectorAioCursorMixin)
_proactor_cursor_types = TypeCache("Proactor", ProactorAioCursorMixin)


class AioCursorMixin:

    def __new__(cls, *args, **kwargs):
        """ Instantiate the proper class instance based on the type of loop """

        loop = get_running_loop()

        # Assumption: All loop implementations besides the ProactorEventLoop
        # support add_reader and add_writer.
        # This is true for the existing loops in the Python stdlib and the
        # uvloop, but it might not be true for a yet unknown loop.
        if isinstance(loop, BaseProactorEventLoop):
            _types = _proactor_cursor_types
        else:
            _types = _selector_cursor_types

        return super().__new__(_types[cls], *args, **kwargs)


class AioCursor(AioCursorMixin, cursor):
    pass
