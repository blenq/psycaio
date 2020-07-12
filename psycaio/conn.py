from psycopg2 import OperationalError, InterfaceError
from psycopg2.extensions import connection

from .conn_selector import SelectorAioConnMixin
from .conn_proactor import ProactorAioConnMixin
from .cursor import _local_state
from .utils import TypeCache, get_running_loop

# Cache for dynamically created types. We need both. In a Windows
# world it can theoretically happen that both a SelectorEventLoop and a
# ProactorEventLoop are instantiated, not necessarily at the same time.
_selector_types = TypeCache("Selector", SelectorAioConnMixin)
_proactor_types = TypeCache("Proactor", ProactorAioConnMixin)


class AioConnMixin:

    def __new__(cls, dsn, async_=False):
        """ Instantiate the proper class instance based on the type of loop """

        if async_:
            args = (dsn, async_)
        else:
            args = (dsn,)

        if issubclass(cls, (ProactorAioConnMixin, SelectorAioConnMixin)):
            return super().__new__(cls, *args)

        if async_:
            _types = _selector_types
        else:
            _types = _proactor_types

        return super().__new__(_types[cls], *args)

    def cursor(self, *args, **kwargs):
        """ Override to add type check """

        # set the loop type, so CursorMixin will instantiate the correct
        # implementation
        _local_state.loop_type = self._loop_type
        cr = super().cursor(*args, **kwargs)
        if not isinstance(cr, self._cursor_check_type):
            raise OperationalError(
                "cursor_factory must return an instance of AioCursorMixin")
        return cr

    async def cancel(self):
        await get_running_loop().run_in_executor(None, super().cancel)

    def _check_closed(self):
        if self.closed:
            raise InterfaceError("connection already closed")


class AioConnection(AioConnMixin, connection):
    pass
