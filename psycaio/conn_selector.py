from asyncio import Lock
try:
    from asyncio import get_running_loop
except ImportError:  # pragma: no cover
    from asyncio import get_event_loop as get_running_loop

from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extensions import (
    ISOLATION_LEVEL_DEFAULT, ISOLATION_LEVEL_READ_COMMITTED,
    ISOLATION_LEVEL_REPEATABLE_READ, ISOLATION_LEVEL_SERIALIZABLE,
    ISOLATION_LEVEL_READ_UNCOMMITTED,
    TRANSACTION_STATUS_IDLE,
    POLL_OK, POLL_READ, POLL_WRITE)

from .cursor_selector import SelectorAioCursorMixin


class SelectorAioConnMixin:
    """ override to restore dbapi transaction behaviour and add asyncio
    behaviour

    """
    _cursor_check_type = SelectorAioCursorMixin
    _loop_type = 'selector'

    def __init__(self, *args, **kwargs):
        self._loop = None
        self._poll_state = None

        super().__init__(*args, **kwargs)
        self._autocommit = False
        self._isolation_level = ISOLATION_LEVEL_DEFAULT
        self._fd = self.fileno()
        self._execute_lock = Lock()

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._autocommit = bool(value)

    @property
    def isolation_level(self):
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, level):
        if self.info.transaction_status != TRANSACTION_STATUS_IDLE:
            raise ProgrammingError(
                "set_session cannot be used inside a transaction")
        if level is None:
            self._isolation_level = ISOLATION_LEVEL_DEFAULT
            return

        if isinstance(level, int):
            if level not in range(1, 5):
                raise ValueError("isolation_level must be between 1 and 4")
            self._isolation_level = level
            return

        if isinstance(level, bytes):
            level = level.decode()
        if isinstance(level, str):
            for level_text, level_value in [
                    ("READ COMMITTED", ISOLATION_LEVEL_READ_COMMITTED),
                    ("REPEATABLE READ", ISOLATION_LEVEL_REPEATABLE_READ),
                    ("SERIALIZABLE", ISOLATION_LEVEL_SERIALIZABLE),
                    ("READ UNCOMMITTED", ISOLATION_LEVEL_READ_UNCOMMITTED),
                    ("DEFAULT", ISOLATION_LEVEL_DEFAULT),
                    ]:
                if level.upper() == level_text:
                    self._isolation_level = level_value
                    return

            raise ValueError(f"bad value for isolation_level: '{level}'")
        raise TypeError("Expected bytes or unicode string, got object instead")

    def _transaction_command(self):
        if (self.autocommit or
                self.info.transaction_status != TRANSACTION_STATUS_IDLE):
            return None
        cmd = ["BEGIN TRANSACTION"]
        if self.isolation_level:
            cmd.append("ISOLATION LEVEL")
            cmd.append({
                ISOLATION_LEVEL_READ_COMMITTED: "READ COMMITTED",
                ISOLATION_LEVEL_READ_UNCOMMITTED: "READ UNCOMMITTED",
                ISOLATION_LEVEL_REPEATABLE_READ: "REPEATABLE READ",
                ISOLATION_LEVEL_SERIALIZABLE: "SERIALIZABLE",
            }[self.isolation_level])
        return ' '.join(cmd)

    async def commit(self):
        async with self._execute_lock:
            await self.cursor()._execute("COMMIT")

    async def rollback(self):
        async with self._execute_lock:
            await self.cursor()._execute("ROLLBACK")

    def _start_connect_poll(self):
        """ Starts polling after connect """
        loop = self._loop = get_running_loop()
        fut = self._fut = loop.create_future()
        self._connect_poll()
        return fut

    def _start_exec_poll(self):
        """ Starts polling after execute """

        loop = self._loop = get_running_loop()
        fut = self._fut = loop.create_future()
        self._exec_poll()
        return fut

    def _try_poll(self):
        """ exception safe version of poll """
        try:
            return self.poll()
        except Exception as ex:
            # done with error, cleanup and notify waiter
            self._reset_io()
            if not self._fut.done():
                self._fut.set_exception(ex)

    def _handle_poll_ok(self):
        fut = self._fut
        if not fut.done():
            fut.set_result(True)

    def _handle_poll_read(self, callback):
        self._poll_state = POLL_READ
        self._loop.add_reader(self._fd, callback)

    def _handle_poll_write(self, callback):
        self._poll_state = POLL_WRITE
        self._loop.add_writer(self._fd, callback)

    def _handle_poll_state(self, state, callback):
        if state == POLL_READ:
            self._handle_poll_read(callback)
        elif state == POLL_OK:
            self._handle_poll_ok()
        elif state == POLL_WRITE:
            self._handle_poll_write(callback)
        else:
            # should not happen
            if not self._fut.done():
                self._fut.set_exception(
                    OperationalError(
                        "Unexpected result from poll: {}".format(state)))

    def _connect_poll(self):
        """ Poll method for connecting

        This resets the io notifications after each event, because file
        descriptor (or underlying socket) might change.

        """
        state = self._try_poll()
        if state is None:
            # error occurred
            return

        self._reset_io()
        self._fd = self.fileno()
        self._handle_poll_state(state, self._connect_poll)

    def _exec_poll(self):
        """Poll method for executing a command

        Slightly more efficient that the connecting version. It reuses existing
        io notifications and does not set the file descriptor after every
        event.

        """
        state = self._try_poll()
        if state == self._poll_state or state is None:
            # just need more of the same or an error occurred
            return

        self._reset_io()
        self._handle_poll_state(state, self._exec_poll)

    def _reset_io(self):
        """ Resets status and io handlers """

        poll_state = self._poll_state
        if poll_state is None:
            # already reset
            return
        self._poll_state = None
        if poll_state == POLL_WRITE:
            self._loop.remove_writer(self._fd)
        else:
            self._loop.remove_reader(self._fd)

    def close(self):
        self._reset_io()
        super().close()

    def __del__(self):
        if self._loop and not self._loop.is_closed():
            self._reset_io()
