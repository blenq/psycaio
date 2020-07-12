from asyncio import Lock

from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extensions import (
    TRANSACTION_STATUS_IDLE, POLL_OK, POLL_READ, POLL_WRITE)

from .cursor_selector import SelectorAioCursorMixin
from .utils import get_running_loop

_iso_levels = [
    "DEFAULT", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE",
    "READ UNCOMMITTED",
]


def _parse_on_off(val):
    if isinstance(val, bytes):
        val = val.decode()

    if isinstance(val, str):
        if val.upper() == "DEFAULT":
            val = None
        else:
            raise ValueError(
                f"the only string accepted is 'default'; got {val}")
    elif val is not None:
        val = bool(val)
    return val


class SelectorAioConnMixin:
    """ override to restore psycopg2 transaction behavior and add asyncio
    polling

    """
    _cursor_check_type = SelectorAioCursorMixin
    _loop_type = 'selector'

    def __init__(self, *args, **kwargs):
        self._loop = None
        self._poll_state = None

        super().__init__(*args, **kwargs)
        self._autocommit = False
        self._isolation_level = None
        self._readonly = None
        self._deferrable = None
        self._fd = self.fileno()
        self._execute_lock = Lock()

    def _check_trans(self):
        self._check_closed()
        if self.info.transaction_status != TRANSACTION_STATUS_IDLE:
            raise ProgrammingError(
                "set_session cannot be used inside a transaction")

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._check_trans()
        self._autocommit = bool(value)

    @property
    def deferrable(self):
        return self._deferrable

    @deferrable.setter
    def deferrable(self, val):
        self._check_trans()
        self._deferrable = _parse_on_off(val)

    @property
    def readonly(self):
        return self._readonly

    @readonly.setter
    def readonly(self, val):
        self._check_trans()
        self._readonly = _parse_on_off(val)

    @property
    def isolation_level(self):
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, level):
        self._check_trans()

        if isinstance(level, bytes):
            level = level.decode()

        if isinstance(level, str):
            try:
                level = _iso_levels.index(level.upper())
            except ValueError:
                raise ValueError(f"bad value for isolation_level: '{level}'")

            if level == 0:
                level = None

        if level is None:
            self._isolation_level = None
        elif isinstance(level, int):
            if level not in range(1, 5):
                raise ValueError("isolation_level must be between 1 and 4")
            self._isolation_level = level
        else:
            raise TypeError(
                "Expected bytes or unicode string, got object instead")

    def _transaction_command(self):
        if (self.autocommit or
                self.info.transaction_status != TRANSACTION_STATUS_IDLE):
            return None
        cmd = ["BEGIN TRANSACTION"]
        if self.isolation_level:
            cmd.append("ISOLATION LEVEL")
            cmd.append(_iso_levels[self._isolation_level])
        if self.readonly is not None:
            cmd.append("READ ONLY" if self.readonly else "READ WRITE")
        if self.deferrable is not None:
            cmd.append("DEFERRABLE" if self.deferrable else "NOT DEFERRABLE")
        return ' '.join(cmd)

    async def commit(self):
        if self.info.transaction_status != TRANSACTION_STATUS_IDLE:
            await self.cursor().execute("COMMIT")

    async def rollback(self):
        if self.info.transaction_status != TRANSACTION_STATUS_IDLE:
            await self.cursor().execute("ROLLBACK")

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
