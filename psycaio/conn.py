from asyncio import (
    Lock, Queue, wrap_future, run_coroutine_threadsafe, shield,
    CancelledError)
from contextlib import contextmanager
from threading import Event
import weakref

from psycopg2 import OperationalError
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, connection)

from .utils import get_running_loop, selector_pool
from .cursor import AioCursorMixin

_iso_levels = [
    "DEFAULT", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE",
    "READ UNCOMMITTED",
]


class NotifyQueue:
    """ Queue that is used for NOTIFY messages """

    def __init__(self, connection):

        # use weak reference to prevent circular reference
        self._connref = weakref.ref(connection)

        self._queue = Queue()

        # psycopg2 will use the append method to add a notify object
        if connection._thread_manager:
            self.append = self._append_threadsafe
            self._loop = get_running_loop()
        else:
            self.append = self._queue.put_nowait
        self._num_getters = 0

    def _append_threadsafe(self, item):
        # In the proactor scenario, the append is executed by psycopg2 in a
        # threadpool thread, but the queue lives in the original thread from
        # where the connection was instantiated. This method bridges the gap
        # between those threads (and loops).
        self._loop.call_soon_threadsafe(self._queue.put_nowait, item)

    async def _pop(self, cn):
        queue = self._queue
        notify = await queue.get()
        queue.task_done()
        return notify

    async def pop(self):
        queue = self._queue
        if not queue.empty():
            return queue.get_nowait()

        # nothing in the Queue. Start reading until we got one
        cn = self._connref()
        if cn is None or cn.closed:
            raise ValueError("Connection is closed")
        if cn._thread_manager is not None:
            with cn._reading_thread():
                return await self._pop(cn)
        else:
            cn._start_reading()
            try:
                return await self._pop(cn)
            finally:
                cn._stop_reading()


class ThreadManager:
    """ Reserves a thread from the pool for usage by the connection

    Multiple consumers might be interested in the connection socket. At most
    one executing command and multiple consumers of NOTIFY messages. When any
    of these operations is in progress, the connection must stay in the same
    loop.
    By using this class as a context manager whenever IO is requested, it will
    hold on to the thread and the containing loop during the operations.

    """
    def __init__(self):
        # The number of operations using this manager
        self._usage = 0

    def __enter__(self):
        if self._usage == 0:
            self.thread = selector_pool.get_thread()
        self._usage += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._usage -= 1
        if self._usage == 0:
            self.thread.decrement()
            self.thread = None

    async def run_coro(self, coro):
        fut = wrap_future(run_coroutine_threadsafe(coro, self.thread.loop))
        await fut
        return fut.result()


class AioConnMixin:
    """ Mixin to add asyncio polling """

    def __init__(self, *args, **kwargs):
        self._num_readers = 0
        self._writing = False
        self._thread_manager = None

        super().__init__(*args, **kwargs)

        self._execute_lock = Lock()

    def cursor(self, *args, **kwargs):
        """ Override to add type check """

        # psycopg2 already checks if it is a valid psyco cursor. This check is
        # only for the psycaio mixin
        cr = super().cursor(*args, **kwargs)
        if not isinstance(cr, AioCursorMixin):
            raise OperationalError(
                "cursor_factory must return an instance of AioCursorMixin")
        return cr

    def _start_reading(self, callback=None):
        """ Adds a reader to the list """

        # Adding the connection to the loop for reading only when there are
        # actual readers.
        # Adding a reader adds a reference to the connection, through the
        # callback. For the garbage collector to collect the connection when
        # the loop is still running we make sure that it is added only when a
        # command is executed or a notify messages is retrieved, by calling
        # _stop_reading when finished with the operation
        if self._num_readers == 0:
            if callback is None:
                callback = self._poll
            self._loop.add_reader(self._fd, callback)
        self._num_readers += 1

    def _stop_reading(self, fut=None):
        """ Removes a reader from the list """
        self._num_readers -= 1
        if self._num_readers == 0:
            # nobody is interested anymore
            self._loop.remove_reader(self._fd)

    def _start_writing(self, callback):
        if self._writing:
            return
        self._loop.add_writer(self._fd, callback)
        self._writing = True

    def _stop_writing(self):
        if not self._writing:
            return
        self._loop.remove_writer(self._fd)
        self._writing = False

    def _reset_connect(self):
        """ Resets status and io handlers """
        self._stop_writing()
        if self._num_readers:
            self._loop.remove_reader(self._fd)
            self._num_readers = 0

    def _connect_poll(self):
        """ Poll method for connecting

        This resets the io notifications after each event, because file
        descriptor (or underlying socket) might change.

        """
        self._reset_connect()
        try:
            state = self.poll()
        except Exception as ex:
            if not self._fut.done():
                self._fut.set_exception(ex)
            return

        self._fd = self.fileno()
        if state == POLL_READ:
            self._start_reading(self._connect_poll)
        elif state == POLL_WRITE:
            self._start_writing(self._connect_poll)
        elif state == POLL_OK:
            # we are connected
            fut = self._fut
            if not fut.done():
                fut.set_result(True)
        else:
            # should not happen
            if not self._fut.done():
                self._fut.set_exception(
                    OperationalError(
                        "Unexpected result from poll: {}".format(state)))

    @contextmanager
    def _selector_thread(self):
        with self._thread_manager as tm:
            self._loop = tm.thread.loop
            yield tm

    @contextmanager
    def _reading_thread(self):
        with self._selector_thread() as tm:
            tm.thread.loop.call_soon_threadsafe(
                self._start_reading, self._poll)
            try:
                yield tm
            finally:
                tm.thread.loop.call_soon_threadsafe(self._stop_reading)

    async def __start_connect_poll(self):
        self._fut = self._loop.create_future()
        self._connect_poll()
        await self._fut

    async def _start_connect_poll(self, proactor):
        """ Starts polling after connect """
        if proactor:
            self._thread_manager = ThreadManager()
            with self._selector_thread() as tm:
                await tm.run_coro(self.__start_connect_poll())
        else:
            self._loop = get_running_loop()
            await self.__start_connect_poll()
        self.notifies = NotifyQueue(self)

    def _poll(self):
        """Poll method for a connected instance

        Used for executing commands and receiving notify messages

        """
        try:
            state = self.poll()
        except Exception as ex:
            self._stop_writing()
            # done with error, cleanup and notify waiter
            if not self._fut.done():
                self._fut.set_exception(ex)
            return

        if state == POLL_READ:
            self._stop_writing()
        elif state == POLL_OK:
            self._stop_writing()
            fut = self._fut
            if not fut.done():
                fut.set_result(True)
        elif state == POLL_WRITE:
            self._start_writing(self._poll)
        else:
            self._stop_writing()
            # should not happen
            if not self._fut.done():
                self._fut.set_exception(
                    OperationalError(
                        "Unexpected result from poll: {}".format(state)))

    async def __start_poll(self):
        """ Starts polling after execute """

        self._fut = fut = self._loop.create_future()
        fut.add_done_callback(self._stop_reading)
        self._start_reading()
        self._poll()
        await self._wait_poll()

    async def _wait_poll(self):
        fut = self._fut
        try:
            # Shield the future so we can still wait for it when we cancel the
            # operation server side as well.
            return await shield(fut)
        except CancelledError:
            if not fut.done():
                # This routine got cancelled, but the server is still busy
                # with our statement. Try to cancel the current server
                # operation as well.
                try:
                    await self.cancel()
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

    async def _start_poll(self):
        if self._thread_manager is not None:
            with self._selector_thread() as tm:
                await tm.run_coro(self.__start_poll())
        else:
            await self.__start_poll()

    async def cancel(self):
        await get_running_loop().run_in_executor(None, super().cancel)

    def _close(self):
        self._reset_connect()
        super().close()
        self._fd = None

    def _close_threadsafe(self):
        try:
            self._close()
        except Exception as ex:
            self._close_ex = ex
        finally:
            self._close_event.set()

    def close(self):
        if self._thread_manager:
            with self._selector_thread():
                ev = self._close_event = Event()
                self._close_ex = None
                self._loop.call_soon_threadsafe(self._close_threadsafe)
                ev.wait()
                if self._close_ex is not None:
                    raise self._close_ex
        else:
            self._close()

#     def __del__(self):
#         if self._loop and not self._loop.is_closed():
#             self._reset_connect()


class AioConnection(AioConnMixin, connection):
    pass
