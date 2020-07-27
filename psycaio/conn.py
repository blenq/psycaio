from asyncio import (
    Lock, Queue, wrap_future, run_coroutine_threadsafe, shield,
    CancelledError, QueueEmpty)
from contextlib import contextmanager

from psycopg2 import OperationalError, InterfaceError
from psycopg2.extensions import (
    POLL_OK, POLL_READ, POLL_WRITE, connection, cursor)

from .utils import get_running_loop, selector_pool
from .cursor import AioCursorMixin


class NotifyQueue:
    """ Queue that is used for NOTIFY messages """

    def __init__(self, connection):

        self._queue = Queue()

        # psycopg2 will use the append method to add a notify object
        if connection._thread_manager is not None:
            self._loop_call_soon_threadsafe = (
                get_running_loop().call_soon_threadsafe)
            self.append = self._append_threadsafe
        else:
            self.append = self._queue.put_nowait

    def _append_threadsafe(self, item):
        # In the proactor scenario, the append is executed by psycopg2 in a
        # selector thread, but the queue lives in the original thread from
        # where the connection was instantiated. This method bridges the gap
        # between those threads (and loops).
        self._loop_call_soon_threadsafe(self._queue.put_nowait, item)

    async def _pop(self):
        queue = self._queue
        notify = await queue.get()
        queue.task_done()
        return notify

    def _pop_nowait(self):
        queue = self._queue
        notify = queue.get_nowait()
        queue.task_done()
        return notify

    def clear(self):
        getters = self._queue._getters
        while getters:
            getter = getters.popleft()
            if not getter.done():
                getter.set_exception(InterfaceError("connection is closed"))


class ThreadManager:
    """ Reserves a thread from the pool for usage by the connection.

    This is used when the connection lives in a proactor loop. It needs a
    selector loop, and it will use one parallel to the proactor loop. This
    manager bridges the gap between those two loops.

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
            # not in use yet, get a thread
            self.thread = selector_pool.get_thread()
        self._usage += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._usage -= 1
        if self._usage == 0:
            # not in use anymore, release the thread
            self.thread.decrement()
            self.thread = None

    def _run_coroutine(self, coro):
        """ Runs a coroutine in the selector loop and returns a
        concurrent.futures.Future (not an asyncio Future)

        """
        return run_coroutine_threadsafe(coro, self.thread.loop)

    def call(self, callback, *args):
        """ Executes a callback that in the selector loop.

        Returns the result of the callback
        """

        async def _coro():
            return callback(*args)

        return self._run_coroutine(_coro()).result()

    async def run_coro(self, coro):
        """ Executes a coroutine in the selector loop and returns the result

        """
        fut = wrap_future(self._run_coroutine(coro))
        await fut
        return fut.result()


class AioConnMixin:
    """ Mixin class to add asyncio behavior to the psycopg2
    :py:class:`connection` class.

    This class should be not be instantiated directly. It should be used as a
    base class when implementing a custom connection.

    """
    __module__ = 'psycaio'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._num_readers = 0
        self._writing = False
        loop = get_running_loop()
        if hasattr(loop, "_proactor"):
            self._thread_manager = ThreadManager()
        else:
            self._thread_manager = None
            self._loop = loop
        self.notifies = NotifyQueue(self)
        self._execute_lock = Lock()

    def cursor(
            self, name=None, cursor_factory=None, scrollable=None,
            withhold=False):
        """ Create and return a new cursor to perform database operations.

        The only supported argument is *cursor_factory*, because named cursors
        are not available in asynchronous mode.

        If set, the *cursor_factory* is used to instantiate the cursor. It
        must return an instance of both an
        :class:`AioCursorMixin <psycaio.AioCursorMixin>` and a psycopg2
        :py:class:`cursor <psycopg2.extensions.cursor>`. The default is the
        :class:`AioCursor <psycaio.AioCursor>` class which just inherits from
        both. The :class:`AioCursorMixin <psycaio.AioCursorMixin>` type must be
        located before the psycopg2
        :py:class:`cursor <psycopg2.extensions.cursor>` type in the class
        hierarchy when performing a method lookup.

        """
        cr = super().cursor(
            name=name, cursor_factory=cursor_factory, scrollable=scrollable,
            withhold=withhold)

        # psycopg2 already checked if it is a valid psycopg2 cursor. Now check
        # for the psycaio mixin and the base class order
        mro = type(cr).__mro__
        try:
            mixin_pos = mro.index(AioCursorMixin)
        except ValueError:
            # cursor is not a AioCursorMixin
            raise TypeError(
                "cursor factory must return a subclass of "
                "psycaio.AioCursorMixin")

        if mro.index(cursor) < mixin_pos:
            # To be able to override behavior, AioCursorMixin should be before
            # the real cursor, in the class hierarchy
            raise TypeError(
                "psycaio.AioCursorMixin must be present before "
                "psycopg2.extensions.cursor in method resolution order. Maybe "
                "base classes should be switched.")
        return cr

    def _start_reading(self, callback):
        """ Adds a reader to the list """

        # Adding the connection to the loop for reading only when there are
        # actual readers.
        # Adding a reader adds a reference to the connection, through the
        # callback. For the garbage collector to collect the connection when
        # the loop is still running we make sure that it is added only when a
        # command is executed or a notify messages is retrieved, by calling
        # _stop_reading when finished with the operation
        if self._num_readers == 0:
            self._loop.add_reader(self._fd, callback)
        self._num_readers += 1

    def _stop_reading(self):
        """ Removes a reader from the list """
        if self._num_readers == 0:
            # can happen if a connection is closed by calling close()
            return
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
        if state == POLL_WRITE:
            self._start_writing(self._connect_poll)
        elif state == POLL_READ:
            self._start_reading(self._connect_poll)
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

    async def __start_connect_poll(self):
        self._fut = self._loop.create_future()
        self._connect_poll()
        await self._fut

    async def _start_connect_poll(self):
        """ Starts polling after connect """
        if self._thread_manager is not None:
            with self._selector_thread() as tm:
                await tm.run_coro(self.__start_connect_poll())
        else:
            await self.__start_connect_poll()

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
            if self.closed:
                self.notifies.clear()
            return

        if state == POLL_WRITE:
            self._start_writing(self._poll)
            return

        self._stop_writing()
        if state == POLL_OK:
            fut = self._fut
            if not fut.done():
                fut.set_result(True)
        elif state != POLL_READ:
            # should not happen
            if not self._fut.done():
                self._fut.set_exception(
                    OperationalError(
                        "Unexpected result from poll: {}".format(state)))

    async def __start_poll(self):
        """ Starts polling after execute """

        self._fut = self._loop.create_future()
        self._start_reading(self._poll)
        try:
            self._poll()
            await self._wait_poll()
        finally:
            self._stop_reading()

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
        """Cancel the current database operation.

        This is the coroutine version of the psycopg2
        :py:meth:`connection.cancel` method.

        """
        # original method is always blocking, so resort to threadpool
        await get_running_loop().run_in_executor(None, super().cancel)

    def get_notify_nowait(self):
        """ Remove and return a psycopg2
        :py:class:`Notify <psycopg2.extensions.Notify>` object from the Notify
        queue if one is immediately available, else raise
        :py:class:`asyncio.QueueEmpty`.

        """
        return self.notifies._pop_nowait()

    async def get_notify(self):
        """ Remove and return a psycopg2
        :py:class:`Notify <psycopg2.extensions.Notify>` object from the Notify
        queue. If the queue is empty, wait until an item is available.

        The :py:attr:`connection.notifies` attribute is replaced by the
        :py:class:`psycaio.AioConnMixin` with a custom version that plays
        nicely with asyncio. Do not set it to anything else or this method will
        probably break.

        Example:

        .. code-block:: python

            async def test_notify(cn1, cn2):
                cr1 = cn1.cursor()
                await cr1.execute("LISTEN queue")

                cr2 = cn2.cursor()
                await cr2.execute("NOTIFY queue, 'hi'")

                notify = await cn1.get_notify()
                print(notify.payload)

        For more information see the PostgreSQL docs on
        `LISTEN <https://www.postgresql.org/docs/current/sql-listen.html>`_.
        """
        try:
            return self.get_notify_nowait()
        except QueueEmpty:
            pass

        # nothing in the Queue. Start reading until we got one
        if self.closed:
            raise InterfaceError("connection already closed")
        if self._thread_manager is not None:
            with self._selector_thread() as tm:
                tm.call(self._start_reading, self._poll)
                try:
                    return await self.notifies._pop()
                finally:
                    tm.call(self._stop_reading)
        else:
            self._start_reading(self._poll)
            try:
                return await self.notifies._pop()
            finally:
                self._stop_reading()

    def _close(self):
        self._reset_connect()
        super().close()
        self._fd = None

    def close(self):
        """ Close the connection.

        Coroutines that are still waiting for a Notify message with
        :py:meth:`get_notify <psycaio.AioConnMixin.get_notify>` will be
        interrupted by a psycopg2
        :py:exc:`InterfaceError <psycopg2.InterfaceError>`.

        """
        if self._thread_manager is not None:
            with self._selector_thread() as tm:
                tm.call(self._close)
        else:
            self._close()
        self.notifies.clear()


class AioConnection(AioConnMixin, connection):
    """ The default connection class used by psycaio.

    It just inherits from both :class:`AioConnMixin <psycaio.AioConnMixin>` for
    the asyncio behavior and the standard psycopg2
    :py:class:`connection <psycopg2.extensions.connection>`, and contains no
    additional implementation.

    This class should not be instantiated directly. Use the
    :func:`connect <psycaio.connect>` function instead.

    """
