from queue import Queue, Empty
import sys
import os
import collections
import functools
import time
from threading import Thread, Event, current_thread, Lock
import enum
import contextlib
import concurrent.futures as cf
from typing import (
    Any, List, NewType, DefaultDict, Set, Optional, Union, Callable,
    Dict, cast, Type, Deque, Iterable, Iterator, TypeVar
)

from .pqueue import PriorityQueue

_T = TypeVar('_T')

TIMER_RESOLUTION = 1e-4  # 100 us

# TODO: define type for time difference
Time = float
Duration = float


# status field can also be None if task is not yet fully initialized by the background thread
class _Status(enum.Enum):
    WAITING = object()
    RUNNING = object
    DONE = object()
    CANCELLED = object()


# http://stackoverflow.com/a/40257140/336527
@contextlib.contextmanager
def non_blocking_lock(lock: Lock) -> Iterator[bool]:
    locked = lock.acquire(False)
    try:
        yield locked
    finally:
        if locked:
            lock.release()


class Future(cf.Future):
    def __init__(self,
                 function: Callable[..., Any],
                 executor: 'ScheduledThreadPoolExecutor',
                 ) -> None:
        self.function = function
        self._executor = executor  # type: ScheduledThreadPoolExecutor
        self._status = None  # type: Optional[_Status]
        self._result = None  # type: Any
        self._exception = None  # type: Optional[Exception]
        self._callbacks = []  # type: List[Callable[..., Any]]
        self._lock = Lock()

    def cancel(self) -> bool:
        # do not ask background thread to cancel; that would mean we have to wait for confirmation
        with non_blocking_lock(self._lock) as locked:
            if locked and self._status == _Status.WAITING:
                self._status = _Status.CANCELLED
                return True
            else:
                return False

    def cancelled(self) -> bool:
        return self._status == _Status.CANCELLED

    def running(self) -> bool:
        return self._status == _Status.RUNNING

    def done(self) -> bool:
        return self._status in (_Status.DONE, _Status.CANCELLED)

    def _ensure_done(self, timeout: Duration = None) -> None:
        if timeout != 0:
            # if we want to support this, we'll need to use Event, Condition or sth similar
            raise NotImplementedError
        if self._status == _Status.DONE:
            return
        else:
            if self._status == _Status.CANCELLED:
                raise cf.CancelledError
            else:
                raise cf.TimeoutError

    def result(self, timeout: Duration = None) -> Any:
        self._ensure_done(timeout)
        if self._exception:
            raise self._exception
        return self._result

    def exception(self, timeout: Duration = None) -> Optional[Exception]:  # type: ignore
        self._ensure_done(timeout)
        return self._exception

    def add_done_callback(self, fn: Callable[..., Any]) -> None:
        self._callbacks.append(fn)


class ScheduledFuture(Future):
    def __init__(self, function: Callable[..., Any], executor: 'ScheduledThreadPoolExecutor',
                 delay: Duration) -> None:
        super().__init__(function, executor)
        self.scheduled_time = time.perf_counter() + delay


class _WaitEvent:
    def __init__(self, tasks: List[Future]) -> None:
        self.event = Event()
        self.tasks = tasks


class _ScheduledWorker(Thread):
    def __init__(self, mq: 'Queue[Union[ScheduledFuture, _WaitEvent]]') -> None:
        self.parent = current_thread()
        self.mq = mq
        super().__init__(name='Scheduled Worker')
        self.wakeup_times = PriorityQueue[ScheduledFuture, Time]()
        self.wait_events = set()  # type: Set[_WaitEvent]

    def run(self) -> None:
        while self.parent.is_alive() or self.wakeup_times or not self.mq.empty():  # type: ignore
            todo = []

            # collect next message or wait until next timed task whichever happens first
            if self.wakeup_times:
                wakeup_time, _ = self.wakeup_times.peek()
                sleep_time = max(0, wakeup_time - time.perf_counter())  # type: Optional[Duration]
            else:
                # if main thread dies, this thread may stay alive forever waiting for a lock
                # this is a simple solution: wake up every few seconds to check on the main thread
                # a much more complex solution is to be notified of the death through a finalizer
                sleep_time = 3
            try:
                msg = self.mq.get(timeout=sleep_time)  # type: Optional[Union[Future, _WaitEvent]]
            except Empty:
                msg = None

            if isinstance(msg, ScheduledFuture):
                self.wakeup_times.add_task(msg, msg.scheduled_time)
            elif isinstance(msg, _WaitEvent):
                self.wait_events.add(msg)
            else:
                assert msg is None

            # we got CPU, let's check timed tasks regardless if we were woken up by msg or timer
            # especially since task received in msg might have 0 delay and is now up for execution

            # collect timed tasks
            while self.wakeup_times:
                wakeup_time, task = self.wakeup_times.peek()
                if time.perf_counter() + TIMER_RESOLUTION < wakeup_time:
                    break
                self.wakeup_times.pop_task()
                todo.append(task)

            # perform collected tasks
            for task in todo:
                # Important: assignment to ._result, ._exception must happen-before .status = DONE
                # Since nobody should read ._result, ._exception unless .status == DONE, this
                # ensures that the two values are already correct by the time anyone reads them
                # Is it worth enforcing this requirement (e.g., through setters/getters)?
                with non_blocking_lock(task._lock) as locked:
                    if locked and task._status != _Status.CANCELLED:
                        task._status = _Status.RUNNING
                        try:
                            task._result, task._exception = task.function(), None
                        except Exception as exc:
                            task._result, task._exception = None, exc
                        task._status = _Status.DONE

                for fn in task._callbacks:
                    try:
                        fn(task)
                    except Exception as exc:
                        print(exc, file=sys.stderr, flush=True)

            # check if any wait_events can be released
            for wait_event in self.wait_events:
                if all(task.done() for task in wait_event.tasks):
                    wait_event.event.set()


class ScheduledThreadPoolExecutor(cf.Executor):
    def __init__(self) -> None:
        self.mq = Queue()  # type: Queue[Union[ScheduledFuture, _WaitEvent]]
        self.thread = _ScheduledWorker(self.mq)
        self.thread.start()

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> ScheduledFuture:
        return self.schedule(fn, 0, *args, **kwargs)

    def schedule(self, fn: Callable[..., Any], delay: Duration,
                 *args: Any, **kwargs: Any) -> ScheduledFuture:
        future = ScheduledFuture(fn, self, delay)
        self.mq.put(future)
        return future

    # chunksize has no effect (included for compatibility with Executor)
    def map(self, func: Callable[..., _T], *iterables: Iterable[Any],
            timeout: Optional[float] = None, chunksize: int = 1) -> Iterator[_T]:
        raise NotImplementedError

    def shutdown(self, wait: bool = True) -> None:
        raise NotImplementedError


class CompletionType:
    pass


FIRST_COMPLETED = CompletionType()
FIRST_EXCEPTION = CompletionType()
ALL_COMPLETED = CompletionType()


# Executor API is insufficient to implement wait, so currently it relies on non-public API
# we need to define a more powerful subinterface of Executor and require that all futures
# in this module are tied to executors that implement that interface

# the only advantage of wait with ALL_COMPLETED over a simple loop over futures
# is that it avoids waking up / messaging between threads for each future separately
# however, with FIRST_EXCEPTION or FIRST_COMPLETED, wait adds new semantics
def wait(fs: Iterable[Future], timeout: Optional[Duration] = None,
         return_when: CompletionType = ALL_COMPLETED) -> None:
    if return_when != ALL_COMPLETED:
        # to support this, we need to pass that information to each background thread
        # and use Event in each of them to provide logical or semantics
        raise NotImplementedError
    if not fs:
        return
    executor = next(iter(fs))._executor
    if any(f._executor != executor for f in fs):
        # to support this, we need to talk to multiple executors
        # and use Event in each of them to provide logical or semantics
        raise NotImplementedError('can only wait on futures from the same executor')
    wait_event = _WaitEvent(list(fs))
    executor.mq.put(wait_event)
    while not wait_event.event.wait(timeout=1):
        if not executor.thread.is_alive():
            raise RuntimeError('Background thread died unexpectedly')


def as_completed(fs: Iterable[Future], timeout: Optional[Duration] = None) -> Iterator[Future]:
    raise NotImplementedError
