from queue import Queue, Empty
import sys
import os
import collections
import functools
import time
from threading import Thread, Event, current_thread
import enum
from concurrent.futures import Executor, ThreadPoolExecutor, CancelledError
from typing import (
    Any, List, NewType, DefaultDict, Set, Optional, Union, Callable,
    Dict, cast, Type, Deque, Iterable
)

from .pqueue import PriorityQueue

TIMER_RESOLUTION = 1e-4  # 100 us

# TODO: define type for time difference
Time = float
Duration = float


class Task:
    def __init__(self,
                 function: Callable[..., Any],
                 suggested_wait_time: Duration,
                 timeout_at: Time,
                 exceptions: List[Type[Exception]],
                 executor: 'RetryLoop',
                 name: str
                 ) -> None:
        self.function = function
        self.suggested_wait_time = suggested_wait_time
        self.timeout_at = timeout_at
        self.exceptions = exceptions
        self.executor = executor
        self.name = name
        self.status = None  # type: Optional[Status]
        self._result = None  # type: Any
        self._exception = None  # type: Optional[Exception]
        self.done_callbacks = collections.deque()  # type: Deque[Callable[..., Any]]

    def cancel(self) -> None:
        # message the retry thread to cancel this task
        # but how do we know it's succeeded?
        # wait for confirmation? that seems slow
        # and what if it's already done? retry loop will get confused as it will look for a task
        # not find it, and may think it's still being initialized
        # we can check if self.done() here, but then there's a race condition (the task may be
        # completed while our msg is on the way)

        # don't set Status.CANCELLED here, to avoid complex logic with changes made in bkgr thread

        # also, call self.done_callback either from here or from the background thread
        raise NotImplementedError

    def cancelled(self) -> bool:
        return self.status == Status.CANCELLED

    def running(self) -> bool:
        return self.status == Status.RUNNING

    def done(self) -> bool:
        return self.status in (Status.TIMEOUT, Status.EXCEPTION, Status.SUCCESS, Status.CANCELLED)

    def result(self, timeout: Duration = None) -> Any:
        if timeout != 0:
            raise NotImplementedError
        if self.status == Status.CANCELLED:
            raise CancelledError
        if self._exception:
            raise self._exception
        return self._result

    def exception(self, timeout: Duration = None) -> Optional[Exception]:
        if timeout != 0:
            raise NotImplementedError
        if self.status == Status.CANCELLED:
            raise CancelledError
        return self._exception

    def add_done_callback(self, fn: Callable[..., Any]) -> None:
        self.done_callbacks.append(fn)


# status field can also be None, which is roughly the same as RETRYING
class Status(enum.Enum):
    TIMEOUT = object()
    EXCEPTION = object()
    RETRYING = object()
    SUCCESS = object()
    RUNNING = object
    CANCELLED = object()


class WaitEvent:
    def __init__(self, tasks: List[Task]) -> None:
        self.event = Event()
        self.tasks = tasks


class ScheduledWorker(Thread):
    def __init__(self, mq: 'Queue[Union[Task, WaitEvent]]') -> None:
        self.parent = current_thread()
        self.mq = mq
        super().__init__(name='Scheduled Worker')
        self.wakeup_times = PriorityQueue[Task, Time]()
        self.wait_events = set()  # type: Set[WaitEvent]

    def run(self) -> None:
        while self.parent.is_alive() or self.wakeup_times or not self.mq.empty():  # type: ignore
            todo = []

            # collect next message or wait until next timed task whichever happens first
            if self.wakeup_times:
                wakeup_time, _ = self.wakeup_times.peek()
                sleep_time = max(0, wakeup_time - time.perf_counter())  # type: Optional[Duration]
            else:
                # prevent thread from staying alive forever after main thread dies
                sleep_time = 3
            try:
                msg = self.mq.get(timeout=sleep_time)  # type: Optional[Union[Task, WaitEvent]]
            except Empty:
                msg = None

            if isinstance(msg, Task):
                todo.append(msg)
            elif isinstance(msg, WaitEvent):
                self.wait_events.add(msg)
            else:
                assert msg is None

            # we got CPU, let's check timed tasks regardless if we were woken up by msg or timer

            # collect timed tasks
            while self.wakeup_times:
                wakeup_time, task = self.wakeup_times.peek()
                if time.perf_counter() + TIMER_RESOLUTION < wakeup_time:
                    break
                self.wakeup_times.pop_task()
                todo.append(task)

            # perform collected tasks
            for task in todo:
                # assigning ._result/._exception is thread-safe because nobody will look at them
                # until .status is set to a non-None and non-RUNNING value
                # when .status is set (an atomic operation), ._result/._exception already correct
                # TODO: rewrite this in a safer way (e.g., assign the entire triple at once)
                task._exception = None
                task.status = Status.RUNNING
                try:
                    task._result = task.function()
                except Exception as exc:
                    task._exception = exc

                if type(task._exception) in task.exceptions:
                    # expected exception
                    current_time = time.perf_counter()
                    # adjust for timer precision to avoid unintended double retry at the end
                    if task.timeout_at < current_time + TIMER_RESOLUTION:
                        # timed out, no more retries
                        task.status = Status.TIMEOUT
                    else:
                        # retry later
                        assert task.suggested_wait_time is not None, "Timer error"
                        # wait suggested time or until timeout, whichever comes first
                        wakeup_time = min(current_time + task.suggested_wait_time, task.timeout_at)
                        # add task back to the priority queue
                        task.suggested_wait_time = 2 * task.suggested_wait_time
                        self.wakeup_times.add_task(task, wakeup_time)
                        task.status = Status.RETRYING
                elif task._exception:
                    task.status = Status.EXCEPTION
                else:
                    task.status = Status.SUCCESS
                if task.status != Status.RETRYING:
                    for fn in task.done_callbacks:
                        try:
                            fn()
                        except Exception as exc:
                            print(exc, file=sys.stderr, flush=True)

            # check if any wait_events can be released
            for wait_event in self.wait_events:
                if all(task.done() for task in wait_event.tasks):
                    wait_event.event.set()


class RetryLoop:
    def __init__(self, default_timeout: Duration, default_start_wait_time: Duration) -> None:
        self.default_timeout = default_timeout
        if default_start_wait_time <= 0:
            raise ValueError('default start wait time must be positive')
        self.default_start_wait_time = default_start_wait_time
        self.mq = Queue()  # type: Queue[Union[Task, WaitEvent]]
        self.thread = ScheduledWorker(self.mq)
        self.thread.start()

    def add(self, function: Callable[[], Any], *, timeout: Duration,
            start_wait_time: Time = 0.001,
            exceptions: List[Type[Exception]] = [Exception],
            name: str='',
            ) -> Task:
        task = Task(
            function=function,
            timeout_at=time.perf_counter() + timeout,
            suggested_wait_time=start_wait_time,
            exceptions=exceptions,
            executor=self,
            name=name,
        )
        self.mq.put(task)
        return task

    def forward(self, function: Callable[..., None]) -> Callable[..., Task]:
        def parse_and_add(*args: Any, **kwargs: Any) -> Task:
            timeout = kwargs.pop('timeout', self.default_timeout)
            start_wait_time = kwargs.pop('start_wait_time', self.default_start_wait_time)
            frozen_function = cast(Callable[[], None],
                                   functools.partial(function, *args, **kwargs))
            name = '{}: {} {}'.format(function.__name__,
                                      ', '.join(args) if args else '',
                                      kwargs if kwargs else '')
            return self.add(frozen_function,
                            timeout=timeout,
                            start_wait_time=start_wait_time,
                            exceptions=[PermissionError],
                            name=name
                            )
        return parse_and_add


def wait(fs: Iterable[Task], timeout: Optional[Duration] = None) -> None:
    if not fs:
        return
    executor = next(iter(fs)).executor
    if any(f.executor != executor for f in fs):
        raise NotImplementedError('can only wait on futures from the same background thread')
    wait_event = WaitEvent(list(fs))
    executor.mq.put(wait_event)
    while not wait_event.event.wait(timeout=1):
        if not executor.thread.is_alive():
            raise RuntimeError('Background thread died unexpectedly')


retry = RetryLoop(default_timeout=1, default_start_wait_time=0.001)


if sys.platform.startswith('win'):
    # None of these functions have return values
    replace = retry.forward(os.replace)
    remove = retry.forward(os.remove)
    unlink = retry.forward(os.unlink)  # alias for os.remove
    rename = retry.forward(os.rename)
else:
    # TODO: fix so it works on Linux (wrong arguments)
    replace = os.replace
    remove = os.remove
    unlink = os.unlink
    rename = os.rename
