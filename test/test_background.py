import time
import pytest  # type: ignore
from typing import Callable, Tuple, List, Type
from unittest.mock import MagicMock, call
from functools import partial

from bgretry import retry, wait


def create_test_function(duration: float,
                         exc: Type[Exception]) -> Tuple[Callable[[], int], List[float]]:
    '''Return a callable that will raise a specified exception every time it is called
    within the specified duration; after that, it will complete normally returning 42.
    '''
    start_time = time.perf_counter()
    call_times = []

    def f() -> int:
        elapsed_time = time.perf_counter() - start_time
        nonlocal call_times
        call_times.append(elapsed_time)
        if elapsed_time < duration:
            raise exc()
        else:
            return 42
    return f, call_times


def test_timeout() -> None:
    f, call_times = create_test_function(100, ZeroDivisionError)
    fut = retry.add(f, timeout=1, start_wait_time=0.1, exceptions=[ZeroDivisionError])
    m = MagicMock()
    cb1 = partial(m, 1)
    cb2 = partial(m, 2)
    fut.add_done_callback(cb1)
    fut.add_done_callback(cb2)
    wait([fut])
    assert call_times == pytest.approx([0.0, 0.1, 0.3, 0.7, 1.0], abs=0.05)
    assert type(fut.exception(timeout=0)) == ZeroDivisionError
    with pytest.raises(ZeroDivisionError):
        fut.result(timeout=0)
    assert m.call_args_list == [call(1, fut), call(2, fut)]


def test_success() -> None:
    f, call_times = create_test_function(0.5, ZeroDivisionError)
    fut = retry.add(f, timeout=2, start_wait_time=0.1, exceptions=[ZeroDivisionError])
    m = MagicMock()
    cb1 = partial(m, 1)
    cb2 = partial(m, 2)
    fut.add_done_callback(cb1)
    fut.add_done_callback(cb2)
    wait([fut])
    assert call_times == pytest.approx([0.0, 0.1, 0.3, 0.7], abs=0.05)
    assert fut.exception(timeout=0) is None
    assert fut.result(timeout=0) == 42
    assert m.call_args_list == [call(1, fut), call(2, fut)]


def test_error() -> None:
    f, call_times = create_test_function(0.5, ZeroDivisionError)
    fut = retry.add(f, timeout=2, start_wait_time=0.1, exceptions=[IndexError])
    m = MagicMock()
    cb1 = partial(m, 1)
    cb2 = partial(m, 2)
    fut.add_done_callback(cb1)
    fut.add_done_callback(cb2)
    wait([fut])
    assert call_times == pytest.approx([0.0], abs=0.05)
    assert type(fut.exception(timeout=0)) == ZeroDivisionError
    with pytest.raises(ZeroDivisionError):
        fut.result(timeout=0)
    assert m.call_args_list == [call(1, fut), call(2, fut)]
