import time
import pytest  # type: ignore
from typing import Callable, Tuple, List, Type

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
            print(elapsed_time, duration, exc)
            raise exc()
        else:
            print(elapsed_time, duration, 42)
            return 42
    return f, call_times


def test_short_timeout() -> None:
    f, call_times = create_test_function(100, ZeroDivisionError)
    fut = retry.add(f, timeout=1, start_wait_time=0.1, exceptions=[ZeroDivisionError])
    wait([fut])
    assert call_times == pytest.approx([0.0, 0.1, 0.3, 0.7, 1.0], abs=0.05)
    assert type(fut.exception(timeout=0)) == ZeroDivisionError
    with pytest.raises(ZeroDivisionError):
        fut.result(timeout=0)


def test_long_timeout() -> None:
    f, call_times = create_test_function(0.5, ZeroDivisionError)
    fut = retry.add(f, timeout=2, start_wait_time=0.1, exceptions=[ZeroDivisionError])
    wait([fut])
    assert call_times == pytest.approx([0.0, 0.1, 0.3, 0.7], abs=0.05)
    assert fut.exception(timeout=0) is None
    assert fut.result(timeout=0) == 42
