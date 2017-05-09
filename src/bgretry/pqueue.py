import heapq
import itertools
from typing import Generic, Tuple, TypeVar

T = TypeVar('T')
P = TypeVar('P')

# Ripped out from python heapq module docs, and
# simplified by removing some features we don't need.
class PriorityQueue(Generic[T, P]):
    def __init__(self) -> None:
        self.pq = []  # type: List[Tuple[P, int, T]]
        self.counter = itertools.count()  # unique sequence count

    def add_task(self, task: T, priority: P) -> None:
        'Add new task.'
        count = next(self.counter)
        entry = (priority, count, task)
        heapq.heappush(self.pq, entry)

    def pop_task(self) -> Tuple[P, T]:
        'Remove and return the lowest priority task. Raise IndexError if empty.'
        priority, count, task = heapq.heappop(self.pq)
        return priority, task

    def peek(self) -> Tuple[P, T]:
        priority, count, task = self.pq[0]
        return priority, task

    def __len__(self) -> int:
        return len(self.pq)

