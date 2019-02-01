import numpy as np

from pybloom.src import BloomFilterException
from pybloom.src.backends import ThreadingBackend


class NumpyBackend(ThreadingBackend):
    def __init__(self, array_size: int, hash_size: int, filter_size: int, **kwargs):
        self._array = None

        super(NumpyBackend, self).__init__(array_size, hash_size, filter_size)

    def _add(self, other):
        with self.lock:
            if self.full:
                raise BloomFilterException('Filter is full')

            if other not in self:
                self._array[self._filter_it(other)] = 1
                self._capacity += 1

        return self

    def reset(self):
        with self.lock:
            self._array = np.zeros(self._array_size, dtype=np.int8)

    def __contains__(self, item):
        return np.all(self._array[self._filter_it(item)])
