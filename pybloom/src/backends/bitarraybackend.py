from bitarray import bitarray as Bitarray

from pybloom.src import BloomFilterException
from pybloom.src.backends import ThreadingBackend


class BitArrayBackend(ThreadingBackend):
    def __init__(self, array_size: int, hash_size: int, filter_size: int, **kwargs):
        self._array = Bitarray(array_size)

        super(BitArrayBackend, self).__init__(array_size, hash_size, filter_size)

    def _add(self, other):
        with self.lock:
            if self.full:
                raise BloomFilterException('Filter is full')

            if other not in self:
                for idx in self._filter_it(other):
                    self._array[idx] = 1
                self._capacity += 1
        return self

    def reset(self):
        with self.lock:
            self._array.setall(0)

    def __contains__(self, item):
        for idx in self._filter_it(item):
            if not self._array[idx]:
                return False
        return True
