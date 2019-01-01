import numpy as np

from pybloom.src.backends import BaseBackend


class NumpyBackend(BaseBackend):
    def __init__(self, array_size: int, hash_size: int, **kwargs):
        self._array = None

        super(NumpyBackend, self).__init__(array_size, hash_size)

    def _add(self, other):
        self._array[self._filter_it(other)] = 1
        self._capacity += 1
        return self

    def reset(self):
        self._array = np.zeros(self._array_size, dtype=np.int8)

    def __contains__(self, item):
        return np.all(self._array[self._filter_it(item)])
