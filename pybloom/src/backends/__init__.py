import mmh3
import math
from abc import ABCMeta, abstractmethod

import numpy as np

from pybloom.src import BloomFilterException


class BaseBackend(set):
    __metaclass__ = ABCMeta

    def __init__(self, array_size: int, optimal_hash: int):
        super(BaseBackend, self).__init__()
        self._array_size = array_size
        self._optimal_hash = optimal_hash
        self._capacity = 0
        self.reset()

    @property
    def full(self):
        return self._capacity >= self._array_size

    @property
    def false_positive_probability(self):
        return (1 - math.e ** ((-self._optimal_hash * (self._capacity + 0.5)) /
                               (self._array_size - 1))) ** self._optimal_hash

    @abstractmethod
    def _add(self, *args, **kwargs):
        raise NotImplementedError('Not implemented yet!')

    @abstractmethod
    def reset(self):
        raise NotImplementedError('Not implemented yet!')

    def add(self, *args, **kwargs):
        if self.full:
            raise BloomFilterException('Filter is full')
        return self._add(*args, **kwargs)

    def __add__(self, other):
        return self._add(other)

    def __iadd__(self, other):
        return self._add(other)

    def _filter_it(self, other):
        """
        Performs hashing operation for bloom filter.\n
        :param other: Value to filter.
        """
        if not isinstance(other, (bytes, str)):
            other = str(other)

        a = np.array([mmh3.hash(other, i, signed=False) % self._array_size for i in range(self._optimal_hash)])
        return a
