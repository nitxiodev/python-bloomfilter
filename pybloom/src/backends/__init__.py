import math
import mmh3
import threading
from abc import ABCMeta, abstractmethod

import numpy as np


class BaseBackend(set):
    __metaclass__ = ABCMeta

    def __init__(self, array_bits_size: int, optimal_hash: int, filter_size: int, capacity=0):
        super(BaseBackend, self).__init__()
        self._array_size = array_bits_size  # number of bits of filter
        self._filter_size = filter_size  # capacity of filter (less than bit size)
        self._optimal_hash = optimal_hash
        self._capacity = capacity

    @property
    def full(self):
        return self._capacity >= self._filter_size

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
        return self._add(*args, **kwargs)

    def __add__(self, other):
        return self.add(other)

    def __iadd__(self, other):
        return self.add(other)

    def __len__(self):
        return self._capacity

    def _filter_it(self, other):
        """
        Performs hashing operation for bloom filter.\n
        :param other: Value to filter.
        """
        if not isinstance(other, (bytes, str)):
            other = str(other)

        a = np.array([mmh3.hash(other, i, signed=False) % self._array_size for i in range(self._optimal_hash)])
        return a


class SharedBackend(BaseBackend):
    """
    Backend intended for shared bloomfilters across 'n' machines in network. The only difference with BaseBackend is
    that it is not necessary to initialize the filter at startup.
    """

    def __init__(self, *args, **kwargs):
        super(SharedBackend, self).__init__(*args, **kwargs)


class ThreadingBackend(BaseBackend):
    """
    Backend intended for local bloomfilters using threads. One instance of this backend can be safely shared across
    threads.
    """

    def __init__(self, *args, **kwargs):
        super(ThreadingBackend, self).__init__(*args, **kwargs)

        self._lock = threading.RLock()
        self.reset()  # init backend

    @property
    def lock(self):
        return self._lock
