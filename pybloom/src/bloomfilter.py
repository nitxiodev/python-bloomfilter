import math
from collections import namedtuple

import psutil

from pybloom.src import BloomFilterException
from pybloom.src.backends.bitarraybackend import BitArrayBackend
from pybloom.src.backends.numpybackend import NumpyBackend
from pybloom.src.backends.redisbackend import RedisBackend
from pybloom import log

MAGNITUDES = {
    'TB': ((1024 ** 2) ** 2),
    'GB': (1024 ** 2) * 1024,
    'MB': 1024 ** 2,
    'KB': 1024,
    'B': 8
}

Size = namedtuple('Size', 'size unit')
Options = namedtuple('Options', 'optimal_size optimal_hash fpp')


def size_to_human_format(size, unit=None):
    """
    Transforms size in bits into most readable unit. For example:

    >>> size_to_human_format(2**32)
    >>> Size(size=4.0, unit='GB')

    It is also possible to get a size in a specific unit. For example:
    >>> size_to_human_format(1024, unit='KB')
    >>> Size(size=1.0, unit='KB')
    :param size: Size in bytes.
    :param unit: Optional. [B, KB, MB, GB, TB]
    """
    if unit is not None:
        return Size(float(size) / MAGNITUDES[unit], unit)

    for mag in MAGNITUDES.keys():
        _size = float(size) / MAGNITUDES[mag]
        if math.floor(_size) > 0:
            return Size(_size, mag)
    return Size(size, 'B')


class BloomFilter(object):
    def __new__(cls, max_number_of_element_expected: int, error_rate=.0005, backend='numpy', **kwargs):
        if max_number_of_element_expected < 0:
            raise BloomFilterException('Size of filter must be > 0, not {!r}'.format(max_number_of_element_expected))

        if error_rate < 0 or error_rate > 1:
            raise BloomFilterException('Error rate must be in range [0, 1]. {!r} found instead.'.format(error_rate))

        try:
            filter_metadata = cls.set_optimal_size_of_filter(max_number_of_element_expected, error_rate)
        except OverflowError:
            raise BloomFilterException('Number of expected elements is too big {}'.
                                       format(max_number_of_element_expected))

        if filter_metadata.optimal_hash == 0:
            raise BloomFilterException('Filter size is too small. '
                                       'Maybe the error_rate ({!r}) is too high. Try to reduce it.'.format(error_rate))

        human_readable_size = size_to_human_format(filter_metadata.optimal_size)
        log.info('BloomFilter using {!r} backend with size equal to {:.2f}{} and {!r} hashes with a false positive '
                 'probability of {!r}'.format(backend,
                                              human_readable_size.size,
                                              human_readable_size.unit,
                                              filter_metadata.optimal_hash,
                                              filter_metadata.fpp))

        if backend == 'numpy':
            if not cls.has_enough_memory(human_readable_size):
                raise BloomFilterException('The optimal filter size is {:.2f} {}, so numpy will raise MemoryError '
                                           'because your system has not enough memory.'
                                           ' Try using redis instead.'.format(human_readable_size.size,
                                                                              human_readable_size.unit))
            return NumpyBackend(filter_metadata.optimal_size, filter_metadata.optimal_hash, **kwargs)
        elif backend == 'redis':
            return RedisBackend(filter_metadata.optimal_size, filter_metadata.optimal_hash, **kwargs)
        elif backend == 'bitarray':
            if not cls.has_enough_memory(human_readable_size):
                raise BloomFilterException('The optimal filter size is {:.2f} {}, so bitarray will raise ValueError '
                                           'because the size is too big.'
                                           ' Try using redis instead.'.format(human_readable_size.size,
                                                                              human_readable_size.unit))
            return BitArrayBackend(filter_metadata.optimal_size, filter_metadata.optimal_hash, **kwargs)

        raise BloomFilterException('Backend {!r} not found.'.format(backend))

    @classmethod
    def has_enough_memory(cls, human_readable_size):
        available_memory_on_system = size_to_human_format(psutil.virtual_memory().available, human_readable_size.unit)
        return human_readable_size.size <= available_memory_on_system.size

    @classmethod
    def set_optimal_size_of_filter(cls, n, p):
        m = math.ceil(-1 * (n * math.log(p)) / (math.log(2) ** 2))
        k = round(math.log(2) * (m / n))
        p = (1 - math.e ** ((-k * (n + 0.5)) / (m - 1))) ** k

        return Options(optimal_size=m, optimal_hash=k, fpp=p)
