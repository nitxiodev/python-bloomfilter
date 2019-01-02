import unittest

import redis
from hamcrest import assert_that, equal_to, raises, is_, instance_of, empty, is_not, greater_than
from mock import mock
from mockredis import mock_strict_redis_client
from redis import StrictRedis

from pybloom.src.backends.bitarraybackend import BitArrayBackend
from pybloom.src.backends.numpybackend import NumpyBackend
from pybloom.src.backends.redisbackend import RedisBackend, RedisProxy
from pybloom.src.bloomfilter import BloomFilter, BloomFilterException, Options, Size, size_to_human_format


class MockRedisProxy(object):
    def __init__(self, *args, **kwargs):
        self._connection = mock.patch('pybloom.src.backends.redisbackend.redis.StrictRedis',
                                      new_callable=mock_strict_redis_client).start()

    def as_pipeline(self):
        return self._connection.pipeline()

    def __getattr__(self, item):
        return getattr(self._connection, item)

    def __enter__(self):
        self._connection = self._connection.pipeline()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class testRedisProxy(unittest.TestCase):
    def setUp(self):
        self._proxy = RedisProxy('')
        self._proxy._connection = mock.patch('pybloom.src.backends.redisbackend.redis.StrictRedis',
                                             new_callable=mock_strict_redis_client).start()
        self._proxy._connection.reset = self._proxy._connection.pipeline()._reset  # bad signature in mockredis

    @mock.patch('pybloom.src.backends.redisbackend.redis.StrictRedis', spec=StrictRedis)
    def testRetryConnectionError(self, rediss):
        rediss.ping.side_effect = redis.exceptions.ConnectionError

        r = RedisProxy('', retries=1, max_retry_wait=1)
        r._connection = rediss

        assert_that(r.ping, raises(redis.exceptions.ConnectionError))
        assert_that(rediss.ping.call_count, equal_to(1))

    @mock.patch('pybloom.src.backends.redisbackend.redis.StrictRedis', spec=StrictRedis)
    def testRetryTimeoutError(self, rediss):
        rediss.ping.side_effect = redis.exceptions.TimeoutError

        r = RedisProxy('', retries=1, max_retry_wait=1)
        r._connection = rediss

        assert_that(r.ping, raises(redis.exceptions.TimeoutError))
        assert_that(rediss.ping.call_count, equal_to(1))

    def testNoRetry(self):
        self._proxy.set('key', 'hello')
        assert_that(self._proxy.ping(), is_(b'PONG'))
        assert_that(self._proxy.get('key'), equal_to(b'hello'))

    def testPipeline(self):
        with self._proxy.as_pipeline() as pipe:
            pipe.set('pipeline', 'pipe')
            pipe.execute()

        assert_that(self._proxy.get('pipeline'), equal_to(b'pipe'))


class testRedisBackend(unittest.TestCase):
    def setUp(self):
        with mock.patch('pybloom.src.backends.redisbackend.RedisProxy', new=MockRedisProxy):
            self._backend = RedisBackend(array_size=10, hash_size=3, redis_connection='')
        #         # self._backend._redis = MockRedisProxy()

    def testRightOffset(self):
        # First, a simple offset (first 2^32)
        k, offset = self._backend._get_right_offset(1)
        assert_that(k, equal_to(1))
        assert_that(offset, equal_to(2 ** 32 - 1))

        # First, a complex offset [2^32, 2^33, 2^34,...]
        k, offset = self._backend._get_right_offset(2 ** 32 + 1)
        assert_that(k, equal_to(2))  # 2 offsets in total (1: [0, 2^32-1], 2: [2^32, 2^33 - 1])
        assert_that(offset, equal_to(2 ** 33 - 1))

    def testResetWithNoData(self):
        self._backend.reset()
        assert_that(list(self._backend._redis.scan_iter('bloomfilter:*')), is_(empty()))

    def testResetWithData(self):
        self._backend.add(4)
        assert_that(list(self._backend._redis.scan_iter('bloomfilter:*')), is_not(empty()))

        self._backend.reset()
        assert_that(list(self._backend._redis.scan_iter('bloomfilter:*')), is_(empty()))

    def testAddandCheck(self):
        self._backend.add('house')
        assert_that('house' in self._backend, is_(True))

        self._backend += 'horse'
        assert_that('horse' in self._backend, is_(True))

class testNumpyBackend(unittest.TestCase):
    def setUp(self):
        self._backend = NumpyBackend(array_size=10, hash_size=3)

    def testResetWithNoData(self):
        self._backend.reset()
        assert_that(not self._backend._array.any(), is_(True))

    def testResetWithData(self):
        self._backend.add(4)
        assert_that(self._backend._array.any(), is_(True))

        self._backend.reset()
        assert_that(not self._backend._array.any(), is_(True))

    def testAddandCheck(self):
        self._backend.add('house')
        assert_that('house' in self._backend, is_(True))

        self._backend += 'horse'
        assert_that('horse' in self._backend, is_(True))

        assert_that(self._backend._capacity, equal_to(2))


class testBitArrayBackend(unittest.TestCase):
    def setUp(self):
        self._backend = BitArrayBackend(array_size=10, hash_size=3)

    def testResetWithNoData(self):
        self._backend.reset()
        assert_that(self._backend._array.count(1), equal_to(0))

    def testResetWithData(self):
        self._backend.add(4)
        assert_that(self._backend._array.count(1), greater_than(0))

        self._backend.reset()
        assert_that(self._backend._array.count(1), equal_to(0))

    def testAddandCheck(self):
        self._backend.add('house')
        assert_that('house' in self._backend, is_(True))

        self._backend += 'horse'
        assert_that('horse' in self._backend, is_(True))

        assert_that(self._backend._capacity, equal_to(2))


class testBloomFilter(unittest.TestCase):
    def testBadNumberofElements(self):
        with self.assertRaises(BloomFilterException) as cm:
            BloomFilter(-1)

        assert_that(str(cm.exception), equal_to('Size of filter must be > 0, not -1'))

    def testBadErrorRate(self):
        with self.assertRaises(BloomFilterException) as cm:
            BloomFilter(11, error_rate=2)

        assert_that(str(cm.exception), equal_to('Error rate must be in range [0, 1]. 2 found instead.'))

    def testBadOptimalHash(self):
        with self.assertRaises(BloomFilterException) as cm:
            with mock.patch('pybloom.src.bloomfilter.BloomFilter.set_optimal_size_of_filter',
                            return_value=Options(optimal_size=11, optimal_hash=0, fpp=1)):
                BloomFilter(11)

        assert_that(str(cm.exception), equal_to('Filter size is too small. Maybe the error_rate (0.0005) is too high. '
                                                'Try to reduce it.'))

    def testTooMuchMemoryforNumpyBackend(self):
        with self.assertRaises(BloomFilterException) as cm:
            with mock.patch('pybloom.src.bloomfilter.BloomFilter.has_enough_memory',
                            return_value=False):
                with mock.patch('pybloom.src.bloomfilter.size_to_human_format',
                                return_value=Size(size=10, unit='B')):
                    BloomFilter(11)

        assert_that(str(cm.exception), equal_to('The optimal filter size is 10.00 B, so numpy will raise '
                                                'MemoryError because your system has not enough memory. '
                                                'Try using redis instead.'))

    def testTooMuchMemoryforBitArrayBackend(self):
        with self.assertRaises(BloomFilterException) as cm:
            with mock.patch('pybloom.src.bloomfilter.BloomFilter.has_enough_memory',
                            return_value=False):
                with mock.patch('pybloom.src.bloomfilter.size_to_human_format',
                                return_value=Size(size=10, unit='B')):
                    BloomFilter(11, backend='bitarray')

        assert_that(str(cm.exception), equal_to('The optimal filter size is 10.00 B, so bitarray will raise'
                                                ' ValueError because the size is too big. Try using redis instead.'))

    def testOverflowError(self):
        with self.assertRaises(BloomFilterException) as cm:
            with mock.patch('pybloom.src.bloomfilter.BloomFilter.set_optimal_size_of_filter',
                            side_effect=OverflowError):
                BloomFilter(11)

        assert_that(str(cm.exception), equal_to('Number of expected elements is too big 11'))

    def testRightBackend(self):
        filter = BloomFilter(100, backend='numpy')
        assert_that(filter, instance_of(NumpyBackend))

        with mock.patch('pybloom.src.backends.redisbackend.RedisProxy', new=MockRedisProxy):
            filter = BloomFilter(100, backend='redis', redis_connection='')
            assert_that(filter, instance_of(RedisBackend))

        filter = BloomFilter(100, backend='bitarray')
        assert_that(filter, instance_of(BitArrayBackend))

    def testBackendNotFound(self):
        with self.assertRaises(BloomFilterException) as cm:
            BloomFilter(100, backend='notfoundbackend')

        assert_that(str(cm.exception), equal_to("Backend 'notfoundbackend' not found."))

    def testSizeUnits(self):
        assert_that(size_to_human_format(1024), equal_to(Size(size=1.0, unit='KB')))
        assert_that(size_to_human_format(2 ** 32, unit='GB'), equal_to(Size(size=4.0, unit='GB')))
