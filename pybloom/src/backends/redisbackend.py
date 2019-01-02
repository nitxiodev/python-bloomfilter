import random
import time

import numpy as np
import redis

from pybloom.src.backends import BaseBackend


def retry(retries, exceptions, max_retry_wait=30):
    skip = retries == 0
    retries = 1 if retries < 1 else retries

    def inner_function(function):
        def wrapper(*args, **kwargs):
            _exception_message = None
            for _retry in range(retries):
                try:
                    return function(*args, **kwargs)
                except exceptions as e:
                    # print("EXC ", e)
                    _exception_message = e
                    retry_time = min(max_retry_wait, 2 ** (_retry + 1) + (random.randint(0, 1000) / 1000.0))
                    if not skip:
                        time.sleep(retry_time)
            raise _exception_message

        return wrapper

    return inner_function


class BaseProxy(object):
    MAX_RETRY_WAIT = 30  # seconds

    def __init__(self, retries=3, max_retry_wait=None):
        self._retries = retries
        self.MAX_RETRY_WAIT = max_retry_wait or self.MAX_RETRY_WAIT

    def as_pipeline(self):
        return RedisPipelineProxy(self._connection)

    def __getattr__(self, item):
        method = getattr(self._connection, item)

        @retry(self._retries, (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError), self.MAX_RETRY_WAIT)
        def exec_command(*args, **kwargs):
            return method(*args, **kwargs)

        return exec_command


class RedisPipelineProxy(BaseProxy):
    def __init__(self, redis_connection: redis.StrictRedis, retries=3, max_retry_wait=None):
        self._connection = redis_connection.pipeline()
        self._reset = self._connection.reset
        self._connection.reset = lambda: None  # Monkey patch reset

        super(RedisPipelineProxy, self).__init__(retries, max_retry_wait)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reset()


class RedisProxy(BaseProxy):
    def __init__(self, redis_connection: str, retries=3, max_retry_wait=None):
        self._connection = redis.StrictRedis.from_url(redis_connection, decode_responses=True)
        super(RedisProxy, self).__init__(retries, max_retry_wait)


class RedisBackend(BaseBackend):
    def __init__(self, array_size: int, hash_size: int, redis_connection: str, connection_retries=3,
                 wait=None, prefix_key='bloomfilter'):
        self._optimal_hash = hash_size
        self._array_size = array_size
        self._max_redis_offset_size = 2 ** 32 - 1
        self._key = prefix_key

        # Wrap connection in redis proxy
        self._redis = RedisProxy(redis_connection,
                                 retries=connection_retries,
                                 max_retry_wait=wait)

        super(RedisBackend, self).__init__(array_size, hash_size)

    def _build_key(self, offset):
        return '{}:{}'.format(self._key, offset)

    def _get_right_offset(self, value):
        name_to_key = int(value / self._max_redis_offset_size) + 1
        offset = ((name_to_key * 2 ** 32) - 1)
        return name_to_key, offset

    def _add(self, other):
        with self._redis.as_pipeline() as pipe:
            for idx in self._filter_it(other):
                _name_to_key, _offset = self._get_right_offset(idx)
                pipe.setbit(self._build_key(_name_to_key), _offset - 1 - idx, 1)

            pipe.execute()
            self._capacity += 1
        return self

    def reset(self):
        with self._redis.as_pipeline() as pipe:
            cursor = '0'
            while cursor != 0:
                cursor, data = self._redis.scan(cursor=cursor, match='{}:*'.format(self._key), count=None)
                for item in data:
                    pipe.delete(item)
            pipe.execute()

    def __contains__(self, item):
        with self._redis.as_pipeline() as pipe:
            for idx in self._filter_it(item):
                _name_to_key, _offset = self._get_right_offset(idx)
                pipe.getbit(self._build_key(_name_to_key), _offset - 1 - idx)

            _array = pipe.execute()
        return np.all(np.array(_array))
