import json
import random
import time

import redis
from redis.exceptions import LockError
from redis.lock import LuaLock as lock

from pybloom.src import BloomFilterException
from pybloom.src.backends import SharedBackend


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

LUA_ADD_KEY = """
    local capacity = tonumber(redis.call('HGET', KEYS[1], 'capacity'))
    local filter_size = tonumber(redis.call('HGET', KEYS[1], 'filter_size'))
    local hash_size = tonumber(redis.call('HGET', KEYS[1], 'hash_size'))

    -- This means that filter has been reset
    if capacity == nil or filter_size == nil then
        return false
    end

    if capacity >= filter_size then
        return false
    end

    local sum = 0
    for i=1, #ARGV do
        local args = cjson.decode(ARGV[i])
        if redis.call('GETBIT', args['key'], args['offset']) == 1 then
            sum = sum + 1
        end

        redis.call('SETBIT', args['key'], args['offset'], 1)
    end

    -- Only if the element don't exists, increase the capacity (i.e. add it)
    if sum ~= hash_size then
        capacity = capacity + 1
        redis.call('HSET', KEYS[1], 'capacity', capacity)
    end

    return capacity
"""


class RedisBackend(SharedBackend):
    def __init__(self, array_size: int, hash_size: int, filter_size: int, redis_connection: str, connection_retries=3,
                 wait=None, prefix_key='bloom_filter'):
        self._max_redis_offset_size = 2 ** 32 - 1
        self._key = prefix_key
        self._metadata_key = '{}_metadata'.format(self._key)
        self._lock_key = 'bloom_filter_lock'
        self._lock_timeout = 10

        # Wrap connection in redis proxy
        self._redis = RedisProxy(redis_connection,
                                 retries=connection_retries,
                                 max_retry_wait=wait)

        self._lua_add = self._redis.register_script(LUA_ADD_KEY)
        array_size, hash_size, filter_size, capacity = self._retrieve_metadata(array_size, hash_size, filter_size)
        super(RedisBackend, self).__init__(array_size, hash_size, filter_size, capacity)

    def _retrieve_metadata(self, array_size, hash_size, filter_size):
        try:
            with lock(self._redis, self._lock_key, timeout=self._lock_timeout):
                metadata = dict(array_size=array_size, hash_size=hash_size, filter_size=filter_size, capacity=0)
                _redis_metadata = self._redis.hgetall(self._metadata_key)
                if not _redis_metadata:
                    self._redis.hmset(self._metadata_key, metadata)

                return map(lambda x: int(x), (_redis_metadata or metadata).values())
        except LockError:
            raise BloomFilterException(
                'Cannot retrieve metadata from redis. Seems another process has acquired the lock'
                ' and did not released. Check if {!r} key is in your redis server.'.
                format(self._lock_key)
            )

    def _build_key(self, offset):
        return '{}:{}'.format(self._key, offset)

    def _get_right_offset(self, value):
        name_to_key = int(value / self._max_redis_offset_size) + 1
        offset = ((name_to_key * 2 ** 32) - 1)
        return name_to_key, offset

    def _add(self, other):
        if self.full:
            raise BloomFilterException('Filter is full')

        metadata = []
        for idx in self._filter_it(other):
            _name_to_key, _offset = self._get_right_offset(idx)
            metadata.append(json.dumps(dict(key=self._build_key(_name_to_key), offset=int(_offset - 1 - idx))))

        _server_response = self._lua_add(keys=[self._metadata_key], args=metadata)
        if _server_response is None:
            raise BloomFilterException('{!r} value has not been added. '
                                       'This can be because another process already filled the filter or '
                                       'has been reset.'.format(other))

        self._capacity = _server_response or self._capacity
        return self

    def reset(self):
        with self._redis.as_pipeline() as pipe:
            cursor = '0'
            while cursor != 0:
                cursor, data = self._redis.scan(cursor=cursor, match='{}:*'.format(self._key), count=None)
                for item in data:
                    pipe.delete(item)

            pipe.hdel(self._metadata_key, 'array_size', 'hash_size', 'filter_size', 'capacity')
            response = pipe.execute()

        if response[-1] == 4:
            self._capacity = 0

    def __contains__(self, item):
        with self._redis.as_pipeline() as pipe:
            for idx in self._filter_it(item):
                _name_to_key, _offset = self._get_right_offset(idx)
                pipe.getbit(self._build_key(_name_to_key), _offset - 1 - idx)

            response = pipe.execute()
        return all(response)
