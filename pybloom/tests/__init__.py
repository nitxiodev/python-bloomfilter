import unittest

import redis
from fakeredis import FakeStrictRedis
from hamcrest import assert_that, equal_to, raises, is_, instance_of, greater_than, empty, is_not
from mock import mock
from redis import StrictRedis
from redis.exceptions import LockError
from redis.lock import LuaLock

from pybloom.src.backends.bitarraybackend import BitArrayBackend
from pybloom.src.backends.numpybackend import NumpyBackend
from pybloom.src.backends.redisbackend import RedisBackend, RedisProxy
from pybloom.src.bloomfilter import BloomFilter, BloomFilterException, Options, Size, size_to_human_format

LUA_ADD_SCRIPT = """

    -- https://gist.github.com/tylerneylon/59f4bcf316be525b30ab
    local json = {}

    local function kind_of(obj)
      if type(obj) ~= 'table' then return type(obj) end
      local i = 1
      for _ in pairs(obj) do
        if obj[i] ~= nil then i = i + 1 else return 'table' end
      end
      if i == 1 then return 'table' else return 'array' end
    end

    local function escape_str(s)
      local in_char  =  { '\\\\', '\\"', '/', '\b', '\f', '\\\n', '\\\r', '\t' }
      local out_char = {'\\\\', '\\"', '/',  'b',  'f',  'n',  'r',  't'}

      for i, c in ipairs(in_char) do
        s = s:gsub(c, '\\\\' .. out_char[i])
      end
      return s
    end


    local function skip_delim(str, pos, delim, err_if_missing)
      pos = pos + #str:match('^%s*', pos)
      if str:sub(pos, pos) ~= delim then
        if err_if_missing then
          error('Expected ' .. delim .. ' near position ' .. pos)
        end
        return pos, false
      end
      return pos + 1, true
    end

    local function parse_str_val(str, pos, val)
      val = val or ''
      local early_end_error = 'End of input found while parsing string.'
      if pos > #str then error(early_end_error) end
      local c = str:sub(pos, pos)
      if c == '\\"'  then return val, pos + 1 end
      if c ~= '\\\\' then return parse_str_val(str, pos + 1, val .. c) end
      -- We must have a \ character.
      local esc_map = {b = '\b', f = '\f', n = '\\\n', r = '\\\r', t = '\t'}
      local nextc = str:sub(pos + 1, pos + 1)
      if not nextc then error(early_end_error) end
      return parse_str_val(str, pos + 2, val .. (esc_map[nextc] or nextc))
    end

    local function parse_num_val(str, pos)
      local num_str = str:match('^-?%d+%.?%d*[eE]?[+-]?%d*', pos)
      local val = tonumber(num_str)
      if not val then error('Error parsing number at position ' .. pos .. '.') end
      return val, pos + #num_str
    end

    function json.stringify(obj, as_key)
      local s = {}  -- We'll build the string as an array of strings to be concatenated.
      local kind = kind_of(obj)  -- This is 'array' if it's an array or type(obj) otherwise.
      if kind == 'array' then
        if as_key then error('Can\\\'t encode array as key.') end
        s[#s + 1] = '['
        for i, val in ipairs(obj) do
          if i > 1 then s[#s + 1] = ', ' end
          s[#s + 1] = json.stringify(val)
        end
        s[#s + 1] = ']'
      elseif kind == 'table' then
        if as_key then error('Can\\\'t encode table as key.') end
        s[#s + 1] = '{'
        for k, v in pairs(obj) do
          if #s > 1 then s[#s + 1] = ', ' end
          s[#s + 1] = json.stringify(k, true)
          s[#s + 1] = ':'
          s[#s + 1] = json.stringify(v)
        end
        s[#s + 1] = '}'
      elseif kind == 'string' then
        return '"' .. escape_str(obj) .. '"'
      elseif kind == 'number' then
        if as_key then return '"' .. tostring(obj) .. '"' end
        return tostring(obj)
      elseif kind == 'boolean' then
        return tostring(obj)
      elseif kind == 'nil' then
        return 'null'
      else
        error('Unjsonifiable type: ' .. kind .. '.')
      end
      return table.concat(s)
    end

    json.null = {}

    function json.parse(str, pos, end_delim)
      pos = pos or 1
      if pos > #str then error('Reached unexpected end of input.') end
      local pos = pos + #str:match('^%s*', pos)  -- Skip whitespace.
      local first = str:sub(pos, pos)
      if first == '{' then  -- Parse an object.
        local obj, key, delim_found = {}, true, true
        pos = pos + 1
        while true do
          key, pos = json.parse(str, pos, '}')
          if key == nil then return obj, pos end
          if not delim_found then error('Comma missing between object items.') end
          pos = skip_delim(str, pos, ':', true)  -- true -> error if missing.
          obj[key], pos = json.parse(str, pos)
          pos, delim_found = skip_delim(str, pos, ',')
        end
      elseif first == '[' then  -- Parse an array.
        local arr, val, delim_found = {}, true, true
        pos = pos + 1
        while true do
          val, pos = json.parse(str, pos, ']')
          if val == nil then return arr, pos end
          if not delim_found then error('Comma missing between array items.') end
          arr[#arr + 1] = val
          pos, delim_found = skip_delim(str, pos, ',')
        end
      elseif first == '"' then  -- Parse a string.
        return parse_str_val(str, pos + 1)
      elseif first == '-' or first:match('%d') then  -- Parse a number.
        return parse_num_val(str, pos)
      elseif first == end_delim then  -- End of an object or array.
        return nil, pos + 1
      else  -- Parse true, false, or null.
        local literals = {['true'] = true, ['false'] = false, ['null'] = json.null}
        for lit_str, lit_val in pairs(literals) do
          local lit_end = pos + #lit_str - 1
          if str:sub(pos, lit_end) == lit_str then return lit_val, lit_end + 1 end
        end
        local pos_info_str = 'position ' .. pos .. ': ' .. str:sub(pos, pos + 10)
        error('Invalid json syntax starting at ' .. pos_info_str)
      end
    end

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
        local args = json.parse(ARGV[i])
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


class MockRedisProxy(object):
    def __init__(self, *args, **kwargs):
        self._connection = mock.patch('pybloom.src.backends.redisbackend.redis.StrictRedis',
                                      new_callable=FakeStrictRedis).start()

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
                                             new_callable=FakeStrictRedis).start()
        self._proxy._connection.reset = self._proxy._connection.pipeline().reset  # bad signature in mockredis

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
        assert_that(self._proxy.ping(), is_(True))
        assert_that(self._proxy.get('key'), equal_to(b'hello'))

    def testPipeline(self):
        with self._proxy.as_pipeline() as pipe:
            pipe.set('pipeline', 'pipe')
            pipe.execute()

        assert_that(self._proxy.get('pipeline'), equal_to(b'pipe'))


class testRedisBackend(unittest.TestCase):
    def setUp(self):
        with mock.patch('pybloom.src.backends.redisbackend.RedisProxy', new=MockRedisProxy):
            self._backend = RedisBackend(array_size=10, hash_size=3, redis_connection='', filter_size=5)
            self._backend._lua_add = self._backend._redis.register_script(LUA_ADD_SCRIPT)
            # self._backend._lua_add = self._backend._redis.register_script(LUA_ADD_KEY)

    def testRightOffset(self):
        # First, a simple offset (first 2^32)
        k, offset = self._backend._get_right_offset(1)
        assert_that(k, equal_to(1))
        assert_that(offset, equal_to(2 ** 32 - 1))

        # First, a complex offset [2^32, 2^33, 2^34,...]
        k, offset = self._backend._get_right_offset(2 ** 32 + 1)
        assert_that(k, equal_to(2))  # 2 offsets in total (1: [0, 2^32-1], 2: [2^32, 2^33 - 1])
        assert_that(offset, equal_to(2 ** 33 - 1))

    @mock.patch('pybloom.src.backends.redisbackend.lock', spec=LuaLock)
    def testMetadataError(self, mock_lock):
        mock_lock.side_effect = LockError
        with mock.patch('pybloom.src.backends.redisbackend.RedisProxy', new=MockRedisProxy):
            with self.assertRaises(BloomFilterException) as cm:
                self._backend = RedisBackend(array_size=10, hash_size=3, redis_connection='', filter_size=5)

            assert_that(str(cm.exception), equal_to("Cannot retrieve metadata from redis. Seems another process has "
                                                    "acquired the lock and did not released. Check if "
                                                    "'bloom_filter_lock' key is in your redis server."))

    # @mock.patch('pybloom.src.backends.redisbackend.lock', spec=LuaLock)
    def testMetadataOk(self):
        # Check we dont have any metadata yet
        response = {key.decode(): val.decode() for key, val in
                    self._backend._redis.hgetall(self._backend._metadata_key).items()}
        assert_that(response, equal_to(dict(array_size='10', hash_size='3', filter_size='5', capacity='0')))

        # Add data
        self._backend.add(4)

        # Check metadata again
        response = {key.decode(): val.decode() for key, val in
                    self._backend._redis.hgetall(self._backend._metadata_key).items()}
        assert_that(response, equal_to(dict(array_size='10', hash_size='3', filter_size='5', capacity='1')))

    def testResetWithData(self):
        self._backend.add(45)
        assert_that(list(self._backend._redis.scan_iter('bloom_filter:*')), is_not(empty()))

        self._backend.reset()
        assert_that(list(self._backend._redis.scan_iter('bloom_filter:*')), is_(empty()))
        assert_that(len(self._backend), is_(0))
        response = self._backend._redis.hgetall(self._backend._metadata_key).items()
        assert_that(response, is_(empty()))

    def testAddandCheck(self):
        self._backend.add('house')
        assert_that('house' in self._backend, is_(True))

        self._backend += 'horse'
        assert_that('horse' in self._backend, is_(True))


class testNumpyBackend(unittest.TestCase):
    def setUp(self):
        self._backend = NumpyBackend(array_size=10, hash_size=3, filter_size=5)

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

        assert_that(len(self._backend), equal_to(2))


class testBitArrayBackend(unittest.TestCase):
    def setUp(self):
        self._backend = BitArrayBackend(array_size=10, hash_size=3, filter_size=5)

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

        assert_that(len(self._backend), equal_to(2))


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
