# python-bloomfilter
Scalable bloom filter using different backends written in Python. Current version **only** works with Python 3.

# Installation
```bash
pip install BloomFilterPy
```
# Backends

Currently, BloomFilterPy has the following backends available: `numpy`, `bitarray` and `redis`. The first two are recommended when the expected number of elements in the filter fit in memory. Redis backend is the preferred when:

- Expect huge amount of data in the filter that it doesn't fit in memory.
- You want a distributed filter available (i.e. more than one machine). Thanks to lua scripts, now is possible to take advantage of redis atomic operations in the server side and share the same filter across multiple machines. 

# Usage & API

BloomFilterPy implements a common API regardless of the backend used. Every backend extends `BaseBackend` class that implements the common API. In turn, this base class extends the default `set` class of Python, but just `add` and `len` operations are properly handled.

## `BloomFilter` class

- `max_number_of_element_expected`: Size of filter. Number of elements it will contain.
- `error_rate`: rate of error you're willing to assume. Default is **0.0005**.
- `backend`: `numpy`, `bitarray` or `redis`. Default is **numpy**.
- Only applies with `redis` backend:
  - `redis_connection`: url for redis connection as accepted by redis-py.
  - `connection_retries`: max number of connection retries in case of losing the connection with redis. Default is **3**.
  - `wait`: max waiting time before trying to make a new request against redis. 
  - `prefix_key`: key used in redis to store bloom filter data. Default is **bloom_filter**.

## API

- `add(element)`: add a new element in the filter.
- `full`: property that indicates if the filter is full.
- `false_positive_probability`: property that indicates current and updated error rate of the filter. This value should match with choosed error_rate when BloomFilterPy was instanciated, but as new items are added, this value will change.
- `reset()`: purge every element from the filter. In the case of bitarray or numpy, after calling `reset()` it is possible to keep  using the filter. However, with redis backend, once `reset()` is called, you **must** reinstantiate the filter.

## Local Example

```python
from pybloom import BloomFilter

if __name__ == '__main__':
    f = BloomFilter(10, error_rate=0.0000003, backend='bitarray')  # or backend='numpy'

    for i in range(10):
        f.add(i)  # or f += i
        assert i in f

    print(f.false_positive_probability, 11 in f) # 6.431432780588261e-07 False
```

In the example above, we have created a bloom filter using `bitarray` backend, with `10` expected elements and max false probability assumed of `0.0000003`.

## Redis example
In order to build a bloom filter in redis, `BloomFilterPy` with `RedisBackend` will do all the work for you. The first process that wins the distributed lock, will be the responsible to initialize the filter. 
```python
from pybloom import BloomFilter

if __name__ == '__main__':
    f = BloomFilter(10, error_rate=0.0000003, backend='redis', redis_connection='redis://localhost:6379/0')

    for i in range(10):
        f.add(i)  # or f += i
        assert i in f

    print(f.false_positive_probability, 11 in f) # 6.431432780588261e-07 False
```
Once the filter is initiallized, if you **don't** change the `prefix_key` in `BloomFilter` object and current `prefix_key` already exists, `BloomFilterPy` will reuse it in a distributed fashion. In this case, `max_number_of_element_expected` and `error_rate` are ignored, but for compatibility with the rest of the backends, it is mandatory to set them up.

# How can I extend it?

If you install this library from sources and are interested in build a new backend, like MongoBackend or FileSystemBackend for example, is very simple. You just need extend your new backend from:

- `ThreadBackend`: if you want to develop a **local** thread-safe backend, like FileSystemBackend.
- `SharedBackend`: if you want to develop a **shared** backend across several machines, like DatabaseBackend.

and implement the following methods:

- `_add(*args, **kwargs)`: this method specify the way of adding new elements in the filter using the backend.
- `reset()`: this method is used to delete or purge **every** element from the filter.

Besides, `__init__` method **must** have two parameters to define the array size and optimal hash. For convention, `array_size: int` and `optimal_hash: int` are used.

For example, in a hypothetical MongoBackend, the skeleton would be something similar to:

```python
class MongoBackend(SharedBackend):
  def __init__(self, array_size: int, optimal_hash: int, **kwargs):
    # In kwargs you can put mongodb connection details, like host, port and so on.
    
    self._mongo_connection = MongoClient(**kwargs)
    super(MongoBackend, self).__init__(array_size, hash_size)
   
  def _add(self, other):
    # perform hashing functions of other and save it in mongo using mongo_connection
   
  def reset(self):
    # purge bloom filter using mongo_connection
   
  def __contains__(self, item):
    # check if item is present in the filter
```

Once you have a new backend ready, you **must** add it into BloomFilter factory class:

```python
class BloomFilter(object):
    def __new__(cls, max_number_of_element_expected: int, error_rate=.0005, backend='numpy', **kwargs):
      ...
      
      elif backend == 'MongoBackend':
        return MongoBackend(filter_metadata.optimal_size, filter_metadata.optimal_hash, max_number_of_element_expected, **kwargs)
      
      ...
```
