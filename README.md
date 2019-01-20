# python-bloomfilter
Scalable bloom filter using different backends written in Python.

# Installation
```bash
pip install BloomFilterPy
```
# Backends

Currently, BloomFilterPy has the following backends available: `numpy`, `bitarray` and `redis`. The first two are recommended when the expected number of elements in the filter fit in memory. Redis backend is the preferred when:

- Expect huge amount of data in the filter that it doesn't fit in memory.
- You want a distributed filter available (i.e. more than one machine).

# Usage & API

BloomFilterPy implements a common API regardless of the backend used. Every backend extends `BaseBackend` class that implements the common API. In turn, this base class extends the default `set` class of Python, but just `add` operation is properly handled.

## `BloomFilter` class

- `max_number_of_element_expected`: Size of filter. Number of elements it will contain.
- `error_rate`: rate of error you're willing to assume. Default is **0.0005**.
- `backend`: `numpy`, `bitarray` or `redis`. Default is **numpy**.
- Only applies with `redis` backend:
  - `redis_connection`: url for redis connection as accepted by redis-py.
  - `connection_retries`: max number of connection retries in case of losing the connection with redis. Default is **3**.
  - `wait`: max waiting time before trying to make a new request against redis. 
  - `prefix_key`: key used in redis to store bloom filter data.

## API

- `add(element)`: add a new element in the filter.
- `full`: property that indicates if the filter is full.
- `false_positive_probability`: property that indicates current and updated error rate of the filter. This value should match with choosed error_rate when BloomFilterPy was instanciated.

## Example

```python
from pybloom.src.bloomfilter import BloomFilter

if __name__ == '__main__':
    f = BloomFilter(10, error_rate=0.0000003, backend='bitarray')

    for i in range(10):
        f.add(i)  # or f += i
        assert i in f

    print(f.false_positive_probability, 11 in f)
```

In the example above, we have created a bloom filter using `bitarray` backend, with `10` expected elements and max false probability assumed of `0.0000003`.

# How can I extend it?

If you install this library from sources and are interested in build a new backend, like MongoBackend or FileSystemBackend for example, is very simple. You just need extend your new backend from `BaseBackend` class and implement the following methods:

- `_add(*args, **kwargs)`: this method specify the way of adding new elements in the filter using the backend.
- `reset()`: this method is used to delete or purge **every** element from the filter.

Besides, `__init__` method **must** have two parameters to define the array size and optimal hash. For convention, `array_size: int` and `optimal_hash: int` are used.

For example, in a hypothetical MongoBackend, the skeleton would be something similar to:

```python
class MongoBackend(BaseBackend):
  def __init__(self, array_size: int, optimal_hash: int, **kwargs):
    # In kwargs you can put mongodb connection details, like host, port and so on.
    
    self._mongo_connection = MongoClient(**kwargs)
    super(MongoBackend, self).__init__(array_size, hash_size)
   
  def _add(self, other):
    # perform hashing functions of other and save it in mongo using mongo_connection
   
  def reset(self):
    # purge bloom filter using mongo_connection
```

Once you have a new backend ready, you **must** add it into BloomFilter factory class:

```python
class BloomFilter(object):
    def __new__(cls, max_number_of_element_expected: int, error_rate=.0005, backend='numpy', **kwargs):
      ...
      
      elif backend == 'MongoBackend':
        return MongoBackend(filter_metadata.optimal_size, filter_metadata.optimal_hash, **kwargs)
      
      ...
```
