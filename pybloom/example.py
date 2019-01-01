from pybloom.src.bloomfilter import BloomFilter

if __name__ == '__main__':
    f = BloomFilter(10, error_rate=0.0000003, backend='bitarray',
                    redis_connection='redis://localhost:6379/0')  # 10000000000

    for i in range(10):
        f.add(i)
        assert i in f

    print(f.false_positive_probability, 11 in f)
