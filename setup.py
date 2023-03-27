import os, re

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

# Read version from init (taken from bitarray setup.py)
regex = re.compile(r'__version__\s*=\s*(\S+)', re.M)
data = open(os.path.join('pybloom', '__init__.py')).read()

setuptools.setup(
    name="BloomFilterPy",
    version=eval(regex.search(data).group(1)),
    author="nitxiodev",
    author_email="smnitxio@gmail.com",
    description="Scalable bloom filter using different backends written in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nitxiodev/python-bloomfilter",
    packages=setuptools.find_packages(),
    install_requires=[
        'redis==4.5.3',
        'psutil==5.6.6',
        'mmh3==2.5.1',
        'bitarray==0.8.3',
        'numpy==1.15.4',
    ],
    test_requires=[
        'mock==2.0.0',
        'PyHamcrest',
        'fakeredis',
        'nose',
        'nose-exclude'
    ],
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
)
