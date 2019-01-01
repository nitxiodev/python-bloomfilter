import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="BloomFilterPy",
    version="1.0",
    author="nitxiodev",
    author_email="smnitxio@gmail.com",
    description="Scalable bloom filter using different backends written in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nitxiodev/python-bloomfilter",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
)
