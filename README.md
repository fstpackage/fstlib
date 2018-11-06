# fstlib

The `fstlib` library is home to the `fst` file format, which can be used to stream _columnar_ in-memory data to disk. The library was designed with three goals in mind:

* cross-language compatibility
* maximum possible speed
* full random access
* flexibility

# Cross-language compatibility

`fstlib` compiles on all major platforms and compilers using the `cmake` tool chain.

# Maximum possible speed

`fstlib` is a multithreaded (`OpenMP`) library which was completely designed around the most important bottleneck for larger-than-memory data analytics: access to disk. It uses background reading and writing and employs fast compression and decompression to get the maximum number of bytes to- and from disk in any given time.

# Full random access

`fstlib` was designed to facilitate computational platforms. Therefore, the file format is fully random access, both in columns- and rows. This allows for high-speed chunk-based processing of data, crucial for larger-than-memory analytics.

# Flexibility

`fstlib` needs an interface to columnar in-memory data, but is agnostic to the memory management and precise format of that data. That makes it very effective for use with wide range of in-memory table-like containers without any overhead for copying data. It can handle `arrow` memory structures, but also native `R` vectors, all zero-copy.
