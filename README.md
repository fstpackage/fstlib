[![Build Status](https://travis-ci.org/MarcusKlik/fstlib.svg?branch=develop)](https://travis-ci.org/MarcusKlik/fstlib)

# The fst format and fstlib library

## Library

The `fstlib` library is home to the `fst` storage format and the framework around it to effectively use it for calculations on larger-than-memory datasets.

## Format

The `fst` format is used to store tabular data. Data can be compressed with fast type-specific algorithms but can still be retrieved with full random access in rows and columns. A wide range of column data types are available and 

## Goals

The library was designed with four goals in mind:

* **cross-language compatibility**: `fstlib` compiles on all major platforms and compilers using the `cmake` tool chain.
* **maximum possible speed**: `fstlib` is a multithreaded (`OpenMP`) library which was completely designed around the most important bottleneck for larger-than-memory data analytics: access to disk. It uses background reading and writing and employs fast compression and decompression to get the maximum number of bytes to- and from disk in any given time.
* **full random access**: `fstlib` was designed to facilitate computational platforms. Therefore, the file format is fully random access, both in columns- and rows. This allows for high-speed chunk-based processing of data, crucial for larger-than-memory analytics.
* **flexibility**: `fstlib` needs an interface to columnar in-memory data, but is agnostic to the memory management and precise format of that data. That makes it very effective for use with wide range of in-memory table-like containers without any overhead for copying data. It can handle `arrow` memory structures, but also native `R` vectors, all zero-copy.
