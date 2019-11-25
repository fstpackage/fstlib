
[![Build Status](https://travis-ci.org/fstpackage/fstlib.svg?branch=develop)](https://travis-ci.org/fstpackage/fstlib)
[![License: AGPLv3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

# The fst format and fstlib library

## Overview

The `fstlib` library is home to the `fst` storage format for columnar tabular data. It also contains very fast multi-threaded streamers for `fst` files and a computational framework that allows for effective use of the format's features for parallel calculations on larger-than-memory datasets. 

### The `fst` format

The `fst` format is used to store columnar tabular data. The format uses hashing and compression for stability, correctness and compactness. A wide range of data-types is available in the format and tabular data can be compressed with a wide range of settings to maximize throughput to storage devices.

### Streaming

The `fstlib` library is build to access tabular data in the `fst` format with maximum possible speeds. It employs multi-threading for background reading and writing, and can (de-)compress using the full resources of the CPU. Speeds of multiple GB/s can be reached on fast (NVME SSD) storage devices.

`fstlib` uses the excellent [LZ4](http://lz4.github.io/lz4/) compressor for high speed compression at lower ratio’s and the [ZSTD](http://facebook.github.io/zstd/) compressor for medium speed compression at higher ratio’s. Compression is done on small (16kB) blocks of data, which allows for (almost) random access of data. Each column uses it’s own compression scheme and different compressors can be mixed within a single column. This flexible setup allows for better optimized and faster compression of data, boosting speeds.

### Computational framework

The `fstlib` library allows for computations on tabular data blocks during loading and decompression of data. This unique approach to processing compressed tabular data enables high-speed computing on large-than-memory datasets.

## Goals

The `fstlib` library was designed with four goals in mind:

* **cross-language compatibility**: `fstlib` compiles on all major platforms and compilers using the `cmake` tool chain, see [here](https://travis-ci.org/fstpackage/fstlib/builds) for Travis builds on the three major platforms.
* **maximum possible speed**: `fstlib` is a multithreaded (`OpenMP`) library which was completely designed around the most important bottleneck for larger-than-memory data analytics: access to storage devices (such as SSD's). It uses background reading and writing and employs fast compression and decompression to get the maximum number of bytes to- and from disk in any given time.
* **full (almost) random access**: `fstlib` was designed to facilitate computational platforms. Therefore, data in the format can be access with almost full random access, both in columns- and rows. This allows for high-speed chunk-based processing of data, crucial for larger-than-memory analytics.
* **flexibility**: `fstlib` needs an interface to columnar in-memory data, but is agnostic to the memory management and precise format of that data. That makes it very effective for use with wide range of in-memory table-like containers without any overhead for copying data. It can handle `arrow` memory structures, but also native `R` vectors, all zero-copy.

## Use cases

Currently, the main use case for `fstlib` is `R`'s [`fst` package](http://www.fstpackage.org/). In that package, `fstlib` provides the backend for accessing `fst` files with very high speeds up to multiple GB/s. In the future, `fstlib` will be part of similar packages for other languages such as `Python`, `Julia`, and `Rust`.