/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this file,
  You can obtain one at https://mozilla.org/MPL/2.0/.

  https://www.mozilla.org/en-US/MPL/2.0/FAQ/

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/

#include <logical/logical_v10.h>
#include <blockstreamer/blockstreamer_v2.h>
#include <compression/compressor.h>

#define BLOCKSIZE_LOGICAL 4096  // number of logicals in default compression block


using namespace std;


// Logical vectors are always compressed to fill all available bits (factor 16 compression).
// On top of that, we can compress the resulting bytes with a custom compressor.
void fdsWriteLogicalVec_v10(ofstream &myfile, int* boolVector, unsigned long long nrOfLogicals, int compression,
  std::string annotation, bool hasAnnotation)
{
  const int blockSize = 4 * BLOCKSIZE_LOGICAL;  // block size in bytes

  if (compression <= 50)  // compress 1 - 50
  {
    Compressor* defaultCompress = new SingleCompressor(CompAlgo::LOGIC64, 0);  // compression not relevant here
    Compressor* compress2 = new SingleCompressor(CompAlgo::LZ4_LOGIC64, 100);  // use maximum compression for LZ4 algorithm
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(defaultCompress, compress2, 2.0F * compression);
    streamCompressor->CompressBufferSize(blockSize);

    fdsStreamcompressed_v2(myfile, reinterpret_cast<char*>(boolVector), nrOfLogicals, 4, streamCompressor, BLOCKSIZE_LOGICAL, annotation, hasAnnotation);

    delete defaultCompress;
    delete compress2;
    delete streamCompressor;

    return;
  }
  else if (compression <= 100)  // compress 51 - 100
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4_LOGIC64, 100);
    Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD_LOGIC64, 2 * (compression - 50));
    StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2.0F * (compression - 50));
    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, (char*) boolVector, nrOfLogicals, 4, streamCompressor, BLOCKSIZE_LOGICAL, annotation, hasAnnotation);

    delete compress1;
    delete compress2;
    delete streamCompressor;
  }

  return;
}


void fdsReadLogicalVec_v10(istream &myfile, int* boolVector, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size)
{
  std::string annotation;
  bool hasAnnotation;

  return fdsReadColumn_v2(myfile, (char*) boolVector, blockPos, startRow, length, size, 4, annotation, BATCH_SIZE_READ_LOGICAL, hasAnnotation);
}
