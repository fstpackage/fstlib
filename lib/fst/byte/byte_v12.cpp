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

#include <byte/byte_v12.h>
#include <blockstreamer/blockstreamer_v2.h>

// External libraries
#include <compression/compressor.h>

using namespace std;


void fdsWriteByteVec_v12(ofstream& myfile, char* byteVector, unsigned long long nrOfRows, unsigned int compression,
	std::string annotation, bool hasAnnotation)
{
  int blockSize = BLOCKSIZE_BYTE; // block size in bytes

  if (compression == 0)
  {
    return fdsStreamUncompressed_v2(myfile, byteVector, nrOfRows, 1, BLOCKSIZE_BYTE, nullptr, annotation, hasAnnotation);
  }

  if (compression <= 50) // low compression: linear mix of uncompressed and LZ4_SHUF
  {
    Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 0);

    StreamCompressor* streamCompressor = new StreamLinearCompressor(compress1, 2.0f * compression);

    streamCompressor->CompressBufferSize(blockSize);
    fdsStreamcompressed_v2(myfile, byteVector, nrOfRows, 1, streamCompressor, BLOCKSIZE_BYTE, annotation, hasAnnotation);

    delete compress1;
    delete streamCompressor;
    return;
  }

  Compressor* compress1 = new SingleCompressor(CompAlgo::LZ4, 0);
  Compressor* compress2 = new SingleCompressor(CompAlgo::ZSTD, 0);
  StreamCompressor* streamCompressor = new StreamCompositeCompressor(compress1, compress2, 2.0F * (compression - 50));
  streamCompressor->CompressBufferSize(blockSize);
  fdsStreamcompressed_v2(myfile, byteVector, nrOfRows, 1, streamCompressor, BLOCKSIZE_BYTE, annotation, hasAnnotation);

  delete compress1;
  delete compress2;
  delete streamCompressor;

  return;
}


void fdsReadByteVec_v12(istream& myfile, char* byteVec, unsigned long long blockPos, unsigned long long startRow, unsigned long long length,
                        unsigned long long size)
{
  std::string annotation;
  bool hasAnnotation;

  return fdsReadColumn_v2(myfile, byteVec, blockPos, startRow, length, size, 1, annotation, BATCH_SIZE_READ_BYTE, hasAnnotation);
}
