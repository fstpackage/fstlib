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

#ifndef BLOCKSTORE_H
#define BLOCKSTORE_H

#include <fstream>

#include <compression/compressor.h>

// Method for writing column data of any type to a ofstream.
void fdsStreamUncompressed_v2(std::ofstream& myfile, char* vec, unsigned long long vecLength, int elementSize, int blockSizeElems,
                              FixedRatioCompressor* fixedRatioCompressor, std::string annotation, bool hasAnnotation);


// Method for writing column data of any type to a stream.
void fdsStreamcompressed_v2(std::ofstream& myfile, char* colVec, unsigned long long nrOfRows, int elementSize,
                            StreamCompressor* streamCompressor, int blockSizeElems, std::string annotation, bool hasAnnotation);


void fdsReadColumn_v2(std::istream& myfile, char* outVec, unsigned long long blockPos, unsigned long long startRow, unsigned long long length,
                      unsigned long long size, int elementSize, std::string& annotation, int maxbatchSize, bool& hasAnnotation);


#endif // BLOCKSTORE_H
