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

#ifndef LOGICAL_v10_H
#define LOGICAL_v10_H


#include <istream>
#include <ostream>


// Logical vectors are always compressed to fill all available bits (factor 16 compression).
// On top of that, we can compress the resulting bytes with a custom compressor.
void fdsWriteLogicalVec_v10(std::ofstream &myfile, int* boolVector, unsigned long long nrOfLogicals, int compression,
  std::string annotation, bool hasAnnotation);


void fdsReadLogicalVec_v10(std::istream &myfile, int* boolVector, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size);

#endif // LOGICAL_v10_H
