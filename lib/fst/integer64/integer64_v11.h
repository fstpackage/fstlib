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

#ifndef INT64_V11_H
#define INT64_V11_H

// System libraries
#include <ostream>


void fdsWriteInt64Vec_v11(std::ofstream &myfile, long long* int64Vector, unsigned long long nrOfRows, unsigned int compression,
  std::string annotation, bool hasAnnotation);

void fdsReadInt64Vec_v11(std::istream &myfile, long long* int64Vector, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size);

#endif // INT64_V11_H
