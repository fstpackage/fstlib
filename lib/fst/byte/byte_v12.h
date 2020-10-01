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

#ifndef BYTE_V12_H
#define BYTE_V12_H


#include <fstream>

void fdsWriteByteVec_v12(std::ofstream& myfile, char* byteVector, unsigned long long nrOfRows, unsigned int compression,
                         std::string annotation, bool hasAnnotation);

void fdsReadByteVec_v12(std::istream& myfile, char* byteVector, unsigned long long blockPos, unsigned long long startRow,
                        unsigned long long length, unsigned long long size);

#endif // BYTE_V12_H
