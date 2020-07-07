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

#ifndef CHARACTER_V6_H
#define CHARACTER_V6_H


#include <iostream>
#include <fstream>

#include "interface/istringwriter.h"
#include "interface/ifstcolumn.h"


void fdsWriteCharVec_v6(std::ofstream &myfile, IStringWriter* blockRunner, int compression, StringEncoding stringEncoding);


void fdsReadCharVec_v6(std::istream &myfile, IStringColumn* blockReader, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long vecLength, unsigned long long size);


#endif  // CHARACTER_V6_H

