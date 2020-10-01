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


#ifndef IBYTEBLOCKCOLUMN_H
#define IBYTEBLOCKCOLUMN_H

#include <memory>

#include "ifstcolumn.h"

class IByteBlockColumn
{
public:
  uint64_t vecLength = 0;

  virtual ~IByteBlockColumn() = default;

  virtual void SetSizesAndPointers(const char** elements, uint64_t* sizes, uint64_t row_start, uint64_t block_size) = 0;
};


#endif  // IBYTEBLOCKCOLUMN_H
