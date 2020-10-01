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


#ifndef IFST_COLUMN_FACTORY_H
#define IFST_COLUMN_FACTORY_H


#include "ifstcolumn.h"

class IColumnFactory
{
public:
  virtual ~IColumnFactory() {};
  virtual IFactorColumn*  CreateFactorColumn(uint64_t nrOfRows, uint64_t nrOfLevels, FstColumnAttribute columnAttribute) = 0;
  virtual ILogicalColumn* CreateLogicalColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual IDoubleColumn* CreateDoubleColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale) = 0;
  virtual IIntegerColumn* CreateIntegerColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale) = 0;
  virtual IByteColumn* CreateByteColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual IInt64Column* CreateInt64Column(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale) = 0;
  virtual IStringColumn* CreateStringColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual IStringArray* CreateStringArray() = 0;
};

#endif // IFST_COLUMN_FACTORY_H

