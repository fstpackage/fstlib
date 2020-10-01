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

#ifndef FACTOR_v7_H
#define FACTOR_v7_H


#include <iostream>
#include <fstream>

#include <interface/istringwriter.h>
#include <interface/ifstcolumn.h>
#include <interface/icolumnfactory.h>
#include <interface/ifsttable.h>


void fdsWriteFactorVec_v7(std::ofstream &myfile, int* intP, IStringWriter* blockRunner, unsigned long long size, unsigned int compression,
	StringEncoding stringEncoding, std::string annotation, bool hasAnnotation);


// Parameter 'startRow' is zero based.
void fdsReadFactorVec_v7(IFstTable &tableReader, std::istream &myfile, unsigned long long blockPos, unsigned long long startRow,
  unsigned long long length, unsigned long long size, FstColumnAttribute col_attribute, IColumnFactory* columnFactory, int colSel);


#endif  // FACTOR_v7_H
