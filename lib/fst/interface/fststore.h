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


#ifndef FST_STORE_H
#define FST_STORE_H


#include <vector>
#include <memory>

#include <interface/icolumnfactory.h>
#include <interface/ifsttable.h>


class FstStore
{
  std::string fstFile;
  std::unique_ptr<char[]> metaDataBlockP;

  public:
    unsigned long long* p_nrOfRows;
    int* keyColPos;

    char* metaDataBlock;

  	// column info
    unsigned short int* colTypes;
    unsigned short int* colBaseTypes;
    unsigned short int* colAttributeTypes;
    unsigned short int* colScales;

    unsigned int tableVersionMax;
    int nrOfCols, keyLength;

    FstStore(std::string fstFile);

    ~FstStore() { }

	/**
     * \brief Stream a data table
     * \param fstTable Table to stream, implementation of IFstTable interface
     * \param compress Compression factor with a value 0-100
     */
    void fstWrite(IFstTable &fstTable, int compress) const;

    void fstMeta(IColumnFactory* columnFactory, IStringColumn* col_names);

    void fstRead(IFstTable &tableReader, IStringArray* columnSelection, int64_t startRow, int64_t endRow,
      IColumnFactory* columnFactory, std::vector<int> &keyIndex, IStringArray* selectedCols, IStringColumn* col_names);
};


#endif  // FST_STORE_H
