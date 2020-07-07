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


#ifndef IFST_TABLE_H
#define IFST_TABLE_H

#include "ifstcolumn.h"
#include "istringwriter.h"
#include "ibyteblockcolumn.h"


/**
  Interface to a fst table. A fst table is a temporary wrapper around an array of columnar data buffers.
  The table only exists to facilitate serialization and deserialization of data.
*/
class IFstTable
{
  public:
    virtual ~IFstTable() {};

    virtual FstColumnType ColumnType(unsigned int colNr, FstColumnAttribute &columnAttribute, short int &scale,
      std::string &annotation, bool &hasAnnotation) = 0;

  // Writer interface
    virtual IStringWriter* GetStringWriter(unsigned int colNr) = 0;

    virtual int* GetLogicalWriter(unsigned int colNr) = 0;

    virtual int* GetIntWriter(unsigned int colNr) = 0;

    virtual long long* GetInt64Writer(unsigned int colNr) = 0;

    virtual char* GetByteWriter(unsigned int colNr) = 0;

    virtual double* GetDoubleWriter(unsigned int colNr) = 0;

    virtual IByteBlockColumn* GetByteBlockWriter(unsigned int col_nr) = 0;

    virtual IStringWriter* GetLevelWriter(unsigned int colNr) = 0;

    virtual IStringWriter* GetColNameWriter() = 0;

    virtual void GetKeyColumns(int* keyColPos) = 0;

    virtual unsigned int NrOfKeys() = 0;

    virtual unsigned int NrOfColumns() = 0;

    virtual unsigned long long NrOfRows() = 0;

	  // Reader interface
    virtual void InitTable(unsigned int nrOfCols, unsigned long long nrOfRows) = 0;

    virtual IByteBlockColumn* add_byte_block_column(unsigned col_nr) = 0;

    virtual void SetStringColumn(IStringColumn* stringColumn, int colNr) = 0;

    virtual void SetLogicalColumn(ILogicalColumn* logicalColumn, int colNr) = 0;

    virtual void SetIntegerColumn(IIntegerColumn* integerColumn, int colNr) = 0;

    virtual void SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr) = 0;

    virtual void SetFactorColumn(IFactorColumn* factorColumn, int colNr) = 0;

    virtual void SetInt64Column(IInt64Column* int64Column, int colNr) = 0;

    virtual void SetByteColumn(IByteColumn* byteColumn, int colNr) = 0;

    // use more efficient string container here (e.g. std::vector<string>)
    virtual void SetColNames(IStringArray* col_names) = 0;

    virtual void SetKeyColumns(int* keyColPos, unsigned int nrOfKeys) = 0;
};

#endif  // IFST_TABLE_H
