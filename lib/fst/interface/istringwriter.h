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


#ifndef ISTRINGWRITER_H
#define ISTRINGWRITER_H

enum StringEncoding
{
	NATIVE = 0,
	LATIN1,
	UTF8
};


class IStringWriter
{
public:
  unsigned int* strSizes = nullptr;
  unsigned int* naInts = nullptr;
  unsigned int bufSize = 0;
  char* activeBuf = nullptr;
  unsigned long long vecLength = 0;

  virtual ~IStringWriter() {}

  virtual StringEncoding Encoding() = 0;

  virtual void SetBuffersFromVec(uint64_t startCount, uint64_t endCount) = 0;
};


#endif  // ISTRINGWRITER_H

