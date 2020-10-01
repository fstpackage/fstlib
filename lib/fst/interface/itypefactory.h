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


#ifndef ITYPE_FACTORY_H
#define ITYPE_FACTORY_H


class IBlobContainer
{
public:
	virtual ~IBlobContainer() {}

	virtual unsigned char* Data() = 0;

	virtual unsigned long long Size() = 0;
};


class ITypeFactory
{
public:
	virtual ~ITypeFactory() {}

	/**
	 * \brief Create a BlobContainer type of size indicated.
	 * \param size Size of BlobContainer to create.
	 * \return Pointer to the generated BlobContainer object;
	 */
	virtual IBlobContainer* CreateBlobContainer(unsigned long long size) = 0;
};


#endif // ITYPE_FACTORY_H

