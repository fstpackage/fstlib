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


#ifndef FSTSTREAMER_H
#define FSTSTREAMER_H

#include <string>


/**
 Interface to a fst file.
*/
class FstStreamer
{
  FstStreamer();

  // Open connection to an existing fst file
  int open(std::string filename, std::string mode);

  int close();

  FstMetaData metaData();

  int cbindChunk(FstChunk chunk);

  int rbindChunk(FstChunk chunk);
};


#endif FSTSTREAMER_H
