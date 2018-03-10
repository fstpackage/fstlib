/*
fstlib - A C++ library for ultra fast storage and retrieval of datasets

Copyright (C) 2017-present, Mark AJ Klik

This file is part of fstlib.

fstlib is free software: you can redistribute it and/or modify it under the
terms of the GNU Affero General Public License version 3 as published by the
Free Software Foundation.

fstlib is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
details.

You should have received a copy of the GNU Affero General Public License
along with fstlib. If not, see <http://www.gnu.org/licenses/>.

You can contact the author at:
- fstlib source repository : https://github.com/fstpackage/fstlib
*/


#include "stack_map.h"
#include <cstring>


stack_map::stack_map()
{
  clear();
}


void stack_map::clear()
{
  // initialize bucket sizes to zero
  memset(bucket_size_, 0, 2 * STACK_MAP_PRIMARY_BUCKETS);
  key_count = 0;
  overflow_count = STACK_MAP_PRIMARY_BUCKETS;
}


unsigned short int &stack_map::size()
{
  return key_count;
}

unsigned short int* stack_map::bucket_counts()
{
  return bucket_size_;
}

unsigned long long* stack_map::keys()
{
  return keys_;
}

unsigned short* stack_map::values()
{
  return values_;
}


unsigned short int stack_map::emplace(unsigned long long key, unsigned short value)
{
  // determine bucket for the specified key
  const unsigned int bucket = (key >> 4) & 4095;

  const int buck_size = bucket_size_[bucket];
  int bucket_start = STACK_MAP_BUCKET_SIZE * bucket;

  // buvket is empty
  if (buck_size == 0)
  {
    // space left in bucket
    bucket_size_[bucket] = 1;
    key_index_[bucket_start] = key_count;
    keys_[key_count] = key;
    values_[key_count] = value;
    key_count++;
    return value;
  }

  //const int full_buckets   = (buck_size - 1) / STACK_MAP_BUCKET_INDEX_MAX;
  //const int remaining_elems = 1 + (buck_size - 1) % STACK_MAP_BUCKET_INDEX_MAX;

  // iterate full buckets
  for (int bucket_count = 0; bucket_count < buck_size - 1; bucket_count++)
  { 
    // iterate bucket elements
    const unsigned short int key_index = key_index_[bucket_start];
    const unsigned long long stored_key = keys_[key_index];
    if (stored_key == key) return values_[key_index];

    bucket_start = key_index_[bucket_start + 1];
  }

  // test last key
  const unsigned short int key_index = key_index_[bucket_start];
  const unsigned long long stored_key = keys_[key_index];

  // return registered value
  if (stored_key == key) return values_[key_index];

  // key not found, so add new key
  const unsigned short int overflow_bucket = overflow_count * STACK_MAP_BUCKET_SIZE;
  overflow_count++;
  key_index_[bucket_start + 1] = overflow_bucket;

  // add key to overflow bucket
  bucket_size_[bucket] = buck_size + 1;
  key_index_[overflow_bucket] = key_count;
  keys_[key_count] = key;
  values_[key_count] = value;
  key_count++;
  return value;
}


