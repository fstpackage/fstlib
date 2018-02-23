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
  memset(bucket_size, 0, 2 * STACK_MAP_NR_OF_BUCKETS);
  key_count = 0;
  overflow_count = STACK_MAP_PRIMARY_BUCKETS;
}


unsigned short int &stack_map::size()
{
  return key_count;
}


unsigned int stack_map::emplace(unsigned long long key, unsigned short value)
{
  // determine bucket for the specified key
  const unsigned int bucket = (key >> 4) & 127;

  const int buck_size = bucket_size[bucket];
  int bucket_start = STACK_MAP_BUCKET_SIZE * bucket;

  // search for existing key
  if (buck_size == 0)
  {
    // space left in bucket
    bucket_size[bucket] = 1;
    key_index[bucket_start] = key_count;
    keys[key_count] = key;
    values[key_count] = value;
    key_count++;
    return key_count;
  }

  int full_buckets   = (buck_size - 1) / STACK_MAP_BUCKET_INDEX_MAX;
  int remaining_elems = 1 + (buck_size - 1) % STACK_MAP_BUCKET_INDEX_MAX;

  // iterate full buckets
  for (int bucket_count = 0; bucket_count < full_buckets; bucket_count++)
  { 
    const int to_element = bucket_start + STACK_MAP_BUCKET_INDEX_MAX;

    // iterate bucket elements
    for (int element = bucket_start; element < to_element; element++)
    {
      const unsigned long long stored_key = keys[key_index[element]];
      if (stored_key == key) return key_count;
    }

    bucket_start = key_index[to_element];
  }

  // iterate partially filled bucket
  const int to_element = bucket_start + remaining_elems;

  for (int element = bucket_start; element < to_element; element++)
  {
    const unsigned long long stored_key = keys[key_index[element]];
    if (stored_key == key) return buck_size;
  }

  // not found and bucket is full
  if (remaining_elems == STACK_MAP_BUCKET_INDEX_MAX)
  {
    // set to new bucket
    const unsigned short int overflow_bucket = overflow_count * STACK_MAP_BUCKET_SIZE;
    overflow_count++;
    key_index[to_element] = overflow_bucket;

    // add key to overflow bucket
    bucket_size[bucket] = buck_size + 1;
    key_index[overflow_bucket] = key_count;
    keys[key_count] = key;
    values[key_count] = value;
    key_count++;
    return key_count;
  }


  // space left in bucket
  bucket_size[bucket] = buck_size + 1;
  key_index[to_element] = key_count;
  keys[key_count] = key;
  values[key_count] = value;
  key_count++;

  return buck_size;
}


