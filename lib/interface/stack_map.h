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


#ifndef STACK_MAP_H
#define STACK_MAP_H

#define STACK_MAP_BUCKET_SIZE      2    // double the average
//#define STACK_MAP_BUCKET_INDEX_MAX 1    // last element used for linked list
#define STACK_MAP_PRIMARY_BUCKETS  4096 // includes bucket overflow
#define STACK_MAP_NR_OF_BUCKETS    6143 // includes bucket overflow
#define STACK_MAP_MAX_NR_ELEMENTS  2048 // remain within 32 kB stack

//#include <unordered_map>

class stack_map
{
private:
  // buffers
  unsigned long long keys_[STACK_MAP_MAX_NR_ELEMENTS];
  unsigned short int values_[STACK_MAP_MAX_NR_ELEMENTS];

  unsigned short int key_index_[STACK_MAP_NR_OF_BUCKETS * STACK_MAP_BUCKET_SIZE];
  unsigned short int bucket_size_[STACK_MAP_PRIMARY_BUCKETS];

  // counters
  unsigned short int key_count;
  unsigned short int overflow_count;

public:
  stack_map();

  void clear();

  unsigned short int &size();

  unsigned short int* bucket_counts();

  unsigned long long* keys();

  unsigned short int* values();

  unsigned short int emplace(unsigned long long key, unsigned short int value);
};


#endif // STACK_MAP_H
