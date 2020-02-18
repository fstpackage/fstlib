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


#ifndef FST_SORT_H
#define FST_SORT_H

#define MAX_INT_SORT_THREADS    8
#define MAX_INT64_SORT_THREADS  8
#define THREAD_INDEX_INT_SIZE   2048
#define THREAD_INDEX_INT64_SIZE 2048

void radix_sort_logical(int* vec, int length);

void radix_sort_logical_order(int* vec, const int length, int* order, int* order_out, const bool default_order);

void radix_sort_int(int* vec, const int length, int* buffer);

void merge_sort_int(const int* left_p, const int* right_p, int length_left, int length_right, int* res_p);

void quick_sort_int(int* vec, int length, int pivot);

#endif  // FST_SORT_H
