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


#include <cstdint>  // int64_t
#include <cstring>  // memcpy
#include <algorithm>

#include <sort/sort.h>
#include <interface/openmphelper.h>
#include <memory>


#define MAX_INT_SORT_THREADS 8


void quick_sort_int(int* vec, int length, int pivot) {

  int pos_left = 0;
  int pos_right = length - 1;

  while (true) {

    // iterate left until value > pivot
    while (vec[pos_left] <= pivot && pos_left != pos_right) ++pos_left;

    // left swap value found, iterate right until value < pivot
    while (vec[pos_right] > pivot && pos_right != pos_left) --pos_right;

    if (pos_left == pos_right) break;

    // swap values
    const int tmp = vec[pos_right];
    vec[pos_right] = vec[pos_left];
    vec[pos_left] = tmp;
  }

  // pos_left == pos_right as this point

  if (vec[pos_left] < pivot) {
    pos_left++;
  }

  // do not use elem_left after this point (as pos_left is possibly updated)

  if (pos_left > 2) {
    const int piv = (vec[0] + vec[pos_left / 2] + vec[pos_left - 1]) / 3;
    quick_sort_int(vec, pos_left, piv);
  }
  else if (pos_left == 2 && vec[0] > vec[1]) {
    // swap first 2 elements
    const int tmp = vec[1];
    vec[1] = vec[0];
    vec[0] = tmp;
  }

  if (pos_left < (length - 2)) {
    const int piv = (vec[pos_left] + vec[(length + pos_left) / 2] + vec[length - 1]) / 3;
    quick_sort_int(&vec[pos_left], length - pos_left, piv);
  } else if (pos_left == (length - 2) && vec[pos_left] > vec[pos_left + 1]) {
    // swap last 2 elements if in reverse order
    const int tmp = vec[pos_left];
    vec[pos_left] = vec[pos_left + 1];
    vec[pos_left + 1] = tmp;
  }
}


void merge_sort_int(const int* left_p, const int* right_p, int length_left, int length_right, int* res_p) {

  int pos_left = 0;
  int pos_right = 0;

  // populate result vector
  int pos = 0;
  while (pos_left < length_left && pos_right < length_right) {

    const int val_left = left_p[pos_left];
    const int val_right = right_p[pos_right];

    if (val_left <= val_right) {
      res_p[pos] = val_left;
      pos_left++;
      pos++;
      continue;
    }

    res_p[pos] = val_right;
    pos_right++;
    pos++;
  }

  // populate remainder
  if (pos_left == length_left) {
    while (pos_right < length_right) {
      res_p[pos] = right_p[pos_right];
      pos++;
      pos_right++;
    }
  } else
  {
    while (pos_left < length_left) {
      res_p[pos] = left_p[pos_left];
      pos++;
      pos_left++;
    }
  }
}


inline void radix_fill1(int* buffer, int* vec, int length, int index1[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = vec[pos];
    const int target_pos = index1[value & 255]++;
    buffer[target_pos] = value;
  }
}


inline void radix_fill2(int* vec, int* buffer, int length, int index2[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = buffer[pos];
    const int target_pos = index2[(value >> 8) & 255]++;
    vec[target_pos] = value;
  }
}


inline void radix_fill3(int* buffer, int* vec, int length, int index3[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = vec[pos];
    const int target_pos = index3[(value >> 16) & 255]++;
    buffer[target_pos] = value;
  }
}


inline void radix_fill4(int* vec, int* buffer, int length, int index4[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = buffer[pos];
    const int target_pos = index4[((value >> 24) & 255) ^ 128]++;
    vec[target_pos] = value;
  }
}


void radix_sort_int(int* vec, int length, int* buffer)
{
  int nr_of_threads = 1;  // single threaded for small sizes

// determine optimal threads

  if (length > 1048576) {
    nr_of_threads = std::min(GetFstThreads(), MAX_INT_SORT_THREADS);
  }

  // index for all threads
  auto index_p = (std::unique_ptr<int[]>)(new int[4 * 256 * MAX_INT_SORT_THREADS]);
  int* index = index_p.get();

  // phase 1: count occurence of each byte

  const auto pair_vec = reinterpret_cast<uint64_t*>(vec);
  const int half_length = length / 2;
  const double batch_size = static_cast<double>(half_length + 0.01) / static_cast<double>(nr_of_threads);

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 128 * ((static_cast<int>(thread * batch_size) + 127) / 128);
      const int pos_end = std::max(128 * ((static_cast<int>((thread + 1) * batch_size) + 127) / 128), half_length);

      // local cache index
      int thread_index0[256] = { 0 };  // 1 kB
      int thread_index1[256] = { 0 };  // 1 kB
      int thread_index2[256] = { 0 };  // 1 kB
      int thread_index3[256] = { 0 };  // 1 kB

      // TODO: some more loop unwinding

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {
        const uint64_t val = pair_vec[pos];

        thread_index0[  val        & 255       ]++;  // byte 0
        thread_index1[( val >> 8 ) & 255       ]++;  // byte 1
        thread_index2[( val >> 16) & 255       ]++;  // byte 2
        thread_index3[((val >> 24) & 255) ^ 128]++;  // byte 3 with flipped 7th bit
        thread_index0[( val >> 32) & 255       ]++;  // byte 4
        thread_index1[( val >> 40) & 255       ]++;  // byte 5
        thread_index2[( val >> 48) & 255       ]++;  // byte 6
        thread_index3[((val >> 56) & 255) ^ 128]++;  // byte 7 with flipped 7th bit
      }

      // to main memory
      const int offset = 4 * 256 * thread;
      for (int pos = 0; pos < 256; pos++)
      {
        index[offset + pos      ] = thread_index0[pos];
        index[offset + pos + 256] = thread_index1[pos];
        index[offset + pos + 512] = thread_index2[pos];
        index[offset + pos + 768] = thread_index3[pos];
      }
    }
  }

  // last element is added to last thread counts
  if (length % 2 == 1) {
    auto val = static_cast<uint32_t>(vec[length - 1]);
    const int offset = 4 * 256 * (nr_of_threads - 1);

    index[offset +         (val        & 255)       ]++;  // byte 0
    index[offset + 256 +  ((val >> 8 ) & 255)       ]++;  // byte 1
    index[offset + 512 +  ((val >> 16) & 255)       ]++;  // byte 2
    index[offset + 768 + (((val >> 24) & 255) ^ 128)]++;  // byte 3 with flipped 7th bit
  }


  // phase 2: determine cumulative positions per thread

  int tot_count = 0;
  for (int ind = 0; ind < 256; ++ind) {
    int pos = ind;
    for (int thread = 0; thread < nr_of_threads; thread++)
    {
      const int tmp = index[pos];
      index[pos] = tot_count;
      tot_count += tmp;
      pos += 1024;
    }
  }

  // phase 3: fill buffer

  // TODO: fill buffer code here
}
