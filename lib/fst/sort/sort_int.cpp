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
#include <ostream>
#include <iostream>


// CPU cycle time
#ifdef _WIN32  //  Windows
  #include <intrin.h>
  uint64_t cpu_cycles() {
    return __rdtsc();
  }
#else  // Linux / GCC
  uint64_t cpu_cycles() {
    unsigned int lo, hi;
    __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t)hi << 32) | lo;
  }
#endif


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


inline void radix_fill1(int* buffer, const int* vec, int length, int index1[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = vec[pos];
    const int target_pos = index1[value & 255]++;
    buffer[target_pos] = value;
  }
}


inline void radix_fill2(int* vec, const int* buffer, int length, int index2[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = buffer[pos];
    const int target_pos = index2[(value >> 8) & 255]++;
    vec[target_pos] = value;
  }
}


inline void radix_fill3(int* buffer, const int* vec, int length, int index3[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = vec[pos];
    const int target_pos = index3[(value >> 16) & 255]++;
    buffer[target_pos] = value;
  }
}


inline void radix_fill4(int* vec, const int* buffer, int length, int index4[256])
{
  for (int pos = 0; pos < length; ++pos) {
    const int value = buffer[pos];
    const int target_pos = index4[((value >> 24) & 255) ^ 128]++;
    vec[target_pos] = value;
  }
}


inline void cumulative_index(int* index, int nr_of_threads)
{
  int tot_count = 0;

  for (int ind = 0; ind < THREAD_INDEX_INT_SIZE; ++ind) {

    // set counters
    int pos = ind;

    for (int thread = 0; thread < nr_of_threads; thread++)
    {
      const int tmp = index[pos];
      index[pos] = tot_count;
      tot_count += tmp;
      pos += THREAD_INDEX_INT_SIZE;
    }
  }
}


inline void count_occurrences1(const int nr_of_threads, int* index, const uint64_t* const pair_vec, const int half_length,
  const double batch_size, const int length, const int* vec)
{
#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE] = { 0 };  // 8 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // get source value
        const uint64_t val = pair_vec[pos];

        thread_index[val & 2047]++;  // byte 0
        thread_index[(val >> 32) & 2047]++;  // byte 4
      }
        
      // to main memory
      memcpy(&index[THREAD_INDEX_INT_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT_SIZE);
    }
  }

  // last element is added to last thread counts
  if (length % 2 == 1) {
    const auto val = vec[length - 1];
    const int offset = THREAD_INDEX_INT_SIZE * (nr_of_threads - 1);

    index[offset + (val & 2047)]++;  // byte 0
  }
}

void radix_sort_int(int* vec, const int length, int* buffer)
{
  int nr_of_threads = 1;  // single threaded for small sizes
 
// set optimal number of threads (determined from empirical measurements)

  if (length > 600000) {
    nr_of_threads = std::min(GetFstThreads(), MAX_INT_SORT_THREADS);
  }
  else if (length > 90000)
  {
    nr_of_threads = 2;
  }

  // index for all threads (on heap)
  const auto index_p = static_cast<std::unique_ptr<int[]>>(new int[THREAD_INDEX_INT_SIZE * MAX_INT_SORT_THREADS]);
  int* index = index_p.get();

  const auto pair_vec = reinterpret_cast<uint64_t*>(vec);
  const int half_length = length / 2;
  const double batch_size = static_cast<double>(half_length + 0.01) / static_cast<double>(nr_of_threads);

  uint64_t t0 = cpu_cycles();

  // phase 1: bit 0 - 10 occurence count
  count_occurrences1(nr_of_threads, index, pair_vec, half_length, batch_size, length, vec);

  uint64_t t1 = cpu_cycles();
  std::cout << "count: " << (t1 - t0) / 10000 << "\n" << std::flush;

  // phase 2: determine cumulative positions per thread
  cumulative_index(index, nr_of_threads);

  t0 = cpu_cycles();
  std::cout << "count: " << (t0 - t1) / 10000 << "\n" << std::flush;

  // phase 3: fill buffer
  // the batch size for each thread is exactly equal to that in the counting phase

  // sort on byte0 and fill buffer

  const auto buffer_p = reinterpret_cast<uint32_t*>(buffer);

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE];  // 1 kB

      // copy relevant index main memory
      const int index_start = THREAD_INDEX_INT_SIZE * thread;
      memcpy(thread_index, &index[index_start], THREAD_INDEX_INT_SIZE * 4);

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // determine value of source vec
        const uint64_t val = pair_vec[pos];

        // set value in target buffer
        const int pos_low  = thread_index[ val        & 2047]++;  // byte 0
        const int pos_high = thread_index[(val >> 32) & 2047]++;  // byte 4

        buffer_p[pos_low ] = val & 4294967295;
        buffer_p[pos_high] = (val >> 32) & 4294967295;

        // order vector must be mutated here
      }

      // check last element in last thread
      if (thread == (nr_of_threads - 1)) {
        if (length % 2 == 1) {
          // determine value of source vec
          const int val = vec[length - 1];  // no casting required (?)

          // set value in target buffer
          const int pos_low = thread_index[val & 2047];  // no need to increment
          buffer[pos_low] = val;
        }
      }
    }
  }

  t1 = cpu_cycles();
  std::cout << "count: " << (t1 - t0) / 10000 << "\n" << std::flush;

  // phase 1: bit 11 - 21 occurence count

  const auto buffer_pair_vec = reinterpret_cast<uint64_t*>(buffer);

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE] = { 0 };  // 1 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {
        const uint64_t val = buffer_pair_vec[pos];

        thread_index[(val >> 11) & 2047]++;  // byte 0
        thread_index[(val >> 43) & 2047]++;  // byte 4
      }

      // to main memory
      // to main memory
      memcpy(&index[THREAD_INDEX_INT_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT_SIZE);
    }
  }

  // last element is added to last thread counts
  if (length % 2 == 1) {
    const auto val = static_cast<uint32_t>(buffer[length - 1]);
    const int offset = THREAD_INDEX_INT_SIZE * (nr_of_threads - 1);

    index[offset + ((val >> 11) & 2047)]++;  // byte 0
  }

  // phase 2: determine cumulative positions per thread

  cumulative_index(index, nr_of_threads);

  const auto uvec = reinterpret_cast<uint32_t*>(vec);

  // phase 2 bits 11-21: fill result vector

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE];  // 1 kB

      // copy relevant index main memory
      const int index_start = THREAD_INDEX_INT_SIZE * thread;
      memcpy(thread_index, &index[index_start], THREAD_INDEX_INT_SIZE * 4);

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // determine value of source vec
        const uint64_t val = buffer_pair_vec[pos];

        // set value in target buffer
        const int pos_low  = thread_index[(val >> 11) & 2047]++;  // byte 0
        const int pos_high = thread_index[(val >> 43) & 2047]++;  // byte 4

        uvec[pos_low] = val & 4294967295;
        uvec[pos_high] = (val >> 32) & 4294967295;

        // order vector must be mutated here
      }

      // check last element in last thread
      if (thread == (nr_of_threads - 1)) {
        if (length % 2 == 1) {
          // determine value of source vec
          const int val = buffer[length - 1];  // no casting required (?)

          // set value in target buffer
          const int pos_low = thread_index[(val >> 11) & 2047];  // no need to increment
          uvec[pos_low] = val;
        }
      }
    }
  }

  // count bit 22 - 31

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE] = { 0 };  // 1 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {
        const uint64_t val = pair_vec[pos];

        thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        thread_index[((val >> 54) & 1023) ^ 512]++;  // byte 4
      }

      // to main memory
      memcpy(&index[THREAD_INDEX_INT_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT_SIZE);
    }
  }

  // last element is added to last thread counts
  if (length % 2 == 1) {
    const auto val = static_cast<uint32_t>(vec[length - 1]);
    const int offset = THREAD_INDEX_INT_SIZE * (nr_of_threads - 1);

    index[offset + (((val >> 22) & 1023) ^ 512)]++;  // byte 0
  }

  // phase 2: determine cumulative positions per thread

  cumulative_index(index, nr_of_threads);

  // phase 2 bits 22-31: fill result vector

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE];  // 1 kB

      // copy relevant index main memory
      const int index_start = THREAD_INDEX_INT_SIZE * thread;
      memcpy(thread_index, &index[index_start], THREAD_INDEX_INT_SIZE * 4);

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // determine value of source vec
        const uint64_t val = pair_vec[pos];

        // set value in target buffer
        const int pos_low  = thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        const int pos_high = thread_index[((val >> 54) & 1023) ^ 512]++;  // byte 4

        buffer_p[pos_low]  = val & 4294967295;
        buffer_p[pos_high] = (val >> 32) & 4294967295;

        // order vector must be mutated here
      }

      // check last element in last thread
      if (thread == (nr_of_threads - 1)) {
        if (length % 2 == 1) {
          // determine value of source vec
          const int val = vec[length - 1];  // no casting required (?)

          // set value in target buffer
          const int pos_low = thread_index[((val >> 22) & 1023) ^ 512];  // no need to increment
          buffer[pos_low] = val;
        }
      }
    }
  }
}
