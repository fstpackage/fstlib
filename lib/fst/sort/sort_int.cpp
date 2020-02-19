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


inline void count_occurrences1(const int nr_of_threads, int* index, const uint64_t* const pair_vec, const int quarter_length,
  const double batch_size, const int length, const int* vec)
{
#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 16 * ((static_cast<int>(thread * batch_size) + 15) / 16);
      const int pos_end = std::min(16 * ((static_cast<int>((thread + 1) * batch_size) + 15) / 16), quarter_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE] = { 0 };  // 8 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // first source value
        uint64_t val = pair_vec[pos++];

        thread_index[val & 2047]++;  // byte 0
        thread_index[(val >> 32) & 2047]++;  // byte 4

        // second source value
        val = pair_vec[pos];

        thread_index[val & 2047]++;  // byte 0
        thread_index[(val >> 32) & 2047]++;  // byte 4
      }

      if (thread == (nr_of_threads - 1)) {
        for (int pos = pos_end * 4; pos < length; pos++)
        {
          const int val = vec[pos];
          thread_index[val & 2047]++;  // byte 0
        }
      }
        
      // to main memory
      memcpy(&index[THREAD_INDEX_INT_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT_SIZE);
    }
  }
}


inline void count_occurrences2(const int nr_of_threads, int* index, const uint64_t* const pair_vec, const int quarter_length,
  const double batch_size, const int length, const int* vec)
{
#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 16 * ((static_cast<int>(thread * batch_size) + 15) / 16);
      const int pos_end = std::min(16 * ((static_cast<int>((thread + 1) * batch_size) + 15) / 16), quarter_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE] = { 0 };  // 8 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // first source value
        uint64_t val = pair_vec[pos++];

        thread_index[(val >> 11) & 2047]++;  // byte 0
        thread_index[(val >> 43) & 2047]++;  // byte 4

        // second source value
        val = pair_vec[pos];

        thread_index[(val >> 11) & 2047]++;  // byte 0
        thread_index[(val >> 43) & 2047]++;  // byte 4
      }

      if (thread == (nr_of_threads - 1)) {
        for (int pos = pos_end * 4; pos < length; pos++)
        {
          const int val = vec[pos];
          thread_index[(val >> 11) & 2047]++;  // byte 0
        }
      }

      // to main memory
      memcpy(&index[THREAD_INDEX_INT_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT_SIZE);
    }
  }
}


inline void count_occurrences3(const int nr_of_threads, int* index, const uint64_t* const pair_vec, const int quarter_length,
  const double batch_size, const int length, const int* vec)
{
#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 16 * ((static_cast<int>(thread * batch_size) + 15) / 16);
      const int pos_end = std::min(16 * ((static_cast<int>((thread + 1) * batch_size) + 15) / 16), quarter_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE] = { 0 };  // 8 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // first source value
        uint64_t val = pair_vec[pos++];

        thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        thread_index[((val >> 43) & 1023) ^ 512]++;  // byte 4

        // second source value
        val = pair_vec[pos];

        thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        thread_index[((val >> 43) & 1023) ^ 512]++;  // byte 4
      }

      if (thread == (nr_of_threads - 1)) {
        for (int pos = pos_end * 4; pos < length; pos++)
        {
          const int val = vec[pos];
          thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        }
      }

      // to main memory
      memcpy(&index[THREAD_INDEX_INT_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT_SIZE);
    }
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
  const int quarter_length = length / 4;
  const double batch_size = static_cast<double>(quarter_length + 0.01) / static_cast<double>(nr_of_threads);  // 4 integers

  uint64_t t0 = cpu_cycles();

  // phase 1: bit 0 - 10 occurence count
  count_occurrences1(nr_of_threads, index, pair_vec, quarter_length, batch_size, length, vec);

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
      // range
      const int pos_start = 16 * ((static_cast<int>(thread * batch_size) + 15) / 16);
      const int pos_end = std::min(16 * ((static_cast<int>((thread + 1) * batch_size) + 15) / 16), quarter_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE];  // 1 kB

      // copy relevant index from main memory
      const int index_start = THREAD_INDEX_INT_SIZE * thread;
      memcpy(thread_index, &index[index_start], THREAD_INDEX_INT_SIZE * 4);

      // iterate uint64_t values
      int hash_pos = pos_start;
      for (int pos = pos_start; pos < pos_end; pos++) {

        // first source vec
        uint64_t val = pair_vec[pos++];

        // set value in target buffer
        const uint64_t val1 =  val        & 2047;
        const uint64_t val2 = (val >> 32) & 2047;
        int pos_low  = thread_index[val1]++;  // byte 0
        int pos_high = thread_index[val2]++;  // byte 4

        buffer_p[pos_low ] = val & 4294967295;
        buffer_p[pos_high] = (val >> 32) & 4294967295;

        // determine value of source vec
        val = pair_vec[pos];  // no increment

        // set value in target buffer
        const uint64_t val3 =  val        & 2047;
        const uint64_t val4 = (val >> 32) & 2047;
        pos_low  = thread_index[val3]++;  // byte 0
        pos_high = thread_index[val4]++;  // byte 4

        buffer_p[pos_low] = val & 4294967295;
        buffer_p[pos_high] = (val >> 32) & 4294967295;

        // overwrite pair_vec[pos] with val1 and val2 (high speed?)
        pair_vec[hash_pos++] = val1 | (val2 << 11) | (val3 << 22) | (val4 << 44);  // no increment
      }

      // check last element in last thread
      if (thread == (nr_of_threads - 1)) {
        for (int pos = pos_end * 4; pos < length; pos++)
        {
          // determine value of source vec
          const int val = vec[pos];

          const uint64_t val1 = val & 2047;

          // set value in target buffer
          const int pos_low = thread_index[val1];  // no need to increment
          buffer[pos_low] = val;

          // this uses only the lower 11 bits for speed
          pair_vec[hash_pos++] = val1;
        }
      }
    }
  }

  t1 = cpu_cycles();
  std::cout << "count: " << (t1 - t0) / 10000 << "\n" << std::flush;

  // phase 1: bit 11 - 21 occurence count

  const auto buffer_pair_vec = reinterpret_cast<uint64_t*>(buffer);

  // phase 1: bit 0 - 10 occurence count
  count_occurrences2(nr_of_threads, index, buffer_pair_vec, quarter_length, batch_size, length, buffer);

  // phase 2: determine cumulative positions per thread

  cumulative_index(index, nr_of_threads);

  const auto uvec = reinterpret_cast<uint32_t*>(vec);

  // phase 2 bits 11-21: fill result vector

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 16 * ((static_cast<int>(thread * batch_size) + 15) / 16);
      const int pos_end = std::min(16 * ((static_cast<int>((thread + 1) * batch_size) + 15) / 16), quarter_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE];  // 1 kB

      // copy relevant index main memory
      const int index_start = THREAD_INDEX_INT_SIZE * thread;
      memcpy(thread_index, &index[index_start], THREAD_INDEX_INT_SIZE * 4);

      // iterate uint64_t values
      int hash_pos = pos_start;
      for (int pos = pos_start; pos < pos_end; pos++) {

        // first source vec
        uint64_t val = buffer_pair_vec[pos++];

        // set value in target buffer
        const uint64_t val1 = (val >> 11) & 2047;
        const uint64_t val2 = (val >> 43) & 2047;
        int pos_low  = thread_index[val1]++;  // byte 0
        int pos_high = thread_index[val2]++;  // byte 4

        uvec[pos_low]  =  val        & 4294967295;
        uvec[pos_high] = (val >> 32) & 4294967295;

        // determine value of source vec
        val = buffer_pair_vec[pos];  // no increment

        // set value in target buffer
        const uint64_t val3 = (val >> 11) & 2047;
        const uint64_t val4 = (val >> 43) & 2047;
        pos_low  = thread_index[val3]++;  // byte 0
        pos_high = thread_index[val4]++;  // byte 4

        uvec[pos_low]  = val & 4294967295;
        uvec[pos_high] = (val >> 32) & 4294967295;

        // overwrite buffer_pair_vec[pos] with values
        buffer_pair_vec[hash_pos++] = val1 | (val2 << 11) | (val3 << 22) | (val4 << 44);  // no increment
      }


      // check last elements in last thread
      if (thread == (nr_of_threads - 1)) {
        for (int pos = pos_end * 4; pos < length; pos++)
        {
          // determine value of source vec
          const int val = buffer[pos];

          const uint64_t val1 = (val >> 11) & 2047;

          // set value in target buffer
          const int pos_low = thread_index[val1];  // no need to increment
          uvec[pos_low] = val;

          // this uses only the lower 11 bits for speed
          buffer_pair_vec[hash_pos++] = val1;
        }
      }
    }
  }

  // count bit 22 - 31

  // phase 3: bit 22 - 31 occurence count
  count_occurrences3(nr_of_threads, index, pair_vec, quarter_length, batch_size, length, vec);

  // phase 3: determine cumulative positions per thread

  cumulative_index(index, nr_of_threads);

  // phase 3 bits 22-31: fill result vector

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 16 * ((static_cast<int>(thread * batch_size) + 15) / 16);
      const int pos_end = std::min(16 * ((static_cast<int>((thread + 1) * batch_size) + 15) / 16), quarter_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT_SIZE];  // 1 kB

      // copy relevant index main memory
      const int index_start = THREAD_INDEX_INT_SIZE * thread;
      memcpy(thread_index, &index[index_start], THREAD_INDEX_INT_SIZE * 4);

      // iterate uint64_t values
      int hash_pos = pos_start;
      for (int pos = pos_start; pos < pos_end; pos++) {

        // first source vec
        uint64_t val = pair_vec[pos++];

        // set value in target buffer
        int pos_low  = thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        int pos_high = thread_index[((val >> 54) & 1023) ^ 512]++;  // byte 4

        buffer_p[pos_low] = val & 4294967295;
        buffer_p[pos_high] = (val >> 32) & 4294967295;

        // determine value of source vec
        val = pair_vec[pos];  // no increment

        // set value in target buffer
        pos_low = thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        pos_high = thread_index[((val >> 54) & 1023) ^ 512]++;  // byte 4

        buffer_p[pos_low] = val & 4294967295;
        buffer_p[pos_high] = (val >> 32) & 4294967295;
      }

      // check last elements in last thread
      if (thread == (nr_of_threads - 1)) {
        for (int pos = pos_end * 4; pos < length; pos++)
        {
          // determine value of source vec
          const int val = vec[pos];

          const uint64_t val1 = ((val >> 22) & 1023) ^ 512;

          // set value in target buffer
          const int pos_low = thread_index[val1];  // no need to increment
          buffer[pos_low] = val;
        }
      }
    }
  }
}
