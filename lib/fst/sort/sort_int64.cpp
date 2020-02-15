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


inline void cumulative_index(int* index, int nr_of_threads)
{
  int tot_count = 0;

  for (int ind = 0; ind < THREAD_INDEX_INT64_SIZE; ++ind) {

    // set counters
    int pos = ind;

    for (int thread = 0; thread < nr_of_threads; thread++)
    {
      const int tmp = index[pos];
      index[pos] = tot_count;
      tot_count += tmp;
      pos += THREAD_INDEX_INT64_SIZE;
    }
  }
}


void radix_sort_int(uint64_t* vec, const int length, uint64_t* buffer)
{
  int nr_of_threads = 1;  // single threaded for small sizes
 
// set optimal number of threads (determined from empirical measurements)

  if (length > 1024) {
    nr_of_threads = std::min(GetFstThreads(), MAX_INT64_SORT_THREADS);
  }

  // index for all threads (on heap)
  auto index_p = static_cast<std::unique_ptr<int[]>>(new int[THREAD_INDEX_INT64_SIZE * MAX_INT64_SORT_THREADS]);
  int* index = index_p.get();

  const double batch_size = static_cast<double>(length + 0.01) / static_cast<double>(nr_of_threads);

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), length);

      // local cache index
      int thread_index[THREAD_INDEX_INT64_SIZE] = { 0 };  // 8 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // get source value
        const uint64_t val = vec[pos];

        thread_index[val & 2047]++;  // byte 0
      }
      
      // to main memory
      memcpy(&index[THREAD_INDEX_INT64_SIZE * thread], &thread_index, 4 * THREAD_INDEX_INT64_SIZE);
    }
  }

  // phase 1: byte0 occurence count

  // last element is added to last thread counts
  if (length % 2 == 1) {
    const auto val = vec[length - 1];
    const int offset = THREAD_INDEX_INT64_SIZE * (nr_of_threads - 1);

    index[offset + (val & 2047)]++;  // byte 0
  }

  // phase 2: determine cumulative positions per thread
  cumulative_index(index, nr_of_threads);

  // phase 3: fill buffer
  // the batch size for each thread are exactly equal to the sizes in the counting phase

  // sort on byte0 and fill buffer

  auto buffer_p = reinterpret_cast<uint32_t*>(buffer);

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT64_SIZE];  // 1 kB

      // copy relevant index main memory
      int index_start = THREAD_INDEX_INT64_SIZE * thread;
      for (int pos = 0; pos < THREAD_INDEX_INT64_SIZE; pos++) {
        thread_index[pos] = index[index_start++];
      }

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // determine value of source vec
        const uint64_t val = pair_vec[pos];

        // set value in target buffer
        // TODO: combine in single statement
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
      int thread_index[THREAD_INDEX_INT64_SIZE] = { 0 };  // 1 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {
        const uint64_t val = buffer_pair_vec[pos];

        thread_index[(val >> 11) & 2047]++;  // byte 0
        thread_index[(val >> 43) & 2047]++;  // byte 4
      }

      // to main memory
      const int offset = THREAD_INDEX_INT64_SIZE * thread;
      for (int pos = 0; pos < THREAD_INDEX_INT64_SIZE; pos++)
      {
        index[offset + pos] = thread_index[pos];
      }
    }
  }

  // last element is added to last thread counts
  if (length % 2 == 1) {
    const auto val = static_cast<uint32_t>(buffer[length - 1]);
    const int offset = THREAD_INDEX_INT64_SIZE * (nr_of_threads - 1);

    index[offset + ((val >> 11) & 2047)]++;  // byte 0
  }

  // phase 2: determine cumulative positions per thread

  cumulative_index(index, nr_of_threads);

  auto uvec = reinterpret_cast<uint32_t*>(vec);

  // phase 2 bits 11-21: fill result vector

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT64_SIZE];  // 1 kB

      // copy relevant index main memory
      int index_start = THREAD_INDEX_INT64_SIZE * thread;
      for (int pos = 0; pos < THREAD_INDEX_INT64_SIZE; pos++) {
        thread_index[pos] = index[index_start++];
      }

      // TODO: some more loop unwinding

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // determine value of source vec
        const uint64_t val = buffer_pair_vec[pos];

        // set value in target buffer
        // TODO: combine in single statement
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
          // TODO: combine in single statement
          const int pos_low = thread_index[(val >> 11) & 2047];  // no need to increment
          uvec[pos_low] = val;
        }
      }
    }
  }

  // bit 22 - 31

#pragma omp parallel num_threads(nr_of_threads)
  {
#pragma omp for
    for (int thread = 0; thread < nr_of_threads; thread++) {

      // range
      const int pos_start = 32 * ((static_cast<int>(thread * batch_size) + 31) / 32);
      const int pos_end = std::min(32 * ((static_cast<int>((thread + 1) * batch_size) + 31) / 32), half_length);

      // local cache index
      int thread_index[THREAD_INDEX_INT64_SIZE] = { 0 };  // 1 kB

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {
        const uint64_t val = pair_vec[pos];

        thread_index[((val >> 22) & 1023) ^ 512]++;  // byte 0
        thread_index[((val >> 54) & 1023) ^ 512]++;  // byte 4
      }

      // to main memory
      const int offset = THREAD_INDEX_INT64_SIZE * thread;
      for (int pos = 0; pos < THREAD_INDEX_INT64_SIZE; pos++)
      {
        index[offset + pos] = thread_index[pos];
      }
    }
  }

  // last element is added to last thread counts
  if (length % 2 == 1) {
    const auto val = static_cast<uint32_t>(vec[length - 1]);
    const int offset = THREAD_INDEX_INT64_SIZE * (nr_of_threads - 1);

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
      int thread_index[THREAD_INDEX_INT64_SIZE];  // 1 kB

      // copy relevant index main memory
      int index_start = THREAD_INDEX_INT64_SIZE * thread;
      for (int pos = 0; pos < THREAD_INDEX_INT64_SIZE; pos++) {
        thread_index[pos] = index[index_start++];
      }

      // TODO: some more loop unwinding

      // iterate uint64_t values
      for (int pos = pos_start; pos < pos_end; pos++) {

        // determine value of source vec
        const uint64_t val = pair_vec[pos];

        // set value in target buffer
        // TODO: combine in single statement
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
          // TODO: combine in single statement
          const int pos_low = thread_index[((val >> 22) & 1023) ^ 512];  // no need to increment
          buffer[pos_low] = val;
        }
      }
    }
  }
}
