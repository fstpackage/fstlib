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


#include <cstdint>   // int64_t
#include <cstring>   // memcpy
#include <algorithm> // max
#include <stdexcept>

#include <sort/sort.h>
#include <interface/openmphelper.h>

#include <iostream>


#define MAX_SORT_THREADS 32
#define LOGICAL_NA 2
#define LOGICAL_FALSE 0
#define LOGICAL_TRUE 1


/**
 * \brief Counts occurrences of NA, 0 and 1 in logical vector 'vec'.
 * 
 * \param vec a logical vector to be counted
 * \param length length of 'vec'
 * \param index count vector for each possible value and each thread, counts are for FALSE,
 * TRUE and NA (in that order).
 * \param nr_of_threads number of threads to use for counting
 * \param batch_size size of each thread batch
 */
inline void count_logical(int* vec, const int length, int index[3 * MAX_SORT_THREADS], const int nr_of_threads)
{
  const auto pair_vec = reinterpret_cast<uint64_t*>(vec);
  const int half_length = length / 2;
  const double batch_size = static_cast<double>(half_length + 0.01) / static_cast<double>(nr_of_threads);

#pragma omp parallel num_threads(nr_of_threads)
  {
    // count occurrences of 0, 1, and NA
#pragma omp for
    for (int batch = 0; batch < nr_of_threads; batch++) {
      int pos = ((static_cast<int>(batch * batch_size) + 127) / 128) * 128;
      int pos_next = ((static_cast<int>((batch + 1) * batch_size) + 127) / 128) * 128;
      pos_next = std::min(pos_next, half_length);  // last batch is capped

      int partial_index[4] = { 0 };  // last element should have zero counts

      for (; pos < pos_next; pos++) {
        ++partial_index[((pair_vec[pos] >> 30) | (pair_vec[pos] & 1)) & 3];
        ++partial_index[((pair_vec[pos] >> 62) | ((pair_vec[pos] >> 32) & 1)) & 3];
      }

      index[batch * 3] = partial_index[0];
      index[batch * 3 + 1] = partial_index[1];
      index[batch * 3 + 2] = partial_index[2];
    }
  }

  if (length % 2 == 1) {  // add last element to last batch
    const auto last_element = reinterpret_cast<uint32_t*>(vec)[length - 1];
    ++index[3 * (nr_of_threads - 1) + (((last_element >> 30) | (last_element & 1)) & 3)];
  }
}


/**
 * \brief Sorts a logical vector 'vec' using counts of the occurrences of NA, FALSE and TRUE
 * 
 * \param vec a logical vector to be sorted
 * \param length length of 'vec'
 * \param nr_of_threads number of threads that were used for counting
 * \param counts count vector containing counts for FALSE, TRUE and NA (in that order)
 */
inline void fill_logical(int* vec, const int length, const int index[3 * MAX_SORT_THREADS], const int nr_of_threads)
{
  int counts[3] = { 0 };

  for (int batch = 0; batch < nr_of_threads; batch++) {
    counts[LOGICAL_FALSE] += index[3 * batch];
    counts[LOGICAL_TRUE] += index[3 * batch + 1];
    counts[LOGICAL_NA] += index[3 * batch + 2];
  }

  if ((counts[LOGICAL_FALSE] + counts[LOGICAL_TRUE] + counts[LOGICAL_NA]) != length) {
    throw std::runtime_error("non-logical elements detected in vector");
  }

  const int byte_na = (1 << 31);  // NA value

  for (int pos = 0; pos < counts[LOGICAL_NA]; pos++) vec[pos] = byte_na;

  memset(&(vec[counts[LOGICAL_NA]]), 0, sizeof(int) * counts[LOGICAL_FALSE]);  // all zero's

  for (int pos = counts[LOGICAL_FALSE] + counts[LOGICAL_NA]; pos < length; pos++) vec[pos] = 1;
}


/**
 * \brief Sorts a logical vector
 * \param vec logical vector to be sorted
 * \param length number of logical (4 byte) elements
 */
void radix_sort_logical(int* vec, int length)
{
  // phase 1: count number of occurrences

  int nr_of_threads = 1;  // single threaded for small sizes

  // algorithm to determine optimal threads

  if (length > 1048576) {
    nr_of_threads = std::min(GetFstThreads(), MAX_SORT_THREADS);
  }
  else if (length > 131072) {
    nr_of_threads = 2;
  }

  // algorithm to determine optimal threads
  int index[3 * MAX_SORT_THREADS] = { 0 };  // a logical can have 3 possible values

  // count occurrences
  count_logical(vec, length, index, nr_of_threads);

  fill_logical(vec, length, index, nr_of_threads);
}

/**
 * \brief Sorts a logical vector 'vec' in ascending order. An integer vector 'order' will be rearranged in exactly
 * the same manner. If parameter 'default_order' is set to true, the 'order' vector will be assumed to contain
 * the (ordered) range 0 to (length - 1), speeding up the sorting process.
 * \param vec logical vector to be sorted
 * \param length number of logical (4 byte) elements
 * \param order order of elements in 'vec', will contain each value in the range 0 to (length - 1)
 * \param order_out resulting reordered vector 'order', with identical permutations as 'cev'
 * \param default_order if true will assume parameter 'order' to contain the range 0 to (length - 1). This reduces
 * computational complexity and will speed up sorting.
 */
void radix_sort_logical_order(int* vec, const int length, int* order, int* order_out, const bool default_order)
{
  // phase 1: count number of occurrences

  int nr_of_threads = 1;  // single threaded for small sizes

  // algorithm to determine optimal threads

  if (length > 1048576) {
    nr_of_threads = std::min(GetFstThreads(), MAX_SORT_THREADS);
  }
  else if (length > 131072) {
    nr_of_threads = 2;
  }

  // algorithm to determine optimal threads
  int index[3 * MAX_SORT_THREADS] = { 0 };  // counter for each of the 3 possible values of a logical

  count_logical(vec, length, index, nr_of_threads);

  // phase 2: populate 'order' vector using multiple threads

  int total_count = 0;  // total counts for each value

  // NA's are first in order
  for (int thread = 0; thread < nr_of_threads; thread++)
  {
    const int pos = LOGICAL_NA + thread * 3;

    const int tmp_value = index[pos];
    index[pos] = total_count;
    total_count += tmp_value;
  }

  // FALSE values are second in order
  for (int thread = 0; thread < nr_of_threads; thread++)
  {
    const int pos = LOGICAL_FALSE + thread * 3;

    const int tmp_value = index[pos];
    index[pos] = total_count;
    total_count += tmp_value;
  }

  // TRUE values are last in order
  for (int thread = 0; thread < nr_of_threads; thread++)
  {
    const int pos = LOGICAL_TRUE + thread * 3;

    const int tmp_value = index[pos];
    index[pos] = total_count;
    total_count += tmp_value;
  }

  // with the 'default_order' parameter set to true, no actual element switches are required in that vector
  // because we can just use the iterator counter for 'vec' to populate the sorted 'order' vector.

  if (default_order)
  {
    const auto pair_vec = reinterpret_cast<uint64_t*>(vec);
    const int half_length = length / 2;
    const double batch_size = static_cast<double>(half_length + 0.01) / static_cast<double>(nr_of_threads);

#pragma omp parallel num_threads(nr_of_threads)
    {
      // count occurrences of 0, 1, and NA
#pragma omp for
      for (int batch = 0; batch < nr_of_threads; batch++) {

        // determine range for this thread
        int pos = ((static_cast<int>(batch * batch_size) + 127) / 128) * 128;
        int pos_next = ((static_cast<int>((batch + 1) * batch_size) + 127) / 128) * 128;
        pos_next = std::min(pos_next, half_length);  // last batch is capped

        // cache local copy for better performance
        int partial_index[4] = { 0 };  // last element should have zero counts
        partial_index[LOGICAL_NA   ] = index[batch * 3 + LOGICAL_NA   ];
        partial_index[LOGICAL_FALSE] = index[batch * 3 + LOGICAL_FALSE];
        partial_index[LOGICAL_TRUE ] = index[batch * 3 + LOGICAL_TRUE ];

        int pos_int = 2 * pos;

        for (; pos < pos_next; pos++) {
          int value = ((pair_vec[pos] >> 30) | (pair_vec[pos] & 1)) & 3;
          order_out[partial_index[value]++] = pos_int++;

          value = ((pair_vec[pos] >> 62) | ((pair_vec[pos] >> 32) & 1)) & 3;
          order_out[partial_index[value]++] = pos_int++;
        }

        // last element
        if (batch == (nr_of_threads - 1) && length % 2 == 1) {
          const auto last_element = reinterpret_cast<uint32_t*>(vec)[length - 1];
          const int value = ((last_element >> 30) | (last_element & 1)) & 3;
          order_out[partial_index[value]] = pos_int;
        }

      }
    }
  }

  fill_logical(vec, length, index, nr_of_threads);
}
