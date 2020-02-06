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

#include <sort/sort.h>
#include <interface/openmphelper.h>

#include <iostream>


// inline void radix_fill4(int* vec, int* buffer, int length, int index4[256])
// {
//   for (int pos = 0; pos < length; ++pos) {
//     const int value = buffer[pos];
//     const int target_pos = index4[((value >> 24) & 255) ^ 128]++;
//     vec[target_pos] = value;
//   }
// }


/*
 In place sorting of a logical vector

 buffer must have a (byte) size equal to vec
 
*/
void radix_ssort_logical(int* vec, int length)
{
  // phase 1: compact to bytes and count

  uint32_t* uvec = reinterpret_cast<uint32_t*>(vec);

  int nr_of_threads = 1;  // single threaded for small sizes

  if (length > 1024) {
    nr_of_threads = std::min(GetFstThreads(), 4);
  }

  const int batch_size = (length + nr_of_threads - 1) / nr_of_threads;
  int index[12] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

#pragma omp parallel num_threads(nr_of_threads)
  {
    // count occurrences of 0, 1, and NA
#pragma omp for
    for (int batch = 0; batch < nr_of_threads; batch++)
    {
      const int pos_next = std::min((batch + 1) * batch_size, length);
      const int index_nr = batch * 3;

      for (int pos = batch * batch_size; pos < pos_next; ++pos) {
        ++index[index_nr + ((uvec[pos] >> 30) | (uvec[pos] & 1)) & 3];
      }
    }
  }

  index[0] += index[3] + index[6] + index[9 ];
  index[1] += index[4] + index[7] + index[10];
  index[2] += index[5] + index[8] + index[11];

  if ((index[0] + index[1] + index[2]) != length) throw("Count error, fix code");

  int byte_na = (1 << 31);  // NA value

  for (int pos = 0; pos < index[2]; pos++) vec[pos] = byte_na;

  memset(&(vec[index[2]]), 0, sizeof(int) * index[0]);  // all zero's

  for (int pos = index[0] + index[2]; pos < length; pos++) vec[pos] = 1;
}

/*
 In place sorting of a logical vector

 buffer must have a (byte) size equal to vec

*/
void radix_msort_logical(int* vec, int length, int* buffer)
{
  int index[256];
  uint32_t index_values[256];

  // phase 1: compact to bytes and count

  // initialize
  for (int ind = 0; ind < 256; ++ind) {
    index[ind] = 0;
    index_values[ind] = 0;
  }

  uint32_t* uvec = reinterpret_cast<uint32_t*>(vec);

  // 4 bytes fit 16 logicals
  const int batch_length = length / 4;

  for (int pos = 0; pos < batch_length; ++pos) {
    const int ind = 4 * pos;

    // create hash in range 0 - 255
    const uint32_t compact =
      ((uvec[ind] >> 24) & 128) | ((uvec[ind]) & 1) |
      ((uvec[ind + 1] >> 25) & 64) | ((uvec[ind + 1] << 1) & 2) |
      ((uvec[ind + 2] >> 26) & 32) | ((uvec[ind + 2] << 2) & 4) |
      ((uvec[ind + 3] >> 27) & 16) | ((uvec[ind + 3] << 3) & 8);

    ++index[compact];
  }

  for (uint32_t val0 = 0; val0 < 3; ++val0)
  {
    for (uint32_t val1 = 0; val1 < 3; ++val1)
    {
      for (uint32_t val2 = 0; val2 < 3; ++val2)
      {
        for (uint32_t val3 = 0; val3 < 3; ++val3)
        {
          //const uint32_t val = (val3 << 6) | (val2 << 4) | (val1 << 2) | val0;
          //index[val] = 
        }
      }
    }
  }

  // int start_pos = 4 * batch_length;
  // int end_length = length - start_pos;
  // 
  // uint32_t compact = 0;
  // for (int pos = 0; pos < end_length; ++pos) {
  //   compact |=
  //     (uvec[start_pos + pos] >> 24 + pos) & (2 ** (7 - pos)) | (uvec[start_pos + pos] & (2 ** pos));
  // }

  // cumulative positions
  int cum_pos = index[0];

  index[0] = 0;

  // test for single populated bin
  int64_t sqr = static_cast<int64_t>(cum_pos)* static_cast<int64_t>(cum_pos);

  for (int ind = 1; ind < 256; ++ind) {

    int64_t old_val = index[ind];
    sqr += old_val * old_val;
    index[ind] = cum_pos;
    cum_pos += old_val;
  }

  // phase 2: determine counts of NA, TRUE and FALSE

}
