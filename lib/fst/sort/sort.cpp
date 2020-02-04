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

#include <sort/sort.h>


void fst_quick_sort(int* vec, int length, int pivot) {

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
    fst_quick_sort(vec, pos_left, piv);
  }
  else if (pos_left == 2 && vec[0] > vec[1]) {
    // swap first 2 elements
    const int tmp = vec[1];
    vec[1] = vec[0];
    vec[0] = tmp;
  }

  if (pos_left < (length - 2)) {
    const int piv = (vec[pos_left] + vec[(length + pos_left) / 2] + vec[length - 1]) / 3;
    fst_quick_sort(&vec[pos_left], length - pos_left, piv);
  } else if (pos_left == (length - 2) && vec[pos_left] > vec[pos_left + 1]) {
    // swap last 2 elements if in reverse order
    const int tmp = vec[pos_left];
    vec[pos_left] = vec[pos_left + 1];
    vec[pos_left + 1] = tmp;
  }
}


void fst_merge_sort(const int* left_p, const int* right_p, int length_left, int length_right, int* res_p) {

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

void fst_radix_sort(int* vec, int length, int* buffer)
{
  int index1[256];
  int index2[256];
  int index3[256];
  int index4[256];

  // phase 1: sort on lower byte

  // initialize
  for (int ind = 0; ind < 256; ++ind) {
    index1[ind] = 0;
    index2[ind] = 0;
    index3[ind] = 0;
    index4[ind] = 0;
  }

  // count each occurence
  const int batch_length = length / 8;

  for (int pos = 0; pos < batch_length; ++pos) {
    const int ind = 8 * pos;

    int val = vec[ind];
    ++index1[val & 255];
    ++index2[(val >> 8) & 255];
    ++index3[(val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 1];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 2];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 3];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 4];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 5];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 6];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];

    val = vec[ind + 7];
    ++index1[  val        & 255];
    ++index2[( val >> 8 )& 255];
    ++index3[( val >> 16) & 255];
    ++index4[((val >> 24) & 255) ^ 128];
  }

  for (int pos = 8 * batch_length; pos < length; ++pos) {
    ++index1[  vec[pos]        & 255];
    ++index2[( vec[pos] >> 8)  & 255];
    ++index3[( vec[pos] >> 16) & 255];
    ++index4[((vec[pos] >> 24) & 255) ^ 128];
  }


  // cumulative positions
  int cum_pos1 = index1[0];
  int cum_pos2 = index2[0];
  int cum_pos3 = index3[0];
  int cum_pos4 = index4[0];

  index1[0] = 0;
  index2[0] = 0;
  index3[0] = 0;
  index4[0] = 0;

  // test for single populated bin
  int64_t sqr1 = static_cast<int64_t>(cum_pos1) * static_cast<int64_t>(cum_pos1);
  int64_t sqr2 = static_cast<int64_t>(cum_pos2) * static_cast<int64_t>(cum_pos2);
  int64_t sqr3 = static_cast<int64_t>(cum_pos3) * static_cast<int64_t>(cum_pos3);
  int64_t sqr4 = static_cast<int64_t>(cum_pos4) * static_cast<int64_t>(cum_pos4);

  for (int ind = 1; ind < 256; ++ind) {

    int64_t old_val = index1[ind];
    sqr1 += old_val * old_val;
    index1[ind] = cum_pos1;
    cum_pos1 += old_val;

    old_val = index2[ind];
    sqr2 += old_val * old_val;
    index2[ind] = cum_pos2;
    cum_pos2 += old_val;

    old_val = index3[ind];
    sqr3 += old_val * old_val;
    index3[ind] = cum_pos3;
    cum_pos3 += old_val;

    old_val = index4[ind];
    sqr4 += old_val * old_val;
    index4[ind] = cum_pos4;
    cum_pos4 += old_val;
  }

  // phase 1: sort on byte 1

  const int64_t single_bin_size = static_cast<int64_t>(length) * static_cast<int64_t>(length);

  if (sqr1 != single_bin_size) {
    radix_fill1(buffer, vec, length, index1);
  } else {  // a single populated bin
    memcpy(buffer, vec, length * sizeof(int));
  }

  // phase 2: sort on byte 2

  if (sqr2 != single_bin_size) {
    radix_fill2(vec, buffer, length, index2);
  } else {  // a single populated bin
    memcpy(vec, buffer, length * sizeof(int));
  }

  // phase 3: sort on byte 3

  if (sqr3 != single_bin_size) {
    radix_fill3(buffer, vec, length, index3);
  } else {  // a single populated bin
    memcpy(buffer, vec, length * sizeof(int));
  }

  // phase 4: sort on byte 3

  if (sqr4 != single_bin_size) {
    radix_fill4(vec, buffer, length, index4);
  } else {  // a single populated bin
    memcpy(vec, buffer, length * sizeof(int));
  }
}
