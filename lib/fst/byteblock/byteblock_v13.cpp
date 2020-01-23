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


// header info
//
// [0 - 3] is_compressed :
//     bit 0: compression
// [4 - 7] block_size_char (uint32_t)

#define BYTE_BLOCK_HEADER_SIZE 8



#include <byteblock/byteblock_v13.h>
#include <interface/ibyteblockwriter.h>
#include <interface/fstdefines.h>

// #include <compression/compressor.h>

using namespace std;


inline uint64_t StoreByteBlock_v13(ofstream& fst_file, IByteBlockWriter* byte_block_writer, uint64_t startCount, uint64_t endCount)
{
  //byte_block_writer->SetBuffersFromVec(startCount, endCount);

  //unsigned int nrOfElements = endCount - startCount; // the string at position endCount is not included
  //unsigned int nrOfNAInts = 1 + nrOfElements / 32; // add 1 bit for NA present flag

  //fst_file.write(reinterpret_cast<char*>(byte_block_writer->strSizes), nrOfElements * 4); // write string lengths
  //fst_file.write(reinterpret_cast<char*>(byte_block_writer->naInts), nrOfNAInts * 4); // write string lengths

  //unsigned int totSize = byte_block_writer->bufSize;

  //fst_file.write(byte_block_writer->activeBuf, totSize);

  return 4;
}


// thread plan
//
// The main thread is reserved to call a user method before compression
// All but the first and last thread compress data blocks
// The last thread writes data to disk after compression


/**
 * \brief write byteblock to file
 * 
 * \param fst_file stream object to write to
 * \param byte_block_writer writer to get data from the column vector
 * \param nr_of_rows of the column vector
 * \param compression compression setting, value between 0 and 100
*/
void fdsWriteByteBlockVec_v13(ofstream& fst_file, IByteBlockWriter* byte_block_writer, uint64_t nr_of_rows, uint32_t compression)
{
  // nothing to write
  if (nr_of_rows == 0) return;

  uint64_t cur_pos = fst_file.tellp();
  uint64_t nr_of_blocks = (nr_of_rows - 1) / BLOCKSIZE_BYTE_BLOCK; // number of blocks minus 1

  if (compression == 0)
  {
    uint32_t meta_size = BYTE_BLOCK_HEADER_SIZE + (nr_of_blocks + 1) * 8;  // one pointer per block address

    // first BYTE_BLOCK_HEADER_SIZE bytes store compression setting and block size
    std::unique_ptr<char[]> metaP(new char[meta_size]);
    char* meta = metaP.get();

    // clear memory for safety
    memset(meta, 0, meta_size);

    // Set column header
    uint32_t* is_compressed = reinterpret_cast<uint32_t*>(meta);
    uint32_t* block_size_char = reinterpret_cast<uint32_t*>(&meta[4]);

    *block_size_char = BLOCKSIZE_BYTE_BLOCK; // size 2048 blocks
    *is_compressed = 0;

    fst_file.write(meta, meta_size); // write metadata

    uint64_t* block_pos = reinterpret_cast<unsigned long long*>(&meta[BYTE_BLOCK_HEADER_SIZE]);
    uint64_t full_size = meta_size;

    // complete blocks
    for (uint64_t block = 0; block < nr_of_blocks; ++block)
    {
      uint64_t totSize = StoreByteBlock_v13(fst_file, byte_block_writer, block * BLOCKSIZE_BYTE_BLOCK, (block + 1) * BLOCKSIZE_BYTE_BLOCK);
      full_size += totSize;
      block_pos[block] = full_size;
    }

    uint64_t totSize = StoreByteBlock_v13(fst_file, byte_block_writer, nr_of_blocks * BLOCKSIZE_BYTE_BLOCK, nr_of_rows);
    full_size += totSize;
    block_pos[nr_of_blocks] = full_size;

    fst_file.seekp(cur_pos + BYTE_BLOCK_HEADER_SIZE);
    fst_file.write(reinterpret_cast<char*>(block_pos), (nr_of_blocks + 1) * 8); // additional zero for index convenience
    fst_file.seekp(cur_pos + full_size); // back to end of file

    return;
  }
}
