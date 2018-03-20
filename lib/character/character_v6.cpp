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

#include "character/character_v6.h"
#include "interface/istringwriter.h"
#include "interface/fstdefines.h"
#include "interface/openmphelper.h"
#include <compression/compressor.h>

#include <fstream>
#include <memory>
#include <algorithm>
#include <cstring>
#include <unordered_map>

using namespace std;


inline unsigned int StoreCharBlock_v6(ofstream& myfile, IStringWriter* blockRunner, unsigned long long startCount, unsigned long long endCount)
{
  blockRunner->SetBuffersFromVec(startCount, endCount);

  const unsigned int nrOfElements = endCount - startCount; // the string at position endCount is not included
  const unsigned int nrOfNAInts = 1 + nrOfElements / 32; // add 1 bit for NA present flag

  myfile.write(reinterpret_cast<char*>(blockRunner->strSizes), nrOfElements * 4); // write string lengths
  myfile.write(reinterpret_cast<char*>(blockRunner->naInts), nrOfNAInts * 4); // write string lengths

  const unsigned int totSize = blockRunner->bufSize;

  myfile.write(blockRunner->activeBuf, totSize);

  return totSize + (nrOfElements + nrOfNAInts) * 4;
}


inline unsigned int storeCharBlockCompressed_v6(ofstream& myfile, IStringWriter* blockRunner, unsigned int startCount,
  unsigned int endCount, StreamCompressor* intCompressor, StreamCompressor* charCompressor, unsigned short int& algoInt,
  unsigned short int& algoChar, int& intBufSize, int blockNr)
{
  // Determine string lengths
  const unsigned int nrOfElements = endCount - startCount; // the string at position endCount is not included
  const unsigned int nrOfNAInts = 1 + nrOfElements / 32; // add 1 bit for NA present flag


  // Compress string size vector
  const unsigned int strSizesBufLength = nrOfElements * 4;

  // 1) Use stack buffer here !!!!!!
  // 2) compress to 1 or 2 bytes if possible with strSizesBufLength
  const int bufSize = intCompressor->CompressBufferSize(strSizesBufLength); // 1 integer per string

  std::unique_ptr<char[]> intBufP(new char[bufSize]);
  char* intBuf = intBufP.get();

  CompAlgo compAlgorithm;
  intBufSize = intCompressor->Compress(reinterpret_cast<char*>(blockRunner->strSizes), strSizesBufLength, intBuf, compAlgorithm, blockNr);
  myfile.write(intBuf, intBufSize);

  //intCompressor->WriteBlock(myfile, (char*)(stringWriter->strSizes), intBuf);
  algoInt = static_cast<unsigned short int>(compAlgorithm); // store selected algorithm

  // Write NA bits uncompressed (add compression later ?)
  myfile.write(reinterpret_cast<char*>(blockRunner->naInts), nrOfNAInts * 4); // write string lengths

  const unsigned int totSize = blockRunner->bufSize;

  const int compBufSize = charCompressor->CompressBufferSize(totSize);

  std::unique_ptr<char[]> compBufP(new char[compBufSize]);
  char* compBuf = compBufP.get();

  // Compress buffer
  const int resSize = charCompressor->Compress(blockRunner->activeBuf, totSize, compBuf, compAlgorithm, blockNr);
  myfile.write(compBuf, resSize);

  algoChar = static_cast<unsigned short int>(compAlgorithm); // store selected algorithm

  return nrOfNAInts * 4 + resSize + intBufSize;
}


void fdsWriteCharVec_v6(ofstream& myfile, IStringWriter* stringWriter, int compression, StringEncoding stringEncoding)
{
  const unsigned long long vec_length = stringWriter->vecLength; // expected to be larger than zero

  // mark file position
  const unsigned long long curPos = myfile.tellp();

  if (vec_length < 1)
  {
    throw std::runtime_error("must be at least 1 element");
  }

  // total number of blocks to be processed (note: imposes limit of 4e12 rows)
  const int nr_of_blocks = 1 + (vec_length - 1) / BLOCKSIZE_CHAR;

  // correct threads used for low number of blocks
  const int nr_of_threads = std::min(nr_of_blocks, GetFstThreads());

  // determine number of blocks per job and number of jobs
  const int blocks_per_job = std::min(1 + (nr_of_blocks - 1) / nr_of_threads, BATCH_SIZE_WRITE_CHAR);
  const int nr_of_jobs = 1 + (nr_of_blocks - 1) / blocks_per_job;

  // total size of all batches for one thread
  unsigned int max_batch_sizes[STD_MAX_CHAR_THREADS];
  unsigned int max_block_sizes[STD_MAX_CHAR_THREADS];

  // thread buffers
  char* thread_buffer[STD_MAX_CHAR_THREADS]; // store batch of compressed blocks
  char* block_buffer[STD_MAX_CHAR_THREADS];  // store single uncompressed block

  for (int thread_id = 0; thread_id < nr_of_threads; thread_id++)
  {
    max_batch_sizes[thread_id] = 0;
    max_block_sizes[thread_id] = 0;

    thread_buffer[thread_id] = nullptr;
    block_buffer[thread_id] = nullptr;
  }

  // column meta data
  // first CHAR_HEADER_SIZE bytes store compression setting and block size
  const unsigned int meta_size = CHAR_HEADER_SIZE + nr_of_blocks * 8;
  std::unique_ptr<char[]> metaP(new char[meta_size]);
  char* meta = metaP.get();
  const auto block_pos = reinterpret_cast<unsigned long long*>(&meta[CHAR_HEADER_SIZE]);

  const unsigned int nr_of_na_ints = 1 + BLOCKSIZE_CHAR / 32; // add 1 bit for NA present flag
  const unsigned int str_sizes_block_size = BLOCKSIZE_CHAR + nr_of_na_ints;
  const unsigned int str_sizes_batch_size = blocks_per_job * (BLOCKSIZE_CHAR + nr_of_na_ints);

  std::unique_ptr<unsigned int[]> str_sizes_bufP(new unsigned int[nr_of_threads * blocks_per_job * (BLOCKSIZE_CHAR + nr_of_na_ints)]);
  unsigned int* str_sizes_buf = str_sizes_bufP.get();

  // Set column header
  const auto is_compressed = reinterpret_cast<unsigned int*>(meta);
  const auto block_size_char = reinterpret_cast<unsigned int*>(&meta[4]);
  *block_size_char = BLOCKSIZE_CHAR; // number of elements in a single block

  //Compressor* compress1 = nullptr;
  //Compressor* compress2 = nullptr;
  //StreamCompressor* stream_compressor = nullptr;

  if (compression == 0)
  {
    *is_compressed = stringEncoding << 1;  // bit 1 and 2 used for character encoding
  }
  else
  {
    //*is_compressed = (stringEncoding << 1) | 1; // set compression flag
    *is_compressed = stringEncoding << 1;  // bit 1 and 2 used for character encoding

    //if (compression <= 50)  // low compression: linear mix of uncompressed and LZ4_SHUF
    //{
    //  compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
    //  stream_compressor = new StreamLinearCompressor(compress1, 2 * compression);
    //}

    //compress1 = new SingleCompressor(CompAlgo::LZ4_SHUF4, 0);
    //compress2 = new SingleCompressor(CompAlgo::ZSTD_SHUF4, 2 * (compression - 50));
    //stream_compressor = new StreamCompositeCompressor(compress1, compress2, 2 * (compression - 50));
  }

  myfile.write(meta, meta_size); // write block offset index

  unsigned long long column_size = meta_size;

  // Start of parallel region

  #pragma omp parallel num_threads(nr_of_threads) shared(column_size)
  {
    #pragma omp for ordered schedule(static, 1)
    for (int job_nr = 0; job_nr < nr_of_jobs; job_nr++)
    {
      // each job contains several blocks to process. The grouping is done to
      // write larger chunks of data to disk in the end to avoid firing to
      // many IO requests to the storage medium

      // thread specific buffers and counters
      const int cur_thread = CurrentFstThread();
      unsigned int tot_batch_size = 0;  // required compression buffer size for this batch
      const int start_block = job_nr * blocks_per_job;
      int str_sizes_counter = cur_thread * str_sizes_batch_size;
      unsigned int max_block_size = 0;

      int block_sizes[BATCH_SIZE_WRITE_CHAR];

      const int end_block = std::min(start_block + blocks_per_job, nr_of_blocks);

      for (int block_nr = start_block; block_nr < end_block; block_nr++)
      {
        const int cur_nr_of_elements = std::min(vec_length, static_cast<unsigned long long>(block_nr + 1) * BLOCKSIZE_CHAR) -
          block_nr * BLOCKSIZE_CHAR;
        const unsigned int na_int_length = 1 + cur_nr_of_elements / 32; // add 1 bit for NA present flag
        const unsigned int cur_na_length = 4 * (cur_nr_of_elements + na_int_length);

        const unsigned int cur_block_size = stringWriter->CalculateSizes(block_nr * BLOCKSIZE_CHAR, cur_nr_of_elements,
          &str_sizes_buf[str_sizes_counter]);

        block_sizes[block_nr - start_block] = cur_block_size + cur_na_length;
        str_sizes_counter += str_sizes_block_size;  // not used anymore after last block

        // retain largest block size
        if (cur_block_size > max_block_size)
        {
          max_block_size = cur_block_size;
        }

        // add block size to batch size
        // compress sizes buffer into batch buffer
        tot_batch_size += cur_block_size + cur_na_length;
      }

      // now we know the total batch size (in number of characters)

      // check if we have enough memory to serialize a single block
      if (max_block_size > max_block_sizes[cur_thread])
      {
        // delete previously allocated memory (if any)
        if (max_block_sizes[cur_thread] > 0)
        {
          delete[] block_buffer[cur_thread];
        }

        max_block_sizes[cur_thread] = static_cast<int>(max_block_size * 1.1);  // 10 percent over allocation

        // allocate larger block buffer
        block_buffer[cur_thread] = new char[max_block_sizes[cur_thread]];
      }

      // check if we have enough buffer memory to compress batch
      if (tot_batch_size > max_batch_sizes[cur_thread])
      {
        // delete previously allocated memory (if any)
        if (max_batch_sizes[cur_thread] > 0)
        {
          delete[] thread_buffer[cur_thread];
        }

        const auto new_buffer_size = static_cast<int>(tot_batch_size * 1.1);
        max_batch_sizes[cur_thread] = new_buffer_size;  // 10 percent over allocation

        // allocate larger thread buffer
        char* bufferP = new char[new_buffer_size];
        thread_buffer[cur_thread] = bufferP;
      }

      // Now read from memory and serialize into thread buffer.
      // Then compress buffer and write to disk.

      str_sizes_counter = cur_thread * str_sizes_batch_size;
      tot_batch_size = 0;
      char* cur_thread_buffer = thread_buffer[cur_thread];

      // rerun each element, serialize sting contents and compress into target buffer
      for (int block_nr = start_block; block_nr < end_block; block_nr++)
      {
        const int cur_nr_of_elements = std::min(vec_length, static_cast<unsigned long long>(block_nr + 1) * BLOCKSIZE_CHAR) -
          block_nr * BLOCKSIZE_CHAR;

        const int start_pos = block_nr * BLOCKSIZE_CHAR;
        char* block_buf = block_buffer[cur_thread];
        const int cur_block_size = block_sizes[block_nr - start_block];

        // compress sizes buffer into batch buffer
        const unsigned int na_int_length = 1 + cur_nr_of_elements / 32; // add 1 bit for NA present flag
        const unsigned int cur_na_length = 4 * (cur_nr_of_elements + na_int_length);

        // compress and determine compressed size
        //CompAlgo algo;
        //int compressed_size = stream_compressor->Compress(reinterpret_cast<char*>(&str_sizes_buf[str_sizes_counter]), cur_na_length * 4,
        //  &cur_thread_buffer[tot_batch_size], algo, block_nr);

        memcpy(&cur_thread_buffer[tot_batch_size], &str_sizes_buf[str_sizes_counter], cur_na_length);

        stringWriter->SerializeCharBlock(start_pos, cur_nr_of_elements, &str_sizes_buf[str_sizes_counter], block_buf);
        str_sizes_counter += str_sizes_block_size;

        // compress block buffer into batch buffer
        memcpy(&cur_thread_buffer[tot_batch_size + cur_na_length], block_buf, cur_block_size - cur_na_length);
        tot_batch_size += cur_block_size;
      }

      // write data to disk
      #pragma omp ordered
      {
        myfile.write(cur_thread_buffer, tot_batch_size); // additional zero for index convenience

        // update block positions serially
        for (int block_nr = start_block; block_nr < end_block; block_nr++)
        {
          column_size += block_sizes[block_nr - start_block];
          block_pos[block_nr] = column_size;
        }
      }
    }
  }

  myfile.seekp(curPos + CHAR_HEADER_SIZE);
  myfile.write(reinterpret_cast<char*>(block_pos), nr_of_blocks * 8); // additional zero for index convenience
  myfile.seekp(0, ios_base::end);  // back to end of file

  for (int thread_id = 0; thread_id < nr_of_threads; thread_id++)
  {
    char* thread_bufferP = thread_buffer[thread_id];
    delete[] thread_bufferP;

    char* block_bufferP = block_buffer[thread_id];
    delete[] block_bufferP;
  }

  return;
}

  //////////////////////////////////////


  // Use compression


  //// Set column header
  //const auto isCompressed = reinterpret_cast<unsigned int*>(meta);
  //const auto blockSizeChar = reinterpret_cast<unsigned int*>(&meta[4]);
  //*blockSizeChar = BLOCKSIZE_CHAR;
  //*isCompressed = (stringEncoding << 1) | 1; // set compression flag

  //myfile.write(meta, metaSize); // write block offset and algorithm index

  //char* blockP = &meta[CHAR_HEADER_SIZE];

  //unsigned long long fullSize = metaSize;

  //// Compressors
  //Compressor* compressInt;
  //Compressor* compressInt2 = nullptr;
  //StreamCompressor* streamCompressInt;
  //Compressor* compressChar;
  //Compressor* compressChar2 = nullptr;
  //StreamCompressor* streamCompressChar;

  //// Compression settings
  //if (compression <= 50)
  //{
  //  // Integer vector compressor
  //  compressInt = new SingleCompressor(LZ4_SHUF4, 0);
  //  streamCompressInt = new StreamLinearCompressor(compressInt, 2 * compression);

  //  // Character vector compressor
  //  compressChar = new SingleCompressor(LZ4, 20);
  //  streamCompressChar = new StreamLinearCompressor(compressChar, 2 * compression); // unknown blockSize
  //}
  //else // 51 - 100
  //{
  //  // Integer vector compressor
  //  compressInt = new SingleCompressor(LZ4_SHUF4, 0);
  //  compressInt2 = new SingleCompressor(ZSTD_SHUF4, 0);
  //  streamCompressInt = new StreamCompositeCompressor(compressInt, compressInt2, 2 * (compression - 50));

  //  // Character vector compressor
  //  compressChar = new SingleCompressor(LZ4, 20);
  //  compressChar2 = new SingleCompressor(ZSTD, 20);
  //  streamCompressChar = new StreamCompositeCompressor(compressChar, compressChar2, 2 * (compression - 50));
  //}

  //for (unsigned long long block = 0; block < nrOfBlocks; ++block)
  //{
  //  const auto blockPos = reinterpret_cast<unsigned long long*>(blockP);
  //  const auto algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  //  const auto algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  //  const auto intBufSize = reinterpret_cast<int*>(blockP + 12);

  //  stringWriter->SetBuffersFromVec(block * BLOCKSIZE_CHAR, (block + 1) * BLOCKSIZE_CHAR);
  //  unsigned long long totSize = storeCharBlockCompressed_v6(myfile, stringWriter, block * BLOCKSIZE_CHAR,
  //                                                           (block + 1) * BLOCKSIZE_CHAR, streamCompressInt, streamCompressChar, *algoInt, *algoChar, *intBufSize, block);

  //  fullSize += totSize;
  //  *blockPos = fullSize;
  //  blockP += CHAR_INDEX_SIZE; // advance one block index entry
  //}

  //unsigned long long* blockPos = reinterpret_cast<unsigned long long*>(blockP);
  //unsigned short int* algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  //unsigned short int* algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  //int* intBufSize = reinterpret_cast<int*>(blockP + 12);

  //stringWriter->SetBuffersFromVec(nrOfBlocks * BLOCKSIZE_CHAR, vecLength);
  //unsigned long long totSize = storeCharBlockCompressed_v6(myfile, stringWriter, nrOfBlocks * BLOCKSIZE_CHAR,
  //  vecLength, streamCompressInt, streamCompressChar, *algoInt, *algoChar, *intBufSize, nrOfBlocks);

  //fullSize += totSize;
  //*blockPos = fullSize;

  //delete streamCompressInt;
  //delete streamCompressChar;
  //delete compressInt;
  //delete compressInt2;
  //delete compressChar;
  //delete compressChar2;

  //myfile.seekp(curPos + CHAR_HEADER_SIZE);
  //myfile.write(static_cast<char*>(&meta[CHAR_HEADER_SIZE]), (nr_of_blocks + 1) * CHAR_INDEX_SIZE); // additional zero for index convenience
  //myfile.seekp(0, ios_base::end);  // back to end of file


inline void ReadDataBlock_v6(istream& myfile, IStringColumn* blockReader, unsigned long long blockSize, unsigned long long nrOfElements,
  unsigned long long startElem, unsigned long long endElem, unsigned long long vecOffset)
{
  const unsigned long long nr_of_na_ints = 1 + nrOfElements / 32; // last bit is NA flag
  const unsigned long long tot_elements = nrOfElements + nr_of_na_ints;

  std::unique_ptr<unsigned int[]> sizeMetaP(new unsigned int[tot_elements]);
  unsigned int* sizeMeta = sizeMetaP.get();

  myfile.read(reinterpret_cast<char*>(sizeMeta), tot_elements * 4); // read cumulative string lengths and NA bits

  const unsigned int char_data_size = blockSize - tot_elements * 4;

  std::unique_ptr<char[]> buf_p(new char[char_data_size]);
  char* buf = buf_p.get();

  myfile.read(buf, char_data_size); // read string lengths

  blockReader->BufferToVec(nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);
}


inline void ReadDataBlockCompressed_v6(istream& myfile, IStringColumn* blockReader, unsigned long long blockSize, unsigned long long nrOfElements,
  unsigned long long start_elem, unsigned long long end_elem, unsigned long long vec_offset,
  unsigned int intBlockSize, Decompressor& decompressor, unsigned short int& algoInt, unsigned short int& algoChar)
{
  unsigned long long nrOfNAInts = 1 + nrOfElements / 32; // NA metadata including overall NA bit
  unsigned long long totElements = nrOfElements + nrOfNAInts;

  std::unique_ptr<unsigned int[]> sizeMetaP(new unsigned int[totElements]);
  unsigned int* sizeMeta = sizeMetaP.get();

  // Read and uncompress str sizes data
  if (algoInt == 0) // uncompressed
  {
    myfile.read(reinterpret_cast<char*>(sizeMeta), totElements * 4); // read cumulative string lengths
  }
  else
  {
    unsigned int intBufSize = intBlockSize;

    std::unique_ptr<char[]> strSizeBufP(new char[intBufSize]);
    char* strSizeBuf = strSizeBufP.get();

    myfile.read(strSizeBuf, intBufSize);
    myfile.read(reinterpret_cast<char*>(&sizeMeta[nrOfElements]), nrOfNAInts * 4); // read cumulative string lengths

    // Decompress size but not NA metadata (which is currently uncompressed)

    decompressor.Decompress(algoInt, reinterpret_cast<char*>(sizeMeta), nrOfElements * 4, strSizeBuf, intBlockSize);
  }

  unsigned int charDataSizeUncompressed = sizeMeta[nrOfElements - 1];

  // Read and uncompress string vector data, use stack if possible here !!!!!
  unsigned int charDataSize = blockSize - intBlockSize - nrOfNAInts * 4;

  // expensive allocation per block!
  std::unique_ptr<char[]> bufP(new char[charDataSizeUncompressed]);
  char* buf = bufP.get();

  if (algoChar == 0)
  {
    myfile.read(buf, charDataSize); // read string lengths
  }
  else
  {
    std::unique_ptr<char[]> bufCompressedP(new char[charDataSize]);
    char* bufCompressed = bufCompressedP.get();

    myfile.read(bufCompressed, charDataSize); // read string lengths
    decompressor.Decompress(algoChar, buf, charDataSizeUncompressed, bufCompressed, charDataSize);
  }

  blockReader->BufferToVec(nrOfElements, start_elem, end_elem, vec_offset, sizeMeta, buf);
}


/**
 * \brief 
 * \param myfile open file stream to a valid fst file
 * \param blockReader interface to an in-memory string vector
 * \param block_pos file offset to the start of string column data in myfile
 * \param startRow zero-based offset to the required starting row
 * \param vec_length number of vector elements to read
 * \param size total on-disk size of the string vector
 */
void fdsReadCharVec_v6(istream& myfile, IStringColumn* blockReader, unsigned long long block_pos, unsigned long long startRow,
  const unsigned long long vec_length, unsigned long long size)
{
  // Jump to startRow size
  myfile.seekg(block_pos);

  // Read algorithm type and block size
  unsigned int meta[2];
  myfile.read(reinterpret_cast<char*>(meta), CHAR_HEADER_SIZE);

  const unsigned int compression = meta[0] & 1; // maximum 8 encodings
  const StringEncoding string_encoding = static_cast<StringEncoding>(meta[0] >> 1 & 7); // at maximum 8 encodings

  const unsigned long long block_size_char = static_cast<unsigned long long>(meta[1]);  // number of elements in a block
  const unsigned long long tot_nr_of_blocks = (size - 1) / block_size_char; // total number of blocks on disk minus 1
  const unsigned long long start_block = startRow / block_size_char;  // zero-based block number containing first required element
  const unsigned long long start_offset = startRow - (start_block * block_size_char);  // zero-based offset of first required element in start_block
  const unsigned long long end_block = (startRow + vec_length - 1) / block_size_char;  // zero-based number of block containing last required lement
  const unsigned long long end_offset = (startRow + vec_length - 1) - end_block * block_size_char;  // zero-based offset of last required element in end_block
  unsigned long long nr_of_blocks = 1 + end_block - start_block; // total number of blocks to read from file (including partial)

  // Allocate result vector
  blockReader->AllocateVec(vec_length);
  blockReader->SetEncoding(string_encoding);

  // Vector data is uncompressed
  if (compression == 0)
  {
    std::unique_ptr<unsigned long long[]> blockOffsetP(new unsigned long long[1 + nr_of_blocks]);
    unsigned long long* blockOffset = blockOffsetP.get(); // block positions

    if (start_block > 0) // include previous block offset
    {
      myfile.seekg(block_pos + CHAR_HEADER_SIZE + (start_block - 1) * 8); // jump to correct block index
      myfile.read(reinterpret_cast<char*>(blockOffset), (1 + nr_of_blocks) * 8);
    }
    else
    {
      blockOffset[0] = CHAR_HEADER_SIZE + (tot_nr_of_blocks + 1) * 8;
      myfile.read(reinterpret_cast<char*>(&blockOffset[1]), nr_of_blocks * 8);
    }


    // Navigate to first selected data block
    unsigned long long offset = blockOffset[0];
    myfile.seekg(block_pos + offset);

    unsigned long long endElem = block_size_char - 1;
    unsigned long long nrOfElements = block_size_char;

    if (start_block == end_block) // subset start and end of block
    {
      endElem = end_offset;
      if (end_block == tot_nr_of_blocks)
      {
        nrOfElements = size - tot_nr_of_blocks * block_size_char; // last block can have less elements
      }
    }

    // Read first block with offset
    unsigned long long blockSize = blockOffset[1] - offset; // size of data block
    ReadDataBlock_v6(myfile, blockReader, blockSize, nrOfElements, start_offset, endElem, 0);

    if (start_block == end_block) // subset start and end of block
    {
      return;
    }

    offset = blockOffset[1];
    unsigned long long vecPos = block_size_char - start_offset;

    if (end_block == tot_nr_of_blocks)
    {
      nrOfElements = size - tot_nr_of_blocks * block_size_char; // last block can have less elements
    }

    --nr_of_blocks; // number of blocks excluding last possibly partial block

    // Parallel reads of character columns have a specific pattern. The master threads is fully
    // utilized to create new string elements. These allocations are the main bottleneck in speed-ups
    // for deserialization of character columns. Data is loaded, decompressed and processed on the
    // other threads. The whole setup is designed in such a way that the main thread can do it's allocations
    // with the highest possible speed.
    // TODO: use custom stack based unordered map to speed up allocations by smart copying

    // determine number of blocks per job and number of jobs
    unsigned long long nr_of_reader_threads = GetFstThreads() - 1;

    // use single threaded implementation
    if (nr_of_reader_threads == 0 | nr_of_blocks < 2)
    {
      for (unsigned long long block = 1; block < nr_of_blocks; ++block)
      {
        const unsigned long long new_pos = blockOffset[block + 1];

        ReadDataBlock_v6(myfile, blockReader, new_pos - offset, block_size_char, 0, block_size_char - 1, vecPos);

        vecPos += block_size_char;
        offset = new_pos;
      }

      // last possibly partial block
      const unsigned long long new_pos = blockOffset[nr_of_blocks + 1];
      ReadDataBlock_v6(myfile, blockReader, new_pos - offset, nrOfElements, 0, end_offset, vecPos);

      return;
    }

    // use multi-threaded implementation for all blocks excluding last

    --nr_of_blocks;  // number of full blocks (at minimum one) left to read (first block is already processed)

    // the number of reader threads determines the number of block batches
    const unsigned long long blocks_per_read_job = std::min(1 + (nr_of_blocks - 1) / nr_of_reader_threads, BATCH_SIZE_READ_CHAR_ULL);
    const unsigned long long nr_of_read_jobs = 1 + (nr_of_blocks - 1) / blocks_per_read_job;  // including possibly partial last jobs
    nr_of_reader_threads = std::min(nr_of_reader_threads, nr_of_read_jobs);  // we require at most nr_of_reader_jobs reader threads

    // Calculate the required buffer size for each thread.
    // The algorithm uses a double buffered strategy (each threads uses two separate buffers).
    unsigned long long read_buffer_sizes[2 * STD_MAX_CHAR_THREADS - 2];  // master thread doesn't need a buffer

    // reset max buffer sizes
    for (unsigned long long read_buffer_id = 0; read_buffer_id < 2 * nr_of_reader_threads; read_buffer_id++)
    {
      read_buffer_sizes[read_buffer_id] = 0;
    }

    // determine largest combined read job size for all threads
    unsigned long long prev_offset = offset;

    // determine read buffer size per read_job (excluding last)
    for (unsigned long long read_job = 0; read_job < nr_of_read_jobs - 1; read_job++)
    {
      const unsigned long long block_end = 1 + (read_job + 1) * blocks_per_read_job;  // block 0 already processed
      offset = blockOffset[block_end + 1];

      const unsigned long long read_job_buffer_size = offset - prev_offset;
      const unsigned long long buffer_index = read_job % (2 * nr_of_reader_threads);
      read_buffer_sizes[buffer_index] = std::max(read_job_buffer_size, read_buffer_sizes[buffer_index]);

      prev_offset = offset;
    }

    // last read job
    offset = blockOffset[nr_of_blocks + 2];  // offset to last block

    const unsigned long long read_job_buffer_size = offset - prev_offset;
    const unsigned long long buffer_index = (nr_of_read_jobs - 1) % (2 * nr_of_reader_threads);
    read_buffer_sizes[buffer_index] = std::max(read_job_buffer_size, read_buffer_sizes[buffer_index]);

    // calculate size of single large buffer memory
    unsigned long long tot_buffer_size = 0;
    unsigned long long buffer_offset = 0;
    for (unsigned long long read_buffer_id = 0; read_buffer_id < 2 * nr_of_reader_threads; read_buffer_id++)
    {
      tot_buffer_size += CACHE_LINE_SIZE * ((read_buffer_sizes[read_buffer_id] + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE);  // avoid false sharing
      read_buffer_sizes[read_buffer_id] = buffer_offset;  // cumulative size
      buffer_offset = tot_buffer_size;
    }

    // allocate read buffer
    std::unique_ptr<char[]> read_buffer_p(new char[tot_buffer_size]);
    char* read_buffer = read_buffer_p.get();


    // The thread pattern works in cycles. In each cycle, all reader threads execute their read job. The main thread
    // processes the buffer data from the previous cycle.

    // There are more phases than read jobs. That's because the main thread does not read data from disk but only processes
    // data read by the reader threads. Also, at the end there can be empty jobs.
    const unsigned long long nr_of_cycles = 1 + (nr_of_read_jobs - 1) / nr_of_reader_threads;
    const long long nr_of_phases = 1 + (nr_of_cycles + 1) * (nr_of_reader_threads + 1);  // end with the master thread


    // Start of parallel region

    #pragma omp parallel num_threads(nr_of_reader_threads + 1) shared(nr_of_reader_threads)
    {
      #pragma omp for ordered schedule(static, 1)
      for (long long phase_id = 0; phase_id < nr_of_phases; phase_id++)
      {
        const int cur_thread = CurrentFstThread();  // thread_id determines type of job
        const int nr_of_threads = nr_of_reader_threads + 1;

        const bool is_master = (phase_id % nr_of_threads) == 0;
        const unsigned long long cycle_nr = phase_id / nr_of_threads;
        const unsigned long long read_job_id = cycle_nr * nr_of_reader_threads + (phase_id - 1) % nr_of_threads;

        unsigned long long block_batch_size = 0;
        char* thread_buffer = nullptr;

        if (read_job_id < nr_of_read_jobs)  // there is something to read in the ordered block
        {
          const unsigned long long buf_id = read_job_id % (2 * nr_of_reader_threads);  // buffer index
          const unsigned long long buf_offset = read_buffer_sizes[buf_id];  // thread buffer offset
          const unsigned long long read_block_start = 1 + read_job_id * blocks_per_read_job;
          const unsigned long long read_block_end = std::min(read_block_start + blocks_per_read_job, nr_of_blocks + 1);  // block 0 already processed

          thread_buffer = &read_buffer[buf_offset];  // each thread has it's own two buffers
          block_batch_size = blockOffset[read_block_end + 1] - blockOffset[read_block_start + 1];
        }

        if (is_master)  // master thread fills the result vector
        {
          if (cycle_nr < 2)  // no buffer data available before cycle 2 
          {
            // work can be done here
          }
          else
          {
            // set vector elements

            // use buffers from two cycles before
            const unsigned long long start_buf_id = (cycle_nr % 2) * nr_of_reader_threads;

            // process a range of job results
            const unsigned long long start_job = (cycle_nr - 2) * nr_of_reader_threads;
            const unsigned long long end_job = start_job + nr_of_reader_threads, nr_of_read_jobs;

            unsigned int str_sizes[BLOCKSIZE_CHAR_NA_INTS + BLOCKSIZE_CHAR];  // default size for full block

            if (end_job < nr_of_read_jobs)
            {
              for (int read_job_id = start_job; read_job_id < end_job; read_job_id++)
              {
                // get buffer
                const unsigned long long page_id = read_job_id % (2 * nr_of_reader_threads);
                const unsigned long long page_offset = read_buffer_sizes[page_id];  // thread buffer offset
                char* page_buffer = &read_buffer[page_offset];  // each thread has it's own two buffers

                unsigned long long start_block = read_job_id * blocks_per_read_job + 1;
                unsigned long long end_block = start_block + blocks_per_read_job;

                for (unsigned long long block_id = start_block; block_id < end_block; block_id++)
                {
                  // copy buffer to stack memory
                  unsigned long long block_size = blockOffset[block_id + 1] - blockOffset[block_id];
                  const unsigned int byte_size = 4 * (BLOCKSIZE_CHAR_NA_INTS + BLOCKSIZE_CHAR);
                  memcpy(str_sizes, page_buffer, byte_size);  // copy to stack buffer

                  //const unsigned int char_data_size = blockSize - byte_size;
                  char* buf = &page_buffer[byte_size];

                  blockReader->BufferToVec(block_size_char, 0, block_size_char - 1, vecPos, str_sizes, buf);
                  vecPos += block_size_char;
                }
              }
            }
            else  // possible partial last block included
            {
              unsigned int str_sizes[BLOCKSIZE_CHAR_NA_INTS + BLOCKSIZE_CHAR];  // default size for full block

              for (int read_job_id = start_job; read_job_id < nr_of_read_jobs; read_job_id++)  // finish last read jobs
              {
                // get buffer
                const unsigned long long page_id = read_job_id % (2 * nr_of_reader_threads);
                const unsigned long long page_offset = read_buffer_sizes[page_id];  // thread buffer offset
                char* page_buffer = &read_buffer[page_offset];  // each thread has it's own two buffers

                unsigned long long start_block = read_job_id * blocks_per_read_job + 1;
                unsigned long long end_block = std::min(start_block + blocks_per_read_job, nr_of_blocks);  // don't include last block

                for (unsigned long long block_id = start_block; block_id < end_block; block_id++)
                {
                  // copy buffer to stack memory
                  unsigned long long block_size = blockOffset[block_id + 1] - blockOffset[block_id];
                  const unsigned int byte_size = 4 * (BLOCKSIZE_CHAR_NA_INTS + BLOCKSIZE_CHAR);
                  memcpy(str_sizes, page_buffer, byte_size);  // copy to stack buffer

                  //const unsigned int char_data_size = blockSize - byte_size;
                  char* buf = &page_buffer[byte_size];

                  blockReader->BufferToVec(block_size_char, 0, block_size_char - 1, vecPos, str_sizes, buf);
                  vecPos += block_size_char;
                }

                // last possibly partial block
                if (end_block == nr_of_blocks)
                {
                  // copy buffer to stack memory
                  unsigned long long block_size = blockOffset[end_block + 1] - blockOffset[end_block];

                  const unsigned long long nr_of_na_ints = 1 + nrOfElements / 32; // last bit is NA flag
                  const unsigned long long tot_elements = nrOfElements + nr_of_na_ints;

                  const unsigned int byte_size = 4 * (BLOCKSIZE_CHAR_NA_INTS + BLOCKSIZE_CHAR);
                  memcpy(str_sizes, page_buffer, byte_size);  // copy to stack buffer

                  //const unsigned int char_data_size = blockSize - byte_size;
                  char* buf = &page_buffer[byte_size];

                  blockReader->BufferToVec(nrOfElements, 0, nrOfElements - 1, vecPos, str_sizes, buf);
                  vecPos += block_size_char;
                }
              }
            }

          }
        }
        else  // reader thread processes buffer data read in previous cycle
        {
          if (cycle_nr > 0)  // no data in buffer before cycle 1
          {
            const unsigned long long read_buf_page_id = (read_job_id + nr_of_reader_threads) % (2 * nr_of_reader_threads);
            const unsigned long long read_buf_offset = read_buffer_sizes[read_buf_page_id];  // thread buffer offset
            char* read_thread_buffer = &read_buffer[read_buf_offset];  // each thread has it's own two buffers

            // uncompress and process raw buffer data
          }
        }

        #pragma omp ordered
        {
          // reading from disk is done on (a single) reader thread
          if (cur_thread != 0 & read_job_id < nr_of_read_jobs)
          {
            myfile.read(thread_buffer, block_batch_size); // read a couple of blocks
          }
        }
      }
    }

    // read and process the last (possibly partial) block
    //unsigned long long last_block_size = blockOffset[nr_of_blocks + 2] - blockOffset[nr_of_blocks + 1];
    //ReadDataBlock_v6(myfile, blockReader, last_block_size, nrOfElements, 0, end_offset, vecPos);

    return;
  }


  // Vector data is compressed

  unsigned int bufLength = (nr_of_blocks + 1) * CHAR_INDEX_SIZE; // 1 long and 2 unsigned int per block

  // add extra first element for convenience
  std::unique_ptr<char[]> blockInfoP = std::unique_ptr<char[]>(new char[bufLength + CHAR_INDEX_SIZE]);
  char* blockInfo = blockInfoP.get();

  if (start_block > 0) // include previous block offset
  {
    myfile.seekg(block_pos + CHAR_HEADER_SIZE + (start_block - 1) * CHAR_INDEX_SIZE); // jump to correct block index
    myfile.read(blockInfo, (nr_of_blocks + 1) * CHAR_INDEX_SIZE);
  }
  else
  {
    unsigned long long* firstBlock = reinterpret_cast<unsigned long long*>(blockInfo);
    *firstBlock = CHAR_HEADER_SIZE + (tot_nr_of_blocks + 1) * CHAR_INDEX_SIZE; // offset of first data block
    myfile.read(&blockInfo[CHAR_INDEX_SIZE], nr_of_blocks * CHAR_INDEX_SIZE);
  }

  // Get block meta data
  unsigned long long* offset = reinterpret_cast<unsigned long long*>(blockInfo);
  char* blockP = &blockInfo[CHAR_INDEX_SIZE];
  unsigned long long* curBlockPos = reinterpret_cast<unsigned long long*>(blockP);
  unsigned short int* algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  unsigned short int* algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  int* intBufSize = reinterpret_cast<int*>(blockP + 12);

  // move to first data block

  myfile.seekg(block_pos + *offset);

  unsigned long long endElem = block_size_char - 1;
  unsigned long long nrOfElements = block_size_char;

  Decompressor decompressor; // uncompresses all available algorithms

  if (start_block == end_block) // subset start and end of block
  {
    endElem = end_offset;
    if (end_block == tot_nr_of_blocks)
    {
      nrOfElements = size - tot_nr_of_blocks * block_size_char; // last block can have less elements
    }
  }

  // Read first block with offset
  unsigned long long blockSize = *curBlockPos - *offset; // size of data block

  ReadDataBlockCompressed_v6(myfile, blockReader, blockSize, nrOfElements, start_offset, endElem, 0, *intBufSize,
    decompressor, *algoInt, *algoChar);


  if (start_block == end_block) // subset start and end of block
  {
    return;
  }

  // more than 1 block

  offset = curBlockPos;

  unsigned long long vecPos = block_size_char - start_offset;

  if (end_block == tot_nr_of_blocks)
  {
    nrOfElements = size - tot_nr_of_blocks * block_size_char; // last block can have less elements
  }

  --nr_of_blocks; // iterate all but last block
  blockP += CHAR_INDEX_SIZE; // move to next index element
  for (unsigned int block = 1; block < nr_of_blocks; ++block)
  {
    curBlockPos = reinterpret_cast<unsigned long long*>(blockP);
    algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
    algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
    intBufSize = reinterpret_cast<int*>(blockP + 12);

    ReadDataBlockCompressed_v6(myfile, blockReader, *curBlockPos - *offset, block_size_char, 0, block_size_char - 1, vecPos, *intBufSize,
      decompressor, *algoInt, *algoChar);

    vecPos += block_size_char;
    offset = curBlockPos;
    blockP += CHAR_INDEX_SIZE; // move to next index element
  }

  curBlockPos = reinterpret_cast<unsigned long long*>(blockP);
  algoInt = reinterpret_cast<unsigned short int*>(blockP + 8);
  algoChar = reinterpret_cast<unsigned short int*>(blockP + 10);
  intBufSize = reinterpret_cast<int*>(blockP + 12);

  ReadDataBlockCompressed_v6(myfile, blockReader, *curBlockPos - *offset, nrOfElements, 0, end_offset, vecPos, *intBufSize,
    decompressor, *algoInt, *algoChar);
}
