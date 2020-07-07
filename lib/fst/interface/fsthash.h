/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this file,
  You can obtain one at https://mozilla.org/MPL/2.0/.

  https://www.mozilla.org/en-US/MPL/2.0/FAQ/

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/


#ifndef FST_HASH_H
#define FST_HASH_H


#include <stdlib.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <climits>

#include "interface/fstdefines.h"
#include "interface/itypefactory.h"
#include "interface/openmphelper.h"

#include "xxhash.h"


class FstHasher
{
public:

	FstHasher()
	{
	}

  /**
	 * \brief Calculate 64-bit xxHash of an arbitrary length input vector
	 * \param blobSource input buffer for hash calculation
	 * \param blobLength length of the input buffer in bytes
	 * \param blockHash if true, use a multi-threaded blocked hash format (hash of hashes)
	 * \return hash value of the input buffer
	 */
	uint64_t HashBlob(unsigned char* blobSource, uint64_t blobLength, bool blockHash = true) const
	{
		return HashBlobSeed(blobSource, blobLength, FST_HASH_SEED, blockHash);
	}

	/**
  * \brief Calculate 64-bit xxHash of an arbitrary length input vector
  * \param blobSource input buffer for hash calculation
  * \param blobLength length of the input buffer in bytes
  * \param seed Seed for hashing
  * \param blockHash if true, use a multi-threaded blocked hash format (hash of hashes)
	* \return hash value of the input buffer
   */
	uint64_t HashBlobSeed(unsigned char* blobSource, uint64_t blobLength, uint64_t seed, bool blockHash = true) const
	{
    if (!blockHash)
    {
      return XXH64(blobSource, blobLength, seed);
    }

		int nrOfThreads = GetFstThreads();

		// Check for empty source
		if (blobLength == 0)
		{
			throw(std::runtime_error("Source contains no data."));
		}

		// block size to use for hashing has a lower bound for compression efficiency
		uint64_t minBlock = std::max(static_cast<uint64_t>(HASH_SIZE),
			1 + (blobLength - 1) / PREV_NR_OF_BLOCKS);

		// And a higher bound for hasher compatability
		unsigned int blockSize = static_cast<unsigned int>(std::min(minBlock, static_cast<uint64_t>(INT_MAX)));

		int nrOfBlocks = static_cast<int>(1 + (blobLength - 1) / blockSize);
		nrOfThreads = std::min(nrOfThreads, nrOfBlocks);

		unsigned int lastBlockSize = 1 + (blobLength - 1) % blockSize;
		float blocksPerThread = static_cast<float>(nrOfBlocks) / nrOfThreads;

		uint64_t* blockHashes = new uint64_t[nrOfBlocks];

#pragma omp parallel num_threads(nrOfThreads)
		{
#pragma omp for schedule(static, 1) nowait
			for (int blockBatch = 0; blockBatch < (nrOfThreads - 1); blockBatch++)  // all but last batch
			{
				float blockOffset = blockBatch * blocksPerThread;
				int blockNr = static_cast<int>(0.00001 + blockOffset);
				int nextblockNr = static_cast<int>(blocksPerThread + 0.00001 + blockOffset);

				// inner loop is executed by current thread
				for (int block = blockNr; block < nextblockNr; block++)
				{
					// Block hash
					blockHashes[block] = XXH64(&blobSource[block * blockSize], blockSize, seed);
				}
			}

#pragma omp single
			{
				int blockNr = static_cast<int>(0.00001 + (nrOfThreads - 1) * blocksPerThread);
				int nextblockNr = static_cast<int>(0.00001 + (nrOfThreads * blocksPerThread)) - 1;  // exclude last block

				// inner loop is executed by current thread
				for (int block = blockNr; block < nextblockNr; block++)
				{
					// Block hash
					blockHashes[block] = XXH64(&blobSource[block * blockSize], blockSize, seed);
				}

				// Last block hash
				blockHashes[nextblockNr] = XXH64(&blobSource[nextblockNr * blockSize], lastBlockSize, seed);
			}

		}  // end parallel region and join all threads

    uint64_t allBlockHash = blockHashes[0];

    // combine multiple hashes
    if (nrOfBlocks > 1)
    {
      allBlockHash = XXH64(blockHashes, nrOfBlocks * 8, seed);
    }
		delete[] blockHashes;

		//return static_cast<unsigned int>((allBlockHash >> 32) & 0xffffffff);
    return allBlockHash;
  }
};

#endif  // FST_HASH_H
