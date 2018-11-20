
#include <cstring>
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fstcompressor.h>
#include <interface/fsthash.h>

#include <columnfactory.h>
#include <typefactory.h>

#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"
#include <fstream>


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;

class CompressTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
};


void CompareBytes(unsigned char* input, unsigned char* input2, int length)
{
	std::vector<unsigned int>* charVec  = new std::vector<unsigned int>(length);
	std::vector<unsigned int>* charVec2 = new std::vector<unsigned int>(length);

	for (int pos = 0; pos < length; pos++)
	{
		(*charVec)[pos]  = (unsigned int) input[pos];
		(*charVec2)[pos] = (unsigned int) input2[pos];

		if (input[pos] != input2[pos])
		{
			int a = 1;
		}
	}


	delete charVec;
	delete charVec2;
}


void CompDecompCycle(unsigned char* testData, unsigned long long length, COMPRESSION_ALGORITHM algo, unsigned int compressionLevel)
{
	TypeFactory typeFactory;
	FstCompressor compressor(algo, compressionLevel, static_cast<ITypeFactory*>(&typeFactory));

	// compress without hashes
	BlobContainer* blobContainer = static_cast<BlobContainer*>(compressor.CompressBlob(testData, length, false));
	shared_ptr<Blob> blob = blobContainer->DataPtr();  // compressed data blob

	// without checking hashes
	BlobContainer* resultContainer = static_cast<BlobContainer*>(compressor.DecompressBlob(blob->Data(), blobContainer->Size(), false));
	shared_ptr<Blob> resBlob = resultContainer->DataPtr();
	//CompareBytes(resBlob->Data(), testData, length);
	int res = std::memcmp(testData, resBlob->Data(), resultContainer->Size());
	EXPECT_EQ(res, 0);
	delete resultContainer;

	// with checking missing hashes
	resultContainer = static_cast<BlobContainer*>(compressor.DecompressBlob(blob->Data(), blobContainer->Size(), true));
	resBlob = resultContainer->DataPtr();
	//CompareBytes(resBlob->Data(), testData, length);
	res = std::memcmp(testData, resBlob->Data(), resultContainer->Size());
	EXPECT_EQ(res, 0);
	delete resultContainer;

	// compress with hashes
	delete blobContainer;
	blobContainer = static_cast<BlobContainer*>(compressor.CompressBlob(testData, length, true));
	blob = blobContainer->DataPtr();  // compressed data blob

   // without checking hashes
	resultContainer = static_cast<BlobContainer*>(compressor.DecompressBlob(blob->Data(), blobContainer->Size(), false));
	resBlob = resultContainer->DataPtr();
	//CompareBytes(resBlob->Data(), testData, length);
	res = std::memcmp(testData, resBlob->Data(), resultContainer->Size());
	EXPECT_EQ(res, 0);
	delete resultContainer;

	// with checking missing hashes
	resultContainer = static_cast<BlobContainer*>(compressor.DecompressBlob(blob->Data(), blobContainer->Size(), true));
	resBlob = resultContainer->DataPtr();

	//CompareBytes(resBlob->Data(), testData, length);
	res = std::memcmp(testData, resBlob->Data(), resultContainer->Size());
	EXPECT_EQ(res, 0);
	delete resultContainer;

	delete blobContainer;
}

TEST_F(CompressTest, SmallVec)
{
	// Create blob with integer data
	const int nrOfInts = 10000;
	int* testData = new int[nrOfInts];

	IntSeq(testData, 10, nrOfInts);  // create test data

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 10);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 0);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 100);

	delete[] testData;
}


TEST_F(CompressTest, MediumVec)
{
	// Create blob with integer data
	const int nrOfInts = 10000;
	int* testData = new int[nrOfInts];

	IntSeq(testData, 10, nrOfInts);  // create test data

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 10);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 100);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 0);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 100);

	delete[] testData;
}


TEST_F(CompressTest, SingleBlock)
{
	// Create blob with integer data
	const int nrOfInts = 4096;
	int* testData = new int[nrOfInts];

	IntSeq(testData, 10, nrOfInts);  // create test data

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 10);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 100);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 0);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 100);

	delete[] testData;
}


TEST_F(CompressTest, LargeVector)
{
	// Create blob with integer data
	const int nrOfInts = 28000000;  // 1e8 bytes

	//const unsigned long long nrOfInts = 1074000001;  // > 4 GB, use for local testing only

	int* testData = new int[nrOfInts];

	IntSeq(testData, nrOfInts, 10);  // create test data

	ThreadsFst(36);

	CompDecompCycle(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 0);

	delete[] testData;
}


void FormatError(unsigned char* testData, unsigned long long length, COMPRESSION_ALGORITHM algo, unsigned int compressionLevel)
{
	TypeFactory typeFactory;
	FstCompressor compressor(COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 50, static_cast<ITypeFactory*>(&typeFactory));

	// Compress
	BlobContainer* blobContainer = static_cast<BlobContainer*>(
		compressor.CompressBlob(testData, length));
	shared_ptr<Blob> blob = blobContainer->DataPtr();  // compressed data blob

	unsigned char* data = blob->Data();

	// Decompress erroneous data
	unsigned char prevChar = data[blobContainer->Size() - 5];
	data[blobContainer->Size() - 5] = 'e';  // mess up compressed block
	EXPECT_THROW(
		compressor.DecompressBlob(blob->Data(), blobContainer->Size()),
		std::runtime_error
	);
	data[blobContainer->Size() - 5] = prevChar;  // mess up compressed block

	// Set second block offset to 0 (non-monotomically increasing offsets)
	unsigned long long* blockOffset = (unsigned long long*) (data + 40);
	unsigned long long prevOffset = *blockOffset;
	*blockOffset = 0;
	EXPECT_THROW(
		compressor.DecompressBlob(blob->Data(), blobContainer->Size()),
		std::runtime_error
	);
	*blockOffset = prevOffset;

	// mess up vector length
	unsigned long long* vecLength = (unsigned long long*) (data + 16);
	prevOffset = *vecLength;
	*vecLength = 5;
	EXPECT_THROW(
		compressor.DecompressBlob(blob->Data(), blobContainer->Size()),
		std::runtime_error
	);
	*vecLength = prevOffset;

	// incorrect input vector size
	EXPECT_THROW(
		compressor.DecompressBlob(blob->Data(), 1000),
		std::runtime_error
	);

	delete blobContainer;
}


TEST_F(CompressTest, ErrorInFormat)
{
	// Create blob with integer data
	const int nrOfInts = 10;
	int* testData = new int[nrOfInts];
	IntSeq(testData, 10, nrOfInts);  // create test data

	FormatError(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 0);
	FormatError(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_LZ4, 100);
	FormatError(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 0);
	FormatError(reinterpret_cast<unsigned char*>(testData), 4 * nrOfInts, COMPRESSION_ALGORITHM::ALGORITHM_ZSTD, 100);

	delete[] testData;
}


TEST_F(CompressTest, FromDiskTestVectors)
{
	std::string filePath = GetFilePath("data1.fst");

	// fst file stream using a stack buffer
	ifstream myfile;
	myfile.open(filePath, ios::binary);
	myfile.seekg(0, myfile.end);
	int length = myfile.tellg();
	myfile.seekg(0, myfile.beg);

	unsigned char* testData = new unsigned char[length];

	myfile.read(reinterpret_cast<char*>(testData), length);
	myfile.close();

	TypeFactory typeFactory;
	FstCompressor compressor(static_cast<ITypeFactory*>(&typeFactory));

	BlobContainer* blobContainer = nullptr;

	// Compress
	EXPECT_THROW(
		blobContainer = static_cast<BlobContainer*>(compressor.DecompressBlob(testData, length)),
		std::runtime_error
	);

	delete blobContainer;
	delete[] testData;
}


unsigned int TestHash(int vecSize)
{
	char* testData = new char[vecSize];
	//IntSeq(reinterpret_cast<int*>(testData), 10, vecSize / 4);  // create test data

	FstHasher hasher;

	unsigned int res = hasher.HashBlob(reinterpret_cast<unsigned char*>(testData), vecSize);

	delete[] testData;

	return res;
}

TEST_F(CompressTest, HashVector)
{
	TestHash(10);
	TestHash(1023);  // single block minus 1
	TestHash(1024);  // single block minus 1
	TestHash(1025);  // single block minus 1
	TestHash(4000);  // few cores
	TestHash(100000);  // more than 48 blocks, so blockSize > 1024
	TestHash(100000000);  // more than 48 blocks, so blockSize > 1024
}
