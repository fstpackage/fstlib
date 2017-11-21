
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fststore.h>
#include <interface/fsthash.h>

#include <fsttable.h>
#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;

class HashTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("int64.fst");
	}
};


TEST_F(HashTest, SmallVec)
{
  FstHasher hasher;

  int data[12];

  for (int pos = 0; pos < 12; pos++)
  {
    data[pos] = pos;
  }

  unsigned long long res = hasher.HashBlob(reinterpret_cast<unsigned char*>(data), 40);

  res = hasher.HashBlob(reinterpret_cast<unsigned char*>(data), 40, false);

  res = hasher.HashBlob(reinterpret_cast<unsigned char*>(data), 40);
}
