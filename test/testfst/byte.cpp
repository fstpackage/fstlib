
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fststore.h>

#include <fsttable.h>
#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;

class ByteTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("byte.fst");
	}
};


TEST_F(ByteTest, SmallVec)
{
	int nrOfRows = 40;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "Byte" };
	fstTable.SetColumnNames(colNames);

	// Add byte column
	ByteVectorAdapter byteVec(nrOfRows);

	char* byteP = byteVec.Data();
	IntSeq(reinterpret_cast<int*>(byteP), 10, 100);

	fstTable.SetByteColumn(&byteVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}

TEST_F(ByteTest, MediumVec)
{
	int nrOfRows = 100000;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "Byte" };
	fstTable.SetColumnNames(colNames);

	// Add byte column
	ByteVectorAdapter byteVec(nrOfRows);

	char* byteP = byteVec.Data();
	IntSeq(reinterpret_cast<int*>(byteP), nrOfRows / 4, 100);

	fstTable.SetByteColumn(&byteVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}
