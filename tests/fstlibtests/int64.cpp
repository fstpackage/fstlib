
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

class Int64Test : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("int64.fst");
	}
};


TEST_F(Int64Test, SmallVec)
{
	int nrOfRows = 10;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "Int64" };
	fstTable.SetColumnNames(colNames);

	// Add Int64 column
	Int64VectorAdapter int64Vec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);

	long long* intP = int64Vec.Data();
	IntSeq(reinterpret_cast<int*>(intP), 20, 100);

	std::vector<long long> longView(10);
	std::memcpy(longView.data(), intP, 80);

	fstTable.SetInt64Column(&int64Vec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}


TEST_F(Int64Test, NanotimeTest)
{
	int nrOfRows = 10;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "Nanotime" };
	fstTable.SetColumnNames(colNames);

	// Add Int64 column
	Int64VectorAdapter int64Vec(nrOfRows, FstColumnAttribute::INT_64_TIME_SECONDS, FstTimeScale::NANOSECONDS);

	long long* intP = int64Vec.Data();
	IntSeq(reinterpret_cast<int*>(intP), 20, 100);

	std::vector<long long> longView(10);
	std::memcpy(longView.data(), intP, 80);

	fstTable.SetInt64Column(&int64Vec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}
