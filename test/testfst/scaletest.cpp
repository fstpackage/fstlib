
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

class ScaleTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("int64.fst");
	}
};


TEST_F(ScaleTest, SmallVec)
{
	int nrOfRows = 10;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "integer" };
	fstTable.SetColumnNames(colNames);

  // Microsecond column
  IntVectorAdapter int32Vec(nrOfRows, FstColumnAttribute::INT_32_TIMESTAMP_SECONDS, FstTimeScale::MICROSECONDS);  // microseconds

	int* intP = int32Vec.Data();
	IntSeq(reinterpret_cast<int*>(intP), 10, 100);

	//std::vector<long long> longView(10);
	//std::memcpy(longView.data(), intP, 80);

  std::string annotation("");
	fstTable.SetIntegerColumn(&int32Vec, 0, annotation);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}


//TEST_F(ScaleTest, BigVec)
//{
//  int nrOfRows = 580000000;
//  FstTable fstTable(nrOfRows);
//  fstTable.InitTable(1, nrOfRows);
//
//  vector<std::string> colNames{ "integer" };
//  fstTable.SetColumnNames(colNames);
//
//  // Basic int column
//  IntVectorAdapter int32Vec(nrOfRows, FstColumnAttribute::INT_32_BASE, 0);
//
//  int* intP = int32Vec.Data();
//  IntSeq(reinterpret_cast<int*>(intP), 10, 100);
//
//  std::string annotation("");
//  fstTable.SetIntegerColumn(&int32Vec, 0, annotation);
//
//  ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 1);
//}


//TEST_F(ScaleTest, BigVecFromR)
//{
//  FstStore fstStore(GetFilePath("big.fst"));
//
//  auto tableReader = new FstTable();
//  auto selectedCols = new StringArray();
//  auto columnFactory = new ColumnFactory();
//  std::vector<int> keyIndex;
//
//  // Read fst file
//  try
//  {
//    fstStore.fstRead(*tableReader, nullptr, 1, -1, columnFactory, keyIndex, selectedCols);
//  }
//  catch (const std::runtime_error&)
//  {
//    throw(runtime_error("Error"));
//  }
//}
