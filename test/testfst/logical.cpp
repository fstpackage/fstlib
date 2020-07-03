
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

class LogicalTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("logical.fst");
	}
};


TEST_F(LogicalTest, SmallVec)
{
	int nrOfRows = 10000000;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "Logical" };
	fstTable.SetColumnNames(colNames);

	// Add byte column


    LogicalVectorAdapter logicalVec(nrOfRows);
	int* logicalP = logicalVec.Data();
	IntSeq(logicalP, nrOfRows, 0, 2);

	fstTable.SetLogicalColumn(&logicalVec, 0);

//	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}
