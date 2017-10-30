
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fststore.h>
#include <interface/fstdefines.h>

#include <fsttable.h>
#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;

class DateTimeTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("datetime.fst");
	}
};


//TEST_F(DateTimeTest, SmallVec)
//{
//	int nrOfRows = 10;
//	FstTable fstTable(nrOfRows);
//	fstTable.InitTable(1, nrOfRows);
//
//	vector<std::string> colNames{ "DateTime" };
//	fstTable.SetColumnNames(colNames);
//
//	// Add DateTime column
//	DateTimeVectorAdapter dateTimeVec(nrOfRows);
//
//	IntSeq(dateTimeVec.Data(), 10, 100);
//	fstTable.SetDateTimeColumn(&dateTimeVec, 0);
//
//	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
//}

