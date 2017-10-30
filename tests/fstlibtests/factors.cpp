
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
using namespace std;

class FactorTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("2.fst");
	}
};


TEST_F(FactorTest, NaLevels)
{
	int nrOfRows = 10;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames {"Factor"};

	fstTable.SetColumnNames(colNames);

	// Add factor column
	FactorVectorAdapter factorVec(nrOfRows);
	IntConstantVal(factorVec.LevelData(), nrOfRows, FST_NA_INT);
	StringColumn* levels = factorVec.DataPtr()->Levels();
	levels->AllocateVec(0);
	fstTable.SetFactorColumn(&factorVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}

