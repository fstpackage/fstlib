
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fststore.h>

#include <fsttable.h>
#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"

#include <vector>


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;

class SpecialTablesTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("zero_rows.fst");
	}
};


TEST_F(SpecialTablesTest, ZeroRows)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(5, nrOfRows);

	vector<std::string> colNames{ "factorNA", "factor", "integer", "bit64", "double" };
	fstTable.SetColumnNames(colNames);

	// factor column without levels
	FactorVectorAdapter factorVecNA(nrOfRows, 0, FstColumnAttribute::FACTOR_BASE);

	// factor column with some levels but no data
	int nr_of_levels = 128;
	FactorVectorAdapter factorVec(nrOfRows, nr_of_levels, FstColumnAttribute::FACTOR_BASE);

	StringColumn* levels = factorVec.DataPtr()->Levels();
	std::vector<std::string> levelVec = *(levels->StrVector()->StrVec());
	for (int pos = 0; pos < nr_of_levels; pos++)
	{
		levelVec[pos] = to_string(pos);
	}

	// integer column
	IntVectorAdapter intVec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);

	// int64 column
	Int64VectorAdapter int64Vec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);

	// double column
	DoubleVectorAdapter doubleVec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);

	fstTable.SetFactorColumn(&factorVecNA, 0);
	fstTable.SetFactorColumn(&factorVec, 1);
	fstTable.SetIntegerColumn(&intVec, 2);
	fstTable.SetInt64Column(&int64Vec, 3);
	fstTable.SetDoubleColumn(&doubleVec, 4);



	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 50);
}
