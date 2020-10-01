
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


TEST_F(SpecialTablesTest, ZeroRowsInt)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "integer" };
	fstTable.SetColumnNames(colNames);
	IntVectorAdapter intVec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);
	fstTable.SetIntegerColumn(&intVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows1.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows2.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsInt64)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "integer64" };
	fstTable.SetColumnNames(colNames);

	Int64VectorAdapter int64Vec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);
	fstTable.SetInt64Column(&int64Vec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows3.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows4.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsString)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "character" };
	fstTable.SetColumnNames(colNames);

	// string column
	StringColumn strColumn{};
	strColumn.AllocateVec(nrOfRows);
	strColumn.SetEncoding(StringEncoding::LATIN1);
	fstTable.SetStringColumn(static_cast<IStringColumn*>(&strColumn), 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows5.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows6.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsLogical)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "logical" };
	fstTable.SetColumnNames(colNames);

	LogicalVectorAdapter logicalVec(nrOfRows);
	fstTable.SetLogicalColumn(&logicalVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows7.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows8.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsByte)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "byte" };
	fstTable.SetColumnNames(colNames);

	ByteVectorAdapter byteVec(nrOfRows);
	fstTable.SetByteColumn(&byteVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows9.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows10.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsFactorNA)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "factorNA" };
	fstTable.SetColumnNames(colNames);

	// factor column without levels
	FactorVectorAdapter factorVecNA(nrOfRows, 0, FstColumnAttribute::FACTOR_BASE);

	fstTable.SetFactorColumn(&factorVecNA, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows11.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows12.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsFactor)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "factor" };
	fstTable.SetColumnNames(colNames);

	// factor column with some levels but no data
	int nr_of_levels = 128;
	FactorVectorAdapter factorVec(nrOfRows, nr_of_levels, FstColumnAttribute::FACTOR_BASE);

	StringColumn* levels = factorVec.DataPtr()->Levels();
	std::vector<std::string> levelVec = *(levels->StrVector()->StrVec());
	for (int pos = 0; pos < nr_of_levels; pos++)
	{
		levelVec[pos] = to_string(pos);
	}

	fstTable.SetFactorColumn(&factorVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows13.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows14.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroRowsDouble)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames{ "double" };
	fstTable.SetColumnNames(colNames);

	// double column
	DoubleVectorAdapter doubleVec(nrOfRows, FstColumnAttribute::NONE, FstScale::UNIT);

	fstTable.SetDoubleColumn(&doubleVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows15.fst"), 0);
	ReadWriteTester::WriteReadSingleColumns(fstTable, GetFilePath("zero_rows16.fst"), 50);
}


TEST_F(SpecialTablesTest, ZeroColumns)
{
	int nrOfRows = 0;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(0, nrOfRows);

	vector<std::string> colNames{};
	fstTable.SetColumnNames(colNames);

	ReadWriteTester::WriteReadFullTable(fstTable, GetFilePath("zero_rows15.fst"), 0);
	ReadWriteTester::WriteReadFullTable(fstTable, GetFilePath("zero_rows16.fst"), 50);
}
