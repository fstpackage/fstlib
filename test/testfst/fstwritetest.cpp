
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/openmphelper.h>
#include <interface/fststore.h>
#include <compression/compression.h>

#include <fsttable.h>
#include <IntegerMethods.h>
#include <columnfactory.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;
using namespace std;

class FstWriteTest : public testing::Test
{
protected:
	FilePath testDataDir;
	string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("2.fst");
	}
};


TEST_F(FstWriteTest, MixedColumns)
{
	int nrOfRows = 20000;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(2, nrOfRows);

	// Add integer column
	IntVectorAdapter intVec(nrOfRows, FstColumnAttribute::INT_32_BASE, 0);
	IntSeq(intVec.Data(), nrOfRows, 0);
	std::string annotation("annotation");
	fstTable.SetIntegerColumn(&intVec, 0, annotation);

	// Add string column
	StringColumn strColumn{};
	strColumn.AllocateVec(nrOfRows);
	strColumn.SetEncoding(StringEncoding::LATIN1);
	shared_ptr<StringVector> strColVec = strColumn.StrVector();
	std::vector<std::string>* strVec = strColVec->StrVec();

	(*strVec)[0] = "hoi";
	(*strVec)[1] = "bla";

	fstTable.SetStringColumn(static_cast<IStringColumn*>(&strColumn), 1);

	// Set column names
	vector<std::string> colnames = { "Integer", "Character" };
	StringArray colNames(colnames);
	fstTable.SetColumnNames(colNames);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 30);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 70);
}

