

#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <string>

#include <interface/fststore.h>
#include <interface/icolumnfactory.h>

#include <fsttable.h>
#include <columnfactory.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"
#include "interface/fsthash.h"

using namespace testing::internal;
using namespace std;


class FstReadTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	IFstTable* tableReader;
	IStringArray* selectedCols;
	IColumnFactory* columnFactory;
	std::vector<int> keyIndex;
	StringArray* columnSelection = nullptr;

	virtual void SetUp()
	{
		tableReader = new FstTable();
		selectedCols = new StringArray();
		columnFactory = new ColumnFactory();
	}

	virtual void TearDown()
	{
		delete tableReader;
		delete selectedCols;
		delete columnFactory;
		delete columnSelection;

		int x = 10;
	}
};


TEST_F(FstReadTest, BasicRead)
{
	// Define column name
	vector<std::string> cols = { "Zdoub" };
	columnSelection = new StringArray(cols);

	FstStore fstStore(GetFilePath("data1.fst"));

  fstStore.fstMeta(columnFactory);

	// Read fst file
	try
	{
		fstStore.fstRead(*tableReader, columnSelection, 1, -1, columnFactory, keyIndex, selectedCols);
	}
	catch (const std::runtime_error&)
	{
		throw(runtime_error("Error"));
	}
}


TEST_F(FstReadTest, WrongFormat)
{
	// Define column name
	FstStore fstStore(FilePath::ConcatPaths(GetTestDataDir(), FilePath("wrongformat.fst")).string());

	// Read fst file
	EXPECT_ANY_THROW(fstStore.fstRead(*tableReader, nullptr, 1, 10, columnFactory, keyIndex, selectedCols));
}


//TEST_F(FstReadTest, FromFileRead)
//{
//	// Define column name
//	vector<std::string> cols = { "Raw" };
//	columnSelection = new StringArray(cols);
//
//	FstStore fstStore(GetFilePath("data1.fst"));
//
//	// Read fst file
//	fstStore.fstRead(*tableReader, columnSelection, 1, 30, columnFactory, keyIndex, selectedCols);
//}
