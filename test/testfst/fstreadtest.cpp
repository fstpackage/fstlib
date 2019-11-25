
#include <string>
#include <climits>

#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

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

	std::unique_ptr<StringColumn> col_names(new StringColumn());
	fstStore.fstMeta(columnFactory, &*col_names);

	// Read fst file
	try
	{
		std::unique_ptr<StringColumn> col_names2(new StringColumn());
		fstStore.fstRead(*tableReader, columnSelection, 1, -1, columnFactory, keyIndex, selectedCols, col_names2.get());
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
	std::unique_ptr<StringColumn> col_names(new StringColumn());
	EXPECT_ANY_THROW(fstStore.fstRead(*tableReader, nullptr, 1, 10, columnFactory, keyIndex, selectedCols, col_names.get()));
}


//TEST_F(FstReadTest, FromFileRead)
//{
//	// Define column name
//	vector<std::string> cols = { "X" };
//	columnSelection = new StringArray(cols);
//
//	FstStore fstStore("C:\\Programming\\repositories\\fst_world\\fst_algorithms\\arbitrary.fst");
//
//	// Read fst file
//	std::unique_ptr<StringColumn> col_names(new StringColumn());
//	fstStore.fstRead(*tableReader, columnSelection, 1, -1, columnFactory, keyIndex, selectedCols, col_names.get());
//}
