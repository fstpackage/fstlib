
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/openmphelper.h>
#include <interface/fststore.h>

#include <fsttable.h>
#include <IntegerMethods.h>
#include <columnfactory.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"


using namespace testing::internal;
using namespace std;

class MultiColumnTest : public ::testing::Test
{
protected:
	FstStore* fstStore;
	FilePath testDataDir;

	virtual void SetUp()
	{
		testDataDir = GetTestDataDir();
		FilePath filePath = FilePath::ConcatPaths(testDataDir, FilePath("2.fst"));
		fstStore = new FstStore(filePath.string());
	}

	// virtual void TearDown() {}
};


TEST_F(MultiColumnTest, ReadUncompressed)
{
	// Define column name
  FstStore fstStore(GetFilePath("data1.fst"));

  FstTable table;
	StringArray selectedCols;
	ColumnFactory columnFactory;
	std::vector<int> keyIndex;

	// Read uncompressed file
	std::unique_ptr<StringColumn> col_names(new StringColumn());
	fstStore.fstRead(table, nullptr, 1, 10000, &columnFactory, keyIndex, &selectedCols, col_names.get());
}


TEST_F(MultiColumnTest, Lowcompression)
{
  // Define column name
  FstStore fstStore(GetFilePath("data1.fst"));

  FstTable table;
  StringArray selectedCols;
  ColumnFactory columnFactory;
  std::vector<int> keyIndex;

  // Read uncompressed file
  std::unique_ptr<StringColumn> col_names(new StringColumn());
  fstStore.fstRead(table, nullptr, 1, 10000, &columnFactory, keyIndex, &selectedCols, col_names.get());
  table.SetColumnNames(selectedCols);

  // Low compression
  FstStore fstStoreWrite(GetFilePath("char_comp_10000_30.fst"));
  fstStoreWrite.fstWrite(table, 30);
}


TEST_F(MultiColumnTest, Highcompression)
{
  // Define column name
  FstStore fstStore(GetFilePath("data1.fst"));

  FstTable table;
  StringArray selectedCols;
  ColumnFactory columnFactory;
  std::vector<int> keyIndex;

  // Read uncompressed file
  std::unique_ptr<StringColumn> col_names(new StringColumn());
  fstStore.fstRead(table, nullptr, 1, 10000, &columnFactory, keyIndex, &selectedCols, col_names.get());
  table.SetColumnNames(selectedCols);

  // High compression
  FstStore fstStoreWrite2(GetFilePath("char_comp_10000_70.fst"));
  fstStoreWrite2.fstWrite(table, 70);
}


TEST_F(MultiColumnTest, PerColumnCompressed)
{
  // Define column name
  FstStore fstStore(GetFilePath("data1.fst"));

  FstTable table;
  StringArray selectedCols;
  ColumnFactory columnFactory;
  std::vector<int> keyIndex;

  std::vector<std::string> colNames{ "Xint" };
  IStringArray* columnSelection = new StringArray(colNames);

  // Read uncompressed file
  std::unique_ptr<StringColumn> col_names(new StringColumn());
  fstStore.fstRead(table, columnSelection, 1, 10000, &columnFactory, keyIndex, &selectedCols, col_names.get());
  table.SetColumnNames(selectedCols);

  // Test equality tables read from different compression settings
  ReadWriteTester::WriteReadSingleColumns(table, GetFilePath("char_comp_10000_0.fst"), 0);

  ReadWriteTester::WriteReadSingleColumns(table, GetFilePath("char_comp_10000_30.fst"), 30);

  ReadWriteTester::WriteReadSingleColumns(table, GetFilePath("char_comp_10000_70.fst"), 70);
}
