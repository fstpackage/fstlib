
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fststore.h>
#include <interface/fstdefines.h>
#include <interface/openmphelper.h>

#include <fsttable.h>
#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;
using namespace std;

class CharacterTest : public ::testing::Test
{
protected:
	//FilePath testDataDir;
	std::string filePath;
  IColumnFactory* columnFactory;

  IFstTable* tableReader;
  IStringArray* selectedCols;
  std::vector<int> keyIndex;
  StringArray* columnSelection = nullptr;

  virtual void SetUp()
  {
    filePath = GetFilePath("character.fst");
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
  }
};


TEST_F(CharacterTest, MultipleBlocks)
{
	int nrOfRows = 7025;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames {"Char"};

	fstTable.SetColumnNames(colNames);

	// Add character column
  StringColumn* strColumn = new StringColumn();
  strColumn->AllocateVec(nrOfRows);

  for (int elem = 0; elem < nrOfRows; elem++)
  {
    (*(strColumn->StrVector()->StrVec()))[elem] = "bla";
  }

  fstTable.SetStringColumn(strColumn, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}

TEST_F(CharacterTest, SingleThreaded)
{
  ThreadsFst(1);

  int nr_of_threads = ThreadsFst(1);
  EXPECT_EQ(nr_of_threads, 1);

  int nrOfRows = 7025;
  FstTable fstTable(nrOfRows);
  fstTable.InitTable(1, nrOfRows);

  vector<std::string> colNames{ "Char" };

  fstTable.SetColumnNames(colNames);

  // Add character column
  StringColumn* strColumn = new StringColumn();
  strColumn->AllocateVec(nrOfRows);

  for (int elem = 0; elem < nrOfRows; elem++)
  {
    (*(strColumn->StrVector()->StrVec()))[elem] = "bla";
  }

  fstTable.SetStringColumn(strColumn, 0);

  ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}
