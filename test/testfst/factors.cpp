
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
  IColumnFactory* columnFactory = nullptr;

  IFstTable* tableReader = nullptr;
  IStringArray* selectedCols = nullptr;
  std::vector<int> keyIndex;
  StringArray* columnSelection = nullptr;

  virtual void SetUp()
  {
    filePath = GetFilePath("2.fst");
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


TEST_F(FactorTest, NaLevels)
{
	int nrOfRows = 10;
	FstTable fstTable(nrOfRows);
	fstTable.InitTable(1, nrOfRows);

	vector<std::string> colNames {"Factor"};

	fstTable.SetColumnNames(colNames);

	// Add factor column
	FactorVectorAdapter factorVec(nrOfRows, 0, FstColumnAttribute::FACTOR_BASE);
	IntConstantVal(factorVec.LevelData(), nrOfRows, FST_NA_INT);
	//StringColumn* levels = factorVec.DataPtr()->Levels();
	//levels->AllocateVec(0);
	fstTable.SetFactorColumn(&factorVec, 0);

	ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 0);
}


TEST_F(FactorTest, MediumLevels)
{
  const int nrOfRows = 1;
  FstTable fstTable(nrOfRows);
  fstTable.InitTable(1, nrOfRows);

  vector<std::string> colNames{ "Factor" };
  fstTable.SetColumnNames(colNames);

  // Add factor column
  int nr_of_levels = 128;
  FactorVectorAdapter factorVec(nrOfRows, nr_of_levels, FstColumnAttribute::FACTOR_BASE);
  IntSeq(factorVec.LevelData(), nrOfRows, 1);

  StringColumn* levels = factorVec.DataPtr()->Levels();
  //levels->AllocateVec(nr_of_levels);

  std::vector<std::string> levelVec = *(levels->StrVector()->StrVec());
  for (int pos = 0; pos < nr_of_levels; pos++)
  {
    levelVec[pos] = to_string(pos);
  }

  fstTable.SetFactorColumn(&factorVec, 0);

  FstStore fstStore(GetFilePath("medium_factor2.fst"));
  fstStore.fstWrite(fstTable, 50);

  ReadWriteTester::WriteReadSingleColumns(fstTable, filePath, 50);
}


TEST_F(FactorTest, FactorRead)
{
  FstStore fstStore(GetFilePath("medium_factor2.fst"));

  // Read fst file
  std::unique_ptr<StringColumn> col_names(new StringColumn());
  fstStore.fstRead(*tableReader, columnSelection, 1, -1, columnFactory, keyIndex, selectedCols, &*col_names);
}
