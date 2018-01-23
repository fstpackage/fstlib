

#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <string>

#include <interface/fststore.h>
#include <interface/icolumnfactory.h>
#include <interface/openmphelper.h>

#include <fsttable.h>
#include <columnfactory.h>

#include "testhelpers.h"

using namespace testing::internal;
using namespace std;


class ThreadsTest : public ::testing::Test
{
protected:
	//FilePath testDataDir;
	IFstTable* tableReader;
	IStringArray* selectedCols;
	IColumnFactory* columnFactory;
	//std::vector<int> keyIndex;
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
	}

	void TestExample()
	{
	}
};


#ifdef _OPENMP
TEST_F(ThreadsTest, SingleThread)
{
	// Define column name
	//vector<std::string> cols = { "A" };
	//columnSelection = new StringArray(cols);
	EXPECT_TRUE(HasOpenMP());

	// more than 1 threads
	int nrOfThreads = GetFstThreads();
	EXPECT_GT(nrOfThreads, 1);

	// Set to specific number of threads
	int prevThreads = ThreadsFst(2);
	EXPECT_EQ(prevThreads, nrOfThreads);

	nrOfThreads = GetFstThreads();
	EXPECT_EQ(nrOfThreads, 2);

  prevThreads = ThreadsFst(0);
  EXPECT_EQ(prevThreads, 2);
}
#endif
