
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

class PreviousVersionTest : public ::testing::Test
{
protected:
	FilePath testDataDir;
	std::string filePath;

	virtual void SetUp()
	{
		filePath = GetFilePath("previousversion.fst");
	}
};


TEST_F(PreviousVersionTest, SmallVec)
{
	FstStore fstStore(filePath);

	ColumnFactory columnFactory;
	std::vector<int> keyIndex;
	StringArray selectedCols;
	FstTable tableRead;

	int result = 0;

	try
	{
		std::unique_ptr<StringColumn> col_names(new StringColumn());
		fstStore.fstRead(tableRead, nullptr, 1, -1, &columnFactory, keyIndex, &selectedCols, col_names.get());
	}
	catch (std::runtime_error& e)
	{
		int res = std::strcmp(e.what(), FSTERROR_NON_FST_FILE);

		EXPECT_EQ(res, 0);
	}
}

