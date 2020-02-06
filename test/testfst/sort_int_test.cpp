
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"
#include <string>


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;


class SortIntTest : public ::testing::Test
{
protected:

	virtual void SetUp()
	{
	}
};


TEST_F(SortIntTest, IncreasingRange)
{
}
