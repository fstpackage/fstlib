
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/stack_map.h>

#include "testhelpers.h"


using namespace testing::internal;


class StackMapTest : public ::testing::Test
{
protected:
  const int nr_of_elements = 2048;
  unsigned long long factors[2048];
  unsigned long long eq_hash_factors[2048];
  stack_map element_map;

  virtual void SetUp()
  {
    // fill factor vector
    for (int elem = 0; elem < nr_of_elements; elem++)
    {
      unsigned long long value = (16 * elem);
      factors[elem] = value;

      value = (128 * elem);
      eq_hash_factors[elem] = value;
    }
  }

  virtual void TearDown()
  {
  }
};


TEST_F(StackMapTest, SmallNrOfLevels)
{
  const int nr_of_factors = 10;

  // initialize factors
  for (int elem = 0; elem < nr_of_elements; elem++)
  {
    unsigned long long value = factors[elem % nr_of_factors];
    element_map.emplace(value, elem);
  }

  unsigned short int levels = element_map.size();

  EXPECT_EQ(levels, nr_of_factors);
}


TEST_F(StackMapTest, MediumNrOfLevels)
{
  const int nr_of_factors = 1000;

  // initialize factors
  for (int elem = 0; elem < nr_of_elements; elem++)
  {
    unsigned long long value = factors[elem % nr_of_factors];
    element_map.emplace(value, elem);
  }

  unsigned short int levels = element_map.size();

  EXPECT_EQ(levels, nr_of_factors);
}


TEST_F(StackMapTest, OverflowNrOfLevels)
{
  const int nr_of_factors = 1000;

  // initialize factors
  for (int elem = 0; elem < nr_of_elements; elem++)
  {
    unsigned long long value = eq_hash_factors[elem % nr_of_factors];
    element_map.emplace(value, elem);
  }

  unsigned short int levels = element_map.size();

  EXPECT_EQ(levels, nr_of_factors);
}
