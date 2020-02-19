
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"
#include <string>
#include "sort/sort.h"


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
	const int vec_size = 2049;

	const std::unique_ptr<uint32_t>	vec = std::unique_ptr<uint32_t>(new uint32_t[vec_size]);
	int* vec_p = reinterpret_cast<int*>(vec.get());

	const std::unique_ptr<uint32_t>	buffer = std::unique_ptr<uint32_t>(new uint32_t[vec_size]);
	int* buffer_p = reinterpret_cast<int*>(buffer.get());

	const int size = 100;
	for (int pos = 0; pos < size; pos++) vec_p[pos] = 1;

	radix_sort_int(vec_p, size, buffer_p);
}


TEST_F(SortIntTest, DecreasingRange)
{
	int vec_size = 6;

  int vec_p[8];
	int buffer[8];

  for (int pos = 0; pos < vec_size; pos++) vec_p[pos] = 5 - pos;
	vec_p[vec_size - 1] = 1;
	radix_sort_int(vec_p, vec_size, buffer);

	// 8 integers
	vec_size = 8;
	for (int pos = 0; pos < vec_size; pos++) vec_p[pos] = 8 - pos;
	radix_sort_int(vec_p, vec_size, buffer);
}


TEST_F(SortIntTest, TwoThreads)
{
	//int vec_size = 95000;
	int vec_size = 129;

	const std::unique_ptr<uint32_t>	vec = std::unique_ptr<uint32_t>(new uint32_t[vec_size]);
	int* vec_p = reinterpret_cast<int*>(vec.get());

	const std::unique_ptr<uint32_t>	buffer = std::unique_ptr<uint32_t>(new uint32_t[vec_size]);
	int* buffer_p = reinterpret_cast<int*>(buffer.get());

	for (int pos = 0; pos < vec_size; pos++) vec_p[pos] = pos % 2000;

	radix_sort_int(vec_p, vec_size, buffer_p);
}
