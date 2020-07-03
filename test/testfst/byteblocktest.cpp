
#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <interface/fststore.h>
#include <interface/fstdefines.h>

#include <fsttable.h>
#include <IntegerMethods.h>

#include "testhelpers.h"
#include "ReadWriteTester.h"
#include "../../lib/fsttable/fsttable.h"
#include "../../lib/fsttable/IntegerMethods.h"
#include "../../ext/gtest/include/gtest/gtest.h"


#ifdef _OPENMP
#include <omp.h>
#endif

using namespace testing::internal;
using namespace std;

class ByteBlockTest : public ::testing::Test
{
protected:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


TEST_F(ByteBlockTest, NaLevels)
{
}


TEST_F(ByteBlockTest, SmallVec)
{
  //const int nr_of_rows = 10000;
  //FstTable fst_table(nr_of_rows);
  //fst_table.InitTable(1, nr_of_rows);

  //vector<std::string> colNames{ "ByteBlock" };
  //fst_table.SetColumnNames(colNames);

  //// Add byteblock column
  //ByteBlockVectorAdapter* byte_block = fst_table.add_byte_block_column(0);

  //byte_block_array_ptr* block_array = byte_block->blocks();
  //uint64_array_ptr* size_array = byte_block->sizes();

  //const char** blocks = block_array->get();
  //uint64_t* sizes = size_array->get();

  //uint64_t pos = 0;
  //for (int count = 0; count < nr_of_rows; ++count)
  //{
  //  sizes[count] = pos;
  //  pos += 2 + count % 4ULL;
  //}

  //uint64_t size = pos;
  //const auto buffer = std::unique_ptr<char[]>(new char[size + 8ULL]);

  //const int nr_of_longs = static_cast<int>((size + 8ULL) / 8);
  //uint64_t* values = reinterpret_cast<uint64_t*>(buffer.get());
  //
  //for (int count = 0; count < nr_of_longs; ++count)
  //{
  //  values[count] = -12354254423 + static_cast<uint64_t>(count) * 10000;
  //}

  //pos = 0;
  //for (int count = 0; count < nr_of_rows; ++count)
  //{
  //  blocks[count] = &buffer[pos];
  //  pos += sizes[pos];
  //}

  //const string file_path = "byteblock.fst";

  //FstStore fst_store(GetFilePath(file_path));
  //fst_store.fstWrite(fst_table, 50);

//  ReadWriteTester::WriteReadSingleColumns(fst_table, file_path, 50);
}
