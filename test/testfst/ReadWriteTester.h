
#ifndef READ_WRITE_TESTER_H
#define READ_WRITE_TESTER_H

#include <cstring>

#include "gtest/gtest.h"

#include <interface/fststore.h>

#include <fsttable.h>
#include <columnfactory.h>
#include <vector>

class ReadWriteTester
{
public:

	ReadWriteTester()
	{
	}

	static int NrOfEqualBytes(char* p1, char* p2, int size)
	{
		int res = -1;
		for (int pos = 0; pos < size; pos++)
		{
			if (p1[pos] != p2[pos])
			{
				res = pos;

				//std::vector<int> testVec(100 + (size - pos) / 4);
				//memcpy(testVec.data(), &p1[pos-400], testVec.size() * 4);

				return res;
			}
		}

		return res;
	}

	static int CompareStringVectors(vector<std::string>* stringVector1, vector<std::string>* stringVector2, size_t from, size_t size)
	{
		if (size == 0) return 0;

		auto strIt2 = stringVector2->begin() + from;
		for (auto strIt = stringVector1->begin(); strIt != stringVector1->end(); ++strIt)
		{
			const int res = strIt->compare(*strIt2);
			if (res != 0) return res;
			++strIt2;
		}

		return 0;
	}

	static bool CompareLogicalVecs(std::shared_ptr<DestructableObject> columnRead, std::shared_ptr<DestructableObject> columnOrig, unsigned long long from, unsigned long long size)
	{
		IntVector* intVecRead = static_cast<IntVector*>(&(*columnRead));
		IntVector* intVecOrig = static_cast<IntVector*>(&(*columnOrig));

		return std::memcmp(intVecRead->Data(), &(intVecOrig->Data()[from]), 4 * size) == 0;
	}

	static bool CompareIntVecs(std::shared_ptr<DestructableObject> columnRead, std::shared_ptr<DestructableObject> columnOrig, unsigned long long from, unsigned long long size)
	{
		IntVector* intVecRead = static_cast<IntVector*>(&(*columnRead));
		IntVector* intVecOrig = static_cast<IntVector*>(&(*columnOrig));

		//std::vector<int> intView(size);
		//std::memcpy(intView.data(), intVecRead->Data(), 4 * size);

		//std::vector<int> intView2(size);
		//std::memcpy(intView2.data(), &(intVecOrig->Data()[from]), 4 * size);

		return std::memcmp(intVecRead->Data(), &(intVecOrig->Data()[from]), 4 * size) == 0;
	}

	static bool CompareDoubleVecs(std::shared_ptr<DestructableObject> columnRead, std::shared_ptr<DestructableObject> columnOrig, unsigned long long from, unsigned long long size)
	{
		DoubleVector* doubleVecRead = static_cast<DoubleVector*>(&(*columnRead));
		DoubleVector* doubleVecOrig = static_cast<DoubleVector*>(&(*columnOrig));

		return std::memcmp(doubleVecRead->Data(), &(doubleVecOrig->Data()[from]), 8 * size) == 0;
	}

	static bool CompareByteVecs(std::shared_ptr<DestructableObject> columnRead, std::shared_ptr<DestructableObject> columnOrig, unsigned long long from, unsigned long long size)
	{
		ByteVector* byteVecRead = static_cast<ByteVector*>(&(*columnRead));
		ByteVector* byteVecOrig = static_cast<ByteVector*>(&(*columnOrig));

		//std::vector<int> byteView(size);
		//std::memcpy(byteView.data(), byteVecRead->Data(), size);

		//std::vector<int> byteView2(size);
		//std::memcpy(byteView2.data(), &(byteVecOrig->Data()[from]), size);

		return std::memcmp(byteVecRead->Data(), &(byteVecOrig->Data()[from]), size) == 0;
	}

	static bool CompareLongVecs(std::shared_ptr<DestructableObject> columnRead, std::shared_ptr<DestructableObject> columnOrig, unsigned long long from, unsigned long long size)
	{
		LongVector* longVecRead = static_cast<LongVector*>(&(*columnRead));
		LongVector* longVecOrig = static_cast<LongVector*>(&(*columnOrig));

		//std::vector<long long> longView(size);
		//std::memcpy(longView.data(), longVecRead->Data(), 8 * size);

		//std::vector<long long> longView2(size);
		//std::memcpy(longView2.data(), &(longVecOrig->Data()[from]), 8 * size);

		return std::memcmp(longVecRead->Data(), &(longVecOrig->Data()[from]), 8 * size) == 0;
	}

	static bool CompareColVecs(std::shared_ptr<DestructableObject> columnRead, std::shared_ptr<DestructableObject> columnOrig, FstColumnType type, unsigned long long from, unsigned long long size)
	{
		bool res;

		switch (type)
		{
		case FstColumnType::BOOL_2: {
				res = CompareLogicalVecs(columnRead, columnOrig, from, size);
				break;
			}

			case FstColumnType::INT_32:			{
				res = CompareIntVecs(columnRead, columnOrig, from, size);
				break;
			}

			case FstColumnType::BYTE:
			{
				res = CompareByteVecs(columnRead, columnOrig, from, size);
				break;
			}

			case FstColumnType::DOUBLE_64: {
				res = CompareDoubleVecs(columnRead, columnOrig, from, size);
				break;
			}

			case FstColumnType::INT_64: {
				res = CompareLongVecs(columnRead, columnOrig, from, size);
				break;
			}

			case FstColumnType::FACTOR:
			{
				auto factVecRead = static_cast<FactorVector*>(&(*columnRead));
				auto factVecOrig = static_cast<FactorVector*>(&(*columnOrig));

				int res1 = std::memcmp(factVecRead->Data(), &(factVecOrig->Data()[from]), 4 * size);
				int res2 = CompareStringVectors(factVecRead->Levels()->StrVector()->StrVec(), factVecOrig->Levels()->StrVector()->StrVec(), 0, factVecRead->Levels()->StrVector()->StrVec()->size());

				res = res1 == 0 && res2 == 0;

				break;
			}

			case FstColumnType::CHARACTER:
			{
				auto strVecRead = static_cast<StringVector*>(&(*columnRead));
				auto strVecOrig = static_cast<StringVector*>(&(*columnOrig));

				int res2 = CompareStringVectors(strVecRead->StrVec(), strVecOrig->StrVec(), from, size);

				res = res2 == 0;

				break;
			}

			default:
			{
				throw("Unknown column type");
			}
		}

		return res;
	}

	// from parameter is one-based
	static void CompareColumns(unsigned long long nrOfRows, FstTable &subSet, StringArray &selectedCols, FstTable &tableRead,
	    unsigned long long from = 1, unsigned long long size = -1)
	{
		if (size == -1) size = 1 + nrOfRows - from;

		std::shared_ptr<DestructableObject> column1;
		FstColumnType type1;
		std::string resColName1;
		std::string resAnnotation1;
    short int scale1;
		tableRead.GetColumn(0, column1, type1, resColName1, scale1, resAnnotation1);

		std::shared_ptr<DestructableObject> column2;
		FstColumnType type2;
		std::string resColName2;
		std::string resAnnotation2;
    short int scale2;
    subSet.GetColumn(0, column2, type2, resColName2, scale2, resAnnotation2);

		EXPECT_EQ(selectedCols.GetStringElement(0), resColName2);
		EXPECT_EQ(type1, type2);
        EXPECT_EQ(scale1, scale2);

		bool res = CompareColVecs(column1, column2, type1, from - 1, size);
		EXPECT_TRUE(res);
	}

	static void WriteReadSingleColumns(FstTable &fstTable, const std::string fileName, const int compression)
	{
		// Get column names
		vector<std::string>* colNames = fstTable.ColumnNames();
		ColumnFactory columnFactory;

		for (std::vector<std::string>::iterator colNameIt = colNames->begin(); colNameIt != colNames->end(); ++colNameIt)
		{
			// Subset table
			std::vector<std::string> colName = { *colNameIt };
			unsigned long long nrOfRows = fstTable.NrOfRows();
			FstTable* subSet = fstTable.SubSet(colName, 1, nrOfRows);

			// Write single column table to disk
			FstStore fstStore(fileName);
			fstStore.fstWrite(*subSet, compression);

			//// Read single column table from disk
			std::vector<int> keyIndex;
			StringArray selectedCols;
			FstTable tableRead;

			std::unique_ptr<StringColumn> col_names(new StringColumn());
			fstStore.fstRead(tableRead, nullptr, 1, -1, &columnFactory, keyIndex, &selectedCols, &*col_names);

			std::unique_ptr<StringColumn> col_names2(new StringColumn());
			fstStore.fstMeta(&columnFactory, &*col_names2);

			CompareColumns(fstTable.NrOfRows(), *subSet, selectedCols, tableRead);

			if (nrOfRows > 2)
			{
				FstTable tableRead2;
				std::unique_ptr<StringColumn> col_names(new StringColumn());
				fstStore.fstRead(tableRead2, nullptr, nrOfRows - 5, -1, &columnFactory, keyIndex, &selectedCols, &*col_names);
				CompareColumns(fstTable.NrOfRows(), *subSet, selectedCols, tableRead2, nrOfRows - 5, static_cast<unsigned long long>(-1));
			}

			// Fix manual column name assignment!
			delete subSet;
		}
	}

	static void WriteReadFullTable(FstTable& table, const std::string file_name, const int compression)
	{
		ColumnFactory columnFactory;

		// Get column names
		FstStore fstStore(file_name);
		fstStore.fstWrite(table, compression);

		//// Read single column table from disk
		std::vector<int> keyIndex;
		StringArray selectedCols;
		FstTable tableRead;

		std::unique_ptr<StringColumn> col_names(new StringColumn());
		fstStore.fstRead(tableRead, nullptr, 1, -1, &columnFactory, keyIndex, &selectedCols, &*col_names);
	}

	~ReadWriteTester()
	{
	}
};

#endif // READ_WRITE_TESTER_H