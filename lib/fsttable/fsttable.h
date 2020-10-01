

#ifndef FSTTABLE_H
#define FSTTABLE_H

#include <vector>
#include <string>
#include <stdexcept>
#include <memory>
#include <algorithm>

#include <interface/ifsttable.h>
#include <interface/fstdefines.h>

#include <byteblock/byteblock_v13.h>
#include "../fst/interface/fstdefines.h"
#include "../fst/interface/istringwriter.h"


class DestructableObject
{
public:
	virtual ~DestructableObject() {}
};


class StringVector : public DestructableObject
{
	std::vector<std::string>* data;

public:
	StringVector(uint64_t vecLength)
	{
		this->data = new std::vector<std::string>(vecLength);
	}

	~StringVector()
	{
		delete data;
	}

	std::vector<std::string>* StrVec() const { return data; }
};


class StringColumn : public IStringColumn
{
	std::shared_ptr<StringVector> shared_data = nullptr;
	StringEncoding string_encoding;

public:
	StringColumn()
	{
		
	}

	~StringColumn()
	{
	}

	void AllocateVec(uint64_t vecLength);

	void SetEncoding(StringEncoding stringEncoding)
	{
    this->string_encoding = stringEncoding;
	}

	StringEncoding GetEncoding()
	{
	    return string_encoding;
	}

	void BufferToVec(uint64_t nrOfElements, uint64_t startElem, uint64_t endElem, uint64_t vecOffset,
    uint32_t* sizeMeta, char* buf);

	const char* GetElement(uint64_t elementNr);

	std::shared_ptr<StringVector> StrVector() const { return shared_data; }
};


class IntVector : public DestructableObject
{
	int* data = nullptr;

public:
	IntVector(uint64_t length)
	{
		if (length >0)
		{
			this->data = new int[length];
		}
	}

	~IntVector()
	{
		delete[] data;
	}

	int* Data()
	{
		return data;
	}
};


class ByteVector : public DestructableObject
{
	char* data;

public:
	ByteVector(uint64_t length)
	{
		this->data = new char[length];
	}

	~ByteVector()
	{
		delete[] data;
	}

	char* Data()
	{
		return data;
	}
};


class LongVector : public DestructableObject
{
	long long* data;

public:
	LongVector(uint64_t length)
	{
		this->data = new long long[length];
	}

	~LongVector()
	{
		delete[] data;
	}

	long long* Data()
	{
		return data;
	}
};


class DoubleVector : public DestructableObject
{
	double* data;

public:
	DoubleVector(uint64_t length)
	{
		this->data = new double[length];
	}

	~DoubleVector()
	{
		delete[] data;
	}

	double* Data()
	{
		return data;
	}

};


class FactorVector : public DestructableObject
{
	int* data = nullptr;
	StringColumn* levels = nullptr;
	uint64_t length;

public:
	FactorVector(uint64_t length)
	{
		this->length = length;
		if (length > 0) this->data = new int[length];
		this->levels = new StringColumn();  // AllocVector HAS to be called?
	}

	~FactorVector()
	{
		if (data != nullptr)
		{
			delete[] data;
		}
		delete levels;
	}

	int* Data()
	{
		return data;
	}

	StringColumn* Levels() const
	{
		return levels;
	}
};


class ByteBlockVectorAdapter : public IByteBlockColumn, public DestructableObject
{
	std::unique_ptr<byte_block_array_ptr> byte_blocks;
	std::unique_ptr<uint64_array_ptr> block_sizes;

  public:
	ByteBlockVectorAdapter(uint64_t length)
	{
		byte_blocks = std::unique_ptr<byte_block_array_ptr>(new byte_block_array_ptr(length));
		block_sizes = std::unique_ptr<uint64_array_ptr>(new uint64_array_ptr(length));
	}

	~ByteBlockVectorAdapter()
	{
	}

	byte_block_array_ptr* blocks()
	{
		return byte_blocks.get();
	}

	uint64_array_ptr* sizes()
	{
		return block_sizes.get();
	}

	void SetSizesAndPointers(const char** elements, uint64_t* sizes, uint64_t row_start, uint64_t block_size)
	{
		auto start_block = this->blocks()->get();
		auto start_block_address = &start_block[row_start];

		auto start_size = this->sizes()->get();
		auto start_size_address = &start_size[row_start];


		memcpy(elements, start_block_address, block_size * 8);
		memcpy(sizes, start_size_address, block_size * 8);
	}
};


class IntVectorAdapter : public IIntegerColumn
{
	std::shared_ptr<IntVector> shared_data;
	FstColumnAttribute columnAttribute;
	short int scale;
	std::string annotation;

public:
	IntVectorAdapter(uint64_t length, FstColumnAttribute columnAttribute, short int scale)
	{
		shared_data = std::make_shared<IntVector>(std::max(length, (uint64_t) 1));
	  this->columnAttribute = columnAttribute;
	  this->scale = scale;
	}

	~IntVectorAdapter()
	{
	}

  FstColumnAttribute Attribute()
	{
    return columnAttribute;
	}

  short int Scale()
	{
    return scale;
	}

	int* Data()
	{
		return shared_data->Data();
	}

	std::shared_ptr<DestructableObject> DataPtr() const
	{
		return shared_data;
	}

  void Annotate(std::string annotation)
	{
    this->annotation = annotation;
	}
};


class ByteVectorAdapter : public IByteColumn
{
	std::shared_ptr<ByteVector> shared_data;

public:
	ByteVectorAdapter(uint64_t length, FstColumnAttribute columnAttribute = FstColumnAttribute::NONE)
	{
		shared_data = std::make_shared<ByteVector>(std::max(length, (uint64_t) 1));
	}

	~ByteVectorAdapter()
	{
	}

	char* Data()
	{
		return shared_data->Data();
	}

	std::shared_ptr<DestructableObject> DataPtr() const
	{
		return shared_data;
	}
};


class Int64VectorAdapter : public IInt64Column
{
	std::shared_ptr<LongVector> shared_data;
	FstColumnAttribute columnAttribute;
	short int scale;

public:
	Int64VectorAdapter(uint64_t length, FstColumnAttribute columnAttribute, short int scale)
	{
		shared_data = std::make_shared<LongVector>(std::max(length, (uint64_t) 1));
		this->columnAttribute = columnAttribute;
	    this->scale = scale;
	}

	~Int64VectorAdapter()
	{
	}

	long long* Data()
	{
		return shared_data->Data();
	};

	std::shared_ptr<LongVector> DataPtr() const
	{
		return shared_data;
	}

	FstColumnAttribute ColumnAttribute() const
	{
		return columnAttribute;
	}

};


class LogicalVectorAdapter : public ILogicalColumn
{
	std::shared_ptr<IntVector> shared_data;

public:
	LogicalVectorAdapter(uint64_t length)
	{
		shared_data = std::make_shared<IntVector>(std::max(length, (uint64_t) 1));
	}

	~LogicalVectorAdapter()
	{
	}

	int* Data()
	{
		return shared_data->Data();
	}

	std::shared_ptr<IntVector> DataPtr() const
	{
		return shared_data;
	}
};


class FactorVectorAdapter : public IFactorColumn
{
	std::shared_ptr<FactorVector> shared_data;

public:
	FactorVectorAdapter(uint64_t length, uint64_t nr_of_levels, FstColumnAttribute columnAttribute)
	{
		shared_data = std::make_shared<FactorVector>(std::max(length, (uint64_t) 1));

		StringColumn* levels = shared_data->Levels();
		levels->AllocateVec(nr_of_levels);
	}

	~FactorVectorAdapter()
	{
	}

	int* LevelData()
	{
		return shared_data->Data();
	}

	IStringColumn* Levels()
	{
		return shared_data->Levels();
	}

	std::shared_ptr<FactorVector> DataPtr() const
	{
		return shared_data;
	}
};


class DoubleVectorAdapter : public IDoubleColumn
{
	std::shared_ptr<DoubleVector> shared_data;
	FstColumnAttribute columnAttribute;
	std::string annotation;
  short int scale;

public:
	DoubleVectorAdapter(uint64_t length, FstColumnAttribute columnAttribute, short int scale)
	{
		shared_data = std::make_shared<DoubleVector>(std::max(length, (uint64_t) 1));
		this->columnAttribute = columnAttribute;
	    this->scale = scale;
	}

	~DoubleVectorAdapter()
	{
	}

	double* Data()
	{
		return shared_data->Data();
	}

	std::shared_ptr<DoubleVector> DataPtr()
	{
		return shared_data;
	}

	void Annotate(std::string annotation)
	{
		this->annotation = annotation;
	}

	std::string Attribute() const
	{
		return annotation;
	}
};


class StringArray : public IStringArray
{
	std::vector<std::string>* strVec = nullptr;
  StringEncoding string_encoding;
  bool external = true;

public:
	StringArray() {	}

	StringArray(std::vector<std::string> &strVector)
	{
		SetArray(strVector);
	}

	~StringArray()
	{
		if (!external)
		{
			delete strVec;
		}
	}

	std::string GetStringElement(uint64_t index)
	{
		return strVec->at(index);
	}

	void SetArray(std::vector<std::string> &strVector)
	{
		if (!external)
		{
			delete strVec;
		}
		external = true;
		strVec = &strVector;
	}

	void AllocateArray(uint64_t vecLength)
	{
		if (!external)
		{
			delete strVec;
		}
		external = false;
		strVec = new std::vector<std::string>(vecLength);
	}

	void SetElement(uint64_t elementNr, const char * str)
	{
		std::string strElem(str);
		(*strVec)[elementNr] = strElem;
	}

	void SetElement(uint64_t elementNr, const char * str, uint32_t strLen)
	{
		std::string strElem(str, strLen);
		(*strVec)[elementNr] = strElem;
	}

	const char* GetElement(uint64_t elementNr)
	{
		return (*strVec)[elementNr].c_str();
	}

	uint64_t Length()
	{
		return (uint64_t) strVec->size();
	}

	void SetEncoding(StringEncoding string_encoding)
	{
		this->string_encoding = string_encoding;
	}
};


class BlockWriter : public IStringWriter
{
	std::vector<std::string>* strVecP;

public:
	// Stack buffers
	uint32_t naIntsBuf[1 + BLOCKSIZE_CHAR / 32];  // we have 32 NA bits per integer
	uint32_t strSizesBuf[BLOCKSIZE_CHAR];  // we have 32 NA bits per integer
	char strBuf[MAX_CHAR_STACK_SIZE];

	// Heap buffer
	uint32_t heapBufSize = BASIC_HEAP_SIZE;
	char* heapBuf = new char[BASIC_HEAP_SIZE];

	BlockWriter(std::vector<std::string> &strVec)
	{
		strVecP = &strVec;

		this->naInts = naIntsBuf;
		this->strSizes = strSizesBuf;
		this->vecLength = static_cast<uint32_t>(strVec.size());
	}

	~BlockWriter() { delete[] heapBuf; }

	void SetBuffersFromVec(uint64_t startCount, uint64_t endCount)
	{
		// Determine string lengths
		// uint32_t startCount = block * BLOCKSIZE_CHAR;
		const uint64_t nrOfElements = endCount - startCount;  // the string at position endCount is not included
		const uint64_t nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag

		uint32_t totSize = 0;
		//uint32_t hasNA = 0;
		int sizeCount = -1;

		memset(naInts, 0, nrOfNAInts * 4);  // clear NA bit metadata block (neccessary?)

		for (uint64_t count = startCount; count != endCount; ++count)
		{
			std::string strElem = (*strVecP)[count];

			// Skip NA string concept for now
			//if (strElem == NA_STRING)  // set NA bit
			//{
			//	++hasNA;
			//	uint32_t intPos = (count - startCount) / 32;
			//	uint32_t bitPos = (count - startCount) % 32;
			//	naInts[intPos] |= 1 << bitPos;
			//}

			totSize += static_cast<uint32_t>(strElem.size());
			strSizes[++sizeCount] = totSize;
		}

		// Skip NA string concept for now
		//if (hasNA != 0)  // NA's present in vector
		//{
		//	// set last bit
		//	int bitPos = nrOfElements % 32;
		//	naInts[nrOfNAInts - 1] |= 1 << bitPos;
		//}


		// Write string data
		uint32_t pos;
		uint32_t lastPos = 0;
		sizeCount = -1;

		activeBuf = strBuf;

		if (totSize > MAX_CHAR_STACK_SIZE)  // don't use cache memory
		{
			if (totSize > heapBufSize)
			{
				delete[] heapBuf;
				heapBufSize = (uint32_t) (totSize * 1.1);
				heapBuf = new char[heapBufSize];
			}

			activeBuf = heapBuf;
		}

		for (uint64_t count = startCount; count < endCount; ++count)
		{
			const char* str = (*strVecP)[count].c_str();
			pos = strSizes[++sizeCount];
			strncpy(activeBuf + lastPos, str, pos - lastPos);
			lastPos = pos;
		}

		bufSize = totSize;
	}

	StringEncoding Encoding()
	{
		return StringEncoding::LATIN1;
	}
};


class FstTable : public IFstTable
{
	std::vector<std::shared_ptr<DestructableObject>>* columns = nullptr;
	std::vector<FstColumnType>* columnTypes = nullptr;
	std::vector<FstColumnAttribute>* columnAttributes = nullptr;
	std::vector<std::string>* colAnnotations = nullptr;
	std::vector<std::string>* colNames = nullptr;
	std::vector<short int>* colScales = nullptr;
	uint64_t nrOfRows;

public:
	FstTable()
	{
		this->nrOfRows = 0;
	}

	FstTable(uint64_t nrOfRows)
	{
		this->nrOfRows = nrOfRows;
	}

	~FstTable()
	{
		delete this->columnTypes;
		delete this->colNames;
		delete this->columns;
		delete this->columnAttributes;
		delete this->colAnnotations;
	  delete this->colScales;
	}

	FstTable* SubSet(std::vector<std::string> &columnNames, uint64_t startRow, uint64_t endRow) const
	{
		FstTable* subset = new FstTable();
		subset->InitTable(static_cast<uint32_t>(columnNames.size()), 1 + endRow - startRow);

		int subSetColNr = 0;
		for (std::vector<std::string>::iterator colIt = columnNames.begin(); colIt != columnNames.end(); ++colIt)
		{
			auto it = std::find(colNames->begin(), colNames->end(), *colIt);
			int index;

			// Search column
			if (it == colNames->end())
			{
				// name not in vector
				throw("Unknown column");
			}
			else
			{
				index = static_cast<int>(it - colNames->begin());
			}

			// Copy smart pointers to columns
			subset->SetColumn((*columns)[index], subSetColNr, (*columnTypes)[index], (*columnAttributes)[index], (*colNames)[index],
				(*colScales)[index], (*colAnnotations)[index]);
			subSetColNr++;
		}

		return subset;
	}

	void GetColumn(int colNr, std::shared_ptr<DestructableObject> &column, FstColumnType &type, std::string &colName,
		short int &colScale, std::string &colAnnotation) const
	{
		column = (*columns)[colNr];
		type = (*columnTypes)[colNr];
		colName = (*colNames)[colNr];
		colAnnotation = (*colAnnotations)[colNr];
	  colScale = (*colScales)[colNr];
	}

	void SetColumn(std::shared_ptr<DestructableObject> column, int colNr, FstColumnType type, FstColumnAttribute attribute, std::string colName,
		short int scale, std::string annotation) const
	{
		(*columns)[colNr] = column;
		(*columnTypes)[colNr] = type;
		(*columnAttributes)[colNr] = attribute;
		(*colNames)[colNr] = colName;
		(*colAnnotations)[colNr] = annotation;
	    (*colScales)[colNr] = scale;
	}

	std::vector<std::string>* ColumnNames()
	{
		return this->colNames;
	}

	void SetColumnNames(StringArray &colnames) const
	{
		for (int colNr = 0; colNr < this->colNames->size(); colNr++)
		{
			(*colNames)[colNr] = colnames.GetElement(colNr);
		}
	}

	void SetColumnNames(std::vector<std::string> colNameVec) const
	{
		int colNr = 0;
		for (std::vector<std::string>::iterator colNameIt = colNameVec.begin(); colNameIt != colNameVec.end(); ++colNameIt)
		{
			(*colNames)[colNr++] = colNameIt->c_str();
		}
	}

	void InitTable(uint32_t nrOfCols, uint64_t nrOfRows)
	{
		if (this->columns != nullptr)
		{
			throw(std::runtime_error("InitTable called multiple times!"));
		}

		this->nrOfRows = nrOfRows;

		this->columns = new std::vector<std::shared_ptr<DestructableObject>>(nrOfCols);
		this->columnTypes = new std::vector<FstColumnType>(nrOfCols);
		this->columnAttributes = new std::vector<FstColumnAttribute>(nrOfCols);
		this->colNames = new std::vector<std::string>(nrOfCols);
		this->colAnnotations = new std::vector<std::string>(nrOfCols);
		this->colScales = new std::vector<short int>(nrOfCols);
  }

	void SetStringColumn(IStringColumn * stringColumn, int colNr)
	{
		StringColumn* strCol = static_cast<StringColumn*>(stringColumn);
		(*columns)[colNr] = strCol->StrVector();
		(*columnTypes)[colNr] = FstColumnType::CHARACTER;
	}

	void SetIntegerColumn(IIntegerColumn * integerColumn, int colNr)
	{
		IntVectorAdapter* intAdapter = (IntVectorAdapter*) integerColumn;
		(*columns)[colNr] = intAdapter->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::INT_32;
		(*columnAttributes)[colNr] = intAdapter->Attribute();
		(*colScales)[colNr] = intAdapter->Scale();
	}

  void SetIntegerColumn(IIntegerColumn * integerColumn, int colNr, std::string &annotation)
  {
    IntVectorAdapter* intAdapter = (IntVectorAdapter*)integerColumn;
    (*columns)[colNr] = intAdapter->DataPtr();
    (*columnTypes)[colNr] = FstColumnType::INT_32;
    (*columnAttributes)[colNr] = intAdapter->Attribute();
    (*colAnnotations)[colNr] = annotation;
    (*colScales)[colNr] = intAdapter->Scale();
  }

	void SetByteColumn(IByteColumn * byteColumn, int colNr)
	{
		ByteVectorAdapter* byteAdapter = (ByteVectorAdapter*) byteColumn;
		(*columns)[colNr] = byteAdapter->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::BYTE;
	}

	void SetLogicalColumn(ILogicalColumn * logicalColumn, int colNr)
	{
		LogicalVectorAdapter* logicalAdapter = (LogicalVectorAdapter*) logicalColumn;
		(*columns)[colNr] = logicalAdapter->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::BOOL_2;
	}

	void SetInt64Column(IInt64Column* int64Column, int colNr)
	{
		Int64VectorAdapter* int64Adapter = (Int64VectorAdapter*) int64Column;
		(*columns)[colNr] = int64Adapter->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::INT_64;
		(*columnAttributes)[colNr] = int64Adapter->ColumnAttribute();
	}

	void SetDoubleColumn(IDoubleColumn * doubleColumn, int colNr, std::string &annotation)
	{
		DoubleVectorAdapter* doubleAdapter = (DoubleVectorAdapter*) doubleColumn;
		(*columns)[colNr] = doubleAdapter->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::DOUBLE_64;
    (*colAnnotations)[colNr] = annotation;
	}

  void SetDoubleColumn(IDoubleColumn* doubleColumn, int colNr)
  {
    DoubleVectorAdapter* doubleAdapter = (DoubleVectorAdapter*) doubleColumn;
    (*columns)[colNr] = doubleAdapter->DataPtr();
    (*columnTypes)[colNr] = FstColumnType::DOUBLE_64;
  }

	ByteBlockVectorAdapter* add_byte_block_column(uint32_t col_nr)
  {
		auto byte_block = new ByteBlockVectorAdapter(this->NrOfRows());

    const std::shared_ptr<DestructableObject> s_byte_block = std::shared_ptr<DestructableObject>(byte_block);

		(*columns)[col_nr] = s_byte_block;
		(*columnTypes)[col_nr] = FstColumnType::BYTE_BLOCK;

		return byte_block;
	}

	void SetFactorColumn(IFactorColumn* factorColumn, int colNr)
	{
		FactorVectorAdapter* factColumn = static_cast<FactorVectorAdapter*>(factorColumn);
		(*columns)[colNr] = factColumn->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::FACTOR;
	}

	void SetKeyColumns(int * keyColPos, uint32_t nrOfKeys)
	{
	}

	FstColumnType ColumnType(uint32_t colNr, FstColumnAttribute &columnAttribute, short int &scale, std::string &annotation, bool &hasAnnotation)
	{
		columnAttribute = (*columnAttributes)[colNr];
		annotation += (*colAnnotations)[colNr];
    scale = (*colScales)[colNr];
    hasAnnotation = false;

		return (*columnTypes)[colNr];
	}

	IStringWriter* GetStringWriter(uint32_t colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		StringVector* strVec = static_cast<StringVector*>(&(*sp));
		std::vector<std::string>* strVecP = strVec->StrVec();
		return new BlockWriter(*strVecP);
	}

	int* GetLogicalWriter(uint32_t colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		IntVector* intVec = static_cast<IntVector*>(&(*sp));
		return intVec->Data();
	}

	int* GetIntWriter(uint32_t colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		IntVector* intVec = static_cast<IntVector*>(&(*sp));
		return intVec->Data();
	}

	char* GetByteWriter(uint32_t colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		ByteVector* byteVec = static_cast<ByteVector*>(&(*sp));
		return byteVec->Data();
	}

	int* GetDateTimeWriter(uint32_t colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		IntVector* intVec = static_cast<IntVector*>(&(*sp));
		return intVec->Data();
	}

	long long* GetInt64Writer(uint32_t colNr)
	{
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		LongVector* int64Vec = static_cast<LongVector*>(&(*sp));
		return int64Vec->Data();
	}


	double* GetDoubleWriter(uint32_t colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		DoubleVector* dblVec = static_cast<DoubleVector*>(&(*sp));
		return dblVec->Data();
	}

	IByteBlockColumn* GetByteBlockWriter(uint32_t col_nr)
	{
		std::shared_ptr<DestructableObject> byte_block_writer = (*columns)[col_nr];

		std::shared_ptr<ByteBlockVectorAdapter> byte_block = std::static_pointer_cast<ByteBlockVectorAdapter>(byte_block_writer);
		return byte_block.get();
	}


	IStringWriter* GetLevelWriter(uint32_t colNr)
	{
		// TODO: Add colType checker

		// Get factor vector
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		FactorVector* factorVec = static_cast<FactorVector*>(&(*sp));

		// Get level vector from factor
		StringColumn* strCol = factorVec->Levels();
		std::shared_ptr<DestructableObject> sharedStrP = strCol->StrVector();
		StringVector* strVec = static_cast<StringVector*>(&(*sharedStrP));

		// Create blockwriter for levels
		std::vector<std::string>* strVecP = strVec->StrVec();
		return new BlockWriter(*strVecP);
	}

	IStringWriter* GetColNameWriter()
	{
		// TODO: Add colType checker
		return new BlockWriter(*colNames);
	}

	void GetKeyColumns(int* keyColPos) {}

	uint32_t NrOfKeys()
	{
		return 0;
	}

	uint32_t NrOfColumns()
	{
		return (uint32_t) columnTypes->size();
	}

	uint64_t NrOfRows()
	{
		return nrOfRows;
	}

  void SetColNames(IStringArray* col_names)
	{
    for (int colNr = 0; colNr < this->colNames->size(); colNr++)
    {
      (*colNames)[colNr] = col_names->GetElement(colNr);
    }
  }
};



#endif  // FSTTABLE_H
