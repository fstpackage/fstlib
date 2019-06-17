

#ifndef FSTTABLE_H
#define FSTTABLE_H

#include <vector>
#include <string>
#include <stdexcept>
#include <memory>
#include <algorithm>

#include <interface/ifsttable.h>
#include <interface/fstdefines.h>


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
	std::shared_ptr<StringVector> shared_data;
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

	void BufferToVec(unsigned long long nrOfElements, unsigned long long startElem, unsigned long long endElem, unsigned long long vecOffset,
    unsigned int* sizeMeta, char* buf);

	const char* GetElement(unsigned long long elementNr);

	std::shared_ptr<StringVector> StrVector() const { return shared_data; }
};


class IntVector : public DestructableObject
{
	int* data;

public:
	IntVector(uint64_t length)
	{
		this->data = new int[length];
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
	StringColumn* levels;
	unsigned long long length;

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


class IntVectorAdapter : public IIntegerColumn
{
	std::shared_ptr<IntVector> shared_data;
  FstColumnAttribute columnAttribute;
  short int scale;
  std::string annotation;

public:
	IntVectorAdapter(uint64_t length, FstColumnAttribute columnAttribute, short int scale)
	{
		shared_data = std::make_shared<IntVector>(length);
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
		shared_data = std::make_shared<ByteVector>(length);
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
		shared_data = std::make_shared<LongVector>(length);
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
		shared_data = std::make_shared<IntVector>(length);
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
		shared_data = std::make_shared<FactorVector>(length);

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
		shared_data = std::make_shared<DoubleVector>(length);
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

	void SetElement(uint64_t elementNr, const char * str, unsigned int strLen)
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
	unsigned int naIntsBuf[1 + BLOCKSIZE_CHAR / 32];  // we have 32 NA bits per integer
	unsigned int strSizesBuf[BLOCKSIZE_CHAR];  // we have 32 NA bits per integer
	char strBuf[MAX_CHAR_STACK_SIZE];

	// Heap buffer
	unsigned int heapBufSize = BASIC_HEAP_SIZE;
	char* heapBuf = new char[BASIC_HEAP_SIZE];

	BlockWriter(std::vector<std::string> &strVec)
	{
		strVecP = &strVec;

		this->naInts = naIntsBuf;
		this->strSizes = strSizesBuf;
		this->vecLength = static_cast<unsigned int>(strVec.size());
	}

	~BlockWriter() { delete[] heapBuf; }

	void SetBuffersFromVec(uint64_t startCount, uint64_t endCount)
	{
		// Determine string lengths
		// unsigned int startCount = block * BLOCKSIZE_CHAR;
		unsigned int nrOfElements = endCount - startCount;  // the string at position endCount is not included
		unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag

		unsigned int totSize = 0;
		//unsigned int hasNA = 0;
		int sizeCount = -1;

		memset(naInts, 0, nrOfNAInts * 4);  // clear NA bit metadata block (neccessary?)

		for (unsigned long long count = startCount; count != endCount; ++count)
		{
			std::string strElem = (*strVecP)[count];

			// Skip NA string concept for now
			//if (strElem == NA_STRING)  // set NA bit
			//{
			//	++hasNA;
			//	unsigned int intPos = (count - startCount) / 32;
			//	unsigned int bitPos = (count - startCount) % 32;
			//	naInts[intPos] |= 1 << bitPos;
			//}

			totSize += static_cast<unsigned int>(strElem.size());
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
		unsigned int pos;
		unsigned int lastPos = 0;
		sizeCount = -1;

		activeBuf = strBuf;

		if (totSize > MAX_CHAR_STACK_SIZE)  // don't use cache memory
		{
			if (totSize > heapBufSize)
			{
				delete[] heapBuf;
				heapBufSize = (unsigned int) (totSize * 1.1);
				heapBuf = new char[heapBufSize];
			}

			activeBuf = heapBuf;
		}

		for (unsigned long long count = startCount; count < endCount; ++count)
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
	unsigned long long nrOfRows;

public:
	FstTable()
	{
		this->nrOfRows = 0;
	}

	FstTable(unsigned long long nrOfRows)
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

	FstTable* SubSet(std::vector<std::string> &columnNames, unsigned long long startRow, unsigned long long endRow) const
	{
		FstTable* subset = new FstTable();
		subset->InitTable(static_cast<unsigned int>(columnNames.size()), 1 + endRow - startRow);

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

	void InitTable(unsigned int nrOfCols, unsigned long long nrOfRows)
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

  void SetDoubleColumn(IDoubleColumn * doubleColumn, int colNr)
  {
    DoubleVectorAdapter* doubleAdapter = (DoubleVectorAdapter*) doubleColumn;
    (*columns)[colNr] = doubleAdapter->DataPtr();
    (*columnTypes)[colNr] = FstColumnType::DOUBLE_64;
  }

	void SetFactorColumn(IFactorColumn* factorColumn, int colNr)
	{
		FactorVectorAdapter* factColumn = static_cast<FactorVectorAdapter*>(factorColumn);
		(*columns)[colNr] = factColumn->DataPtr();
		(*columnTypes)[colNr] = FstColumnType::FACTOR;
	}

	void SetKeyColumns(int * keyColPos, unsigned int nrOfKeys)
	{
	}

	FstColumnType ColumnType(unsigned int colNr, FstColumnAttribute &columnAttribute, short int &scale, std::string &annotation, bool &hasAnnotation)
	{
		columnAttribute = (*columnAttributes)[colNr];
		annotation += (*colAnnotations)[colNr];
    scale = (*colScales)[colNr];
    hasAnnotation = false;

		return (*columnTypes)[colNr];
	}

	IStringWriter* GetStringWriter(unsigned int colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		StringVector* strVec = static_cast<StringVector*>(&(*sp));
		std::vector<std::string>* strVecP = strVec->StrVec();
		return new BlockWriter(*strVecP);
	}

	int* GetLogicalWriter(unsigned int colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		IntVector* intVec = static_cast<IntVector*>(&(*sp));
		return intVec->Data();
	}

	int* GetIntWriter(unsigned int colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		IntVector* intVec = static_cast<IntVector*>(&(*sp));
		return intVec->Data();
	}

	char* GetByteWriter(unsigned int colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		ByteVector* byteVec = static_cast<ByteVector*>(&(*sp));
		return byteVec->Data();
	}

	int* GetDateTimeWriter(unsigned int colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		IntVector* intVec = static_cast<IntVector*>(&(*sp));
		return intVec->Data();
	}

	long long* GetInt64Writer(unsigned int colNr)
	{
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		LongVector* int64Vec = static_cast<LongVector*>(&(*sp));
		return int64Vec->Data();
	}


	double* GetDoubleWriter(unsigned int colNr)
	{
		// TODO: Add colType checker
		std::shared_ptr<DestructableObject> sp = (*columns)[colNr];
		DoubleVector* dblVec = static_cast<DoubleVector*>(&(*sp));
		return dblVec->Data();
	}

	IStringWriter* GetLevelWriter(unsigned int colNr)
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

	unsigned int NrOfKeys()
	{
		return 0;
	}

	unsigned int NrOfColumns()
	{
		return (unsigned int) columnTypes->size();
	}

	unsigned long long NrOfRows()
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
