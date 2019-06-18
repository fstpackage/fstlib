

#ifndef COLUMN_FACTORY_H
#define COLUMN_FACTORY_H

#include <interface/icolumnfactory.h>

#include <fsttable.h>


class ColumnFactory : public IColumnFactory
{
	// Inherited via IColumnFactory
	IFactorColumn* CreateFactorColumn(uint64_t nrOfRows, uint64_t nrOfLevels, FstColumnAttribute columnAttribute)
	{
		return new FactorVectorAdapter(nrOfRows, nrOfLevels, columnAttribute);
	}

	ILogicalColumn* CreateLogicalColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new LogicalVectorAdapter(nrOfRows);
	}

	IInt64Column* CreateInt64Column(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
	{
		return new Int64VectorAdapter(nrOfRows, columnAttribute, scale);
	}

	IDoubleColumn* CreateDoubleColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
	{
		return new DoubleVectorAdapter(nrOfRows, columnAttribute, scale);
	}

	IIntegerColumn* CreateIntegerColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
	{
		return new IntVectorAdapter(nrOfRows, columnAttribute, scale);
	}

	IByteColumn* CreateByteColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new ByteVectorAdapter(nrOfRows, columnAttribute);
	}

	IStringColumn* CreateStringColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new StringColumn();
	}

	IStringArray* CreateStringArray()
	{
		return nullptr;
	}
};


#endif  // COLUMN_FACTORY_H
