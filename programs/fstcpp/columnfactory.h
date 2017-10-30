

#ifndef COLUMN_FACTORY_H
#define COLUMN_FACTORY_H

#include <interface/icolumnfactory.h>

#include <fsttable.h>


class ColumnFactory : public IColumnFactory
{
	// Inherited via IColumnFactory
	IFactorColumn* CreateFactorColumn(int nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new FactorVectorAdapter(nrOfRows);
	}

	ILogicalColumn* CreateLogicalColumn(int nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new LogicalVectorAdapter(nrOfRows);
	}

	IInt64Column* CreateInt64Column(int nrOfRows, FstColumnAttribute columnAttribute, short int scale)
	{
		return new Int64VectorAdapter(nrOfRows, columnAttribute, scale);
	}

	IDoubleColumn* CreateDoubleColumn(int nrOfRows, FstColumnAttribute columnAttribute, short int scale)
	{
		return new DoubleVectorAdapter(nrOfRows, columnAttribute, scale);
	}

	IIntegerColumn* CreateIntegerColumn(int nrOfRows, FstColumnAttribute columnAttribute, short int scale)
	{
		return new IntVectorAdapter(nrOfRows, columnAttribute, scale);
	}

	IByteColumn* CreateByteColumn(int nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new ByteVectorAdapter(nrOfRows, columnAttribute);
	}

	IStringColumn* CreateStringColumn(int nrOfRows, FstColumnAttribute columnAttribute)
	{
		return new StringColumn();
	}

	IStringArray* CreateStringArray()
	{
		return nullptr;
	}
};


#endif  // COLUMN_FACTORY_H
