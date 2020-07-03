

#ifndef TYPE_FACTORY_H
#define TYPE_FACTORY_H


#include <interface/fstcompressor.h>


class Blob
{
	unsigned char* pData = nullptr;

public:
	Blob(unsigned long long size)
	{
		pData = new unsigned char[size];
	}

	unsigned char* Data()
	{
		return pData;
	}

	~Blob()
	{
		delete[] pData;
	}
};


class BlobContainer : public IBlobContainer
{
	std::shared_ptr<Blob> shared_data;

	unsigned long long blobSize = 0;

public:
	BlobContainer(unsigned long long size)
	{
		shared_data = std::make_shared<Blob>(std::max(size, 1ULL));
		blobSize = size;
	}

	unsigned char* Data()
	{
		return shared_data->Data();
	}

	std::shared_ptr<Blob> DataPtr() const
	{
		return shared_data;
	}

	unsigned long long Size()
	{
		return blobSize;
	}
};


class TypeFactory : public ITypeFactory
{
public:
	~TypeFactory() {}

	IBlobContainer* CreateBlobContainer(unsigned long long size)
	{
		return new BlobContainer(size);
	}
};


#endif  // TYPE_FACTORY_H
