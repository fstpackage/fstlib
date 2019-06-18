
#include <vector>
#include <cstring>
#include <string>
#include <stdexcept>

#include "fsttable.h"

using namespace std;


// Inherited via IStringColumn
void StringColumn::AllocateVec(uint64_t vecLength)
{
	shared_data = make_shared<StringVector>(vecLength);
}

void StringColumn::BufferToVec(unsigned long long nrOfElements, unsigned long long startElem, unsigned long long endElem,
  unsigned long long vecOffset, unsigned int* sizeMeta, char* buf)
{
	unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // last bit is NA flag
	unsigned int* bitsNA = &sizeMeta[nrOfElements];
	unsigned int pos = 0;

	if (startElem != 0)
	{
		pos = sizeMeta[startElem - 1];  // offset previous element
	}

	// Test NA flag TODO: set as first flag and remove parameter nrOfNAInts
	unsigned int flagNA = bitsNA[nrOfNAInts - 1] & (1 << (nrOfElements % 32));
	if (flagNA == 0)  // no NA's in vector
	{
		for (unsigned long long blockElem = startElem; blockElem <= endElem; ++blockElem)
		{
			unsigned int newPos = sizeMeta[blockElem];
			string str(buf + pos, newPos - pos);
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
			pos = newPos;  // update to new string offset
		}

		return;
	}

	// We process the datablock in cycles of 32 strings. This minimizes the impact of NA testing for vectors with a small number of NA's

	unsigned long long startCycle = startElem / 32;
	unsigned long long endCycle = endElem / 32;
	unsigned int cycleNAs = bitsNA[startCycle];

	// A single 32 string cycle

	if (startCycle == endCycle)
	{
		for (unsigned long long blockElem = startElem; blockElem <= endElem; ++blockElem)
		{
			unsigned int bitMask = 1 << (blockElem % 32);

			if ((cycleNAs & bitMask) != 0)  // set string to NA
			{
				(*shared_data->StrVec())[vecOffset + blockElem - startElem] = "NA";
				pos = sizeMeta[blockElem];  // update to new string offset
				continue;
			}

			// Get string from data stream

			unsigned int newPos = sizeMeta[blockElem];

			string str(buf + pos, newPos - pos);
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
			pos = newPos;  // update to new string offset
		}

		return;
	}

	// Get possibly partial first cycle

	unsigned int firstCylceEnd = startCycle * 32 + 31;
	for (unsigned long long blockElem = startElem; blockElem <= firstCylceEnd; ++blockElem)
	{
		unsigned int bitMask = 1 << (blockElem % 32);

		if ((cycleNAs & bitMask) != 0)  // set string to NA
		{
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = "NA";
			pos = sizeMeta[blockElem];  // update to new string offset
			continue;
		}

		// Get string from data stream

		unsigned int newPos = sizeMeta[blockElem];

		string str(buf + pos, newPos - pos);
		(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
		pos = newPos;  // update to new string offset
	}

	// Get all but last cycle with fast NA test

	for (unsigned int cycle = startCycle + 1; cycle != endCycle; ++cycle)
	{
		unsigned int cycleNAs = bitsNA[cycle];
		unsigned int middleCycleEnd = cycle * 32 + 32;

		if (cycleNAs == 0)  // no NA's
		{
			for (unsigned int blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
			{
				unsigned int newPos = sizeMeta[blockElem];

				string str(buf + pos, newPos - pos);
				(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
				pos = newPos;  // update to new string offset
			}
			continue;
		}

		// Cycle contains one or more NA's

		for (unsigned int blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
		{
			unsigned int bitMask = 1 << (blockElem % 32);
			unsigned int newPos = sizeMeta[blockElem];

			if ((cycleNAs & bitMask) != 0)  // set string to NA
			{
				(*shared_data->StrVec())[vecOffset + blockElem - startElem] = "NA";
				pos = newPos;  // update to new string offset
				continue;
			}

			// Get string from data stream

			string str(buf + pos, newPos - pos);
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
			pos = newPos;  // update to new string offset
		}
	}

	// Last cycle

	cycleNAs = bitsNA[endCycle];

	++endElem;
	for (unsigned int blockElem = endCycle * 32; blockElem != endElem; ++blockElem)
	{
		unsigned int bitMask = 1 << (blockElem % 32);
		unsigned int newPos = sizeMeta[blockElem];

		if ((cycleNAs & bitMask) != 0)  // set string to NA
		{
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = "NA";
			pos = newPos;  // update to new string offset
			continue;
		}

		// Get string from data stream

		string str(buf + pos, newPos - pos);
		(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
		pos = newPos;  // update to new string offset
	}
}

const char * StringColumn::GetElement(unsigned long long elementNr)
{
	return (*shared_data->StrVec())[elementNr].c_str();
}

