
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

void StringColumn::BufferToVec(uint64_t nrOfElements, uint64_t startElem, uint64_t endElem,
	uint64_t vecOffset, uint32_t* sizeMeta, char* buf)
{
	uint32_t nrOfNAInts = 1 + nrOfElements / 32;  // last bit is NA flag
	uint32_t* bitsNA = &sizeMeta[nrOfElements];
	uint32_t pos = 0;

	if (startElem != 0)
	{
		pos = sizeMeta[startElem - 1];  // offset previous element
	}

	// Test NA flag TODO: set as first flag and remove parameter nrOfNAInts
	uint32_t flagNA = bitsNA[nrOfNAInts - 1] & (1 << (nrOfElements % 32));
	if (flagNA == 0)  // no NA's in vector
	{
		for (uint64_t blockElem = startElem; blockElem <= endElem; ++blockElem)
		{
			uint32_t newPos = sizeMeta[blockElem];
			string str(buf + pos, newPos - pos);
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
			pos = newPos;  // update to new string offset
		}

		return;
	}

	// We process the datablock in cycles of 32 strings. This minimizes the impact of NA testing for vectors with a small number of NA's

	uint64_t startCycle = startElem / 32;
	uint64_t endCycle = endElem / 32;
	uint32_t cycleNAs = bitsNA[startCycle];

	// A single 32 string cycle

	if (startCycle == endCycle)
	{
		for (uint64_t blockElem = startElem; blockElem <= endElem; ++blockElem)
		{
			uint32_t bitMask = 1 << (blockElem % 32);

			if ((cycleNAs & bitMask) != 0)  // set string to NA
			{
				(*shared_data->StrVec())[vecOffset + blockElem - startElem] = "NA";
				pos = sizeMeta[blockElem];  // update to new string offset
				continue;
			}

			// Get string from data stream

			uint32_t newPos = sizeMeta[blockElem];

			string str(buf + pos, newPos - pos);
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
			pos = newPos;  // update to new string offset
		}

		return;
	}

	// Get possibly partial first cycle

	uint32_t firstCylceEnd = startCycle * 32 + 31;
	for (uint64_t blockElem = startElem; blockElem <= firstCylceEnd; ++blockElem)
	{
		uint32_t bitMask = 1 << (blockElem % 32);

		if ((cycleNAs & bitMask) != 0)  // set string to NA
		{
			(*shared_data->StrVec())[vecOffset + blockElem - startElem] = "NA";
			pos = sizeMeta[blockElem];  // update to new string offset
			continue;
		}

		// Get string from data stream

		uint32_t newPos = sizeMeta[blockElem];

		string str(buf + pos, newPos - pos);
		(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
		pos = newPos;  // update to new string offset
	}

	// Get all but last cycle with fast NA test

	for (uint32_t cycle = startCycle + 1; cycle != endCycle; ++cycle)
	{
		uint32_t cycleNAs = bitsNA[cycle];
		uint32_t middleCycleEnd = cycle * 32 + 32;

		if (cycleNAs == 0)  // no NA's
		{
			for (uint32_t blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
			{
				uint32_t newPos = sizeMeta[blockElem];

				string str(buf + pos, newPos - pos);
				(*shared_data->StrVec())[vecOffset + blockElem - startElem] = str;
				pos = newPos;  // update to new string offset
			}
			continue;
		}

		// Cycle contains one or more NA's

		for (uint32_t blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
		{
			uint32_t bitMask = 1 << (blockElem % 32);
			uint32_t newPos = sizeMeta[blockElem];

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
	for (uint32_t blockElem = endCycle * 32; blockElem != endElem; ++blockElem)
	{
		uint32_t bitMask = 1 << (blockElem % 32);
		uint32_t newPos = sizeMeta[blockElem];

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

const char * StringColumn::GetElement(uint64_t elementNr)
{
	return (*shared_data->StrVec())[elementNr].c_str();
}

