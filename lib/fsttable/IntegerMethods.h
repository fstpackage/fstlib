
#ifndef INTEGER_METHODS_H
#define INTEGER_METHODS_H

#include <cstdlib>
#include <climits>


inline void IntSeq(int* vec, const unsigned long long size, const int valStart, const int maxValue = INT_MAX)
{
	for (unsigned long long int count = 0; count != size; count++)
	{
		vec[count] = (valStart + count) % maxValue;
	}
}


inline void IntConstantVal(int* vec, const unsigned long long size, const int value)
{
	for (unsigned long long count = 0; count != size; count++)
	{
		vec[count] = value;
	}
}


/**
 * \brief Generate a logical vector of random 0, 1 and INT_MAX values
 * \param res result vector
 * \param size Size of the vector
 * \return pointer to generated logical vector
 */
inline void LogicalRandom(int* res, int size, int nrOfNAs)
{
	const int naInt = 0b10000000000000000000000000000000;

	for (int count = 0; count != size; count++)
	{
		res[count] = rand() % 2;
	}

	for (int count = 0; count != nrOfNAs; count++)
	{
		res[rand() % size] = naInt;
	}
}


inline void DoubleSeq(double* res, int size, double valStart, double delta)
{
	for (int count = 0; count != size; count++)
	{
		res[count] = valStart += delta;
	}
}


#endif  // INTEGER_METHODS_H
