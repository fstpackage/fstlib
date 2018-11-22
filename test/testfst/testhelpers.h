

#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include "gtest/gtest.h"
#include "gtest/internal/gtest-filepath.h"

#include <string>


using namespace testing::internal;
using namespace std;


template<typename Out>
void split(const std::string &s, char delim, Out result) {
	std::stringstream ss;
	ss.str(s);
	std::string item;
	while (std::getline(ss, item, delim)) {
		*(result++) = item;
	}
}

inline std::vector<std::string> split(const std::string &s, char delim)
{
	std::vector<std::string> elems;
	split(s, delim, std::back_inserter(elems));
	return elems;
}

inline FilePath GetTestDataDir()
{
	FilePath curDir = FilePath::GetCurrentDir();
	vector<std::string> dirs = split(curDir.string(), '\\');

	// account for forward slashes
	if (dirs.size() == 1)
	{
		dirs = split(curDir.string(), '/');
	}

	std::string projectDir = dirs[0];
	for (unsigned int strCount = 1; strCount < dirs.size() - 1; strCount++) {
		projectDir += '/' + dirs[strCount];
	}

	FilePath testDataDir = FilePath::ConcatPaths(FilePath(projectDir), FilePath("/testdata"));

	return testDataDir;
}

inline std::string GetFilePath(std::string fileName)
{
	FilePath filePath = FilePath::ConcatPaths(GetTestDataDir(), FilePath(fileName));
	return filePath.string();
}

#endif // TEST_HELPERS_H