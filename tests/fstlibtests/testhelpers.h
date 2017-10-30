

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

	int cutCount = 0;
	for (int strElem = (int) dirs.size() - 1; strElem >= 0; strElem--) {
		if (dirs[strElem] != "build") {
			cutCount++;
			continue;
		}
		break;
	}

	std::string projectDir = dirs[0];

	for (int strElem = 1; strElem < dirs.size() - cutCount - 1; strElem++) {
		projectDir += '\\' + dirs[strElem];
	}

	FilePath testDataDir = FilePath::ConcatPaths(FilePath(projectDir), FilePath("tests\\testData"));
	testDataDir.CreateFolder();

	return testDataDir;
}

inline std::string GetFilePath(std::string fileName)
{
	FilePath filePath = FilePath::ConcatPaths(GetTestDataDir(), FilePath(fileName));
	return filePath.string();
}

#endif // TEST_HELPERS_H