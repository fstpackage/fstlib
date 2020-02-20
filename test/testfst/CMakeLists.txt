
# define test files
set(testfst_SRCS
	byte.cpp
	date.cpp
	factors.cpp
	byteblocktest.cpp
	fstcompress.cpp
	fstcoretest.cpp
	fstreadtest.cpp
	fstwritetest.cpp
	hashtest.cpp
	int64.cpp
	logical.cpp
	multicolumntest.cpp
	previousversion.cpp
	scaletest.cpp
	SetThreads.cpp
	special_tables.cpp
)

# create test executable
add_executable(testfst
    ${testfst_SRCS}
)

# include_directories(src ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
target_link_libraries(testfst libfst libfsttable gtest gtest_main)
add_test(testfst testfst)
