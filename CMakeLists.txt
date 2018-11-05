
cmake_minimum_required(VERSION 2.8)


# use at least C++ 11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# generate position independent code
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# define fstlib project
project(fstlib)


# add googletest library: https://github.com/google/googletest
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
add_subdirectory(ext/gtest)
enable_testing(true)


# add external compression libraries
add_subdirectory(ext/lz4)
add_subdirectory(ext/zstd)


# add the fst library components
add_subdirectory(lib/fst)
add_subdirectory(lib/fsttable)


# add unit tests
add_subdirectory(test/testfst)

# copy test data
configure_file(test/testdata/data1.fst ${PROJECT_BINARY_DIR}/test/testdata/data1.fst COPYONLY)
configure_file(test/testdata/previousversion.fst ${PROJECT_BINARY_DIR}/test/testdata/previousversion.fst COPYONLY)
