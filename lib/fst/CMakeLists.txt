
# define code files
set(libfst_SRCS
	compression/compression.cpp
	compression/compressor.cpp
	interface/openmphelper.cpp
	interface/fststore.cpp
	logical/logical_v10.cpp
	integer/integer_v8.cpp
	byte/byte_v12.cpp
	byteblock/byteblock_v13.cpp
	double/double_v9.cpp
	character/character_v6.cpp
	factor/factor_v7.cpp
	blockstreamer/blockstreamer_v2.cpp
	integer64/integer64_v11.cpp
)

# declare fst library
add_library(libfst STATIC
    ${libfst_SRCS}
)

# add zstd and lz4 compression libraries
target_link_libraries(libfst
    liblz4
	libzstd
)

# exported include directories
target_include_directories(libfst PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
