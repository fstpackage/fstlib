
# define code files
set(liblz4_SRCS
    lz4.c
	xxhash.c
)

# declare LZ4 library
add_library(liblz4 STATIC
    ${liblz4_SRCS}
)

# exported include directories
target_include_directories(liblz4 PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
