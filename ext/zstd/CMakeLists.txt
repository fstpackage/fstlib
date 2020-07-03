# define code files
set(libzstd_SRCS
	common/entropy_common.c
	common/error_private.c
	common/fse_decompress.c
	common/zstd_common.c
	common/pool.c
	common/debug.c
	compress/fse_compress.c
	compress/hist.c
	compress/huf_compress.c
	compress/zstd_compress.c
	compress/zstd_compress_sequences.c
	compress/zstd_compress_literals.c
	compress/zstd_fast.c
	compress/zstd_lazy.c
	compress/zstd_ldm.c
	compress/zstd_opt.c
	compress/zstd_double_fast.c
    compress/zstd_compress_superblock.c
    decompress/zstd_ddict.c
	decompress/zstd_decompress.c
	decompress/zstd_decompress_block.c
	decompress/huf_decompress.c
	decompress/zstd_decompress.c
	dictBuilder/cover.c
	dictBuilder/divsufsort.c
	dictBuilder/zdict.c
	dictBuilder/fastcover.c
)

# declare ZSTD library
add_library(libzstd STATIC
    ${libzstd_SRCS}
)

# exported include directories
target_include_directories(libzstd PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/common
    ${CMAKE_CURRENT_SOURCE_DIR}/compress
    ${CMAKE_CURRENT_SOURCE_DIR}/decompress
    ${CMAKE_CURRENT_SOURCE_DIR}/dictBuilder
)
