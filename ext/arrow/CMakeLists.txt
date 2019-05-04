
# define code files
set(libarrow_SRCS
    arrow/array.cc
    arrow/buffer.cc
    arrow/compare.cc
    arrow/memory_pool.cc
	arrow/pretty_print.cc
	arrow/record_batch.cc
    arrow/status.cc
    arrow/table.cc
    arrow/tensor.cc
    arrow/type.cc
    arrow/visitor.cc
	arrow/util/bit-util.cc
	arrow/util/decimal.cc
	arrow/util/key_value_metadata.cc
	arrow/util/logging.cc
)

add_definitions(-DARROW_EXPORTING -DARROW_EXTRA_ERROR_CONTEXT)


# declare LZ4 library
add_library(libarrow STATIC
    ${libarrow_SRCS}
)

# exported include directories
target_include_directories(libarrow PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
