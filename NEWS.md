
# fstlib 0.1.5 (in development)

## Library updates

* Serialization of zero-row tables for all column types (#10)

## Enhancements


# fstlib 0.1.4

This release of fstlib brings updates of the compression libraries and allows for 64-bit sizes of
columns and number of factor levels.

## Library updates

* LZ4 updated to version 0.9.2
* ZSTD updated to version 1.4.4

## Enhancements

* Error on missing column outputs columns name
* Allow 64-bit size for number of factor levels
* Column factory uses 64 bit length values
