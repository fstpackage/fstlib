
# fstlib 0.1.6 (in development)

## License update

* The license of the fstlib library was changed from the GNU AFFERO GENERAL PUBLIC LICENSE version 3 to MPL version 2.
This allows for less restrictive use of the library, for example in comercial applications where fstlib is used as a unmodified component.


# fstlib 0.1.5

## Library updates

* Serialization of zero-row tables for all column types (#10)

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
