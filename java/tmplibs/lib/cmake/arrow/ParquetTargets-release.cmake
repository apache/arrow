#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "parquet_shared" for configuration "RELEASE"
set_property(TARGET parquet_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(parquet_shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libparquet.1000.0.0.dylib"
  IMPORTED_SONAME_RELEASE "@rpath/libparquet.1000.dylib"
  )

list(APPEND _IMPORT_CHECK_TARGETS parquet_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_parquet_shared "${_IMPORT_PREFIX}/lib/libparquet.1000.0.0.dylib" )

# Import target "parquet_static" for configuration "RELEASE"
set_property(TARGET parquet_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(parquet_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libparquet.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS parquet_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_parquet_static "${_IMPORT_PREFIX}/lib/libparquet.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
