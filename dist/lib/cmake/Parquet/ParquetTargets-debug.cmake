#----------------------------------------------------------------
# Generated CMake target import file for configuration "DEBUG".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "Parquet::parquet_shared" for configuration "DEBUG"
set_property(TARGET Parquet::parquet_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(Parquet::parquet_shared PROPERTIES
  IMPORTED_LINK_DEPENDENT_LIBRARIES_DEBUG "thrift::thrift"
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libparquet.1800.0.0.dylib"
  IMPORTED_SONAME_DEBUG "@rpath/libparquet.1800.dylib"
  )

list(APPEND _cmake_import_check_targets Parquet::parquet_shared )
list(APPEND _cmake_import_check_files_for_Parquet::parquet_shared "${_IMPORT_PREFIX}/lib/libparquet.1800.0.0.dylib" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
