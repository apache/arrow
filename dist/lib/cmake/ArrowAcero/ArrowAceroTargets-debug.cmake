#----------------------------------------------------------------
# Generated CMake target import file for configuration "DEBUG".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "ArrowAcero::arrow_acero_shared" for configuration "DEBUG"
set_property(TARGET ArrowAcero::arrow_acero_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(ArrowAcero::arrow_acero_shared PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarrow_acero.1800.0.0.dylib"
  IMPORTED_SONAME_DEBUG "@rpath/libarrow_acero.1800.dylib"
  )

list(APPEND _cmake_import_check_targets ArrowAcero::arrow_acero_shared )
list(APPEND _cmake_import_check_files_for_ArrowAcero::arrow_acero_shared "${_IMPORT_PREFIX}/lib/libarrow_acero.1800.0.0.dylib" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
