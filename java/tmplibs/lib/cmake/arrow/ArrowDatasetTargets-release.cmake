#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "arrow_dataset_shared" for configuration "RELEASE"
set_property(TARGET arrow_dataset_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(arrow_dataset_shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libarrow_dataset.1000.0.0.dylib"
  IMPORTED_SONAME_RELEASE "@rpath/libarrow_dataset.1000.dylib"
  )

list(APPEND _IMPORT_CHECK_TARGETS arrow_dataset_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_arrow_dataset_shared "${_IMPORT_PREFIX}/lib/libarrow_dataset.1000.0.0.dylib" )

# Import target "arrow_dataset_static" for configuration "RELEASE"
set_property(TARGET arrow_dataset_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(arrow_dataset_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libarrow_dataset.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS arrow_dataset_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_arrow_dataset_static "${_IMPORT_PREFIX}/lib/libarrow_dataset.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
