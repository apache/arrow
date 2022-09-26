#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "arrow_shared" for configuration "RELEASE"
set_property(TARGET arrow_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(arrow_shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libarrow.1000.0.0.dylib"
  IMPORTED_SONAME_RELEASE "@rpath/libarrow.1000.dylib"
  )

list(APPEND _IMPORT_CHECK_TARGETS arrow_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_arrow_shared "${_IMPORT_PREFIX}/lib/libarrow.1000.0.0.dylib" )

# Import target "arrow_static" for configuration "RELEASE"
set_property(TARGET arrow_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(arrow_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C;CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libarrow.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS arrow_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_arrow_static "${_IMPORT_PREFIX}/lib/libarrow.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
