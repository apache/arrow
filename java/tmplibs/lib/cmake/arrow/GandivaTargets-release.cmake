#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "gandiva_shared" for configuration "RELEASE"
set_property(TARGET gandiva_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(gandiva_shared PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgandiva.1000.0.0.dylib"
  IMPORTED_SONAME_RELEASE "@rpath/libgandiva.1000.dylib"
  )

list(APPEND _IMPORT_CHECK_TARGETS gandiva_shared )
list(APPEND _IMPORT_CHECK_FILES_FOR_gandiva_shared "${_IMPORT_PREFIX}/lib/libgandiva.1000.0.0.dylib" )

# Import target "gandiva_static" for configuration "RELEASE"
set_property(TARGET gandiva_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(gandiva_static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libgandiva.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gandiva_static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gandiva_static "${_IMPORT_PREFIX}/lib/libgandiva.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
