find_path(LIBRADOS_INCLUDE_DIR
    rados/librados.hpp)

find_library(LIBRADOS_LIBRARY
    NAMES rados)

mark_as_advanced(LIBRADOS_LIBRARY LIBRADOS_INCLUDE_DIR)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(librados DEFAULT_MSG
    LIBRADOS_LIBRARY LIBRADOS_INCLUDE_DIR)

if (LIBRADOS_FOUND)
    set(LIBRADOS_INCLUDE_DIRS ${LIBRADOS_INCLUDE_DIR})
    set(LIBRADOS_LIBRARIES ${LIBRADOS_LIBRARY})
endif()
