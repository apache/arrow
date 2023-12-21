## gflags CMake configuration file

# library version information
set (GFLAGS_VERSION_STRING "2.2.2")
set (GFLAGS_VERSION_MAJOR  2)
set (GFLAGS_VERSION_MINOR  2)
set (GFLAGS_VERSION_PATCH  0)

# import targets
if (NOT DEFINED GFLAGS_USE_TARGET_NAMESPACE)
  set (GFLAGS_USE_TARGET_NAMESPACE FALSE)
endif ()
if (GFLAGS_USE_TARGET_NAMESPACE)
  include ("${CMAKE_CURRENT_LIST_DIR}/gflags-targets.cmake")
  set (GFLAGS_TARGET_NAMESPACE gflags)
else ()
  include ("${CMAKE_CURRENT_LIST_DIR}/gflags-nonamespace-targets.cmake")
  set (GFLAGS_TARGET_NAMESPACE)
endif ()
if (GFLAGS_TARGET_NAMESPACE)
  set (GFLAGS_TARGET_PREFIX ${GFLAGS_TARGET_NAMESPACE}::)
else ()
  set (GFLAGS_TARGET_PREFIX)
endif ()

# installation prefix
get_filename_component (CMAKE_CURRENT_LIST_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
get_filename_component (_INSTALL_PREFIX "${CMAKE_CURRENT_LIST_DIR}/." ABSOLUTE)

# include directory
#
# Newer versions of CMake set the INTERFACE_INCLUDE_DIRECTORIES property
# of the imported targets. It is hence not necessary to add this path
# manually to the include search path for targets which link to gflags.
set (GFLAGS_INCLUDE_DIR "${_INSTALL_PREFIX}/include")

if (gflags_FIND_COMPONENTS)
  foreach (gflags_FIND_COMPONENT IN LISTS gflags_FIND_COMPONENTS)
    if (gflags_FIND_REQUIRED_${gflags_FIND_COMPONENT} AND NOT TARGET gflags_${gflags_FIND_COMPONENT})
      message (FATAL_ERROR "Package gflags was installed without required component ${gflags_FIND_COMPONENT}!")
    endif ()
  endforeach ()
  list (GET gflags_FIND_COMPONENTS 0 gflags_FIND_COMPONENT)
else ()
  set (gflags_FIND_COMPONENT)
endif ()

# default settings of GFLAGS_SHARED and GFLAGS_NOTHREADS
#
# It is recommended to use either one of the following find_package commands
# instead of setting the GFLAGS_(SHARED|NOTHREADS) variables:
# - find_package(gflags REQUIRED)
# - find_package(gflags COMPONENTS nothreads_static)
# - find_package(gflags COMPONENTS nothreads_shared)
# - find_package(gflags COMPONENTS static)
# - find_package(gflags COMPONENTS shared)
if (NOT DEFINED GFLAGS_SHARED)
  if (DEFINED gflags_SHARED)
    set (GFLAGS_SHARED ${gflags_SHARED})
  elseif (gflags_FIND_COMPONENT)
    if (gflags_FIND_COMPONENT MATCHES "shared")
      set (GFLAGS_SHARED TRUE)
    else ()
      set (GFLAGS_SHARED FALSE)
    endif ()
  elseif (TARGET ${GFLAGS_TARGET_PREFIX}gflags_shared OR TARGET ${GFLAGS_TARGET_PREFIX}gflags_nothreads_shared)
    set (GFLAGS_SHARED TRUE)
  else ()
    set (GFLAGS_SHARED FALSE)
  endif ()
endif ()
if (NOT DEFINED GFLAGS_NOTHREADS)
  if (DEFINED gflags_NOTHREADS)
    set (GFLAGS_NOTHREADS ${gflags_NOTHREADS})
  elseif (gflags_FIND_COMPONENT)
    if (gflags_FIND_COMPONENT MATCHES "nothreads")
      set (GFLAGS_NOTHREADS TRUE)
    else ()
      set (GFLAGS_NOTHREADS FALSE)
    endif ()
  elseif (TARGET ${GFLAGS_TARGET_PREFIX}PACKAGE_NAME@_static OR TARGET ${GFLAGS_TARGET_PREFIX}gflags_shared)
    set (GFLAGS_NOTHREADS FALSE)
  else ()
    set (GFLAGS_NOTHREADS TRUE)
  endif ()
endif ()

# choose imported library target
if (NOT GFLAGS_TARGET)
  if (gflags_TARGET)
    set (GFLAGS_TARGET ${gflags_TARGET})
  elseif (GFLAGS_SHARED)
    if (GFLAGS_NOTHREADS)
      set (GFLAGS_TARGET ${GFLAGS_TARGET_PREFIX}gflags_nothreads_shared)
    else ()
      set (GFLAGS_TARGET ${GFLAGS_TARGET_PREFIX}gflags_shared)
    endif ()
  else ()
    if (GFLAGS_NOTHREADS)
      set (GFLAGS_TARGET ${GFLAGS_TARGET_PREFIX}gflags_nothreads_static)
    else ()
      set (GFLAGS_TARGET ${GFLAGS_TARGET_PREFIX}gflags_static)
    endif ()
  endif ()
endif ()
if (NOT TARGET ${GFLAGS_TARGET})
  message (FATAL_ERROR "Your gflags installation does not contain a ${GFLAGS_TARGET} library target!"
                       " Try a different combination of GFLAGS_SHARED and GFLAGS_NOTHREADS.")
endif ()

# add more convenient "${GFLAGS_TARGET_PREFIX}gflags" import target
if (NOT TARGET ${GFLAGS_TARGET_PREFIX}gflags)
  if (GFLAGS_SHARED)
    add_library (${GFLAGS_TARGET_PREFIX}gflags SHARED IMPORTED)
  else ()
    add_library (${GFLAGS_TARGET_PREFIX}gflags STATIC IMPORTED)
  endif ()
  # copy INTERFACE_* properties
  foreach (_GFLAGS_PROPERTY_NAME IN ITEMS
    COMPILE_DEFINITIONS
    COMPILE_FEATURES
    COMPILE_OPTIONS
    INCLUDE_DIRECTORIES
    LINK_LIBRARIES
    POSITION_INDEPENDENT_CODE
  )
    get_target_property (_GFLAGS_PROPERTY_VALUE ${GFLAGS_TARGET} INTERFACE_${_GFLAGS_PROPERTY_NAME})
    if (_GFLAGS_PROPERTY_VALUE)
      set_target_properties(${GFLAGS_TARGET_PREFIX}gflags PROPERTIES
        INTERFACE_${_GFLAGS_PROPERTY_NAME} "${_GFLAGS_PROPERTY_VALUE}"
      )
    endif ()
  endforeach ()
  # copy IMPORTED_*_<CONFIG> properties
  get_target_property (_GFLAGS_CONFIGURATIONS ${GFLAGS_TARGET} IMPORTED_CONFIGURATIONS)
  set_target_properties (${GFLAGS_TARGET_PREFIX}gflags PROPERTIES IMPORTED_CONFIGURATIONS "${_GFLAGS_CONFIGURATIONS}")
  foreach (_GFLAGS_PROPERTY_NAME IN ITEMS
    IMPLIB
    LOCATION
    LINK_DEPENDENT_LIBRARIES
    LINK_INTERFACE_LIBRARIES
    LINK_INTERFACE_LANGUAGES
    LINK_INTERFACE_MULTIPLICITY
    NO_SONAME
    SONAME
  )
    foreach (_GFLAGS_CONFIG IN LISTS _GFLAGS_CONFIGURATIONS)
      get_target_property (_GFLAGS_PROPERTY_VALUE ${GFLAGS_TARGET} IMPORTED_${_GFLAGS_PROPERTY_NAME}_${_GFLAGS_CONFIG})
      if (_GFLAGS_PROPERTY_VALUE)
        set_target_properties(${GFLAGS_TARGET_PREFIX}gflags PROPERTIES
          IMPORTED_${_GFLAGS_PROPERTY_NAME}_${_GFLAGS_CONFIG} "${_GFLAGS_PROPERTY_VALUE}"
        )
      endif ()
    endforeach ()
  endforeach ()
  unset (_GFLAGS_CONFIGURATIONS)
  unset (_GFLAGS_CONFIG)
  unset (_GFLAGS_PROPERTY_NAME)
  unset (_GFLAGS_PROPERTY_VALUE)
endif ()

# alias for default import target to be compatible with older CMake package configurations
set (GFLAGS_LIBRARIES "${GFLAGS_TARGET}")

# set gflags_* variables for backwards compatibility
if (NOT "^gflags$" STREQUAL "^GFLAGS$")
  foreach (_GFLAGS_VARIABLE IN ITEMS
    VERSION_STRING
    VERSION_MAJOR
    VERSION_MINOR
    VERSION_PATCH
    INCLUDE_DIR
    LIBRARIES
    TARGET
  )
    set (gflags_${_GFLAGS_VARIABLE} "${GFLAGS_${_GFLAGS_VARIABLE}}")
  endforeach ()
  unset (_GFLAGS_VARIABLE)
endif ()

# unset private variables
unset (gflags_FIND_COMPONENT)
unset (_INSTALL_PREFIX)
