# Copyright 2019 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE_1_0.txt or copy at http://boost.org/LICENSE_1_0.txt)

# This CMake configuration file, installed as part of the Boost build
# and installation procedure done by `b2 install`, provides support
# for find_package(Boost).
#
# It's roughly, but not perfectly, compatible with the behavior
# of find_package(Boost) as provided by FindBoost.cmake.
#
# A typical use might be
#
# find_package(Boost 1.70 REQUIRED COMPONENTS filesystem regex PATHS C:/Boost)
#
# On success, the above invocation would define the targets Boost::headers,
# Boost::filesystem and Boost::regex. Boost::headers represents all
# header-only libraries. An alias, Boost::boost, for Boost::headers is
# provided for compatibility.
#
# Requesting the special component "ALL" will make all installed components
# available, as in the following example:
#
# find_package(Boost 1.73 REQUIRED COMPONENTS ALL)
#
# Since COMPONENTS is optional when REQUIRED is specified, the above can be
# shortened to
#
# find_package(Boost 1.73 REQUIRED ALL)
#
# When ALL is used, a variable Boost_ALL_TARGETS will be set and will contain
# the names of all created targets.
#
# The ALL component cannot be combined with named components.
#
# Since Boost libraries can coexist in many variants - 32/64 bit,
# static/dynamic runtime, debug/release, the following variables can be used
# to control which variant is chosen:
#
# Boost_USE_DEBUG_LIBS:     When OFF, disables debug libraries.
# Boost_USE_RELEASE_LIBS:   When OFF, disables release libraries.
# Boost_USE_MULTITHREADED:  When OFF, uses single-threaded libraries.
#                           The default is multithreaded.
# Boost_USE_STATIC_LIBS:    When ON, uses static Boost libraries; when OFF,
#                           uses shared Boost libraries; when not set, uses
#                           static on Windows, shared otherwise.
# Boost_USE_STATIC_RUNTIME: When ON, uses Boost libraries linked against the
#                           static runtime. The default is shared runtime.
# Boost_USE_DEBUG_RUNTIME:  When ON, uses Boost libraries linked against the
#                           debug runtime. When OFF, against the release
#                           runtime. The default is to use either.
# Boost_COMPILER:           The compiler that has been used to build Boost,
#                           such as vc141, gcc7, clang37. The default is
#                           determined from CMAKE_CXX_COMPILER_ID.
# Boost_PYTHON_VERSION:     The version of Python against which Boost.Python
#                           has been built; only required when more than one
#                           Boost.Python library is present.
#
# The following variables control the verbosity of the output:
#
# Boost_VERBOSE:            Enable verbose output
# Boost_DEBUG:              Enable debug (even more verbose) output

if(Boost_VERBOSE OR Boost_DEBUG)

  message(STATUS "Found Boost ${Boost_VERSION} at ${Boost_DIR}")

  # Output requested configuration (f.ex. "REQUIRED COMPONENTS filesystem")

  if(Boost_FIND_QUIETLY)
    set(_BOOST_CONFIG "${_BOOST_CONFIG} QUIET")
  endif()

  if(Boost_FIND_REQUIRED)
    set(_BOOST_CONFIG "${_BOOST_CONFIG} REQUIRED")
  endif()

  foreach(__boost_comp IN LISTS Boost_FIND_COMPONENTS)
    if(${Boost_FIND_REQUIRED_${__boost_comp}})
      list(APPEND _BOOST_COMPONENTS ${__boost_comp})
    else()
      list(APPEND _BOOST_OPTIONAL_COMPONENTS ${__boost_comp})
    endif()
  endforeach()

  if(_BOOST_COMPONENTS)
    set(_BOOST_CONFIG "${_BOOST_CONFIG} COMPONENTS ${_BOOST_COMPONENTS}")
  endif()

  if(_BOOST_OPTIONAL_COMPONENTS)
    set(_BOOST_CONFIG "${_BOOST_CONFIG} OPTIONAL_COMPONENTS ${_BOOST_OPTIONAL_COMPONENTS}")
  endif()

  if(_BOOST_CONFIG)
    message(STATUS "  Requested configuration:${_BOOST_CONFIG}")
  endif()

  unset(_BOOST_CONFIG)
  unset(_BOOST_COMPONENTS)
  unset(_BOOST_OPTIONAL_COMPONENTS)

endif()

macro(boost_find_component comp required quiet)

  set(_BOOST_QUIET)
  if(Boost_FIND_QUIETLY OR ${quiet})
    set(_BOOST_QUIET QUIET)
  endif()

  set(_BOOST_REQUIRED)
  if(${required} AND Boost_FIND_REQUIRED)
    set(_BOOST_REQUIRED REQUIRED)
  endif()

  if("${comp}" MATCHES "^(python|numpy|mpi_python)([1-9])([0-9][0-9]?)$")

    # handle pythonXY and numpyXY versioned components for compatibility

    set(Boost_PYTHON_VERSION "${CMAKE_MATCH_2}.${CMAKE_MATCH_3}")
    set(__boost_comp_nv "${CMAKE_MATCH_1}")

  elseif("${comp}" MATCHES "^(python|numpy|mpi_python)([1-9])$")

    # handle python2/python3 for compatibility

    set(Boost_PYTHON_VERSION_MAJOR "${CMAKE_MATCH_2}")
    set(__boost_comp_nv "${CMAKE_MATCH_1}")

  else()

    set(__boost_comp_nv "${comp}")

  endif()

  get_filename_component(_BOOST_CMAKEDIR "${CMAKE_CURRENT_LIST_DIR}/../" ABSOLUTE)

  if(Boost_DEBUG)
    message(STATUS "BoostConfig: find_package(boost_${__boost_comp_nv} ${Boost_VERSION} EXACT CONFIG ${_BOOST_REQUIRED} ${_BOOST_QUIET} HINTS ${_BOOST_CMAKEDIR})")
  endif()
  find_package(boost_${__boost_comp_nv} ${Boost_VERSION} EXACT CONFIG ${_BOOST_REQUIRED} ${_BOOST_QUIET} HINTS ${_BOOST_CMAKEDIR})

  set(__boost_comp_found ${boost_${__boost_comp_nv}_FOUND})

  # FindPackageHandleStandardArgs expects <package>_<component>_FOUND
  set(Boost_${comp}_FOUND ${__boost_comp_found})

  # FindBoost sets Boost_<COMPONENT>_FOUND
  string(TOUPPER ${comp} _BOOST_COMP)
  set(Boost_${_BOOST_COMP}_FOUND ${__boost_comp_found})

  # FindBoost compatibility variables: Boost_LIBRARIES, Boost_<C>_LIBRARY
  if(__boost_comp_found)

    list(APPEND Boost_LIBRARIES Boost::${__boost_comp_nv})
    set(Boost_${_BOOST_COMP}_LIBRARY Boost::${__boost_comp_nv})

    if(NOT "${comp}" STREQUAL "${__boost_comp_nv}" AND NOT TARGET Boost::${comp})

      # Versioned target alias (f.ex. Boost::python27) for compatibility
      add_library(Boost::${comp} INTERFACE IMPORTED)
      set_property(TARGET Boost::${comp} APPEND PROPERTY INTERFACE_LINK_LIBRARIES Boost::${__boost_comp_nv})

    endif()

  endif()

  unset(_BOOST_REQUIRED)
  unset(_BOOST_QUIET)
  unset(_BOOST_CMAKEDIR)
  unset(__boost_comp_nv)
  unset(__boost_comp_found)
  unset(_BOOST_COMP)

endmacro()

macro(boost_find_all_components)

  # Search for all available component-configuration directories...
  file(GLOB __boost_all_components
       LIST_DIRECTORIES true RELATIVE "${CMAKE_CURRENT_LIST_DIR}/.."
       "${CMAKE_CURRENT_LIST_DIR}/../boost_*-${Boost_VERSION}")
  # ...and extract component names from it.
  string(REGEX REPLACE "boost_([_a-z0-9]+)-${Boost_VERSION}" "\\1"
         __boost_all_components "${__boost_all_components}")

  if(Boost_DEBUG)
    message(STATUS "BoostConfig: discovered components: ${__boost_all_components}")
  endif()

  list(REMOVE_ITEM __boost_all_components "headers")

  # Try to find each component.
  foreach(__boost_comp IN LISTS __boost_all_components)

    boost_find_component(${__boost_comp} 0 1)

    # Append to list of all targets (if found).
    if(Boost_${__boost_comp}_FOUND)
      list(APPEND Boost_ALL_TARGETS Boost::${__boost_comp})
    endif()

  endforeach()

  unset(__boost_all_components)

  if(Boost_DEBUG)
    message(STATUS "BoostConfig: Boost_ALL_TARGETS: ${Boost_ALL_TARGETS}")
  endif()

endmacro()

# Find boost_headers

boost_find_component(headers 1 0)

if(NOT boost_headers_FOUND)

  set(Boost_FOUND 0)
  set(Boost_NOT_FOUND_MESSAGE "A required dependency, boost_headers, has not been found.")

  return()

endif()

# Compatibility variables

set(Boost_MAJOR_VERSION ${Boost_VERSION_MAJOR})
set(Boost_MINOR_VERSION ${Boost_VERSION_MINOR})
set(Boost_SUBMINOR_VERSION ${Boost_VERSION_PATCH})

set(Boost_VERSION_STRING ${Boost_VERSION})
set(Boost_VERSION_MACRO ${Boost_VERSION_MAJOR}0${Boost_VERSION_MINOR}0${Boost_VERSION_PATCH})

get_target_property(Boost_INCLUDE_DIRS Boost::headers INTERFACE_INCLUDE_DIRECTORIES)
set(Boost_LIBRARIES "")

# Save project's policies
cmake_policy(PUSH)
cmake_policy(SET CMP0057 NEW) # if IN_LIST

# Find components

if("ALL" IN_LIST Boost_FIND_COMPONENTS)

  # Make sure "ALL" is the only requested component.
  list(LENGTH Boost_FIND_COMPONENTS __boost_find_components_count)
  if(NOT ${__boost_find_components_count} EQUAL 1)
    message(AUTHOR_WARNING "ALL cannot be combined with named components; the named components will be ignored.")
  endif()

  unset(__boost_find_components_count)

  set(Boost_ALL_TARGETS Boost::headers)

  boost_find_all_components()

else()

  foreach(__boost_comp IN LISTS Boost_FIND_COMPONENTS)

    boost_find_component(${__boost_comp} ${Boost_FIND_REQUIRED_${__boost_comp}} 0)

  endforeach()

endif()

# Compatibility targets

if(NOT TARGET Boost::boost)

  add_library(Boost::boost INTERFACE IMPORTED)
  set_property(TARGET Boost::boost APPEND PROPERTY INTERFACE_LINK_LIBRARIES Boost::headers)

  add_library(Boost::diagnostic_definitions INTERFACE IMPORTED)
  add_library(Boost::disable_autolinking INTERFACE IMPORTED)
  add_library(Boost::dynamic_linking INTERFACE IMPORTED)

  if(WIN32)

    set_property(TARGET Boost::diagnostic_definitions PROPERTY INTERFACE_COMPILE_DEFINITIONS "BOOST_LIB_DIAGNOSTIC")
    set_property(TARGET Boost::disable_autolinking PROPERTY INTERFACE_COMPILE_DEFINITIONS "BOOST_ALL_NO_LIB")
    set_property(TARGET Boost::dynamic_linking PROPERTY INTERFACE_COMPILE_DEFINITIONS "BOOST_ALL_DYN_LINK")

  endif()

endif()

# Compatibility variable when using meta-component "ALL"

if("ALL" IN_LIST Boost_FIND_COMPONENTS)
  set(Boost_ALL_FOUND ${boost_headers_FOUND})
endif()

# Restore project's policies
cmake_policy(POP)
