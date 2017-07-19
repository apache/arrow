# Licensed to the Apache Software Foundation (ASF) under one
# or more cod ntributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Includes code assembled from BSD/MIT/Apache-licensed code from some 3rd-party
# projects, including Kudu, Impala, and libdynd. See python/LICENSE.txt

cmake_minimum_required(VERSION 2.7)
project(pyarrow)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

include(CMakeParseArguments)

# Compatibility with CMake 3.1
if(POLICY CMP0054)
  # http://www.cmake.org/cmake/help/v3.1/policy/CMP0054.html
  cmake_policy(SET CMP0054 NEW)
endif()

# Allow "make install" to not depend on all targets.
#
# Must be declared in the top-level CMakeLists.txt.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

set(CMAKE_MACOSX_RPATH 1)
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.9)

# Generate a Clang compile_commands.json "compilation database" file for use
# with various development tools, such as Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1")
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(PYARROW_BUILD_PARQUET
    "Build the PyArrow Parquet integration"
    OFF)
  option(PYARROW_BUNDLE_ARROW_CPP
    "Bundle the Arrow C++ libraries"
    OFF)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

############################################################
# Compiler flags
############################################################

include(BuildUtils)
include(SetupCxxFlags)
include(CompilerInfo)

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")

if (NOT MSVC)
  # Enable perf and other tools to work properly
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")

  # Suppress Cython warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable")
endif()

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")

  # Cython warnings in clang
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-parentheses-equality -Wno-constant-logical-operand")
endif()

set(PYARROW_LINK "a")

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Code coverage
if ("${PYARROW_GENERATE_COVERAGE}")
  if("${CMAKE_CXX_COMPILER}" MATCHES ".*clang.*")
    # There appears to be some bugs in clang 3.3 which cause code coverage
    # to have link errors, not locating the llvm_gcda_* symbols.
    # This should be fixed in llvm 3.4 with http://llvm.org/viewvc/llvm-project?view=revision&revision=184666
    message(SEND_ERROR "Cannot currently generate coverage with clang")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage -DCOVERAGE_BUILD")

  # For coverage to work properly, we need to use static linkage. Otherwise,
  # __gcov_flush() doesn't properly flush coverage from every module.
  # See http://stackoverflow.com/questions/28164543/using-gcov-flush-within-a-library-doesnt-force-the-other-modules-to-yield-gc
  if("${PYARROW_LINK}" STREQUAL "a")
    message("Using static linking for coverage build")
    set(PYARROW_LINK "s")
  elseif("${PYARROW_LINK}" STREQUAL "d")
    message(SEND_ERROR "Cannot use coverage with static linking")
  endif()
endif()

# If we still don't know what kind of linking to perform, choose based on
# build type (developers like fast builds).
if ("${PYARROW_LINK}" STREQUAL "a")
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
      "${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
    message("Using dynamic linking for ${CMAKE_BUILD_TYPE} builds")
    set(PYARROW_LINK "d")
  else()
    message("Using static linking for ${CMAKE_BUILD_TYPE} builds")
    set(PYARROW_LINK "s")
  endif()
endif()

# Are we using the gold linker? It doesn't work with dynamic linking as
# weak symbols aren't properly overridden, causing tcmalloc to be omitted.
# Let's flag this as an error in RELEASE builds (we shouldn't release a
# product like this).
#
# See https://sourceware.org/bugzilla/show_bug.cgi?id=16979 for details.
#
# The gold linker is only for ELF binaries, which OSX doesn't use. We can
# just skip.
if (NOT APPLE AND NOT MSVC)
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -Wl,--version OUTPUT_VARIABLE LINKER_OUTPUT)
endif ()

if (LINKER_OUTPUT MATCHES "gold")
  if ("${PYARROW_LINK}" STREQUAL "d" AND
      "${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
    message(SEND_ERROR "Cannot use gold with dynamic linking in a RELEASE build "
      "as it would cause tcmalloc symbols to get dropped")
  else()
    message("Using gold linker")
  endif()
  set(PYARROW_USING_GOLD 1)
else()
  message("Using ld linker")
endif()

# Having set PYARROW_LINK due to build type and/or sanitizer, it's now safe to
# act on its value.
if ("${PYARROW_LINK}" STREQUAL "d")
  set(BUILD_SHARED_LIBS ON)
endif()

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
  # Link build/latest to the current build directory, to avoid developers
  # accidentally running the latest debug build when in fact they're building
  # release builds.
  FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  if (NOT APPLE)
    set(MORE_ARGS "-T")
  endif()
EXECUTE_PROCESS(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
  ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else()
  if (MSVC)
    # MSVC makes its own output directories based on the build configuration
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/")
  else()
    set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
  endif()
endif()

message(STATUS "Build output directory: ${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

## Python and libraries
find_package(PythonLibsNew REQUIRED)
find_package(NumPy REQUIRED)
include(UseCython)

include_directories(SYSTEM
  ${NUMPY_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS}
  src)

############################################################
# Dependencies
############################################################

## Arrow
find_package(Arrow REQUIRED)
include_directories(SYSTEM ${ARROW_INCLUDE_DIR})

## Plasma
find_package(Plasma)
if (PLASMA_FOUND)
  include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})
endif()

function(bundle_arrow_lib library_path)
  get_filename_component(LIBRARY_DIR ${${library_path}} DIRECTORY)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME_WE)
  configure_file(${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
      COPYONLY)

  if (APPLE)
    configure_file(${LIBRARY_DIR}/${LIBRARY_NAME}.${ARROW_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.${ARROW_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        COPYONLY)
    configure_file(${LIBRARY_DIR}/${LIBRARY_NAME}.${ARROW_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.${ARROW_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        COPYONLY)
  else(NOT MSVC)
    configure_file(${${library_path}}.${ARROW_ABI_VERSION}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${ARROW_ABI_VERSION}
        COPYONLY)
    configure_file(${${library_path}}.${ARROW_SO_VERSION}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${ARROW_SO_VERSION}
        COPYONLY)
  endif()

  SET(ARROW_SHARED_LIB
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
endfunction(bundle_arrow_lib)

if (PYARROW_BUNDLE_ARROW_CPP)
  # arrow
  file(COPY ${ARROW_INCLUDE_DIR}/arrow DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)
  bundle_arrow_lib(ARROW_SHARED_LIB)
  bundle_arrow_lib(ARROW_PYTHON_SHARED_LIB)
  if (PLASMA_FOUND)
    bundle_arrow_lib(PLASMA_SHARED_LIB)
  endif()
endif()

if (MSVC)
  ADD_THIRDPARTY_LIB(arrow
    SHARED_LIB ${ARROW_SHARED_IMP_LIB})
  ADD_THIRDPARTY_LIB(arrow_python
    SHARED_LIB ${ARROW_PYTHON_SHARED_IMP_LIB})
else()
  ADD_THIRDPARTY_LIB(arrow
    SHARED_LIB ${ARROW_SHARED_LIB})
  ADD_THIRDPARTY_LIB(arrow_python
    SHARED_LIB ${ARROW_PYTHON_SHARED_LIB})
endif()

############################################################
# Subdirectories
############################################################

if (UNIX)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
endif()

set(CYTHON_EXTENSIONS
  lib
)

if (PLASMA_FOUND)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} plasma)
endif()

set(LINK_LIBS
  arrow_shared
  arrow_python_shared
  ${PLASMA_SHARED_LIB}
)

if (PYARROW_BUILD_PARQUET)
  ## Parquet
  find_package(Parquet)

  if(NOT PARQUET_FOUND)
    message(FATAL_ERROR "Unable to locate Parquet libraries")
  endif()
  include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})

  if (PYARROW_BUNDLE_ARROW_CPP)
      get_filename_component(PARQUET_LIBRARY_DIR ${PARQUET_SHARED_LIB} DIRECTORY)
      get_filename_component(PARQUET_LIBRARY_NAME ${PARQUET_SHARED_LIB} NAME_WE)
      message(STATUS "Configure start")
      configure_file(${PARQUET_SHARED_LIB}
          ${BUILD_OUTPUT_ROOT_DIRECTORY}/${PARQUET_LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
          COPYONLY)
      if (APPLE)
          configure_file(${PARQUET_LIBRARY_DIR}/libparquet.${PARQUET_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
              ${BUILD_OUTPUT_ROOT_DIRECTORY}/libparquet.${PARQUET_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
              COPYONLY)
          configure_file(${PARQUET_LIBRARY_DIR}/libparquet.${PARQUET_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
              ${BUILD_OUTPUT_ROOT_DIRECTORY}/libparquet.${PARQUET_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
              COPYONLY)
      elseif (NOT MSVC)
          configure_file(${PARQUET_LIBRARY_DIR}/libparquet${CMAKE_SHARED_LIBRARY_SUFFIX}.${PARQUET_ABI_VERSION}
              ${BUILD_OUTPUT_ROOT_DIRECTORY}/libparquet${CMAKE_SHARED_LIBRARY_SUFFIX}.${PARQUET_ABI_VERSION}
              COPYONLY)
          configure_file(${PARQUET_LIBRARY_DIR}/libparquet${CMAKE_SHARED_LIBRARY_SUFFIX}.${PARQUET_SO_VERSION}
              ${BUILD_OUTPUT_ROOT_DIRECTORY}/libparquet${CMAKE_SHARED_LIBRARY_SUFFIX}.${PARQUET_SO_VERSION}
              COPYONLY)
      endif()
      message(STATUS "Configure end")
      #SET(PARQUET_SHARED_LIB
      #    ${BUILD_OUTPUT_ROOT_DIRECTORY}/libparquet${CMAKE_SHARED_LIBRARY_SUFFIX})
  endif()
  if (MSVC)
    ADD_THIRDPARTY_LIB(parquet
      SHARED_LIB ${PARQUET_SHARED_IMP_LIB})
  else()
    ADD_THIRDPARTY_LIB(parquet
      SHARED_LIB ${PARQUET_SHARED_LIB})
  endif()
  set(LINK_LIBS
    ${LINK_LIBS}
    parquet_shared)
  set(CYTHON_EXTENSIONS
    ${CYTHON_EXTENSIONS}
    _parquet)
endif()

############################################################
# Setup and build Cython modules
############################################################

foreach(module ${CYTHON_EXTENSIONS})
    string(REPLACE "." ";" directories ${module})
    list(GET directories -1 module_name)
    list(REMOVE_AT directories -1)

    string(REPLACE "." "/" module_root "${module}")
    set(module_SRC pyarrow/${module_root}.pyx)
    set_source_files_properties(${module_SRC} PROPERTIES CYTHON_IS_CXX 1)

    cython_add_module(${module_name}
      ${module_name}_pyx
      ${module_name}_output
      ${module_SRC})

    if (directories)
        string(REPLACE ";" "/" module_output_directory ${directories})
        set_target_properties(${module_name} PROPERTIES
          LIBRARY_OUTPUT_DIRECTORY ${module_output_directory})
    endif()

    if (PYARROW_BUNDLE_ARROW_CPP)
      # In the event that we are bundling the shared libraries (e.g. in a
      # manylinux1 wheel), we need to set the RPATH of the extensions to the
      # root of the pyarrow/ package so that libarrow/libarrow_python are able
      # to be loaded properly
      if(APPLE)
        set(module_install_rpath "@loader_path")
      else()
        set(module_install_rpath "\$ORIGIN")
      endif()
      list(LENGTH directories i)
      while(${i} GREATER 0)
        set(module_install_rpath "${module_install_rpath}/..")
        math(EXPR i "${i} - 1" )
      endwhile(${i} GREATER 0)

      set_target_properties(${module_name} PROPERTIES
        INSTALL_RPATH ${module_install_rpath})
    endif()

    target_link_libraries(${module_name} ${LINK_LIBS})
endforeach(module)

if (PLASMA_FOUND)
  file(COPY ${PLASMA_EXECUTABLE} DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY})
endif()
