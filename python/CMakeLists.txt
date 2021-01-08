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

cmake_minimum_required(VERSION 3.2)
project(pyarrow)

# Running from a Python sdist tarball
set(LOCAL_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/cmake_modules")
if(EXISTS "${LOCAL_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${LOCAL_CMAKE_MODULES})
endif()

# Running from a git source tree
set(CPP_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")
if(EXISTS "${CPP_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CPP_CMAKE_MODULES})
endif()

include(CMakeParseArguments)

# Only interpret if() arguments as variables or keywords when unquoted.
# https://www.cmake.org/cmake/help/latest/policy/CMP0054.html
cmake_policy(SET CMP0054 NEW)

# Use the first Python installation on PATH, not the newest one
set(Python3_FIND_STRATEGY "LOCATION")
# On Windows, use registry last, not first
set(Python3_FIND_REGISTRY "LAST")
# On macOS, use framework last, not first
set(Python3_FIND_FRAMEWORK "LAST")

# Allow "make install" to not depend on all targets.
#
# Must be declared in the top-level CMakeLists.txt.
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

set(CMAKE_MACOSX_RPATH 1)
set(CMAKE_OSX_DEPLOYMENT_TARGET 10.9)

# Generate a Clang compile_commands.json "compilation database" file for use
# with various development tools, such as Vim's YouCompleteMe plugin.
# See http://clang.llvm.org/docs/JSONCompilationDatabase.html
if("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1")
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(PYARROW_BUILD_CUDA "Build the PyArrow CUDA support" OFF)
  option(PYARROW_BUILD_FLIGHT "Build the PyArrow Flight integration" OFF)
  option(PYARROW_BUILD_DATASET "Build the PyArrow Dataset integration" OFF)
  option(PYARROW_BUILD_GANDIVA "Build the PyArrow Gandiva integration" OFF)
  option(PYARROW_BUILD_PARQUET "Build the PyArrow Parquet integration" OFF)
  option(PYARROW_PARQUET_USE_SHARED "Rely on parquet shared libraries where relevant" ON)
  option(PYARROW_BOOST_USE_SHARED
         "Rely on boost shared libraries on linking static parquet" ON)
  option(PYARROW_BUILD_PLASMA "Build the PyArrow Plasma integration" OFF)
  option(PYARROW_USE_TENSORFLOW "Build PyArrow with TensorFlow support" OFF)
  option(PYARROW_BUILD_ORC "Build the PyArrow ORC integration" OFF)
  option(PYARROW_BUNDLE_ARROW_CPP "Bundle the Arrow C++ libraries" OFF)
  option(PYARROW_BUNDLE_BOOST "Bundle the Boost libraries when we bundle Arrow C++" OFF)
  option(PYARROW_GENERATE_COVERAGE "Build with Cython code coverage enabled" OFF)
  set(PYARROW_CXXFLAGS "" CACHE STRING "Compiler flags to append when compiling Arrow")
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

#
# Compiler flags
#

include(BuildUtils)

# Cython generated code emits way to many warnings at CHECKIN and EVERYTHING
set(BUILD_WARNING_LEVEL "PRODUCTION")

# This must be synchronized with the definition in
# cpp/cmake_modules/DefineOptions.cmake.
set(ARROW_ARMV8_ARCH
    "armv8-a"
    CACHE STRING "Arm64 arch and extensions: armv8-a, armv8-a or armv8-a+crc+crypto")
include(SetupCxxFlags)

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PYARROW_CXXFLAGS}")

if(MSVC)
  # MSVC version of -Wno-return-type-c-linkage
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4190")

  # Cython generates some bitshift expressions that MSVC does not like in
  # __Pyx_PyFloat_DivideObjC
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4293")

  # Converting to/from C++ bool is pretty wonky in Cython. The C4800 warning
  # seem harmless, and probably not worth the effort of working around it
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4800")

  # See https://github.com/cython/cython/issues/2731. Change introduced in
  # Cython 0.29.1 causes "unsafe use of type 'bool' in operation"
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4804")
else()
  # Enable perf and other tools to work properly
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")

  # Suppress Cython warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable -Wno-maybe-uninitialized")

  if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"
     OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Cython warnings in clang
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-parentheses-equality")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-constant-logical-operand")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-declarations")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sometimes-uninitialized")

    # We have public Cython APIs which return C++ types, which are in an extern
    # "C" blog (no symbol mangling) and clang doesn't like this
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-return-type-c-linkage")
  endif()
endif()

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Add C++-only flags, like -std=c++11
set(CMAKE_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CMAKE_CXX_FLAGS}")

if(MSVC)
  # MSVC makes its own output directories based on the build configuration
  set(BUILD_SUBDIR_NAME "")
else()
  # Set compile output directory
  string(TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)
endif()

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if(${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY
      "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}")
  # Link build/latest to the current build directory, to avoid developers
  # accidentally running the latest debug build when in fact they're building
  # release builds.
  file(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  if(NOT APPLE)
    set(MORE_ARGS "-T")
  endif()
  execute_process(COMMAND ln
                          ${MORE_ARGS}
                          -sf
                          ${BUILD_OUTPUT_ROOT_DIRECTORY}
                          ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}")
endif()

message(STATUS "Generator: ${CMAKE_GENERATOR}")
message(STATUS "Build output directory: ${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

if(PYARROW_USE_TENSORFLOW)
  # TensorFlow uses the old GLIBCXX ABI, so we have to use it too
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
endif()

# Python and Numpy libraries
find_package(Python3Alt REQUIRED)
include(UseCython)

include_directories(SYSTEM ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS} src)

#
# Dependencies
#

if(PYARROW_BUILD_FLIGHT)
  set(ARROW_FLIGHT TRUE)
endif()

# Arrow
find_package(ArrowPython REQUIRED)
include_directories(SYSTEM ${ARROW_INCLUDE_DIR})

function(bundle_arrow_lib library_path)
  set(options)
  set(one_value_args SO_VERSION)
  set(multi_value_args)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  get_filename_component(LIBRARY_DIR ${${library_path}} DIRECTORY)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME_WE)

  # Only copy the shared library with ABI version on Linux and macOS

  if(MSVC)
    configure_file(
      ${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
      COPYONLY)
  elseif(APPLE)
    configure_file(
      ${LIBRARY_DIR}/${LIBRARY_NAME}.${ARG_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.${ARG_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
      COPYONLY)
  else()
    configure_file(
      ${${library_path}}.${ARG_SO_VERSION}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${ARG_SO_VERSION}
      COPYONLY)
  endif()

endfunction(bundle_arrow_lib)

function(bundle_arrow_import_lib library_path)
  get_filename_component(LIBRARY_DIR ${${library_path}} DIRECTORY)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME_WE)
  configure_file(${${library_path}} ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.lib
                 COPYONLY)
endfunction(bundle_arrow_import_lib)

function(bundle_boost_lib library_path)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME)
  get_filename_component(LIBRARY_NAME_WE ${${library_path}} NAME_WE)
  configure_file(${${library_path}} ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}
                 COPYONLY)
  set(Boost_SO_VERSION
      "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
  if(APPLE)
    configure_file(
      ${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME_WE}${CMAKE_SHARED_LIBRARY_SUFFIX}
      COPYONLY)
  else()
    configure_file(
      ${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME_WE}${CMAKE_SHARED_LIBRARY_SUFFIX}.${Boost_SO_VERSION}
      COPYONLY)
  endif()
endfunction()

function(bundle_arrow_dependency library_name)
  if(MSVC)
    if(DEFINED ENV{CONDA_PREFIX})
      file(TO_CMAKE_PATH "$ENV{CONDA_PREFIX}\\Library" SHARED_LIB_HOME)
    endif()
  else()
    if(DEFINED ENV{CONDA_PREFIX})
      file(TO_CMAKE_PATH "$ENV{CONDA_PREFIX}" SHARED_LIB_HOME)
    endif()
  endif()
  if(DEFINED ENV{${library_name}_HOME})
    file(TO_CMAKE_PATH "$ENV{${library_name}_HOME}" SHARED_LIB_HOME)
  endif()
  arrow_build_shared_library_name(shared_lib_name "${library_name}")
  unset(SHARED_LIB_PATH CACHE)
  if(MSVC)
    set(CMAKE_SHARED_LIBRARY_SUFFIXES_ORIGINAL ${CMAKE_FIND_LIBRARY_SUFFIXES})
    # .dll isn't found by find_library with MSVC because .dll isn't included in
    # CMAKE_FIND_LIBRARY_SUFFIXES.
    list(APPEND CMAKE_FIND_LIBRARY_SUFFIXES "${CMAKE_SHARED_LIBRARY_SUFFIX}")
  endif()
  if(SHARED_LIB_HOME)
    find_library(SHARED_LIB_PATH
                 NAMES "${shared_lib_name}"
                 PATHS "${SHARED_LIB_HOME}"
                 PATH_SUFFIXES ${ARROW_SEARCH_LIB_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
  else()
    find_library(SHARED_LIB_PATH
                 NAMES "${shared_lib_name}"
                 PATH_SUFFIXES ${ARROW_SEARCH_LIB_PATH_SUFFIXES})
  endif()
  if(MSVC)
    set(CMAKE_SHARED_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES_ORIGINAL})
  endif()
  if(SHARED_LIB_PATH)
    get_filename_component(SHARED_LIB_REALPATH ${SHARED_LIB_PATH} REALPATH)
    get_filename_component(SHARED_LIB_NAME ${SHARED_LIB_PATH} NAME)
    message(
      STATUS
        "Bundle dependency ${library_name}: ${SHARED_LIB_REALPATH} as ${SHARED_LIB_NAME}")
    configure_file(${SHARED_LIB_REALPATH}
                   ${BUILD_OUTPUT_ROOT_DIRECTORY}/${SHARED_LIB_NAME} COPYONLY)
  else()
    message(FATAL_ERROR "Unable to bundle dependency: ${library_name}")
  endif()
endfunction()

# Always bundle includes
file(COPY ${ARROW_INCLUDE_DIR}/arrow DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)

if(PYARROW_BUNDLE_ARROW_CPP)
  # arrow
  bundle_arrow_lib(ARROW_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})
  bundle_arrow_lib(ARROW_PYTHON_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})

  # boost
  if(PYARROW_BOOST_USE_SHARED AND PYARROW_BUNDLE_BOOST)
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_MULTITHREADED ON)
    if(MSVC AND ARROW_USE_STATIC_CRT)
      set(Boost_USE_STATIC_RUNTIME ON)
    endif()
    set(Boost_ADDITIONAL_VERSIONS
        "1.66.0"
        "1.66"
        "1.65.0"
        "1.65"
        "1.64.0"
        "1.64"
        "1.63.0"
        "1.63"
        "1.62.0"
        "1.61"
        "1.61.0"
        "1.62"
        "1.60.0"
        "1.60")
    list(GET Boost_ADDITIONAL_VERSIONS 0 BOOST_LATEST_VERSION)
    string(REPLACE "." "_" BOOST_LATEST_VERSION_IN_PATH ${BOOST_LATEST_VERSION})
    if(MSVC)
      # disable autolinking in boost
      add_definitions(-DBOOST_ALL_NO_LIB)
    endif()
    find_package(Boost COMPONENTS regex REQUIRED)
    bundle_boost_lib(Boost_REGEX_LIBRARY)
  endif()

  if(MSVC)
    # TODO(kszucs): locate msvcp140.dll in a portable fashion and bundle it
    bundle_arrow_import_lib(ARROW_IMPORT_LIB)
    bundle_arrow_import_lib(ARROW_PYTHON_IMPORT_LIB)
  endif()
endif()

#
# Subdirectories
#

if(UNIX)
  set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
endif()

set(CYTHON_EXTENSIONS
    lib
    _fs
    _compute
    _csv
    _json)

set(LINK_LIBS arrow_shared arrow_python_shared)

if(PYARROW_BUILD_S3)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _s3fs)
endif()

if(PYARROW_BUILD_HDFS)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _hdfs)
endif()

if(PYARROW_BUILD_CUDA)
  # Arrow CUDA
  find_package(ArrowCUDA REQUIRED)

  if(PYARROW_BUNDLE_ARROW_CPP)
    bundle_arrow_lib(ARROW_CUDA_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})
    if(MSVC)
      bundle_arrow_import_lib(ARROW_CUDA_IMPORT_LIB)
    endif()
  endif()
  set(CUDA_LINK_LIBS arrow_cuda_shared)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _cuda)
endif()

# Dataset
if(PYARROW_BUILD_DATASET)
  # Arrow Dataset
  find_package(ArrowDataset REQUIRED)

  if(PYARROW_BUNDLE_ARROW_CPP)
    bundle_arrow_lib(ARROW_DATASET_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})
    if(MSVC)
      bundle_arrow_import_lib(ARROW_DATASET_IMPORT_LIB)
    endif()
  endif()

  set(DATASET_LINK_LIBS arrow_dataset_shared)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _dataset)
endif()

if(PYARROW_BUILD_PARQUET)
  # Parquet
  find_package(Parquet REQUIRED)

  include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})

  if(PYARROW_BUNDLE_ARROW_CPP)
    file(COPY ${PARQUET_INCLUDE_DIR}/parquet
         DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)
  endif()

  if(PYARROW_PARQUET_USE_SHARED)
    if(PYARROW_BUNDLE_ARROW_CPP)
      bundle_arrow_lib(PARQUET_SHARED_LIB SO_VERSION ${PARQUET_SO_VERSION})
      if(MSVC)
        bundle_arrow_import_lib(PARQUET_IMPORT_LIB)
      endif()
    endif()
    set(PARQUET_LINK_LIBS parquet_shared)
  else()
    find_package(Thrift)
    if(PYARROW_BOOST_USE_SHARED)
      set(Boost_USE_STATIC_LIBS OFF)
    else()
      set(Boost_USE_STATIC_LIBS ON)
    endif()
    find_package(Boost COMPONENTS regex REQUIRED)
    add_thirdparty_lib(boost_regex STATIC_LIB ${Boost_REGEX_LIBRARY_RELEASE})
    add_thirdparty_lib(thrift STATIC_LIB ${THRIFT_STATIC_LIB})
    set(PARQUET_LINK_LIBS parquet_static thrift_static boost_regex_static)
  endif()
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _parquet)
endif()

# Plasma
if(PYARROW_BUILD_PLASMA)
  find_package(Plasma REQUIRED)

  include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})

  file(COPY ${ARROW_INCLUDE_DIR}/plasma
       DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)

  if(PYARROW_BUNDLE_ARROW_CPP)
    bundle_arrow_lib(PLASMA_SHARED_LIB SO_VERSION ${PLASMA_SO_VERSION})
  endif()
  set(PLASMA_LINK_LIBS plasma_shared)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _plasma)
  file(COPY ${PLASMA_STORE_SERVER} DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY})
endif()

if(PYARROW_BUILD_ORC)
  # ORC
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _orc)
endif()

# Flight
if(PYARROW_BUILD_FLIGHT)
  # Arrow Flight
  find_package(ArrowPythonFlight REQUIRED)

  if(PYARROW_BUNDLE_ARROW_CPP)
    bundle_arrow_lib(ARROW_FLIGHT_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})
    bundle_arrow_lib(ARROW_PYTHON_FLIGHT_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})
    if(MSVC)
      bundle_arrow_import_lib(ARROW_FLIGHT_IMPORT_LIB)
      bundle_arrow_import_lib(ARROW_PYTHON_FLIGHT_IMPORT_LIB)
      # XXX Hardcoded library names because CMake is too stupid to give us
      # the shared library paths.
      # https://gitlab.kitware.com/cmake/cmake/issues/16210
      # bundle_arrow_dependency(libcrypto-1_1-x64)
      # bundle_arrow_dependency(libssl-1_1-x64)
    endif()
  endif()

  set(FLIGHT_LINK_LIBS arrow_flight_shared arrow_python_flight_shared)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _flight)
endif()

# Gandiva
if(PYARROW_BUILD_GANDIVA)
  find_package(Gandiva REQUIRED)

  include_directories(SYSTEM ${GANDIVA_INCLUDE_DIR})

  if(PYARROW_BUNDLE_ARROW_CPP)
    file(COPY ${GANDIVA_INCLUDE_DIR}/gandiva
         DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)

    bundle_arrow_lib(GANDIVA_SHARED_LIB SO_VERSION ${ARROW_SO_VERSION})

    if(MSVC)
      bundle_arrow_import_lib(GANDIVA_IMPORT_LIB)
    endif()
  endif()

  set(GANDIVA_LINK_LIBS gandiva_shared)
  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} gandiva)
endif()

#
# Setup and build Cython modules
#

if(PYARROW_GENERATE_COVERAGE)
  set(CYTHON_FLAGS "${CYTHON_FLAGS}" "-Xlinetrace=True")
endif()

foreach(module ${CYTHON_EXTENSIONS})
  string(REPLACE "." ";" directories ${module})
  list(GET directories -1 module_name)
  list(REMOVE_AT directories -1)

  string(REPLACE "." "/" module_root "${module}")
  set(module_SRC pyarrow/${module_root}.pyx)
  set_source_files_properties(${module_SRC} PROPERTIES CYTHON_IS_CXX 1)

  cython_add_module(${module_name} ${module_name}_pyx ${module_name}_output ${module_SRC})

  if(directories)
    string(REPLACE ";" "/" module_output_directory ${directories})
    set_target_properties(${module_name}
                          PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${module_output_directory})
  endif()

  if(PYARROW_BUNDLE_ARROW_CPP)
    # In the event that we are bundling the shared libraries (e.g. in a
    # manylinux1 wheel), we need to set the RPATH of the extensions to the
    # root of the pyarrow/ package so that libarrow/libarrow_python are able
    # to be loaded properly
    if(APPLE)
      set(module_install_rpath "@loader_path/")
    else()
      set(module_install_rpath "\$ORIGIN")
    endif()

    # XXX(wesm): ARROW-2326 this logic is only needed when we have Cython
    # modules in interior directories. Since all of our C extensions and
    # bundled libraries are in the same place, we can skip this part

    # list(LENGTH directories i)
    # while(${i} GREATER 0)
    #   set(module_install_rpath "${module_install_rpath}/..")
    #   math(EXPR i "${i} - 1" )
    # endwhile(${i} GREATER 0)

    set_target_properties(${module_name} PROPERTIES INSTALL_RPATH ${module_install_rpath})
  endif()

  if(PYARROW_GENERATE_COVERAGE)
    set_target_properties(${module_name}
                          PROPERTIES COMPILE_DEFINITIONS
                                     "CYTHON_TRACE=1;CYTHON_TRACE_NOGIL=1")
  endif()

  target_link_libraries(${module_name} PRIVATE ${LINK_LIBS})

  # Generated files will be moved to the right directory by setup.py.
endforeach(module)

# Additional link libraries

if(PYARROW_BUILD_CUDA)
  target_link_libraries(_cuda PRIVATE ${CUDA_LINK_LIBS})
endif()

if(PYARROW_BUILD_FLIGHT)
  target_link_libraries(_flight PRIVATE ${FLIGHT_LINK_LIBS})
endif()

if(PYARROW_BUILD_DATASET)
  target_link_libraries(_dataset PRIVATE ${DATASET_LINK_LIBS})
endif()

if(PYARROW_BUILD_GANDIVA)
  target_link_libraries(gandiva PRIVATE ${GANDIVA_LINK_LIBS})
endif()

if(PYARROW_BUILD_PARQUET)
  target_link_libraries(_parquet PRIVATE ${PARQUET_LINK_LIBS})
endif()

if(PYARROW_BUILD_PLASMA)
  target_link_libraries(_plasma PRIVATE ${PLASMA_LINK_LIBS})
endif()
