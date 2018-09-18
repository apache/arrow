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
#
# TODO(ARROW-3209): rename arrow_gpu to arrow_cuda
#

cmake_minimum_required(VERSION 2.7)
project(pyarrow)

# Running from a Python sdist tarball
set(LOCAL_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/cmake_modules")
if (EXISTS "${LOCAL_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${LOCAL_CMAKE_MODULES})
endif()

# Running from a git source tree
set(CPP_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")
if (EXISTS "${CPP_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CPP_CMAKE_MODULES})
endif()

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
  option(PYARROW_BUILD_CUDA
    "Build the PyArrow CUDA support"
    OFF)
  option(PYARROW_BUILD_PARQUET
    "Build the PyArrow Parquet integration"
    OFF)
  option(PYARROW_PARQUET_USE_SHARED
    "Rely on parquet shared libraries where relevant"
    ON)
  option(PYARROW_BOOST_USE_SHARED
    "Rely on boost shared libraries on linking static parquet"
    ON)
  option(PYARROW_BUILD_PLASMA
    "Build the PyArrow Plasma integration"
    OFF)
  option(PYARROW_USE_TENSORFLOW
    "Build PyArrow with TensorFlow support"
    OFF)
  option(PYARROW_BUILD_ORC
    "Build the PyArrow ORC integration"
    OFF)
  option(PYARROW_BUNDLE_ARROW_CPP
    "Bundle the Arrow C++ libraries"
    OFF)
  option(PYARROW_BUNDLE_BOOST
    "Bundle the Boost libraries when we bundle Arrow C++"
    OFF)
  option(PYARROW_GENERATE_COVERAGE
    "Build with Cython code coverage enabled"
    OFF)
  set(PYARROW_CXXFLAGS "" CACHE STRING
    "Compiler flags to append when compiling Arrow")
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
include(CompilerInfo)
include(SetupCxxFlags)

# Add common flags
set(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PYARROW_CXXFLAGS}")

if (NOT MSVC)
  # Enable perf and other tools to work properly
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")

  # Suppress Cython warnings
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable -Wno-maybe-uninitialized")
else()
  # MSVC version of -Wno-return-type-c-linkage
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4190")

  # Cython generates some bitshift expressions that MSVC does not like in
  # __Pyx_PyFloat_DivideObjC
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4293")

  # Converting to/from C++ bool is pretty wonky in Cython. The C4800 warning
  # seem harmless, and probably not worth the effort of working around it
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4800")
endif()

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Cython warnings in clang
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-parentheses-equality")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-constant-logical-operand")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-declarations")

  # We have public Cython APIs which return C++ types, which are in an extern
  # "C" blog (no symbol mangling) and clang doesn't like this
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-return-type-c-linkage")
endif()

# For any C code, use the same flags.
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")

# Add C++-only flags, like -std=c++11
set(CMAKE_CXX_FLAGS "${CXX_ONLY_FLAGS} ${CMAKE_CXX_FLAGS}")

if (MSVC)
  # MSVC makes its own output directories based on the build configuration
  set(BUILD_SUBDIR_NAME "")
else()
  # Set compile output directory
  string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)
endif()

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}")
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
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}")
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

if (PYARROW_USE_TENSORFLOW)
  # TensorFlow uses the old GLIBCXX ABI, so we have to use it too
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
endif()

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

function(bundle_arrow_lib library_path)
  set(options)
  set(one_value_args ABI_VERSION SO_VERSION)
  set(multi_value_args)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  get_filename_component(LIBRARY_DIR ${${library_path}} DIRECTORY)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME_WE)
  configure_file(${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
      COPYONLY)

  if (APPLE)
    configure_file(${LIBRARY_DIR}/${LIBRARY_NAME}.${ARG_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.${ARG_ABI_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        COPYONLY)
    configure_file(${LIBRARY_DIR}/${LIBRARY_NAME}.${ARG_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.${ARG_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        COPYONLY)
  elseif(NOT MSVC)
    configure_file(${${library_path}}.${ARG_ABI_VERSION}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${ARG_ABI_VERSION}
        COPYONLY)
    configure_file(${${library_path}}.${ARG_SO_VERSION}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${ARG_SO_VERSION}
        COPYONLY)
  endif()

endfunction(bundle_arrow_lib)

function(bundle_arrow_implib library_path)
  get_filename_component(LIBRARY_DIR ${${library_path}} DIRECTORY)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME_WE)
  configure_file(${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}.lib
      COPYONLY)
endfunction(bundle_arrow_implib)

function(bundle_boost_lib library_path)
  get_filename_component(LIBRARY_NAME ${${library_path}} NAME)
  get_filename_component(LIBRARY_NAME_WE ${${library_path}} NAME_WE)
  configure_file(${${library_path}}
      ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME}
      COPYONLY)
  set(Boost_SO_VERSION "${Boost_MAJOR_VERSION}.${Boost_MINOR_VERSION}.${Boost_SUBMINOR_VERSION}")
  if (APPLE)
    configure_file(${${library_path}}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME_WE}.${Boost_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX}
        COPYONLY)
  else()
    configure_file(${${library_path}}
        ${BUILD_OUTPUT_ROOT_DIRECTORY}/${LIBRARY_NAME_WE}${CMAKE_SHARED_LIBRARY_SUFFIX}.${Boost_SO_VERSION}
        COPYONLY)
  endif()
endfunction()

function(bundle_zlib)
  # We can assume that manylinux1 and macosx have system zlib.
  # See https://mail.python.org/mm3/archives/list/distutils-sig@python.org/thread/ZZG6GL3XTBLBJXSITYHEXMFKN43EREB7/
  # for manylinux1.
  if (MSVC)
    # zlib uses zlib.dll for Windows
    set(ZLIB_SHARED_LIB_NAME zlib.dll)
    if (DEFINED ENV{ARROW_BUILD_TOOLCHAIN})
      set(ZLIB_HOME "$ENV{ARROW_BUILD_TOOLCHAIN}")
    endif()
    if (DEFINED ENV{ZLIB_HOME})
      set(ZLIB_HOME "$ENV{ZLIB_HOME}")
    endif()
    if ("${ZLIB_HOME}" STREQUAL "")
      find_library(ZLIB_SHARED_LIB NAMES ${ZLIB_SHARED_LIB_NAME})
    else()
      find_library(ZLIB_SHARED_LIB
	NAMES ${ZLIB_SHARED_LIB_NAME}
	PATHS ${ZLIB_HOME} NO_DEFAULT_PATH
	PATH_SUFFIXES "bin")
    endif()
    if (ZLIB_SHARED_LIB)
      file(COPY
	${ZLIB_SHARED_LIB}
	DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY})
    endif()
  endif()
endfunction()

# Always bundle includes
file(COPY ${ARROW_INCLUDE_DIR}/arrow DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)

if (PYARROW_BUNDLE_ARROW_CPP)
  # arrow
  bundle_arrow_lib(ARROW_SHARED_LIB
    ABI_VERSION ${ARROW_ABI_VERSION}
    SO_VERSION ${ARROW_SO_VERSION})
  bundle_arrow_lib(ARROW_PYTHON_SHARED_LIB
    ABI_VERSION ${ARROW_ABI_VERSION}
    SO_VERSION ${ARROW_SO_VERSION})

  # boost
  if (PYARROW_BOOST_USE_SHARED AND PYARROW_BUNDLE_BOOST)
    set(Boost_USE_STATIC_LIBS OFF)
    set(Boost_USE_MULTITHREADED ON)
    if (MSVC AND ARROW_USE_STATIC_CRT)
      set(Boost_USE_STATIC_RUNTIME ON)
    endif()
    set(Boost_ADDITIONAL_VERSIONS
      "1.66.0" "1.66"
      "1.65.0" "1.65"
      "1.64.0" "1.64"
      "1.63.0" "1.63"
      "1.62.0" "1.61"
      "1.61.0" "1.62"
      "1.60.0" "1.60")
    list(GET Boost_ADDITIONAL_VERSIONS 0 BOOST_LATEST_VERSION)
    string(REPLACE "." "_" BOOST_LATEST_VERSION_IN_PATH ${BOOST_LATEST_VERSION})
    if (MSVC)
      # disable autolinking in boost
      add_definitions(-DBOOST_ALL_NO_LIB)
    endif()
    find_package(Boost COMPONENTS system filesystem regex REQUIRED)
    bundle_boost_lib(Boost_REGEX_LIBRARY)
    bundle_boost_lib(Boost_FILESYSTEM_LIBRARY)
    bundle_boost_lib(Boost_SYSTEM_LIBRARY)
  endif()

  bundle_zlib()

  if (MSVC)
    bundle_arrow_implib(ARROW_SHARED_IMP_LIB)
    bundle_arrow_implib(ARROW_PYTHON_SHARED_IMP_LIB)
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

set(LINK_LIBS
  arrow_shared
  arrow_python_shared
)

if (PYARROW_BUILD_CUDA)
  ## Arrow CUDA
  find_package(ArrowCuda)
  if(NOT ARROW_CUDA_FOUND)
    message(FATAL_ERROR "Unable to locate Arrow CUDA libraries")
  else()
    if (PYARROW_BUNDLE_ARROW_CPP)
      bundle_arrow_lib(ARROW_CUDA_SHARED_LIB
        ABI_VERSION ${ARROW_ABI_VERSION}
        SO_VERSION ${ARROW_SO_VERSION})
      if (MSVC)
        bundle_arrow_implib(ARROW_CUDA_SHARED_IMP_LIB)
      endif()
    endif()
    if (MSVC)
      ADD_THIRDPARTY_LIB(arrow_gpu
        SHARED_LIB ${ARROW_CUDA_SHARED_IMP_LIB})
    else()
      ADD_THIRDPARTY_LIB(arrow_gpu
        SHARED_LIB ${ARROW_CUDA_SHARED_LIB})
    endif()
    set(LINK_LIBS ${LINK_LIBS} arrow_gpu_shared)
    set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _cuda)
  endif()
endif()

if (PYARROW_BUILD_PARQUET)
  ## Parquet
  find_package(Parquet)

  if(NOT PARQUET_FOUND)
    message(FATAL_ERROR "Unable to locate Parquet libraries")
  endif()
  include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})

  if (PYARROW_PARQUET_USE_SHARED)
    if (PYARROW_BUNDLE_ARROW_CPP)
      bundle_arrow_lib(PARQUET_SHARED_LIB
        ABI_VERSION ${PARQUET_ABI_VERSION}
        SO_VERSION ${PARQUET_SO_VERSION})
      if (MSVC)
        bundle_arrow_implib(PARQUET_SHARED_IMP_LIB)
      endif()
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
  else()
    find_package(Thrift)
    if (PYARROW_BOOST_USE_SHARED)
      set(Boost_USE_STATIC_LIBS OFF)
    else()
      set(Boost_USE_STATIC_LIBS ON)
    endif()
    find_package(Boost COMPONENTS regex REQUIRED)
    ADD_THIRDPARTY_LIB(boost_regex
      STATIC_LIB ${Boost_REGEX_LIBRARY_RELEASE})
    ADD_THIRDPARTY_LIB(parquet
      STATIC_LIB ${PARQUET_STATIC_LIB})
    ADD_THIRDPARTY_LIB(thrift
      STATIC_LIB ${THRIFT_STATIC_LIB})
    set(LINK_LIBS
      ${LINK_LIBS}
      parquet_static
      thrift_static
      boost_regex_static)
  endif()
  set(CYTHON_EXTENSIONS
    ${CYTHON_EXTENSIONS}
    _parquet)
endif()

## Plasma
if (PYARROW_BUILD_PLASMA)
  find_package(Plasma)

  if(NOT PLASMA_FOUND)
    message(FATAL_ERROR "Unable to locate Plasma libraries")
  endif()

  include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(libplasma
    SHARED_LIB ${PLASMA_SHARED_LIB})

  file(COPY ${ARROW_INCLUDE_DIR}/plasma DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY}/include)

  if (PYARROW_BUNDLE_ARROW_CPP)
    bundle_arrow_lib(PLASMA_SHARED_LIB
      ABI_VERSION ${ARROW_ABI_VERSION}
      SO_VERSION ${ARROW_SO_VERSION})
  endif()
  set(LINK_LIBS
    ${LINK_LIBS}
    libplasma_shared)

  set(CYTHON_EXTENSIONS ${CYTHON_EXTENSIONS} _plasma)
  file(COPY ${PLASMA_EXECUTABLE} DESTINATION ${BUILD_OUTPUT_ROOT_DIRECTORY})
endif()


if (PYARROW_BUILD_ORC)
  ## ORC
  set(CYTHON_EXTENSIONS
      ${CYTHON_EXTENSIONS}
      _orc)
endif()

############################################################
# Setup and build Cython modules
############################################################

if (PYARROW_GENERATE_COVERAGE)
  set(CYTHON_FLAGS "${CYTHON_FLAGS}" "-Xlinetrace=True")
endif()

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

      set_target_properties(${module_name} PROPERTIES
        INSTALL_RPATH ${module_install_rpath})
    endif()

    if (PYARROW_GENERATE_COVERAGE)
      set_target_properties(${module_name} PROPERTIES
        COMPILE_DEFINITIONS "CYTHON_TRACE=1;CYTHON_TRACE_NOGIL=1")
    endif()

    target_link_libraries(${module_name} ${LINK_LIBS})

    # Generated files will be moved to the right directory by setup.py.
endforeach(module)
