# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
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
# arrow_python
#

cmake_minimum_required(VERSION 3.5)

# RPATH settings on macOS do not affect install_name.
# https://cmake.org/cmake/help/latest/policy/CMP0068.html
if(POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW)
endif()

#
# Define
# ARROW_SOURCE_DIR: location of arrow/cpp
# CMAKE_MODULE_PATH: location of cmake_modules in python
#

get_filename_component(PYARROW_SOURCE_DIR ${CMAKE_SOURCE_DIR} DIRECTORY)
get_filename_component(PYTHON_SOURCE_DIR ${PYARROW_SOURCE_DIR} DIRECTORY)
get_filename_component(ARROW_SOURCE ${PYTHON_SOURCE_DIR} DIRECTORY)
set(ARROW_CPP_SOURCE_DIR "${ARROW_SOURCE}/cpp")
set(ARROW_SOURCE_DIR "${ARROW_CPP_SOURCE_DIR}")

# normalize ARROW_HOME path
file(TO_CMAKE_PATH "$ENV{ARROW_HOME}" ARROW_HOME)
set(CMAKE_MODULE_PATH "${PYTHON_SOURCE_DIR}/cmake_modules" "${ARROW_HOME}/lib/cmake/arrow")

#
# Arrow version
#

set(ARROW_PYTHON_VERSION "9.0.0-SNAPSHOT")
string(REGEX MATCH "^[0-9]+\\.[0-9]+\\.[0-9]+" ARROW_PYTHON_BASE_VERSION "${ARROW_PYTHON_VERSION}")
# Need to set to ARRROW_VERSION before finding Arrow package!
project(arrow_python VERSION "${ARROW_PYTHON_BASE_VERSION}")

if(NOT DEFINED CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

#
# Arrow
#

find_package(Arrow REQUIRED)
include(ArrowOptions)

#
# Python
#
# Use the first Python installation on PATH, not the newest one
set(Python3_FIND_STRATEGY "LOCATION")
# On Windows, use registry last, not first
set(Python3_FIND_REGISTRY "LAST")
# On macOS, use framework last, not first
set(Python3_FIND_FRAMEWORK "LAST")

find_package(Python3Alt 3.7 REQUIRED)
include_directories(SYSTEM ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS} src)

add_custom_target(arrow_python-all)
add_custom_target(arrow_python)
add_custom_target(arrow_python-tests)
add_dependencies(arrow_python-all arrow_python arrow_python-tests)

set(ARROW_PYTHON_SRCS
    arrow_to_pandas.cc
    benchmark.cc
    common.cc
    datetime.cc
    decimal.cc
    deserialize.cc
    extension_type.cc
    gdb.cc
    helpers.cc
    inference.cc
    init.cc
    io.cc
    ipc.cc
    numpy_convert.cc
    numpy_to_arrow.cc
    python_to_arrow.cc
    pyarrow.cc
    serialize.cc
    udf.cc)

set_source_files_properties(init.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                               SKIP_UNITY_BUILD_INCLUSION ON)

#
# Arrow vs C PyArrow options
#

# Check all the options from Arrow and C PyArrow to be in line
if(PYARROW_WITH_DATASET)
  find_package(ArrowDataset REQUIRED)
endif()

if(PYARROW_WITH_PARQUET_ENCRYPTION)
  if(PARQUET_REQUIRE_ENCRYPTION)
    list(APPEND ARROW_PYTHON_SRCS parquet_encryption.cc)
    find_package(Parquet REQUIRED)
  else()
    message(FATAL_ERROR "You must build Arrow C++ with PARQUET_REQUIRE_ENCRYPTION=ON")
  endif()
endif()

if(PYARROW_WITH_HDFS)
  if(NOT ARROW_HDFS)
    message(FATAL_ERROR "You must build Arrow C++ with ARROW_HDFS=ON")
  endif()
endif()

# Check for only Arrow C++ options
if(ARROW_CSV)
  list(APPEND ARROW_PYTHON_SRCS csv.cc)
endif()

if(ARROW_FILESYSTEM)
  list(APPEND ARROW_PYTHON_SRCS filesystem.cc)
endif()

# Link to arrow dependecies
if(ARROW_BUILD_SHARED)
  set(ARROW_PYTHON_DEPENDENCIES arrow_shared)
else()
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
  set(ARROW_PYTHON_DEPENDENCIES arrow_static Threads::Threads)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_property(SOURCE pyarrow.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-cast-qual ")
endif()

#
# Compiler stuff
#

include(GNUInstallDirs)

# This ensures that things like gnu++11 get passed correctly
if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 11)
endif()

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Needed gdb flags

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
if(ARROW_GGDB_DEBUG)
  set(ARROW_DEBUG_SYMBOL_TYPE "gdb")
  set(C_FLAGS_DEBUG "-g${ARROW_DEBUG_SYMBOL_TYPE} -O0")
  set(C_FLAGS_FASTDEBUG "-g${ARROW_DEBUG_SYMBOL_TYPE} -O1")
  set(CXX_FLAGS_DEBUG "-g${ARROW_DEBUG_SYMBOL_TYPE} -O0")
  set(CXX_FLAGS_FASTDEBUG "-g${ARROW_DEBUG_SYMBOL_TYPE} -O1")
else()
  set(C_FLAGS_DEBUG "-g -O0")
  set(C_FLAGS_FASTDEBUG "-g -O1")
  set(CXX_FLAGS_DEBUG "-g -O0")
  set(CXX_FLAGS_FASTDEBUG "-g -O1")
endif()

set(C_FLAGS_RELEASE "-O3 -DNDEBUG")
set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

set(C_FLAGS_PROFILE_GEN "${CXX_FLAGS_RELEASE} -fprofile-generate")
set(C_FLAGS_PROFILE_BUILD "${CXX_FLAGS_RELEASE} -fprofile-use")
set(CXX_FLAGS_PROFILE_GEN "${CXX_FLAGS_RELEASE} -fprofile-generate")
set(CXX_FLAGS_PROFILE_BUILD "${CXX_FLAGS_RELEASE} -fprofile-use")

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)
# Set compile flags based on the build type.
message(STATUS "Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,...})"
)

if("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_DEBUG}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_DEBUG}")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "RELWITHDEBINFO")

elseif("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_FASTDEBUG}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_FASTDEBUG}")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_RELEASE}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELEASE}")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_GEN")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_PROFILE_GEN}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_GEN}")
elseif("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_BUILD")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_PROFILE_BUILD}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_BUILD}")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")

#
# Linker flags
#

# Localize thirdparty symbols using a linker version script. This hides them
# from the client application. The OS X linker does not support the
# version-script option.
if(CMAKE_VERSION VERSION_LESS 3.18)
  if(APPLE OR WIN32)
    set(CXX_LINKER_SUPPORTS_VERSION_SCRIPT FALSE)
  else()
    set(CXX_LINKER_SUPPORTS_VERSION_SCRIPT TRUE)
  endif()
else()
  include(CheckLinkerFlag)
  check_linker_flag(CXX
                    "-Wl,--version-script=${ARROW_CPP_SOURCE_DIR}/src/arrow/symbols.map"
                    CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
endif()

# Need to set ARROW_VERSION_SCRIPT_FLAGS for add_arrow_lib
if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
  set(ARROW_VERSION_SCRIPT_FLAGS
      "-Wl,--version-script=${ARROW_CPP_SOURCE_DIR}/src/arrow/symbols.map")
endif()

#
# shred/static link libs
#

set(ARROW_PYTHON_SHARED_LINK_LIBS arrow_shared)
set(ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS)
set(ARROW_PYTHON_STATIC_LINK_LIBS ${PYTHON_OTHER_LIBS})

if(WIN32)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS ${PYTHON_LIBRARIES} ${PYTHON_OTHER_LIBS})
endif()
if(PARQUET_REQUIRE_ENCRYPTION AND PYARROW_WITH_PARQUET_ENCRYPTION)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS parquet_shared)
endif()

if((NOT ARROW_SIMD_LEVEL STREQUAL "NONE") OR (NOT ARROW_RUNTIME_SIMD_LEVEL STREQUAL "NONE"
                                             ))
  set(ARROW_USE_XSIMD TRUE)
else()
  set(ARROW_USE_XSIMD FALSE)
endif()

if(ARROW_USE_XSIMD)
  set(XSIMD_PREFIX "${ARROW_CPP_SOURCE_DIR}/${ARROW_BUILD_DIR}/xsimd_ep/src/xsimd_ep-install")
  if(EXISTS ${XSIMD_PREFIX}/include)
    set(XSIMD_INCLUDE_DIR "${XSIMD_PREFIX}/include")
  else()
    set(XSIMD_PREFIX "$ENV{CONDA_PREFIX}")
    set(XSIMD_INCLUDE_DIR "${XSIMD_PREFIX}/include")
  endif()

  add_library(xsimd INTERFACE IMPORTED)
  if(CMAKE_VERSION VERSION_LESS 3.11)
    set_target_properties(xsimd PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                          "${XSIMD_INCLUDE_DIR}")
  else()
    target_include_directories(xsimd INTERFACE "${XSIMD_INCLUDE_DIR}")
  endif()

  list(APPEND ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS xsimd)
  list(APPEND ARROW_PYTHON_STATIC_LINK_LIBS xsimd)
endif()

set(ARROW_PYTHON_INCLUDES ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

# Inlude macros needed to find and use add_arrow_lib function
include(BuildUtils)
include(CMakePackageConfigHelpers)

# Set the output directory for cmake module
# (CMAKE_INSTALL_PREFIX = python/build/dist! should be set in setup.py!)
set(ARROW_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")

add_arrow_lib(arrow_python
              CMAKE_PACKAGE_NAME
              ArrowPython
              PKG_CONFIG_NAME
              arrow-python
              SOURCES
              ${ARROW_PYTHON_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:pch.h>"
              OUTPUTS
              ARROW_PYTHON_LIBRARIES
              DEPENDENCIES
              ${ARROW_PYTHON_DEPENDENCIES}
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS}
              SHARED_LINK_LIBS
              ${ARROW_PYTHON_SHARED_LINK_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_PYTHON_SHARED_PRIVATE_LINK_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_PYTHON_STATIC_LINK_LIBS}
              EXTRA_INCLUDES
              "${ARROW_PYTHON_INCLUDES}")

add_dependencies(arrow_python ${ARROW_PYTHON_LIBRARIES})

foreach(LIB_TARGET ${ARROW_PYTHON_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_PYTHON_EXPORTING)
endforeach()

if(ARROW_BUILD_STATIC AND MSVC)
  target_compile_definitions(arrow_python_static PUBLIC ARROW_STATIC)
endif()

if(ARROW_FLIGHT AND ARROW_BUILD_SHARED)

  find_package(ArrowFlight REQUIRED)
  include_directories("${ARROW_CPP_SOURCE_DIR}/src" "${ARROW_CPP_SOURCE_DIR}/}/${ARROW_BUILD_DIR}/src")

  set(FLIGHT_LINK_LIBS arrow_flight_shared)

  add_arrow_lib(arrow_python_flight
                CMAKE_PACKAGE_NAME
                ArrowPythonFlight
                PKG_CONFIG_NAME
                arrow-python-flight
                SOURCES
                flight.cc
                OUTPUTS
                ARROW_PYFLIGHT_LIBRARIES
                SHARED_LINK_FLAGS
                ${ARROW_VERSION_SCRIPT_FLAGS}
                SHARED_LINK_LIBS
                arrow_python_shared
                arrow_flight_shared
                STATIC_LINK_LIBS
                ${PYTHON_OTHER_LIBS}
                EXTRA_INCLUDES
                "${ARROW_PYTHON_INCLUDES}"
                PRIVATE_INCLUDES
                "${Protobuf_INCLUDE_DIRS}")

  add_dependencies(arrow_python ${ARROW_PYFLIGHT_LIBRARIES})

  foreach(LIB_TARGET ${ARROW_PYFLIGHT_LIBRARIES})
    target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_PYFLIGHT_EXPORTING)
  endforeach()

  if(ARROW_BUILD_STATIC AND MSVC)
    target_compile_definitions(arrow_python_flight_static PUBLIC ARROW_STATIC)
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  # Clang, be quiet. Python C API has lots of macros
  set_property(SOURCE ${ARROW_PYTHON_SRCS}
               APPEND_STRING
               PROPERTY COMPILE_FLAGS -Wno-parentheses-equality)
endif()

arrow_install_all_headers("arrow/python")

# ----------------------------------------------------------------------

if(ARROW_BUILD_TESTS)
  enable_testing()

  # Set necessary paths for cmake to find GTest
  set(GTEST_ROOT ${ARROW_CPP_SOURCE_DIR}/${ARROW_BUILD_DIR}/googletest_ep-prefix)
  set(GTEST_INCLUDE_DIR "${GTEST_ROOT}/include")
  set(GTEST_LIBRARY ${GTEST_ROOT}/lib)
  set(GTEST_MAIN_LIBRARY ${GTEST_ROOT}/lib)

  #
  # Taken from Matlab CMakeLists.txt (enable_gtest and build_gtest)
  #

  set(ARROW_GTEST_PREFIX "${GTEST_ROOT}")
  set(ARROW_GTEST_MAIN_PREFIX "${GTEST_ROOT}")

  if(WIN32)
    set(ARROW_GTEST_SHARED_LIB_DIR "${ARROW_GTEST_PREFIX}/bin")
    set(ARROW_GTEST_MAIN_SHARED_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/bin")

    set(ARROW_GTEST_LINK_LIB_DIR "${ARROW_GTEST_PREFIX}/lib")
    set(ARROW_GTEST_LINK_LIB
        "${ARROW_GTEST_LINK_LIB_DIR}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtestd${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )

    set(ARROW_GTEST_MAIN_LINK_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
    set(ARROW_GTEST_MAIN_LINK_LIB
        "${ARROW_GTEST_MAIN_LINK_LIB_DIR}/${CMAKE_IMPORT_LIBRARY_PREFIX}gtest_maind${CMAKE_IMPORT_LIBRARY_SUFFIX}"
    )
  else()
    set(ARROW_GTEST_SHARED_LIB_DIR "${ARROW_GTEST_PREFIX}/lib")
    set(ARROW_GTEST_MAIN_SHARED_LIB_DIR "${ARROW_GTEST_MAIN_PREFIX}/lib")
  endif()

  set(ARROW_GTEST_INCLUDE_DIR "${ARROW_GTEST_PREFIX}/include")
  set(ARROW_GTEST_SHARED_LIB
      "${ARROW_GTEST_SHARED_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtestd${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )

  set(ARROW_GTEST_MAIN_INCLUDE_DIR "${ARROW_GTEST_MAIN_PREFIX}/include")
  set(ARROW_GTEST_MAIN_SHARED_LIB
      "${ARROW_GTEST_MAIN_SHARED_LIB_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}gtest_maind${CMAKE_SHARED_LIBRARY_SUFFIX}"
  )

  file(MAKE_DIRECTORY "${ARROW_GTEST_INCLUDE_DIR}")

  # Create target GTest::gtest
  add_library(GTest::gtest SHARED IMPORTED)
  set_target_properties(GTest::gtest
                        PROPERTIES IMPORTED_LOCATION ${ARROW_GTEST_SHARED_LIB}
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   ${ARROW_GTEST_INCLUDE_DIR})
  if(WIN32)
    set_target_properties(GTest::gtest PROPERTIES IMPORTED_IMPLIB ${ARROW_GTEST_LINK_LIB})
  endif()

  # ArrowTesting
  # needed to be able to use arrow_testing_shared target
  find_package(ArrowTesting REQUIRED)

  add_custom_target(all-tests)

  add_library(arrow_python_test_main STATIC util/test_main.cc)

  target_link_libraries(arrow_python_test_main GTest::gtest)
  target_include_directories(arrow_python_test_main SYSTEM
                             PUBLIC ${ARROW_PYTHON_INCLUDES})

  # Link libraries to avoid include error on Linux
  if(ARROW_TEST_LINKAGE STREQUAL shared)
    target_link_libraries(arrow_python_test_main arrow_shared)
  else()
    target_link_libraries(arrow_python_test_main arrow_static)
  endif()

  if(APPLE)
    target_link_libraries(arrow_python_test_main ${CMAKE_DL_LIBS})
    set_target_properties(arrow_python_test_main PROPERTIES LINK_FLAGS
                                                            "-undefined dynamic_lookup")
  elseif(NOT MSVC)
    target_link_libraries(arrow_python_test_main pthread ${CMAKE_DL_LIBS})
  endif()

  if(ARROW_TEST_LINKAGE STREQUAL shared)
    set(ARROW_PYTHON_TEST_LINK_LIBS arrow_python_test_main arrow_python_shared
                                    arrow_testing_shared arrow_shared)
  else()
    set(ARROW_PYTHON_TEST_LINK_LIBS arrow_python_test_main arrow_python_static
                                    arrow_testing_static arrow_static)
  endif()

  #
  # Add a test case
  #

  set(REL_TEST_NAME "python_test")
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)
  set(TEST_NAME "arrow-${TEST_NAME}")
  set(SOURCES "${REL_TEST_NAME}.cc")

  # # Make sure the executable name contains only hyphens, not underscores
  string(REPLACE "_" "-" TEST_NAME ${TEST_NAME})

  set(TEST_PATH "${CMAKE_BINARY_DIR}/${TEST_NAME}")
  add_executable(${TEST_NAME} ${SOURCES})

  # We need to set the correct RPATH so that dependencies
  set_target_properties(${TEST_NAME}
                        PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE
                                  INSTALL_RPATH_USE_LINK_PATH TRUE
                                  INSTALL_RPATH
                                  "${PYTHON_SOURCE_DIR}/pyarrow;$ENV{CONDA_PREFIX}/lib")

  # Customize link libraries
  target_link_libraries(${TEST_NAME} PRIVATE "${ARROW_PYTHON_TEST_LINK_LIBS}")
  # Extra link libs
  target_link_libraries(${TEST_NAME} PRIVATE ${PYTHON_LIBRARIES})
  # Extra includes
  target_include_directories(${TEST_NAME} SYSTEM PUBLIC "${ARROW_PYTHON_INCLUDES}")

  # Add the test
  if(ARROW_TEST_MEMCHECK)
    add_test(${TEST_NAME}
            bash
            -c
            "cd '${CMAKE_SOURCE_DIR}'; \
              valgrind --suppressions=valgrind.supp --tool=memcheck --gen-suppressions=all \
                --num-callers=500 --leak-check=full --leak-check-heuristics=stdstring \
                --error-exitcode=1 ${TEST_PATH} ${ARG_TEST_ARGUMENTS}")
  elseif(WIN32)
    add_test(${TEST_NAME} ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
            ${ARROW_CPP_SOURCE_DIR}/build-support/run-test.sh
            ${CMAKE_BINARY_DIR}
            test
            ${TEST_PATH})
  endif()
endif()