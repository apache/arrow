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

find_package(Python3Alt REQUIRED)

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
    helpers.cc
    inference.cc
    init.cc
    io.cc
    ipc.cc
    numpy_convert.cc
    numpy_to_arrow.cc
    python_to_arrow.cc
    pyarrow.cc
    serialize.cc)

set_source_files_properties(init.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                               SKIP_UNITY_BUILD_INCLUSION ON)

if(ARROW_CSV)
  list(APPEND ARROW_PYTHON_SRCS csv.cc)
endif()

if(ARROW_FILESYSTEM)
  list(APPEND ARROW_PYTHON_SRCS filesystem.cc)
endif()

set(ARROW_PYTHON_DEPENDENCIES arrow_dependencies)

if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set_property(SOURCE pyarrow.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-cast-qual ")
endif()

set(ARROW_PYTHON_SHARED_LINK_LIBS arrow_shared)
if(WIN32)
  list(APPEND ARROW_PYTHON_SHARED_LINK_LIBS ${PYTHON_LIBRARIES} ${PYTHON_OTHER_LIBS})
endif()

set(ARROW_PYTHON_INCLUDES ${NUMPY_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})

add_arrow_lib(arrow_python
              CMAKE_PACKAGE_NAME
              ArrowPython
              PKG_CONFIG_NAME
              arrow-python
              SOURCES
              ${ARROW_PYTHON_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/python/pch.h>"
              OUTPUTS
              ARROW_PYTHON_LIBRARIES
              DEPENDENCIES
              ${ARROW_PYTHON_DEPENDENCIES}
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              ${ARROW_PYTHON_SHARED_LINK_LIBS}
              STATIC_LINK_LIBS
              ${PYTHON_OTHER_LIBS}
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
  # Must link to shared libarrow_flight: we don't want to link more than one
  # copy of gRPC into the eventual Cython shared object, otherwise gRPC calls
  # fail with weird errors due to multiple copies of global static state (The
  # other solution is to link gRPC shared everywhere instead of statically only
  # in Flight)
  add_arrow_lib(arrow_python_flight
                CMAKE_PACKAGE_NAME
                ArrowPythonFlight
                PKG_CONFIG_NAME
                arrow-python-flight
                SOURCES
                flight.cc
                OUTPUTS
                ARROW_PYFLIGHT_LIBRARIES
                DEPENDENCIES
                flight_grpc_gen
                SHARED_LINK_FLAGS
                ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
                SHARED_LINK_LIBS
                arrow_python_shared
                arrow_flight_shared
                STATIC_LINK_LIBS
                ${PYTHON_OTHER_LIBS}
                EXTRA_INCLUDES
                "${ARROW_PYTHON_INCLUDES}")

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
  add_library(arrow_python_test_main STATIC util/test_main.cc)

  target_link_libraries(arrow_python_test_main GTest::gtest)
  target_include_directories(arrow_python_test_main SYSTEM
                             PUBLIC ${ARROW_PYTHON_INCLUDES})

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

  add_arrow_test(python_test
                 STATIC_LINK_LIBS
                 "${ARROW_PYTHON_TEST_LINK_LIBS}"
                 EXTRA_LINK_LIBS
                 ${PYTHON_LIBRARIES}
                 EXTRA_INCLUDES
                 "${ARROW_PYTHON_INCLUDES}"
                 LABELS
                 "arrow_python-tests"
                 NO_VALGRIND)
endif()
