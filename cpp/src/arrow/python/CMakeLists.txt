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

#######################################
# arrow_python
#######################################

find_package(PythonLibsNew REQUIRED)
find_package(NumPy REQUIRED)

add_custom_target(arrow_python)

set(ARROW_PYTHON_SRCS
  arrow_to_pandas.cc
  benchmark.cc
  common.cc
  config.cc
  decimal.cc
  deserialize.cc
  helpers.cc
  inference.cc
  init.cc
  io.cc
  numpy_convert.cc
  numpy_to_arrow.cc
  python_to_arrow.cc
  pyarrow.cc
  serialize.cc
)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE pyarrow.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-cast-qual ")
endif()

set(ARROW_PYTHON_SHARED_LINK_LIBS
  arrow_shared
  ${PYTHON_OTHER_LIBS}
)

if (WIN32)
  set(ARROW_PYTHON_SHARED_LINK_LIBS
    ${ARROW_PYTHON_SHARED_LINK_LIBS}
    ${PYTHON_LIBRARIES}
    )
endif()

set(ARROW_PYTHON_INCLUDES
  ${NUMPY_INCLUDE_DIRS}
  ${PYTHON_INCLUDE_DIRS})

ADD_ARROW_LIB(arrow_python
  SOURCES ${ARROW_PYTHON_SRCS}
  OUTPUTS ARROW_PYTHON_LIBRARIES
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS ${ARROW_PYTHON_SHARED_LINK_LIBS}
  STATIC_LINK_LIBS ${PYTHON_OTHER_LIBS}
  EXTRA_INCLUDES "${ARROW_PYTHON_INCLUDES}"
)

foreach(LIB_TARGET ${ARROW_PYTHON_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
    PRIVATE ARROW_EXPORTING)
endforeach()

if (ARROW_BUILD_STATIC AND MSVC)
  target_compile_definitions(arrow_python_static PUBLIC ARROW_STATIC)
endif()

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Clang, be quiet. Python C API has lots of macros
  set_property(SOURCE ${ARROW_PYTHON_SRCS}
    APPEND_STRING
    PROPERTY
    COMPILE_FLAGS -Wno-parentheses-equality)
endif()

install(FILES
  api.h
  arrow_to_pandas.h
  benchmark.h
  common.h
  config.h
  decimal.h
  deserialize.h
  helpers.h
  inference.h
  init.h
  io.h
  iterators.h
  numpy_convert.h
  numpy_interop.h
  numpy_to_arrow.h
  python_to_arrow.h
  platform.h
  pyarrow.h
  serialize.h
  type_traits.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/python")

# pkg-config support
configure_file(arrow-python.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-python.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-python.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

# ----------------------------------------------------------------------

if (ARROW_BUILD_TESTS)
  add_library(arrow_python_test_main STATIC
	util/test_main.cc)

  target_link_libraries(arrow_python_test_main
    gtest_static)
  target_include_directories(arrow_python_test_main SYSTEM PUBLIC
    ${ARROW_PYTHON_INCLUDES})

  if (APPLE)
	target_link_libraries(arrow_python_test_main
      ${CMAKE_DL_LIBS})
	set_target_properties(arrow_python_test_main
      PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  elseif(NOT MSVC)
	target_link_libraries(arrow_python_test_main
      pthread
      ${CMAKE_DL_LIBS})
  endif()

  set(ARROW_PYTHON_MIN_TEST_LIBS
    arrow_python_test_main
    arrow_python_shared
    arrow_testing_shared
    arrow_shared)

  set(ARROW_PYTHON_TEST_LINK_LIBS ${ARROW_PYTHON_MIN_TEST_LIBS})

  ADD_ARROW_TEST(python-test
    STATIC_LINK_LIBS "${ARROW_PYTHON_TEST_LINK_LIBS}"
    EXTRA_LINK_LIBS ${PYTHON_LIBRARIES}
    EXTRA_INCLUDES "${ARROW_PYTHON_INCLUDES}"
    LABELS "arrow_python"
    NO_VALGRIND)
endif()
