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

if (ARROW_BUILD_TESTS)
  add_library(arrow_python_test_main STATIC
	util/test_main.cc)

  target_link_libraries(arrow_python_test_main
    gtest)

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
endif()

set(ARROW_PYTHON_MIN_TEST_LIBS
  arrow_python_test_main
  arrow_python_static
  arrow_shared)

set(ARROW_PYTHON_TEST_LINK_LIBS ${ARROW_PYTHON_MIN_TEST_LIBS})

# ----------------------------------------------------------------------

set(ARROW_PYTHON_SRCS
  arrow_to_pandas.cc
  arrow_to_python.cc
  builtin_convert.cc
  common.cc
  config.cc
  helpers.cc
  init.cc
  io.cc
  numpy_convert.cc
  numpy_to_arrow.cc
  python_to_arrow.cc
  pyarrow.cc
)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE pyarrow.cc
    APPEND_STRING
    PROPERTY COMPILE_FLAGS
    " -Wno-cast-qual ")
endif()

set(ARROW_PYTHON_SHARED_LINK_LIBS
  arrow_shared
)

if (MSVC)
  set(ARROW_PYTHON_SHARED_LINK_LIBS
    ${ARROW_PYTHON_SHARED_LINK_LIBS}
    ${PYTHON_LIBRARIES}
    )
endif()

ADD_ARROW_LIB(arrow_python
  SOURCES ${ARROW_PYTHON_SRCS}
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS ${ARROW_PYTHON_SHARED_LINK_LIBS}
  STATIC_LINK_LIBS ""
)

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
  arrow_to_python.h
  builtin_convert.h
  common.h
  config.h
  helpers.h
  init.h
  io.h
  numpy_convert.h
  numpy_interop.h
  numpy_to_arrow.h
  python_to_arrow.h
  platform.h
  pyarrow.h
  type_traits.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/python")

# pkg-config support
configure_file(arrow-python.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-python.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-python.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

if (ARROW_BUILD_TESTS)
  ADD_ARROW_TEST(python-test
    STATIC_LINK_LIBS "${ARROW_PYTHON_TEST_LINK_LIBS}"
    NO_VALGRIND)
  target_link_libraries(python-test
    ${PYTHON_LIBRARIES})
endif()
