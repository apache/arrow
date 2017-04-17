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
  arrow_static
  ${BOOST_REGEX_LIBRARY})

set(ARROW_PYTHON_TEST_LINK_LIBS ${ARROW_PYTHON_MIN_TEST_LIBS})

# ----------------------------------------------------------------------

set(ARROW_PYTHON_SRCS
  builtin_convert.cc
  common.cc
  config.cc
  helpers.cc
  init.cc
  io.cc
  numpy_convert.cc
  pandas_convert.cc
)

set(ARROW_PYTHON_SHARED_LINK_LIBS
  arrow_shared
)

ADD_ARROW_LIB(arrow_python
  SOURCES ${ARROW_PYTHON_SRCS}
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS ${ARROW_PYTHON_SHARED_LINK_LIBS}
  STATIC_LINK_LIBS ""
)

if (MSVC)
  target_link_libraries(arrow_python_shared
    ${PYTHON_LIBRARIES})
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
  builtin_convert.h
  common.h
  config.h
  helpers.h
  init.h
  io.h
  numpy_convert.h
  numpy_interop.h
  pandas_convert.h
  platform.h
  type_traits.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/python")

if (ARROW_BUILD_TESTS)
  ADD_ARROW_TEST(python-test
    STATIC_LINK_LIBS "${ARROW_PYTHON_TEST_LINK_LIBS}")
  target_link_libraries(python-test
    ${PYTHON_LIBRARIES})
endif()
