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
# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_ARROW_PYTHON_TEST REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
    target_link_libraries(${TEST_NAME} ${PYARROW_TEST_LINK_LIBS})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  add_test(${TEST_NAME}
    ${BUILD_SUPPORT_DIR}/run-test.sh ${TEST_PATH})
  if(ARGN)
    set_tests_properties(${TEST_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_TESTS.
function(ADD_ARROW_PYTHON_TEST_DEPENDENCIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_dependencies(${TEST_NAME} ${ARGN})
endfunction()

if (ARROW_BUILD_TESTS)
  add_library(pyarrow_test_main STATIC
	util/test_main.cc)

  if (APPLE)
	target_link_libraries(pyarrow_test_main
      gtest
      dl)
	set_target_properties(pyarrow_test_main
      PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  else()
	target_link_libraries(pyarrow_test_main
      gtest
      pthread
      dl
	  )
  endif()
endif()

# ----------------------------------------------------------------------

set(ARROW_PYTHON_SRCS
  common.cc
  config.cc
  helpers.cc
  io.cc
  adapters/builtin.cc
  adapters/pandas.cc
)

set(ARROW_PYTHON_SHARED_LINK_LIBS
  arrow_io_shared
  arrow_ipc_shared
  arrow_shared
)

set(ARROW_PYTHON_TEST_LINK_LIBS
  arrow_ipc_static
  arrow_io_static)

ADD_ARROW_LIB(arrow_python
  SOURCES ${ARROW_PYTHON_SRCS}
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS ${ARROW_PYTHON_SHARED_LINK_LIBS}
  STATIC_LINK_LIBS ${ARROW_IO_SHARED_PRIVATE_LINK_LIBS}
)

install(FILES
  api.h
  common.h
  config.h
  helper.h
  io.h
  numpy_interop.h
  adapters/builtin.h
  adapters/pandas.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/python")

# set_target_properties(pyarrow PROPERTIES
#     INSTALL_RPATH "\$ORIGIN")

ADD_ARROW_PYTHON_TEST(adapters/pandas-test)
