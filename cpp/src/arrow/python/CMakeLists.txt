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

  if (APPLE)
	target_link_libraries(arrow_python_test_main
      gtest
      dl)
	set_target_properties(arrow_python_test_main
      PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
  else()
	target_link_libraries(arrow_python_test_main
      gtest
      pthread
      dl
	  )
  endif()
endif()

set(ARROW_PYTHON_MIN_TEST_LIBS
  arrow_python_test_main
  arrow_python_static
  arrow_ipc_static
  arrow_io_static
  arrow_static)

if(NOT APPLE AND ARROW_BUILD_TESTS)
  ADD_THIRDPARTY_LIB(python
    SHARED_LIB "${PYTHON_LIBRARIES}")
  list(APPEND ARROW_PYTHON_MIN_TEST_LIBS python)
endif()

set(ARROW_PYTHON_TEST_LINK_LIBS ${ARROW_PYTHON_MIN_TEST_LIBS})

# ----------------------------------------------------------------------

set(ARROW_PYTHON_SRCS
  builtin_convert.cc
  common.cc
  config.cc
  helpers.cc
  io.cc
  pandas_convert.cc
)

set(ARROW_PYTHON_SHARED_LINK_LIBS
  arrow_io_shared
  arrow_ipc_shared
  arrow_shared
)

ADD_ARROW_LIB(arrow_python
  SOURCES ${ARROW_PYTHON_SRCS}
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS ${ARROW_PYTHON_SHARED_LINK_LIBS}
  STATIC_LINK_LIBS ${ARROW_IO_SHARED_PRIVATE_LINK_LIBS}
)

install(FILES
  api.h
  builtin_convert.h
  common.h
  config.h
  do_import_numpy.h
  helpers.h
  io.h
  numpy_interop.h
  pandas_convert.h
  type_traits.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/python")

# set_target_properties(arrow_python_shared PROPERTIES
#     INSTALL_RPATH "\$ORIGIN")

if (ARROW_BUILD_TESTS)
  ADD_ARROW_TEST(python-test
    STATIC_LINK_LIBS "${ARROW_PYTHON_TEST_LINK_LIBS}")
endif()
