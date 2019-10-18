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
# arrow_orc
#

# Headers: top level
install(FILES adapter.h DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/adapters/orc")

# pkg-config support
arrow_add_pkg_config("arrow-orc")

set(ORC_MIN_TEST_LIBS GTest::Main GTest::GTest)

if(ARROW_BUILD_STATIC)
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS arrow_testing_static arrow_static)
else()
  set(ARROW_LIBRARIES_FOR_STATIC_TESTS arrow_testing_shared arrow_shared)
endif()

if(APPLE)
  set(ORC_MIN_TEST_LIBS ${ORC_MIN_TEST_LIBS} ${CMAKE_DL_LIBS})
elseif(NOT MSVC)
  set(ORC_MIN_TEST_LIBS ${ORC_MIN_TEST_LIBS} pthread ${CMAKE_DL_LIBS})
endif()

set(ORC_STATIC_TEST_LINK_LIBS ${ORC_MIN_TEST_LIBS} ${ARROW_LIBRARIES_FOR_STATIC_TESTS}
                              orc::liborc)

add_arrow_test(adapter_test
               PREFIX
               "orc"
               STATIC_LINK_LIBS
               ${ORC_STATIC_TEST_LINK_LIBS})
