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
install(FILES adapter.h options.h
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/adapters/orc")

# pkg-config support
arrow_add_pkg_config("arrow-orc")

if(ARROW_BUILD_STATIC)
  set(ARROW_ORC_STATIC_LINK_LIBS ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_ORC_STATIC_LINK_LIBS ${ARROW_TEST_SHARED_LINK_LIBS})
endif()
add_arrow_test(adapter_test
               PREFIX
               "arrow-orc"
               STATIC_LINK_LIBS
               ${ARROW_ORC_STATIC_LINK_LIBS})

set_source_files_properties(adapter_test.cc PROPERTIES SKIP_PRECOMPILE_HEADERS ON
                                                       SKIP_UNITY_BUILD_INCLUSION ON)
