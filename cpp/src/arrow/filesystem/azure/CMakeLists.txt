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

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_arrow_lib_azure(azurefs
                    SHARED_LINK_LIBS
                    ${ARROW_AZURE_LINK_LIBS}
                    SHARED_PRIVATE_LINK_LIBS
                    ${ARROW_AZURE_SHARED_PRIVATE_LINK_LIBS}
                    STATIC_LINK_LIBS
                    ${ARROW_AZURE_STATIC_LINK_LIBS})

arrow_install_all_headers("arrow/filesystem/azure")

set(TEST_LIBS_AZURE ${ARROW_TEST_LINK_LIBS})
list(APPEND TEST_LIBS_AZURE azurefs_shared)
add_arrow_test_azure(azurefs_test EXTRA_LABELS filesystem
                     STATIC_LINK_LIBS
                     ${TEST_LIBS_AZURE}
                     )