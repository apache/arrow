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
# arrow_table
#######################################

set(TABLE_SRCS
  column.cc
)

set(TABLE_LIBS
)

add_library(arrow_table STATIC
  ${TABLE_SRCS}
)
target_link_libraries(arrow_table ${TABLE_LIBS})
SET_TARGET_PROPERTIES(arrow_table PROPERTIES LINKER_LANGUAGE CXX)

# Headers: top level
install(FILES
  DESTINATION include/arrow/table)

ADD_ARROW_TEST(column-test)
