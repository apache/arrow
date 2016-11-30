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

# Headers: top level
install(FILES
  api.h
  array.h
  column.h
  builder.h
  pretty_print.h
  schema.h
  table.h
  type.h
  type_fwd.h
  type_traits.h
  test-util.h
  DESTINATION include/arrow)

#######################################
# Unit tests
#######################################

set(ARROW_TEST_LINK_LIBS ${ARROW_MIN_TEST_LIBS})

ADD_ARROW_TEST(array-test)
ADD_ARROW_TEST(column-test)
ADD_ARROW_TEST(pretty_print-test)
ADD_ARROW_TEST(schema-test)
ADD_ARROW_TEST(table-test)

ADD_ARROW_BENCHMARK(column-benchmark)
