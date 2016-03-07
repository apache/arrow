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
# arrow_types
#######################################

# Headers: top level
install(FILES
  boolean.h
  collection.h
  construct.h
  datetime.h
  decimal.h
  floating.h
  integer.h
  json.h
  list.h
  primitive.h
  string.h
  struct.h
  union.h
  DESTINATION include/arrow/types)


ADD_ARROW_TEST(list-test)
ADD_ARROW_TEST(primitive-test)
ADD_ARROW_TEST(string-test)
ADD_ARROW_TEST(struct-test)
