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

add_gandiva_test(filter-test)
add_gandiva_test(projector-test)
add_gandiva_test(projector-build-validation-test)
add_gandiva_test(if-expr-test)
add_gandiva_test(literal-test)
add_gandiva_test(boolean-expr-test)
add_gandiva_test(binary-test)
add_gandiva_test(date-time-test)
add_gandiva_test(to-string-test)
add_gandiva_test(utf8-test)
add_gandiva_test(hash-test)
add_gandiva_test(in-expr-test)
add_gandiva_test(null-validity-test)
add_gandiva_test(decimal-test)
add_gandiva_test(decimal-single-test)
add_gandiva_test(filter-project-test)

if(ARROW_BUILD_STATIC)
  add_gandiva_test(projector_test_static SOURCES projector-test.cc USE_STATIC_LINKING)
  add_arrow_benchmark(micro-benchmarks
                      PREFIX
                      "gandiva"
                      EXTRA_LINK_LIBS
                      gandiva_static)
endif()
