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

project(gandiva)

foreach(lib_type "shared" "static")
  add_gandiva_integ_test(filter_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(projector_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(if_expr_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(literal_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(projector_build_validation_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(boolean_expr_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(utf8_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(binary_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(date_time_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(micro_benchmarks.cc gandiva_${lib_type})
  add_gandiva_integ_test(to_string_test.cc gandiva_${lib_type})
  add_gandiva_integ_test(hash_test.cc gandiva_${lib_type})
endforeach(lib_type)
