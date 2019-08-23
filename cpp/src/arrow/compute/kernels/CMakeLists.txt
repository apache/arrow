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

arrow_install_all_headers("arrow/compute/kernels")

add_arrow_test(boolean_test PREFIX "arrow-compute")
add_arrow_test(cast_test PREFIX "arrow-compute")
add_arrow_test(hash_test PREFIX "arrow-compute")
add_arrow_test(isin_test PREFIX "arrow-compute")
add_arrow_test(sort_to_indices_test PREFIX "arrow-compute")
add_arrow_test(util_internal_test PREFIX "arrow-compute")
add_arrow_benchmark(sort_to_indices_benchmark PREFIX "arrow-compute")

# Aggregates
add_arrow_test(aggregate_test PREFIX "arrow-compute")
add_arrow_benchmark(aggregate_benchmark PREFIX "arrow-compute")

# Comparison
add_arrow_test(compare_test PREFIX "arrow-compute")
add_arrow_benchmark(compare_benchmark PREFIX "arrow-compute")

# Selection
add_arrow_test(take_test PREFIX "arrow-compute")
add_arrow_test(filter_test PREFIX "arrow-compute")
add_arrow_benchmark(filter_benchmark PREFIX "arrow-compute")
add_arrow_benchmark(take_benchmark PREFIX "arrow-compute")
