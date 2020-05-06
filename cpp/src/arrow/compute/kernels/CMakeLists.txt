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

# ----------------------------------------------------------------------
# Scalar kernels

add_arrow_compute_test(scalar_test
                       SOURCES
                       scalar_arithmetic_test.cc
                       scalar_boolean_test.cc
                       scalar_compare_test.cc
                       scalar_set_lookup_test.cc)

# add_arrow_compute_test(cast_test)

add_arrow_benchmark(scalar_compare_benchmark PREFIX "arrow-compute")

# ----------------------------------------------------------------------
# Vector kernels

add_arrow_compute_test(vector_test
                       SOURCES
                       vector_partition_test.cc)

# add_arrow_compute_test(hash_test)

# add_arrow_benchmark(hash_benchmark PREFIX "arrow-compute")

# Single-array sorting

# add_arrow_compute_test(sort_to_indices_test)
# add_arrow_benchmark(sort_to_indices_benchmark PREFIX "arrow-compute")
# add_arrow_benchmark(nth_to_indices_benchmark PREFIX "arrow-compute")

# Array value selection

# add_arrow_compute_test(filter_test)
# add_arrow_compute_test(take_test)

# add_arrow_benchmark(filter_benchmark PREFIX "arrow-compute")a
# add_arrow_benchmark(take_benchmark PREFIX "arrow-compute")

# ----------------------------------------------------------------------
# Aggregate kernels

# Aggregates

add_arrow_compute_test(aggregate_test)
# add_arrow_benchmark(aggregate_benchmark PREFIX "arrow-compute")
