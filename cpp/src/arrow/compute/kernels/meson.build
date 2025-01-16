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

# TODO: arrow_install_all_headers
# TODO: add_pkg_config

compute_kernels_test_util_srcs = [
    '../test_util_internal.cc',
    'test_util_internal.cc',
]

tests = {'scalar-cast-test': {'sources': ['scalar_cast_test.cc']}}

if needs_compute
    tests += {
        'arrow-compute-scalar-type-test': {
            'sources': [
                'scalar_boolean_test.cc',
                'scalar_nested_test.cc',
                'scalar_string_test.cc',
            ],
        },
        'arrow-compute-scalar-if-else-test': {
            'sources': ['scalar_if_else_test.cc'],
        },
        'arrow-compute-scalar-temporal-test': {
            'sources': ['scalar_temporal_test.cc'],
        },
        'arrow-compute-scalar-match-test': {
            'sources': [
                'scalar_arithmetic_test.cc',
                'scalar_compare_test.cc',
                'scalar_round_arithmetic_test.cc',
            ],
        },
        'arrow-compute-scalar-utility-test': {
            'sources': [
                'scalar_random_test.cc',
                'scalar_set_lookup_test.cc',
                'scalar_validity_test.cc',
            ],
        },
        # Vector tests
        'arrow-compute-vector-test': {
            'sources': [
                'vector_cumulative_ops_test.cc',
                'vector_pairwise_test.cc',
                'vector_hash_test.cc',
                'vector_nested_test.cc',
                'vector_replace_test.cc',
                'vector_run_end_encode_test.cc',
                'select_k_test.cc',
            ],
        },
        'arrow-compute-vector-sort-test': {'sources': ['vector_sort_test.cc']},
        'arrow-compute-vector-selection-test': {
            'sources': ['vector_selection_test.cc'],
        },
        'arrow-compute-vector-swizzle-test': {
            'sources': ['vector_swizzle_test.cc'],
        },
        # Aggregates
        'arrow-compute-aggregate-test': {'sources': ['aggregate_test.cc']},
        # Utilities
        'arrow-compute-kernel-utility-test': {
            'sources': ['codegen_internal_test.cc'],
        },
    }
endif

foreach key, val : tests
    exc = executable(
        key,
        sources: compute_kernels_test_util_srcs + val['sources'],
        dependencies: [arrow_test_dep],
    )
    test(key, exc)
endforeach

# TODO: benchmarks
