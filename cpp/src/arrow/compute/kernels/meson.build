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
# Tests that don't require the full kernel library

# Define arrow_compute_kernels_testing object library for common test files
kernel_testing_srcs = []
if needs_testing
    kernel_testing_srcs += files('test_util_internal.cc')
endif

exc = executable(
    'arrow-scalar-cast-test',
    sources: ['scalar_cast_test.cc'] + kernel_testing_srcs,
    dependencies: [arrow_compute_test_dep],
)
test('arrow-scalar-cast-test', exc)

# ----------------------------------------------------------------------
# Scalar kernels

scalar_kernel_tests = {
    'arrow-compute-scalar-type-test': {
        'sources': [
            'scalar_boolean_test.cc',
            'scalar_nested_test.cc',
            'scalar_string_test.cc',
        ],
    },
    'arrow-compute-scalar-if-else-test': {'sources': ['scalar_if_else_test.cc']},
    'arrow-compute-scalar-temporal-test': {
        'sources': ['scalar_temporal_test.cc'],
    },
    'arrow-compute-scalar-math-test': {
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
}

foreach key, val : scalar_kernel_tests
    exc = executable(
        key,
        sources: val['sources'] + kernel_testing_srcs,
        dependencies: [arrow_compute_test_dep],
    )
    test(key, exc)
endforeach

scalar_kernel_benchmarks = [
    'scalar_arithmetic_benchmark',
    'scalar_boolean_benchmark',
    'scalar_cast_benchmark',
    'scalar_compare_benchmark',
    'scalar_if_else_benchmark',
    'scalar_list_benchmark',
    'scalar_random_benchmark',
    'scalar_set_lookup_benchmark',
    'scalar_string_benchmark',
    'scalar_temporal_benchmark',
]

foreach benchmark : scalar_kernel_benchmarks
    benchmark_name = 'arrow-compute-@0@'.format(benchmark.replace('_', '-'))
    exc = executable(
        benchmark_name,
        sources: '@0@.cc'.format(benchmark),
        dependencies: [arrow_benchmark_dep, gmock_dep],
    )
    benchmark(benchmark_name, exc)
endforeach

# ----------------------------------------------------------------------
# Vector kernels

vector_kernel_tests = {
    'arrow-compute-vector-test': {
        'sources': [
            'vector_cumulative_ops_test.cc',
            'vector_pairwise_test.cc',
            'vector_hash_test.cc',
            'vector_nested_test.cc',
            'vector_replace_test.cc',
            'vector_run_end_encode_test.cc',
            'vector_statistics_test.cc',
            'select_k_test.cc',
        ],
    },
    'arrow-compute-vector-sort-test': {'sources': ['vector_sort_test.cc']},
    'arrow-compute-vector-selection-test': {
        'sources': ['vector_selection_test.cc'],
    },
    'arrow-compute-vector-swizzle-test': {'sources': ['vector_swizzle_test.cc']},
}

foreach key, val : vector_kernel_tests
    exc = executable(
        key,
        sources: val['sources'] + kernel_testing_srcs,
        dependencies: [arrow_compute_test_dep],
    )
    test(key, exc)
endforeach

vector_kernel_benchmarks = [
    'vector_hash_benchmark',
    'vector_sort_benchmark',
    'vector_partition_benchmark',
    'vector_topk_benchmark',
    'vector_replace_benchmark',
    'vector_selection_benchmark',
]

foreach benchmark : vector_kernel_benchmarks
    benchmark_name = 'arrow-compute-@0@'.format(benchmark.replace('_', '-'))
    exc = executable(
        benchmark_name,
        sources: '@0@.cc'.format(benchmark),
        dependencies: [arrow_benchmark_dep],
    )
    benchmark(benchmark_name, exc)
endforeach

# ----------------------------------------------------------------------
# Aggregate kernels

# Aggregates
exc = executable(
    'arrow-compute-aggregate-test',
    sources: ['aggregate_test.cc'] + kernel_testing_srcs,
    dependencies: [arrow_compute_test_dep, filesystem_dep],
)
test('arrow-compute-aggregate-test', exc)

# ----------------------------------------------------------------------
# Utilities

exc = executable(
    'arrow-compute-kernel-utility-test',
    sources: ['codegen_internal_test.cc'],
    dependencies: [arrow_compute_test_dep],
)
test('arrow-compute-kernel-utility-test', exc)
