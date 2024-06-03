# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if needs_testing
    scalar_cast_test = executable(
        'scalar_cast_test',
        'scalar_cast_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-scalar-cast', scalar_cast_test)

    scalar_type_test = executable(
        'scalar_type_test',
        'scalar_boolean_test.cc',
        'scalar_nested_test.cc',
        'scalar_string_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-scalar-type', scalar_type_test)

    scalar_if_else_test = executable(
        'scalar_if_else_test',
        'scalar_if_else_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-scalar-if-else', scalar_if_else_test)

    scalar_temporal_test = executable(
        'scalar_temporal_test',
        'scalar_temporal_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-scalar-temporal', scalar_temporal_test)

    scalar_math_test = executable(
        'scalar_math_test',
        'scalar_arithmetic_test.cc',
        'scalar_compare_test.cc',
        'scalar_round_arithmetic_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-scalar-math', scalar_math_test)

    scalar_utility_test = executable(
        'scalar_utility_test',
        'scalar_random_test.cc',
        'scalar_set_lookup_test.cc',
        'scalar_validity_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-scalar-utility', scalar_utility_test)

    # Vector kernels
    vector_test = executable(
        'vector_test',
        'vector_cumulative_ops_test.cc',
        'vector_pairwise_test.cc',
        'vector_hash_test.cc',
        'vector_nested_test.cc',
        'vector_replace_test.cc',
        'vector_run_end_encode_test.cc',
        'select_k_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-vector', vector_test)

    vector_sort_test = executable(
        'vector_sort_test',
        'vector_sort_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-vector-sort', vector_sort_test)

    vector_selection_test = executable(
        'vector_selection_test',
        'vector_selection_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-vector-selection', vector_selection_test)

    # Aggregates
    aggregate_test = executable(
        'aggregate_test',
        'aggregate_test.cc',
        'test_util.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-aggregate', aggregate_test)

    # Utilities
    kernel_utility_test = executable(
        'kernel_utility_test',
        'codegen_internal_test.cc',
        include_directories: [root_dir],
        link_with: [arrow_lib, arrow_testing_lib],
        dependencies: [gtest_main_dep, gmock_dep],
    )
    test('arrow-compute-kernel-utility', kernel_utility_test)
endif

if get_option('ARROW_BUILD_BENCHMARKS')
    kernel_benchmarks = {
        'scalar-arithmetic' : {
            'src_name': 'scalar_arithmetic',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-boolean' : {
            'src_name': 'scalar_boolean',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-cast' : {
            'src_name': 'scalar_cast',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-compare' : {
            'src_name': 'scalar_compare',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-if-else' : {
            'src_name': 'scalar_if_else',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gtest_dep],
        },
        'scalar-random' : {
            'src_name': 'scalar_random',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-set-lookup' : {
            'src_name': 'scalar_set_lookup',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-string' : {
            'src_name': 'scalar_string',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'scalar-temporal' : {
            'src_name': 'scalar_temporal',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
    }

    foreach nm, conf : kernel_benchmarks
        exc = executable(
            'arrow-' + nm + '-benchmark',
            conf['src_name'] + '_benchmark.cc',
            include_directories: [root_dir],
            link_with: conf['link_with'],
            dependencies: conf['deps'],
        )
        benchmark('arrow-' + nm, exc)
    endforeach

    vector_benchmarks = {
        'vector-hash' : {
            'src_name': 'vector_hash',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gtest_dep],
        },
        'vector-sort' : {
            'src_name': 'vector_sort',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'vector-partition' : {
            'src_name': 'vector_partition',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'vector-topk' : {
            'src_name': 'vector_topk',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
        'vector-replace' : {
            'src_name': 'vector_replace',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gtest_dep],
        },
        'vector-selection' : {
            'src_name': 'vector_selection',
            'link_with': [arrow_lib, arrow_testing_lib],
            'deps': [gbenchmark_dep, gmock_dep],
        },
    }

    foreach nm, conf : vector_benchmarks
        exc = executable(
            'arrow-' + nm + '-benchmark',
            conf['src_name'] + '_benchmark.cc',
            include_directories: [root_dir],
            link_with: conf['link_with'],
            dependencies: conf['deps'],
        )
        benchmark('arrow-' + nm, exc)
    endforeach
endif
