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

install_headers(
    [
        'accumulation_queue.h',
        'aggregate_node.h',
        'api.h',
        'asof_join_node.h',
        'backpressure_handler.h',
        'benchmark_util.h',
        'bloom_filter.h',
        'exec_plan.h',
        'hash_join_dict.h',
        'hash_join.h',
        'hash_join_node.h',
        'map_node.h',
        'options.h',
        'order_by_impl.h',
        'partition_util.h',
        'query_context.h',
        'schema_util.h',
        'task_util.h',
        'test_nodes.h',
        'time_series_util.h',
        'tpch_node.h',
        'type_fwd.h',
        'util.h',
        'visibility.h',
    ],
    subdir: 'arrow/acero',
)

arrow_acero_srcs = [
    'accumulation_queue.cc',
    'scalar_aggregate_node.cc',
    'groupby_aggregate_node.cc',
    'aggregate_internal.cc',
    'asof_join_node.cc',
    'bloom_filter.cc',
    'exec_plan.cc',
    'fetch_node.cc',
    'filter_node.cc',
    'hash_join.cc',
    'hash_join_dict.cc',
    'hash_join_node.cc',
    'map_node.cc',
    'options.cc',
    'order_by_node.cc',
    'order_by_impl.cc',
    'partition_util.cc',
    'pivot_longer_node.cc',
    'project_node.cc',
    'query_context.cc',
    'sink_node.cc',
    'sorted_merge_node.cc',
    'source_node.cc',
    'swiss_join.cc',
    'task_util.cc',
    'time_series_util.cc',
    'tpch_node.cc',
    'union_node.cc',
    'util.cc',
]

arrow_acero_lib = library(
    'arrow-acero',
    sources: arrow_acero_srcs,
    dependencies: [arrow_compute_dep, arrow_dep, threads_dep],
    gnu_symbol_visibility: 'inlineshidden',
)

arrow_acero_dep = declare_dependency(link_with: [arrow_acero_lib])

arrow_acero_testing_sources = ['test_nodes.cc', 'test_util_internal.cc']

arrow_acero_tests = {
    'plan-test': {'sources': ['plan_test.cc', 'test_nodes_test.cc']},
    'source-node-test': {'sources': ['source_node_test.cc']},
    'fetch-node-test': {'sources': ['fetch_node_test.cc']},
    'order-by-node-test': {'sources': ['order_by_node_test.cc']},
    'hash-join-node-test': {
        'sources': ['hash_join_node_test.cc', 'bloom_filter_test.cc'],
    },
    'pivot-longer-node-test': {'sources': ['pivot_longer_node_test.cc']},
    'asof-join-node-test': {'sources': ['asof_join_node_test.cc']},
    'sorted-merge-node-test': {'sources': ['sorted_merge_node_test.cc']},
    'tpch-node-test': {'sources': ['tpch_node_test.cc']},
    'union-node-test': {'sources': ['union_node_test.cc']},
    'aggregate-node-test': {'sources': ['aggregate_node_test.cc']},
    'util-test': {'sources': ['util_test.cc', 'task_util_test.cc']},
    'hash-aggregate-test': {'sources': ['hash_aggregate_test.cc']},
    'test-util-internal-test': {'sources': ['test_util_internal_test.cc']},
}

foreach key, val : arrow_acero_tests
    exc = executable(
        'arrow-acero-@0@'.format(key),
        sources: val['sources'] + arrow_acero_testing_sources,
        dependencies: [arrow_acero_dep, arrow_compute_test_dep],
    )
    test(key, exc)
endforeach

arrow_acero_benchmarks = {
    'expression-benchmark': {'sources': ['expression_benchmark.cc']},
    'filter-benchmark': {
        'sources': ['benchmark_util.cc', 'filter_benchmark.cc'],
    },
    'project-benchmark': {
        'sources': ['benchmark_util.cc', 'project_benchmark.cc'],
    },
    'asof-join-benchmark': {'sources': ['asof_join_benchmark.cc']},
    'tpch-benchmark': {'sources': ['tpch_benchmark.cc']},
    'aggregate-benchmark': {'sources': ['aggregate_benchmark.cc']},
    'hash-join-benchmark': {'sources': ['hash_join_benchmark.cc']},
}

foreach key, val : arrow_acero_benchmarks
    exc = executable(
        key,
        sources: val['sources'] + arrow_acero_testing_sources,
        dependencies: [
            arrow_acero_dep,
            arrow_compute_test_dep,
            arrow_benchmark_dep,
            gmock_dep,
        ],
    )
    benchmark(key, exc)
endforeach

pkg.generate(
    filebase: 'arrow-acero',
    name: 'Apache Arrow Acero Engine',
    description: 'Apache Arrow\'s Acero Engine',
    requires: ['arrow-compute'],
)
