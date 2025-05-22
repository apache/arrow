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
        'api.h',
        'dataset.h',
        'dataset_writer.h',
        'discovery.h',
        'file_base.h',
        'file_csv.h',
        'file_ipc.h',
        'file_json.h',
        'file_orc.h',
        'file_parquet.h',
        'parquet_encryption_config.h',
        'partition.h',
        'plan.h',
        'projector.h',
        'scanner.h',
        'type_fwd.h',
        'visibility.h',
    ],
    subdir: 'arrow/dataset',
)

arrow_dataset_srcs = [
    'dataset.cc',
    'dataset_writer.cc',
    'discovery.cc',
    'file_base.cc',
    'file_ipc.cc',
    'partition.cc',
    'plan.cc',
    'projector.cc',
    'scanner.cc',
    'scan_node.cc',
]

if needs_csv
    arrow_dataset_srcs += ['file_csv.cc']
endif

if needs_json
    arrow_dataset_srcs += ['file_json.cc']
endif

# requires https://github.com/apache/arrow/pull/46409
#if needs_orc
#    arrow_dataset_srcs += ['file_orc.cc']
#endif

# requires https://github.com/apache/arrow/issues/46410
# if needs_parquet
#    arrow_dataset_srcs += ['file_parquet.cc']
# endif

arrow_dataset_lib = library(
    'arrow-dataset',
    sources: arrow_dataset_srcs,
    dependencies: [arrow_dep, arrow_acero_dep],
)

arrow_dataset_testing_srcs = files('test_util_internal.cc')

arrow_dataset_tests = {
    'dataset-test': {'sources': ['dataset_test.cc']},
    'dataset-writer-test': {'sources': ['dataset_writer_test.cc']},
    'discovery-test': {'sources': ['discovery_test.cc']},
    'file-ipc-test': {'sources': ['file_ipc_test.cc']},
    'file-test': {'sources': ['file_test.cc'] + arrow_acero_testing_sources},
    'partition-test': {'sources': ['partition_test.cc']},
    'scanner-test': {'sources': ['scanner_test.cc']},
    'subtree-test': {'sources': ['subtree_test.cc']},
    'write-node-test': {'sources': ['write_node_test.cc']},
}

if needs_csv
    arrow_dataset_tests += {'file-csv-test': {'sources': ['file_csv_test.cc']}}
endif

if needs_json
    arrow_dataset_tests += {
        'file-json-test': {
            'sources': ['file_json_test.cc'],
            'dependencies': [rapidjson_dep],
        },
    }
endif


# requires https://github.com/apache/arrow/pull/46409
#if needs_orc
#    arrow_dataset_test_srcs += ['file_orc_test']
#endif

# requires https://github.com/apache/arrow/issues/46410
# if needs_parquet
#    arrow_dataset_test_srcs += ['file_parquet_test']
#    if needs_parquet_encryption
#        ...
#    endif
# endif

foreach key, val : arrow_dataset_tests
    exc = executable(
        'arrow-dataset-@0@'.format(key),
        sources: val['sources'] + arrow_dataset_testing_srcs,
        dependencies: [
            arrow_acero_dep,
            arrow_test_dep,
            val.get('dependencies', []),
        ],
        link_with: arrow_dataset_lib,
    )
    test('arrow-dataset-@0@'.format(key), exc)
endforeach

arrow_dataset_benchmarks = ['file_benchmark', 'scanner_benchmark']

foreach arrow_dataset_benchmark : arrow_dataset_benchmarks
    exc = executable(
        'arrow-dataset-@0@'.format(arrow_dataset_benchmark.replace('_', '-')),
        sources: '@0@.cc'.format(arrow_dataset_benchmark),
        dependencies: [arrow_acero_dep, arrow_benchmark_dep, gmock_dep],
        link_with: arrow_dataset_lib,
    )
endforeach
