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

arrow_dataset_srcs = files(
    'dataset.cc',
    'dataset_writer.cc',
    'discovery.cc',
    'file_base.cc',
    'file_ipc.cc',
    'partition.cc',
    'plan.cc',
    'projector.cc',
    'scan_node.cc',
    'scanner.cc',
)

arrow_dataset_deps = [arrow_acero_dep, arrow_compute_dep]
arrow_pkgconfig_requires = ['arrow-acero', 'arrow-compute']
if needs_csv
    arrow_dataset_srcs += ['file_csv.cc']
endif

if needs_json
    arrow_dataset_srcs += ['file_json.cc']
endif

if needs_orc
    arrow_dataset_srcs += ['file_orc.cc']
endif

if needs_parquet
    arrow_dataset_srcs += ['file_parquet.cc']
    arrow_dataset_deps += [parquet_dep]
    arrow_pkgconfig_requires += ['parquet']
endif

if needs_opentelemetry
    arrow_dataset_deps += [opentelemetry_dep]
endif

arrow_dataset_lib = library(
    'arrow_dataset',
    sources: arrow_dataset_srcs,
    dependencies: arrow_dataset_deps,
    cpp_static_args: ['-DARROW_DS_STATIC'],
    cpp_shared_args: ['-DARROW_DS_EXPORTING'],
    gnu_symbol_visibility: 'inlineshidden',
)

arrow_dataset_args = []
if get_option('default_library') == 'static'
    arrow_dataset_args += ['-DARROW_DS_STATIC']
endif

arrow_dataset_dep = declare_dependency(
    link_with: [arrow_dataset_lib],
    dependencies: arrow_dataset_deps,
    compile_args: arrow_dataset_args,
)
meson.override_dependency('arrow-dataset', arrow_dataset_dep)

pkg_config_cflags = get_option('default_library') == 'static' ? '-DARROW_DS_STATIC' : ''
pkg_config_cflags_private = get_option('default_library') != 'static' ? '-DARROW_DS_STATIC' : ''
pkg.generate(
    arrow_dataset_lib,
    filebase: 'arrow-dataset',
    name: 'Apache Arrow Dataset',
    description: 'Apache Arrow Dataset provides an API to read and write semantic datasets stored in different locations and formats.',
    extra_cflags: [pkg_config_cflags],
    requires: arrow_pkgconfig_requires,
    variables: {'Cflags.private': pkg_config_cflags_private},
)

if needs_testing
    arrow_dataset_testing_lib = static_library(
        'arrow_dataset_testing',
        sources: ['test_util_internal.cc'],
        dependencies: [arrow_dataset_dep, arrow_acero_test_dep],
    )
    arrow_dataset_test_dep = declare_dependency(
        link_with: [arrow_dataset_testing_lib],
        dependencies: [arrow_dataset_dep, arrow_acero_test_dep],
    )
else
    arrow_dataset_test_dep = disabler()
endif

dataset_tests = {
    'dataset': {'sources': ['dataset_test.cc']},
    'dataset_writer': {'sources': ['dataset_writer_test.cc']},
    'discovery': {'sources': ['discovery_test.cc']},
    'file_ipc': {'sources': ['file_ipc_test.cc']},
    'file': {'sources': ['file_test.cc']},
    'partition': {'sources': ['partition_test.cc']},
    'scanner': {'sources': ['scanner_test.cc']},
    'subtree': {'sources': ['subtree_test.cc']},
    'write_node': {'sources': ['write_node_test.cc']},
}

if needs_csv
    dataset_tests += {'file_csv': {'sources': ['file_csv_test.cc']}}
endif

if needs_json
    dataset_tests += {
        'file_json': {
            'sources': ['file_json_test.cc'],
            'dependencies': [rapidjson_dep],
        },
    }
endif

if needs_orc
    dataset_tests += {
        'file_orc': {'sources': ['file_orc_test.cc'], 'dependencies': [orc_dep]},
    }
endif

if needs_parquet
    dataset_tests += {'file_parquet': {'sources': ['file_parquet_test.cc']}}
    if needs_parquet_encryption
        dataset_tests += {
            'file_parquet_encryption': {
                'sources': [
                    'file_parquet_encryption_test.cc',
                    meson.project_source_root() / 'src/parquet/encryption/test_in_memory_kms.cc',
                ],
            },
        }
    endif
endif

foreach key, value : dataset_tests
    test_name = 'arrow-dataset-@0@'.format(key.replace('_', '-'))
    exc = executable(
        test_name,
        sources: value['sources'],
        dependencies: [arrow_dataset_test_dep, val.get('dependencies', [])],
    )
    test(test_name, exc)
endforeach

dataset_benchmarks = ['file', 'scanner']
foreach benchmark : dataset_benchmarks
    benchmark_name = f'arrow-dataset-@benchmark@-benchmark'
    exc = executable(
        benchmark_name,
        sources: [f'@benchmark@_test.cc'],
        dependencies: [
            arrow_dataset_dep,
            arrow_benchmark_dep,
            arrow_acero_test_dep,
        ],
    )
    benchmark(benchmark_name, exc)
endforeach
