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

# Meson does not allow you to glob for headers to install. See also
# https://mesonbuild.com/FAQ.html#why-cant-i-specify-target-files-with-a-wildcard
# install_subdir would be usable if the directory only contained headers

install_headers(
    [
        'api.h',
        'api_scalar.h',
        'api_vector.h',
        'cast.h',
        'exec.h',
        'expression.h',
        'function.h',
        'function_options.h',
        'kernel.h',
        'ordering.h',
        'registry.h',
        'type_fwd.h',
        'util.h',
    ],
    subdir: 'arrow/compute',
)

if needs_compute
    pkg.generate(
        filebase: 'arrow-compute',
        name: 'Apache Arrow Compute Kernels',
        description: 'Apache Arrow\'s Compute Kernels',
        requires: ['arrow'],
    )
endif

# Define arrow_compute_core_testing object library for common test files requiring
# only core compute. No extra kernels are required.
if needs_testing
    arrow_compute_core_test_lib = library(
        'arrow-compute-core-testing',
        sources: files('test_util_internal.cc'),
        dependencies: arrow_test_dep,
    )
    arrow_compute_core_test_dep = declare_dependency(
        link_with: arrow_compute_core_test_lib,
    )
else
    arrow_compute_core_test_dep = disabler()
endif

# Define arrow_compute_testing object library for test files requiring extra kernels.
if needs_testing and needs_compute
    arrow_compute_testing_lib = library(
        'arrow-compute-testing',
        sources: files('test_env.cc'),
        dependencies: [
            arrow_compute_dep,
            arrow_compute_core_test_dep,
            arrow_test_dep_no_main,
        ],
    )
    arrow_compute_test_dep = declare_dependency(
        link_with: arrow_compute_testing_lib,
        dependencies: [
            arrow_compute_dep,
            arrow_compute_core_test_dep,
            arrow_test_dep_no_main,
        ],
    )
else
    arrow_compute_test_dep = disabler()
endif

exc = executable(
    'arrow-internals-test',
    sources: [
        'function_test.cc',
        'exec_test.cc',
        'kernel_test.cc',
        'registry_test.cc',
    ],
    dependencies: [arrow_compute_core_test_dep, arrow_test_dep],
)
test('arrow-internals-test', exc)

compute_tests = {
    'arrow-compute-expression-test': {'sources': ['expression_test.cc']},
    'arrow-compute-row-test': {
        'sources': [
            'key_hash_test.cc',
            'light_array_test.cc',
            'row/compare_test.cc',
            'row/grouper_test.cc',
            'row/row_encoder_internal_test.cc',
            'row/row_test.cc',
            'util_internal_test.cc',
        ],
    },
}

# This will only add the test if ARROW_COMPUTE is enabled, meaning the full kernel registry is available.
#
# The following kernels are always present in default builds:
#  - array_filter
#  - array_take
#  - cast
#  - dictionary_encode
#  - drop_null
#  - filter
#  - indices_nonzero
#  - take
#  - unique
#  - value_counts
#
# Also see: GH-34388, GH-34615
foreach key, val : compute_tests
    exc = executable(
        key,
        sources: val['sources'],
        dependencies: [arrow_compute_test_dep],
    )
    test(key, exc)
endforeach

exc = executable(
    'arrow-compute-function-benchmark',
    sources: ['function_benchmark.cc'],
    dependencies: [arrow_benchmark_dep],
)
benchmark('arrow-compute-function-benchmark', exc)

subdir('kernels')

subdir('row')
