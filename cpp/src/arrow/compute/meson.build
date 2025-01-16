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

arrow_compute_objlibs = []

# TODO: the CMake configuration builds test_util_internal.cc
# as an object library, but it still requires the gtest dependency
# In Meson, the gtest dependency being declared here then transists
# down to libraries that try to use this, causing all of the symbols
# to be duplicated. For now, we are just adding test_util_internal.cc
# to the appropriate sources as a workaround
'''
if needs_testing
  arrow_compute_testing_lib = static_library(
      'arrow_compute_testing',
      sources: ['test_util_internal.cc'],
      include_directories: [include_dir],
      build_by_default: false,
  )
  arrow_compute_objlibs += arrow_compute_testing_lib.extract_all_objects(recursive: true)
endif
'''
compute_test_util_srcs = ['test_util_internal.cc']

tests = {
    'arrow-internals-test': {
        'sources': [
            'function_test.cc',
            'exec_test.cc',
            'kernel_test.cc',
            'registry_test.cc',
        ],
    },
}

if needs_compute
    tests += {
        'arrow-compute-expression-test': {'sources': ['expression_test.cc']},
        'arrow-compute-row-test': {
            'sources': compute_test_util_srcs + [
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
endif

foreach key, val : tests
    exc = executable(
        key,
        sources: val['sources'],
        dependencies: [arrow_test_dep],
    )
    test(key, exc)
endforeach

# add_arrow_benchmark(function_benchmark)

subdir('kernels')
subdir('row')

