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
        'async_test_util.h',
        'builder.h',
        'executor_util.h',
        'extension_type.h',
        'fixed_width_test_util.h',
        'future_util.h',
        'generator.h',
        'gtest_compat.h',
        'gtest_util.h',
        'matchers.h',
        'math.h',
        'process.h',
        'random.h',
        'uniform_real.h',
        'util.h',
        'visibility.h',
    ],
    subdir: 'arrow/testing',
)

testing_tests = {
    'arrow-generator-test': {'sources': ['generator_test.cc']},
    'arrow-gtest-util-test': {'sources': ['gtest_util_test.cc']},
    'arrow-random-test': {'sources': ['random_test.cc']},
}

foreach key, val : testing_tests
    exc = executable(
        key,
        sources: val['sources'],
        dependencies: [arrow_test_dep, val.get('dependencies', [])],
    )
    test(key, exc)
endforeach

if needs_tests and needs_filesystem
    arrow_filesystem_example = shared_module(
        'arrow-filesystem-example',
        sources: ['examplefs.cc'],
        dependencies: [arrow_dep, gtest_dep],
    )
else
    arrow_filesystem_example = disabler()
endif
