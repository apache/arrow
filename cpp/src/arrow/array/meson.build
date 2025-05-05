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

array_tests = ['concatenate_test', 'diff_test']
foreach array_test : array_tests
    test_name = 'arrow-@0@'.format(array_test.replace('_', '-'))
    exc = executable(
        test_name,
        sources: '@0@.cc'.format(array_test),
        dependencies: [arrow_test_dep],
    )
    test(test_name, exc)
endforeach

install_headers(
    [
        'array_base.h',
        'array_binary.h',
        'array_decimal.h',
        'array_dict.h',
        'array_nested.h',
        'array_primitive.h',
        'array_run_end.h',
        'builder_adaptive.h',
        'builder_base.h',
        'builder_binary.h',
        'builder_decimal.h',
        'builder_dict.h',
        'builder_nested.h',
        'builder_primitive.h',
        'builder_run_end.h',
        'builder_time.h',
        'builder_union.h',
        'concatenate.h',
        'data.h',
        'diff.h',
        'statistics.h',
        'util.h',
        'validate.h',
    ],
)
