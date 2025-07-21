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

canonical_extension_tests = ['bool8_test.cc', 'json_test.cc', 'uuid_test.cc']

if needs_json
    canonical_extension_tests += [
        'fixed_shape_tensor_test.cc',
        'opaque_test.cc',
    ]
endif

exc = executable(
    'arrow-canonical-extensions-test',
    sources: canonical_extension_tests,
    dependencies: [arrow_test_dep],
)
test('arrow-canonical-extensions-test', exc)

install_headers(
    ['bool8.h', 'fixed_shape_tensor.h', 'json.h', 'opaque.h', 'uuid.h'],
)
