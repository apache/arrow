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

extension_test_srcs = ['bool8_test.cc', 'json_test.cc', 'uuid_test.cc']

if needs_json
    extension_test_srcs += ['fixed_shape_tensor_test.cc', 'opaque_test.cc']
endif

extension_exc = executable(
    key,
    sources: extension_test_srcs,
    dependencies: [arrow_test_dep],
)
test('arrow-canonical-extensions', extension_exc)

# TODO: arrow_install_all_headers
