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

if needs_compute
    arrow_c_bridge_deps = [arrow_compute_test_dep]
else
    arrow_c_bridge_deps = [arrow_test_dep]
endif

exc = executable(
    'arrow-c-bridge-test',
    sources: ['bridge_test.cc'],
    dependencies: arrow_c_bridge_deps,
)
test('arrow-c-bridge-test', exc)

exc = executable(
    'arrow-dlpack-test',
    sources: ['dlpack_test.cc'],
    dependencies: [arrow_test_dep],
)
test('arrow-dlpack-test', exc)

exc = executable(
    'arrow-bridge-benchmark',
    sources: ['bridge_benchmark.cc'],
    dependencies: [arrow_benchmark_dep],
)
benchmark('arrow-bridge-benchmark', exc)

install_headers(['abi.h', 'bridge.h', 'dlpack_abi.h', 'dlpack.h', 'helpers.h'])
