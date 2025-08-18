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

# Contains utilities for working with Arrow data been stored
# in a row-major order.

install_headers(['grouper.h'], subdir: 'arrow/compute/row')

if needs_compute
    exc = executable(
        'arrow-compute-grouper-benchmark',
        sources: ['grouper_benchmark.cc'],
        dependencies: [arrow_compute_dep, arrow_benchmark_dep],
    )
endif
