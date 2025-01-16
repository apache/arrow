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

if needs_tests
    tests = {
        'arrow-feather-test': {
            'sources': ['feather_test.cc'],
            'include_dir': [],
        },
        'arrow-ipc-json-simple-test': {
            'sources': ['json_simple_test.cc'],
            'include_dir': [],
        },
        'arrow-ipc-message-internal-test': {
            'sources': ['message_internal_test.cc'],
            'include_dir': [
                include_directories('../../../thirdparty/flatbuffers/include'),
            ],
        },
        'arrow-ipc-read-write-test': {
            'sources': ['read_write_test.cc'],
            'include_dir': [
                include_directories('../../../thirdparty/flatbuffers/include'),
            ],
        },
        'arrow-ipc-tensor-test': {
            'sources': ['tensor_test.cc'],
            'include_dir': [],
        },
    }

    foreach key, val : tests
        exc = executable(
            key,
            sources: val['sources'],
            include_directories: val['include_dir'],
            dependencies: [arrow_test_dep],
        )
        test(key, exc)
    endforeach
endif

# TODO: integrations and utilities
# TODO: fuzzing
