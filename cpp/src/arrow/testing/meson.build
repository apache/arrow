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

# TODO: arrow_install_all_headers equivalent

if needs_tests
    tests = {
        'arrow-random-test': {'sources': ['random_test.cc']},
        'arrow-gtest-util-test': {'sources': ['gtest_util_test.cc']},
    }

    foreach key, val : tests
        exc = executable(
            key,
            sources: val['sources'],
            dependencies: [arrow_test_dep],
        )
        test(key, exc)
    endforeach

    # if needs_filesystem
    #   ...
    # endif
endif
