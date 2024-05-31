# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if get_option('ARROW_BUILD_TESTS')
    io_tests = ['buffered', 'compressed', 'file', 'memory']

    foreach io_test : io_tests
        exc = executable(
            io_test,
            io_test + '_test.cc',
            include_directories: [root_dir],
            # Don't mangle local stdio.h with system
            implicit_include_directories: false,
            link_with: [arrow_lib, arrow_testing_lib],
            dependencies: [gtest_main_dep, gmock_dep],
        )
        test('arrow-io-' + io_test, exc)
    endforeach

    # TODO: HDFS test

     # TODO: benchmarks
endif
