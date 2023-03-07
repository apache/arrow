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

set(CSV_TEST_SRCS
    chunker_test.cc
    column_builder_test.cc
    column_decoder_test.cc
    converter_test.cc
    parser_test.cc
    reader_test.cc
    writer_test.cc)

add_arrow_test(csv-test SOURCES ${CSV_TEST_SRCS})

add_arrow_benchmark(converter_benchmark PREFIX "arrow-csv")
add_arrow_benchmark(parser_benchmark PREFIX "arrow-csv")
add_arrow_benchmark(writer_benchmark PREFIX "arrow-csv")

arrow_install_all_headers("arrow/csv")

# pkg-config support
arrow_add_pkg_config("arrow-csv")
