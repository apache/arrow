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

# the rest of the function is covered in test-memory-pool
test_that("arrow_info()", {
  arrow_info_output <- arrow_info()
  expect_is(arrow_info_output, "arrow_info")
  expect_output(print(arrow_info_output), "Arrow package version")
  expect_true(all(names(arrow_info_output$capabilities) %in%
                    c("s3", "snappy", "gzip", "brotli", "zstd", "lz4", "lz4_frame", "lzo", "bz2")))
  expect_true(all(names(arrow_info_output$runtime_info) %in%
                    c("simd_level", "detected_simd_level")))
  options(arrow.foo=FALSE)
  expect_output(print(arrow_info_output), "arrow.foo")
})
