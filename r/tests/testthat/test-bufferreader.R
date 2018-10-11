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

context("test-bufferreader")

test_that("BufferReader can be created from R objects", {
  num <- buffer_reader(numeric(13))
  int <- buffer_reader(integer(13))
  raw <- buffer_reader(raw(16))

  expect_is(num, "arrow::io::BufferReader")
  expect_is(int, "arrow::io::BufferReader")
  expect_is(raw, "arrow::io::BufferReader")

  expect_equal(num$GetSize(), 13*8)
  expect_equal(int$GetSize(), 13*4)
  expect_equal(raw$GetSize(), 16)
})

test_that("BufferReader can be created from Buffer", {
  buf <- buffer(raw(76))
  reader <- buffer_reader(buf)

  expect_is(reader, "arrow::io::BufferReader")
  expect_equal(reader$GetSize(), 76)
})
