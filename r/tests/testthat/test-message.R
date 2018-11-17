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

context("arrow::ipc::Message")

test_that("read_message can read from input stream", {
  batch <- record_batch(tibble::tibble(x = 1:10))
  bytes <- write_record_batch(batch, raw())
  stream <- buffer_reader(bytes)

  message <- read_message(stream)
  expect_equal(message$type(), MessageType$SCHEMA)
  expect_is(message$body, "arrow::Buffer")
  expect_is(message$metadata, "arrow::Buffer")

  message <- read_message(stream)
  expect_equal(message$type(), MessageType$RECORD_BATCH)
  expect_is(message$body, "arrow::Buffer")
  expect_is(message$metadata, "arrow::Buffer")

  message <- read_message(stream)
  expect_null(read_message(stream))
})
