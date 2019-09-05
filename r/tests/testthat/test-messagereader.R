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

context("MessageReader")

test_that("MessageReader can be created from raw vectors", {
  batch <- record_batch(x = 1:10)
  bytes <- batch$serialize()

  reader <- MessageReader$create(bytes)

  message <- reader$ReadNextMessage()
  expect_is(message, "Message")
  expect_equal(message$type, MessageType$RECORD_BATCH)
  expect_is(message$body, "Buffer")
  expect_is(message$metadata, "Buffer")

  message <- reader$ReadNextMessage()
  expect_null(message)

  schema <- schema(x = int32())
  bytes <- schema$serialize()

  reader <- MessageReader$create(bytes)

  message <- reader$ReadNextMessage()
  expect_is(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)
  expect_is(message$body, "Buffer")
  expect_is(message$metadata, "Buffer")

  message <- reader$ReadNextMessage()
  expect_null(message)
})

test_that("MessageReader can be created from input stream", {
  batch <- record_batch(x = 1:10)
  bytes <- batch$serialize()

  stream <- BufferReader$create(bytes)
  expect_is(stream, "BufferReader")

  reader <- MessageReader$create(stream)
  expect_is(reader, "MessageReader")

  message <- reader$ReadNextMessage()
  expect_is(message, "Message")
  expect_equal(message$type, MessageType$RECORD_BATCH)
  expect_is(message$body, "Buffer")
  expect_is(message$metadata, "Buffer")

  message <- reader$ReadNextMessage()
  expect_null(message)

  schema <- schema(x = int32())
  bytes <- schema$serialize()

  stream <- BufferReader$create(bytes)
  expect_is(stream, "BufferReader")

  reader <- MessageReader$create(stream)
  expect_is(reader, "MessageReader")

  message <- reader$ReadNextMessage()
  expect_is(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)
  expect_is(message$body, "Buffer")
  expect_is(message$metadata, "Buffer")

  message <- reader$ReadNextMessage()
  expect_null(message)
})
