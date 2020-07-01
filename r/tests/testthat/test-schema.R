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

context("Schema")

test_that("Alternate type names are supported", {
  expect_equal(
    schema(b = double(), c = bool(), d = string(), e = float(), f = halffloat()),
    schema(b = float64(), c = boolean(), d = utf8(), e = float32(), f = float16())
  )
  expect_equal(names(schema(b = double(), c = bool(), d = string())), c("b", "c", "d"))
})

test_that("Schema print method", {
  expect_output(
    print(schema(b = double(), c = bool(), d = string())),
    paste(
      "Schema",
      "b: double",
      "c: bool",
      "d: string",
      sep = "\n"
    ),
    fixed = TRUE
  )
})

test_that("Schema $GetFieldByName", {
  schm <- schema(b = double(), c = string())
  expect_equal(schm$GetFieldByName("b"), field("b", double()))
  expect_null(schm$GetFieldByName("f"))
  # TODO: schema(b = double(), b = string())$GetFieldByName("b")
  # also returns NULL and probably should error bc duplicated names

  expect_equal(schm$b, field("b", double()))
  expect_equal(schm[["b"]], field("b", double()))
})

test_that("reading schema from Buffer", {
  # TODO: this uses the streaming format, i.e. from RecordBatchStreamWriter
  #       maybe there is an easier way to serialize a schema
  batch <- record_batch(x = 1:10)
  expect_is(batch, "RecordBatch")

  stream <- BufferOutputStream$create()
  writer <- RecordBatchStreamWriter$create(stream, batch$schema)
  expect_is(writer, "RecordBatchStreamWriter")
  writer$close()

  buffer <- stream$finish()
  expect_is(buffer, "Buffer")

  reader <- MessageReader$create(buffer)
  expect_is(reader, "MessageReader")

  message <- reader$ReadNextMessage()
  expect_is(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)

  stream <- BufferReader$create(buffer)
  expect_is(stream, "BufferReader")
  message <- read_message(stream)
  expect_is(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)
})

test_that("Input validation when creating a table with a schema", {
  expect_error(
    Table$create(b = 1, schema = c(b = float64())), # list not Schema
    "`schema` must be an arrow::Schema or NULL"
  )
})

test_that("Schema$Equals", {
  a <- schema(b = double(), c = bool())
  b <- a$WithMetadata(list(some="metadata"))

  # different metadata
  expect_failure(expect_equal(a, b))
  expect_false(a$Equals(b, check_metadata = TRUE))

  # Metadata not checked
  expect_equivalent(a, b)

  # Non-schema object
  expect_false(a$Equals(42))
})

test_that("unify_schemas", {
  a <- schema(b = double(), c = bool())
  z <- schema(b = double(), k = utf8())
  expect_equal(
    unify_schemas(a, z),
    schema(b = double(), c = bool(), k = utf8())
  )
})
