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

context("read_record_batch()")

test_that("RecordBatchFileWriter / RecordBatchFileReader roundtrips", {
  tab <- table(tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  ))
  tf <- local_tempfile()

  writer <- RecordBatchFileWriter(tf, tab$schema)
  expect_is(writer, "arrow::ipc::RecordBatchFileWriter")
  writer$write_table(tab)
  writer$close()
  tab2 <- read_table(tf)
  expect_equal(tab, tab2)

  stream <- FileOutputStream(tf)
  writer <- RecordBatchFileWriter(stream, tab$schema)
  expect_is(writer, "arrow::ipc::RecordBatchFileWriter")
  writer$write_table(tab)
  writer$close()
  tab3 <- read_table(tf)
  expect_equal(tab, tab3)
})

test_that("read_record_batch() handles (raw|Buffer|InputStream, Schema) (ARROW-3450, ARROW-3505)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  batch <- record_batch(tbl)
  schema <- batch$schema

  raw <- batch$serialize()
  batch2 <- read_record_batch(raw, schema)
  batch3 <- read_record_batch(buffer(raw), schema)
  batch4 <- read_record_batch(close_on_exit(BufferReader(raw)), schema)

  expect_equal(batch, batch2)
  expect_equal(batch, batch3)
  expect_equal(batch, batch4)
})

test_that("read_record_batch() can handle (Message, Schema) parameters (ARROW-3499)", {
  batch <- record_batch(tibble::tibble(x = 1:10))
  schema <- batch$schema

  raw <- batch$serialize()
  stream <- close_on_exit(BufferReader(raw))

  message <- read_message(stream)
  batch2 <- read_record_batch(message, schema)
  expect_equal(batch, batch2)
})
