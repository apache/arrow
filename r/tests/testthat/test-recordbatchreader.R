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

context("arrow::RecordBatch.*(Reader|Writer)")

test_that("RecordBatchStreamReader / Writer", {
  batch <- record_batch(
    x = 1:10,
    y = letters[1:10]
  )

  sink <- BufferOutputStream()
  writer <- RecordBatchStreamWriter(sink, batch$schema)
  expect_is(writer, "arrow::ipc::RecordBatchStreamWriter")
  writer$write_batch(batch)
  writer$close()

  buf <- sink$getvalue()
  expect_is(buf, "arrow::Buffer")

  reader <- RecordBatchStreamReader(buf)
  expect_is(reader, "arrow::ipc::RecordBatchStreamReader")

  batch1 <- reader$read_next_batch()
  expect_is(batch1, "arrow::RecordBatch")
  expect_equal(batch, batch1)

  expect_null(reader$read_next_batch())
})

test_that("RecordBatchFileReader / Writer", {
  batch <- record_batch(
    x = 1:10,
    y = letters[1:10]
  )

  sink <- BufferOutputStream()
  writer <- RecordBatchFileWriter(sink, batch$schema)
  expect_is(writer, "arrow::ipc::RecordBatchFileWriter")
  writer$write_batch(batch)
  writer$close()

  buf <- sink$getvalue()
  expect_is(buf, "arrow::Buffer")

  reader <- RecordBatchFileReader(buf)
  expect_is(reader, "arrow::ipc::RecordBatchFileReader")

  batch1 <- reader$get_batch(0L)
  expect_is(batch1, "arrow::RecordBatch")
  expect_equal(batch, batch1)

  expect_equal(reader$num_record_batches, 1L)
})
