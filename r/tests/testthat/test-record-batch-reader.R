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


test_that("RecordBatchStreamReader / Writer", {
  tbl <- tibble::tibble(
    x = 1:10,
    y = letters[1:10]
  )
  batch <- record_batch(tbl)
  tab <- Table$create(tbl)

  sink <- BufferOutputStream$create()
  expect_equal(sink$tell(), 0)
  writer <- RecordBatchStreamWriter$create(sink, batch$schema)
  expect_r6_class(writer, "RecordBatchWriter")
  writer$write(batch)
  writer$write(tab)
  writer$write(tbl)
  expect_true(sink$tell() > 0)
  writer$close()

  buf <- sink$finish()
  expect_r6_class(buf, "Buffer")

  reader <- RecordBatchStreamReader$create(buf)
  expect_r6_class(reader, "RecordBatchStreamReader")

  batch1 <- reader$read_next_batch()
  expect_r6_class(batch1, "RecordBatch")
  expect_equal(batch, batch1)
  batch2 <- reader$read_next_batch()
  expect_r6_class(batch2, "RecordBatch")
  expect_equal(batch, batch2)
  batch3 <- reader$read_next_batch()
  expect_r6_class(batch3, "RecordBatch")
  expect_equal(batch, batch3)
  expect_null(reader$read_next_batch())
})

test_that("RecordBatchFileReader / Writer", {
  sink <- BufferOutputStream$create()
  writer <- RecordBatchFileWriter$create(sink, batch$schema)
  expect_r6_class(writer, "RecordBatchWriter")
  writer$write(batch)
  writer$write(tab)
  writer$write(tbl)
  writer$close()

  buf <- sink$finish()
  expect_r6_class(buf, "Buffer")

  reader <- RecordBatchFileReader$create(buf)
  expect_r6_class(reader, "RecordBatchFileReader")

  batch1 <- reader$get_batch(0)
  expect_r6_class(batch1, "RecordBatch")
  expect_equal(batch, batch1)

  expect_equal(reader$num_record_batches, 3)
})

test_that("StreamReader read_table", {
  sink <- BufferOutputStream$create()
  writer <- RecordBatchStreamWriter$create(sink, batch$schema)
  expect_r6_class(writer, "RecordBatchWriter")
  writer$write(batch)
  writer$write(tab)
  writer$write(tbl)
  writer$close()
  buf <- sink$finish()

  reader <- RecordBatchStreamReader$create(buf)
  out <- reader$read_table()
  expect_identical(dim(out), c(30L, 2L))
})

test_that("FileReader read_table", {
  sink <- BufferOutputStream$create()
  writer <- RecordBatchFileWriter$create(sink, batch$schema)
  expect_r6_class(writer, "RecordBatchWriter")
  writer$write(batch)
  writer$write(tab)
  writer$write(tbl)
  writer$close()
  buf <- sink$finish()

  reader <- RecordBatchFileReader$create(buf)
  out <- reader$read_table()
  expect_identical(dim(out), c(30L, 2L))
})

test_that("MetadataFormat", {
  expect_identical(get_ipc_metadata_version(5), 4L)
  expect_identical(get_ipc_metadata_version("V4"), 3L)
  expect_identical(get_ipc_metadata_version(NULL), 4L)
  Sys.setenv(ARROW_PRE_0_15_IPC_FORMAT = 1)
  expect_identical(get_ipc_metadata_version(NULL), 3L)
  Sys.setenv(ARROW_PRE_0_15_IPC_FORMAT = "")

  expect_identical(get_ipc_metadata_version(NULL), 4L)
  Sys.setenv(ARROW_PRE_1_0_METADATA_VERSION = 1)
  expect_identical(get_ipc_metadata_version(NULL), 3L)
  Sys.setenv(ARROW_PRE_1_0_METADATA_VERSION = "")

  expect_error(
    get_ipc_metadata_version(99),
    "99 is not a valid IPC MetadataVersion"
  )
  expect_error(
    get_ipc_metadata_version("45"),
    '"45" is not a valid IPC MetadataVersion'
  )
})

test_that("reader with 0 batches", {
  # IPC stream containing only a schema (ARROW-10642)
  sink <- BufferOutputStream$create()
  writer <- RecordBatchStreamWriter$create(sink, schema(a = int32()))
  writer$close()
  buf <- sink$finish()

  reader <- RecordBatchStreamReader$create(buf)
  tab <- reader$read_table()
  expect_r6_class(tab, "Table")
  expect_identical(dim(tab), c(0L, 1L))
})
