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

tbl <- tibble::tibble(
  x = 1:10,
  y = letters[1:10]
)
batch <- record_batch(tbl)
tab <- Table$create(tbl)

test_that("RecordBatchStreamReader / Writer", {
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

test_that("reader head method edge cases", {
  batch <- record_batch(
    x = 1:10,
    y = letters[1:10]
  )
  sink <- BufferOutputStream$create()
  writer <- RecordBatchStreamWriter$create(sink, batch$schema)
  writer$write(batch)
  writer$write(batch)
  writer$close()
  buf <- sink$finish()

  reader <- RecordBatchStreamReader$create(buf)
  expect_error(head(reader, -1)) # Not (yet) supported
  expect_equal(
    Table$create(head(reader, 0)),
    Table$create(x = integer(0), y = character(0))
  )
  expect_equal(
    Table$create(head(reader, 100)),
    Table$create(batch, batch)
  )
})

test_that("RBR methods", {
  batch <- record_batch(
    x = 1:10,
    y = letters[1:10]
  )
  sink <- BufferOutputStream$create()
  writer <- RecordBatchStreamWriter$create(sink, batch$schema)
  writer$write(batch)
  writer$write(batch)
  writer$close()
  buf <- sink$finish()

  reader <- RecordBatchStreamReader$create(buf)
  expect_output(
    print(reader),
    "RecordBatchStreamReader
x: int32
y: string"
  )
  expect_equal(names(reader), c("x", "y"))
  expect_identical(dim(reader), c(NA_integer_, 2L))

  expect_equal(
    as.data.frame(reader),
    rbind(as.data.frame(batch), as.data.frame(batch))
  )
})

test_that("as_record_batch_reader() works for RecordBatchReader", {
  skip_if_not_available("dataset")

  batch <- record_batch(a = 1, b = "two")
  reader <- Scanner$create(batch)$ToRecordBatchReader()
  expect_identical(as_record_batch_reader(reader), reader)
})

test_that("as_record_batch_reader() works for Scanner", {
  skip_if_not_available("dataset")

  batch <- record_batch(a = 1, b = "two")
  scanner <- Scanner$create(batch)
  reader <- as_record_batch_reader(scanner)
  expect_equal(reader$read_next_batch(), batch)
})

test_that("as_record_batch_reader() works for Dataset", {
  skip_if_not_available("dataset")

  dataset <- InMemoryDataset$create(arrow_table(a = 1, b = "two"))
  reader <- as_record_batch_reader(dataset)
  expect_equal(
    reader$read_next_batch(),
    record_batch(a = 1, b = "two")
  )
})

test_that("as_record_batch_reader() works for Table", {
  table <- arrow_table(a = 1, b = "two")
  reader <- as_record_batch_reader(table)
  expect_equal(reader$read_next_batch(), record_batch(a = 1, b = "two"))
})

test_that("as_record_batch_reader() works for RecordBatch", {
  batch <- record_batch(a = 1, b = "two")
  reader <- as_record_batch_reader(batch)
  expect_equal(reader$read_next_batch(), batch)
})

test_that("as_record_batch_reader() works for data.frame", {
  df <- tibble::tibble(a = 1, b = "two")
  reader <- as_record_batch_reader(df)
  expect_equal(reader$read_next_batch(), record_batch(a = 1, b = "two"))
})

test_that("as_record_batch_reader() works for function", {
  batches <- list(
    record_batch(a = 1, b = "two"),
    record_batch(a = 2, b = "three")
  )

  i <- 0
  fun <- function() {
    i <<- i + 1
    if (i > length(batches)) NULL else batches[[i]]
  }

  reader <- as_record_batch_reader(fun, schema = batches[[1]]$schema)
  expect_equal(reader$read_next_batch(), batches[[1]])
  expect_equal(reader$read_next_batch(), batches[[2]])
  expect_null(reader$read_next_batch())

  # check invalid returns
  fun_bad_type <- function() "not a record batch"
  reader <- as_record_batch_reader(fun_bad_type, schema = schema())
  expect_error(
    reader$read_next_batch(),
    "Expected fun\\(\\) to return an arrow::RecordBatch"
  )

  fun_bad_schema <- function() record_batch(a = 1)
  reader <- as_record_batch_reader(fun_bad_schema, schema = schema(a = string()))
  expect_error(
    reader$read_next_batch(),
    "Expected fun\\(\\) to return batch with schema 'a: string'"
  )
})

test_that("as_record_batch_reader() errors on data.frame with NULL names", {
  df <- data.frame(a = 1, b = "two")
  names(df) <- NULL
  expect_error(as_record_batch_reader(df), "Input data frame columns must be named")
})
