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

context("arrow::RecordBatch")

test_that("RecordBatch", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10],
    fct = factor(letters[1:10])
  )
  batch <- record_batch(tbl)

  expect_true(batch == batch)
  expect_equal(
    batch$schema(),
    schema(
      int = int32(), dbl = float64(),
      lgl = boolean(), chr = utf8(),
      fct = dictionary(int32(), array(letters[1:10]))
    )
  )
  expect_equal(batch$num_columns(), 5L)
  expect_equal(batch$num_rows(), 10L)
  expect_equal(batch$column_name(0), "int")
  expect_equal(batch$column_name(1), "dbl")
  expect_equal(batch$column_name(2), "lgl")
  expect_equal(batch$column_name(3), "chr")
  expect_equal(batch$column_name(4), "fct")
  expect_equal(names(batch), c("int", "dbl", "lgl", "chr", "fct"))

  col_int <- batch$column(0)
  expect_true(inherits(col_int, 'arrow::Array'))
  expect_equal(col_int$as_vector(), tbl$int)
  expect_equal(col_int$type(), int32())

  col_dbl <- batch$column(1)
  expect_true(inherits(col_dbl, 'arrow::Array'))
  expect_equal(col_dbl$as_vector(), tbl$dbl)
  expect_equal(col_dbl$type(), float64())

  col_lgl <- batch$column(2)
  expect_true(inherits(col_dbl, 'arrow::Array'))
  expect_equal(col_lgl$as_vector(), tbl$lgl)
  expect_equal(col_lgl$type(), boolean())

  col_chr <- batch$column(3)
  expect_true(inherits(col_chr, 'arrow::Array'))
  expect_equal(col_chr$as_vector(), tbl$chr)
  expect_equal(col_chr$type(), utf8())

  col_fct <- batch$column(4)
  expect_true(inherits(col_fct, 'arrow::Array'))
  expect_equal(col_fct$as_vector(), tbl$fct)
  expect_equal(col_fct$type(), dictionary(int32(), array(letters[1:10])))


  batch2 <- batch$RemoveColumn(0)
  expect_equal(
    batch2$schema(),
    schema(dbl = float64(), lgl = boolean(), chr = utf8(), fct = dictionary(int32(), array(letters[1:10])))
  )
  expect_equal(batch2$column(0), batch$column(1))
  expect_identical(as_tibble(batch2), tbl[,-1])

  batch3 <- batch$Slice(5)
  expect_identical(as_tibble(batch3), tbl[6:10,])

  batch4 <- batch$Slice(5, 2)
  expect_identical(as_tibble(batch4), tbl[6:7,])
})

test_that("RecordBatch with 0 rows are supported", {
  tbl <- tibble::tibble(
    int = integer(),
    dbl = numeric(),
    lgl = logical(),
    chr = character(),
    fct = factor(character(), levels = c("a", "b"))
  )

  batch <- record_batch(tbl)
  expect_equal(batch$num_columns(), 5L)
  expect_equal(batch$num_rows(), 0L)
  expect_equal(
    batch$schema(),
    schema(
      int = int32(),
      dbl = float64(),
      lgl = boolean(),
      chr = utf8(),
      fct = dictionary(int32(), array(c("a", "b")))
    )
  )

  tf <- local_tempfile()
  write_record_batch(batch, tf)
  res <- read_record_batch(tf)
  expect_equal(res, batch)
})

test_that("read_record_batch handles various streams (ARROW-3450, ARROW-3505)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  batch <- record_batch(tbl)
  tf <- local_tempfile()
  write_record_batch(batch, tf)

  bytes <- write_record_batch(batch, raw())
  buf_reader <- buffer_reader(bytes)

  batch1 <- read_record_batch(tf)
  batch2 <- read_record_batch(fs::path_abs(tf))

  readable_file <- close_on_exit(file_open(tf))
  batch3 <- read_record_batch(readable_file)

  mmap_file <- close_on_exit(mmap_open(tf))
  batch4 <- read_record_batch(mmap_file)
  batch5 <- read_record_batch(bytes)
  batch6 <- read_record_batch(buf_reader)
  expect_error(read_record_batch(buf_reader))

  stream_reader <- record_batch_stream_reader(bytes)
  batch7 <- read_record_batch(stream_reader)
  expect_null(read_record_batch(stream_reader))

  file_reader <- record_batch_file_reader(tf)
  batch8 <- read_record_batch(file_reader)
  expect_null(read_record_batch(file_reader, i = 2))

  expect_equal(batch, batch1)
  expect_equal(batch, batch2)
  expect_equal(batch, batch3)
  expect_equal(batch, batch4)
  expect_equal(batch, batch5)
  expect_equal(batch, batch6)
  expect_equal(batch, batch7)
  expect_equal(batch, batch8)
})
