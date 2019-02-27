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

context("arrow::Table")

test_that("read_table handles various input streams (ARROW-3450, ARROW-3505)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  tab <- arrow::table(tbl)

  tf <- local_tempfile()
  write_arrow(tab, tf)

  bytes <- write_arrow(tab, raw())

  tab1 <- read_table(tf)
  tab2 <- read_table(fs::path_abs(tf))

  readable_file <- close_on_exit(ReadableFile(tf))
  tab3 <- read_table(close_on_exit(RecordBatchFileReader(readable_file)))

  mmap_file <- close_on_exit(mmap_open(tf))
  tab4 <- read_table(close_on_exit(RecordBatchFileReader(mmap_file)))

  tab5 <- read_table(bytes)

  stream_reader <- RecordBatchStreamReader(bytes)
  tab6 <- read_table(stream_reader)

  file_reader <- RecordBatchFileReader(tf)
  tab7 <- read_table(file_reader)

  expect_equal(tab, tab1)
  expect_equal(tab, tab2)
  expect_equal(tab, tab3)
  expect_equal(tab, tab4)
  expect_equal(tab, tab5)
  expect_equal(tab, tab6)
  expect_equal(tab, tab7)
})

test_that("Table cast (ARROW-3741)", {
  tab <- table(tibble::tibble(x = 1:10, y  = 1:10))

  expect_error(tab$cast(schema(x = int32())))
  expect_error(tab$cast(schema(x = int32(), z = int32())))

  s2 <- schema(x = int16(), y = int64())
  tab2 <- tab$cast(s2)
  expect_equal(tab2$schema, s2)
  expect_equal(tab2$column(0L)$type, int16())
  expect_equal(tab2$column(1L)$type, int64())
})

test_that("Table dim() and nrow() (ARROW-3816)", {
  tab <- table(tibble::tibble(x = 1:10, y  = 1:10))
  expect_equal(dim(tab), c(10L, 2L))
  expect_equal(nrow(tab), 10L)
})
