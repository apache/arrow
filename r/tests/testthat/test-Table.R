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

context("Table")

test_that("read_table handles various input streams (ARROW-3450, ARROW-3505)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  tab <- Table$create(!!!tbl)

  tf <- tempfile()
  write_arrow(tab, tf)

  bytes <- write_arrow(tab, raw())

  tab1 <- read_table(tf)
  tab2 <- read_table(fs::path_abs(tf))

  readable_file <- ReadableFile$create(tf)
  file_reader1 <- RecordBatchFileReader$create(readable_file)
  tab3 <- read_table(file_reader1)
  readable_file$close()

  mmap_file <- mmap_open(tf)
  file_reader2 <- RecordBatchFileReader$create(mmap_file)
  tab4 <- read_table(file_reader2)
  mmap_file$close()

  tab5 <- read_table(bytes)

  stream_reader <- RecordBatchStreamReader$create(bytes)
  tab6 <- read_table(stream_reader)

  file_reader <- RecordBatchFileReader$create(tf)
  tab7 <- read_table(file_reader)

  expect_equal(tab, tab1)
  expect_equal(tab, tab2)
  expect_equal(tab, tab3)
  expect_equal(tab, tab4)
  expect_equal(tab, tab5)
  expect_equal(tab, tab6)
  expect_equal(tab, tab7)

  unlink(tf)

})

test_that("Table cast (ARROW-3741)", {
  tab <- Table$create(x = 1:10, y = 1:10)

  expect_error(tab$cast(schema(x = int32())))
  expect_error(tab$cast(schema(x = int32(), z = int32())))

  s2 <- schema(x = int16(), y = int64())
  tab2 <- tab$cast(s2)
  expect_equal(tab2$schema, s2)
  expect_equal(tab2$column(0L)$type, int16())
  expect_equal(tab2$column(1L)$type, int64())
})

test_that("Table dim() and nrow() (ARROW-3816)", {
  tab <- Table$create(x = 1:10, y  = 1:10)
  expect_equal(dim(tab), c(10L, 2L))
  expect_equal(nrow(tab), 10L)
})

test_that("table() handles record batches with splicing", {
  batch <- record_batch(x = 1:2, y = letters[1:2])
  tab <- Table$create(batch, batch, batch)
  expect_equal(tab$schema, batch$schema)
  expect_equal(tab$num_rows, 6L)
  expect_equivalent(
    as.data.frame(tab),
    vctrs::vec_rbind(as.data.frame(batch), as.data.frame(batch), as.data.frame(batch))
  )

  batches <- list(batch, batch, batch)
  tab <- Table$create(!!!batches)
  expect_equal(tab$schema, batch$schema)
  expect_equal(tab$num_rows, 6L)
  expect_equivalent(
    as.data.frame(tab),
    vctrs::vec_rbind(!!!purrr::map(batches, as.data.frame))
  )
})

test_that("table() handles ... of arrays, chunked arrays, vectors", {
  a <- Array$create(1:10)
  ca <- chunked_array(1:5, 6:10)
  v <- rnorm(10)
  tbl <- tibble::tibble(x = 1:10, y = letters[1:10])

  tab <- Table$create(a = a, b = ca, c = v, !!!tbl)
  expect_equal(
    tab$schema,
    schema(a = int32(), b = int32(), c = float64(), x = int32(), y = utf8())
  )
  res <- as.data.frame(tab)
  expect_equal(names(res), c("a", "b", "c", "x", "y"))
  expect_equal(res,
    tibble::tibble(a = 1:10, b = 1:10, c = v, x = 1:10, y = letters[1:10])
  )
})

test_that("table() auto splices (ARROW-5718)", {
  df <- tibble::tibble(x = 1:10, y = letters[1:10])

  tab1 <- Table$create(df)
  tab2 <- Table$create(!!!df)
  expect_equal(tab1, tab2)
  expect_equal(tab1$schema, schema(x = int32(), y = utf8()))
  expect_equivalent(as.data.frame(tab1), df)

  s <- schema(x = float64(), y = utf8())
  tab3 <- Table$create(df, schema = s)
  tab4 <- Table$create(!!!df, schema = s)
  expect_equal(tab3, tab4)
  expect_equal(tab3$schema, s)
  expect_equivalent(as.data.frame(tab3), df)
})

