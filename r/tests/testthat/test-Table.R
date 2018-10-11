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

test_that("read_table handles various input streams (ARROW-3450)", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  tab <- arrow::table(tbl)
  tf <- tempfile(); on.exit(unlink(tf))
  tab$to_file(tf)

  bytes <- tab$to_stream()
  buf_reader <- buffer_reader(bytes)

  tab1 <- read_table(tf)
  tab2 <- read_table(fs::path_abs(tf))

  readable_file <- file_open(tf); on.exit(readable_file$Close())
  tab3 <- read_table(readable_file)

  mmap_file <- mmap_open(tf); on.exit(mmap_file$Close())
  tab4 <- read_table(mmap_file)

  tab5 <- read_table(bytes)
  tab6 <- read_table(buf_reader)

  expect_equal(tab, tab1)
  expect_equal(tab, tab2)
  expect_equal(tab, tab3)
  expect_equal(tab, tab4)
  expect_equal(tab, tab5)
  expect_equal(tab, tab6)
})
