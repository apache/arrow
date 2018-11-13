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

context("Feather")

test_that("feather read/write round trip", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- local_tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tf2 <- fs::path_abs(local_tempfile())
  write_feather(tib, tf2)
  expect_true(fs::file_exists(tf2))

  tf3 <- local_tempfile()
  stream <- close_on_exit(file_output_stream(tf3))
  write_feather(tib, stream)
  expect_true(fs::file_exists(tf3))

  tab1 <- read_feather(tf1)
  expect_is(tab1, "arrow::Table")

  tab2 <- read_feather(tf2)
  expect_is(tab2, "arrow::Table")

  tab3 <- read_feather(tf3)
  expect_is(tab3, "arrow::Table")

  # reading directly from arrow::io::MemoryMappedFile
  tab4 <- read_feather(mmap_open(tf3))
  expect_is(tab4, "arrow::Table")

  # reading directly from arrow::io::ReadableFile
  tab5 <- read_feather(file_open(tf3))
  expect_is(tab5, "arrow::Table")

  expect_equal(tib, as_tibble(tab1))
  expect_equal(tib, as_tibble(tab2))
  expect_equal(tib, as_tibble(tab3))
  expect_equal(tib, as_tibble(tab4))
  expect_equal(tib, as_tibble(tab5))
})
