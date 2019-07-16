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

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tf2 <- fs::path_abs(tempfile())
  write_feather(tib, tf2)
  expect_true(fs::file_exists(tf2))

  tf3 <- tempfile()
  stream <- FileOutputStream(tf3)
  write_feather(tib, stream)
  stream$close()
  expect_true(fs::file_exists(tf3))

  tab1 <- read_feather(tf1)
  expect_is(tab1, "data.frame")

  tab2 <- read_feather(tf2)
  expect_is(tab2, "data.frame")

  tab3 <- read_feather(tf3)
  expect_is(tab3, "data.frame")

  # reading directly from arrow::io::MemoryMappedFile
  tab4 <- read_feather(mmap_open(tf3))
  expect_is(tab4, "data.frame")

  # reading directly from arrow::io::ReadableFile
  tab5 <- read_feather(ReadableFile(tf3))
  expect_is(tab5, "data.frame")

  expect_equal(tib, tab1)
  expect_equal(tib, tab2)
  expect_equal(tib, tab3)
  expect_equal(tib, tab4)
  expect_equal(tib, tab5)

  unlink(tf1)
  unlink(tf2)
  unlink(tf3)
})

test_that("feather handles col_select = <names>", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, col_select = c("x", "y"))
  expect_is(tab1, "data.frame")

  expect_equal(tib$x, tab1$x)
  expect_equal(tib$y, tab1$y)

  unlink(tf1)
})

test_that("feather handles col_select = <integer>", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, col_select = 1:2)
  expect_is(tab1, "data.frame")

  expect_equal(tib$x, tab1$x)
  expect_equal(tib$y, tab1$y)
  unlink(tf1)
})

test_that("feather handles col_select = <tidyselect helper>", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, col_select = everything())
  expect_identical(tib, tab1)

  tab2 <- read_feather(tf1, col_select = starts_with("x"))
  expect_identical(tab2, tib[, "x", drop = FALSE])

  tab3 <- read_feather(tf1, col_select = c(starts_with("x"), contains("y")))
  expect_identical(tab3, tib[, c("x", "y"), drop = FALSE])

  tab4 <- read_feather(tf1, col_select = -z)
  expect_identical(tab4, tib[, c("x", "y"), drop = FALSE])

  unlink(tf1)
})

test_that("feather read/write round trip", {
  tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

  tf1 <- tempfile()
  write_feather(tib, tf1)
  expect_true(fs::file_exists(tf1))

  tab1 <- read_feather(tf1, as_tibble = FALSE)
  expect_is(tab1, "arrow::Table")

  expect_equal(tib, as.data.frame(tab1))
  unlink(tf1)
})


