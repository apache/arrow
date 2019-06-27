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

context("arrow::csv::TableReader")

test_that("Can read csv file", {
  tf <- tempfile()

  write.csv(iris, tf, row.names = FALSE, quote = FALSE)

  tab1 <- read_csv_arrow(tf, as_tibble = FALSE)
  tab2 <- read_csv_arrow(mmap_open(tf), as_tibble = FALSE)
  tab3 <- read_csv_arrow(ReadableFile(tf), as_tibble = FALSE)

  iris$Species <- as.character(iris$Species)
  tab0 <- table(!!!iris)
  expect_equal(tab0, tab1)
  expect_equal(tab0, tab2)
  expect_equal(tab0, tab3)

  unlink(tf)
})

test_that("read_csv_arrow(as_tibble=TRUE)", {
  tf <- tempfile()

  write.csv(iris, tf, row.names = FALSE, quote = FALSE)

  tab1 <- read_csv_arrow(tf, as_tibble = TRUE)
  tab2 <- read_csv_arrow(mmap_open(tf), as_tibble = TRUE)
  tab3 <- read_csv_arrow(ReadableFile(tf), as_tibble = TRUE)

  iris$Species <- as.character(iris$Species)
  expect_equivalent(iris, tab1)
  expect_equivalent(iris, tab2)
  expect_equivalent(iris, tab3)

  unlink(tf)
})

test_that("read_csv_arrow() respects col_select", {
  tf <- tempfile()

  write.csv(iris, tf, row.names = FALSE, quote = FALSE)

  tab <- read_csv_arrow(tf, col_select = starts_with("Sepal"), as_tibble = FALSE)
  expect_equal(tab, table(Sepal.Length = iris$Sepal.Length, Sepal.Width = iris$Sepal.Width))

  tib <- read_csv_arrow(tf, col_select = starts_with("Sepal"), as_tibble = TRUE)
  expect_equal(tib, tibble::tibble(Sepal.Length = iris$Sepal.Length, Sepal.Width = iris$Sepal.Width))

  unlink(tf)
})
