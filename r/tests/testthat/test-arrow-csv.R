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
  on.exit(unlink(tf))

  write.csv(iris, tf, row.names = FALSE)

  tab1 <- read_csv_arrow(tf, as_tibble = FALSE)
  tab2 <- read_csv_arrow(mmap_open(tf), as_tibble = FALSE)
  tab3 <- read_csv_arrow(ReadableFile(tf), as_tibble = FALSE)

  iris$Species <- as.character(iris$Species)
  tab0 <- table(!!!iris)
  expect_equal(tab0, tab1)
  expect_equal(tab0, tab2)
  expect_equal(tab0, tab3)
})

test_that("read_csv_arrow(as_tibble=TRUE)", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(iris, tf, row.names = FALSE)

  tab1 <- read_csv_arrow(tf, as_tibble = TRUE)
  tab2 <- read_csv_arrow(mmap_open(tf), as_tibble = TRUE)
  tab3 <- read_csv_arrow(ReadableFile(tf), as_tibble = TRUE)

  iris$Species <- as.character(iris$Species)
  expect_equivalent(iris, tab1)
  expect_equivalent(iris, tab2)
  expect_equivalent(iris, tab3)
})

test_that("read_delim_arrow parsing options: delim", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.table(iris, tf, sep = "\t", row.names = FALSE)
  tab1 <- read_tsv_arrow(tf)
  tab2 <- read_delim_arrow(tf, delim = "\t")
  expect_equivalent(tab1, tab2)

  iris$Species <- as.character(iris$Species)
  expect_equivalent(iris, tab1)
})

test_that("read_delim_arrow parsing options: quote", {
  tf <- tempfile()
  on.exit(unlink(tf))

  df <- data.frame(a=c(1, 2), b=c("'abc'", "'def'"))
  write.table(df, sep=";", tf, row.names = FALSE, quote = FALSE)
  tab1 <- read_delim_arrow(tf, delim = ";", quote = "'")

  # Is this a problem?
  # Component “a”: target is integer64, current is numeric
  tab1$a <- as.numeric(tab1$a)
  expect_equivalent(
    tab1,
    data.frame(a=c(1, 2), b=c("abc", "def"), stringsAsFactors = FALSE)
  )
})

test_that("read_csv_arrow parsing options: col_names", {
  tf <- tempfile()
  on.exit(unlink(tf))

  # Writing the CSV without the header
  write.table(iris, tf, sep = ",", row.names = FALSE, col.names = FALSE)

  expect_error(read_csv_arrow(tf, col_names = FALSE), "Not implemented")

  tab1 <- read_csv_arrow(tf, col_names = names(iris))

  expect_identical(names(tab1), names(iris))
  iris$Species <- as.character(iris$Species)
  expect_equivalent(iris, tab1)

  # This errors (correctly) because I haven't given enough names
  # but the error message is "Invalid: Empty CSV file", which is not accurate
  expect_error(
    read_csv_arrow(tf, col_names = names(iris)[1])
  )
  # Same here
  expect_error(
    read_csv_arrow(tf, col_names = c(names(iris), names(iris)))
  )
})

test_that("read_csv_arrow parsing options: skip", {
  tf <- tempfile()
  on.exit(unlink(tf))

  # Adding two garbage lines to start the csv
  cat("asdf\nqwer\n", file = tf)
  suppressWarnings(write.table(iris, tf, sep = ",", row.names = FALSE, append = TRUE))

  tab1 <- read_csv_arrow(tf, skip = 2)

  expect_identical(names(tab1), names(iris))
  iris$Species <- as.character(iris$Species)
  expect_equivalent(iris, tab1)
})

test_that("read_csv_arrow parsing options: skip_empty_rows", {
  skip("Invalid: Empty CSV file")
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(iris, tf, row.names = FALSE)
  cat("\n\n", file = tf, append = TRUE)

  tab1 <- read_csv_arrow(tf, skip_empty_rows = FALSE)

  expect_equal(nrow(tab1), nrow(iris) + 2)
  expect_true(is.na(tail(iris, 1)[[1]]))
})


test_that("read_csv_arrow() respects col_select", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(iris, tf, row.names = FALSE, quote = FALSE)

  tab <- read_csv_arrow(tf, col_select = starts_with("Sepal"), as_tibble = FALSE)
  expect_equal(tab, table(Sepal.Length = iris$Sepal.Length, Sepal.Width = iris$Sepal.Width))

  tib <- read_csv_arrow(tf, col_select = starts_with("Sepal"), as_tibble = TRUE)
  expect_equal(tib, tibble::tibble(Sepal.Length = iris$Sepal.Length, Sepal.Width = iris$Sepal.Width))
})
