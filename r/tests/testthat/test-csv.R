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

context("CsvTableReader")

# Not all types round trip via CSV 100% identical by default
tbl <- example_data[, c("dbl", "lgl", "false", "chr")]

test_that("Can read csv file", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE)

  tab0 <- Table$create(tbl)
  tab1 <- read_csv_arrow(tf, as_data_frame = FALSE)
  expect_equal(tab0, tab1)
  tab2 <- read_csv_arrow(mmap_open(tf), as_data_frame = FALSE)
  expect_equal(tab0, tab2)
  tab3 <- read_csv_arrow(ReadableFile$create(tf), as_data_frame = FALSE)
  expect_equal(tab0, tab3)
})

test_that("read_csv_arrow(as_data_frame=TRUE)", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE)
  tab1 <- read_csv_arrow(tf, as_data_frame = TRUE)
  expect_equivalent(tbl, tab1)
})

test_that("read_delim_arrow parsing options: delim", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.table(tbl, tf, sep = "\t", row.names = FALSE)
  tab1 <- read_tsv_arrow(tf)
  tab2 <- read_delim_arrow(tf, delim = "\t")
  expect_equivalent(tab1, tab2)
  expect_equivalent(tbl, tab1)
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
  write.table(tbl, tf, sep = ",", row.names = FALSE, col.names = FALSE)

  # Reading with col_names = FALSE autogenerates names
  no_names <- read_csv_arrow(tf, col_names = FALSE)
  expect_equal(no_names$f0, tbl[[1]])

  tab1 <- read_csv_arrow(tf, col_names = names(tbl))

  expect_identical(names(tab1), names(tbl))
  expect_equivalent(tbl, tab1)

  # This errors (correctly) because I haven't given enough names
  # but the error message is "Invalid: Empty CSV file", which is not accurate
  expect_error(
    read_csv_arrow(tf, col_names = names(tbl)[1])
  )
  # Same here
  expect_error(
    read_csv_arrow(tf, col_names = c(names(tbl), names(tbl)))
  )
})

test_that("read_csv_arrow parsing options: skip", {
  tf <- tempfile()
  on.exit(unlink(tf))

  # Adding two garbage lines to start the csv
  cat("asdf\nqwer\n", file = tf)
  suppressWarnings(write.table(tbl, tf, sep = ",", row.names = FALSE, append = TRUE))

  tab1 <- read_csv_arrow(tf, skip = 2)

  expect_identical(names(tab1), names(tbl))
  expect_equivalent(tbl, tab1)
})

test_that("read_csv_arrow parsing options: skip_empty_rows", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE)
  cat("\n\n", file = tf, append = TRUE)

  tab1 <- read_csv_arrow(tf, skip_empty_rows = FALSE)

  expect_equal(nrow(tab1), nrow(tbl) + 2)
  expect_true(is.na(tail(tab1, 1)[[1]]))
})

test_that("read_csv_arrow parsing options: na strings", {
  tf <- tempfile()
  on.exit(unlink(tf))

  df <- data.frame(
    a = c(1.2, NA, NA, 3.4),
    b = c(NA, "B", "C", NA),
    stringsAsFactors = FALSE
  )
  write.csv(df, tf, row.names=FALSE)
  expect_equal(grep("NA", readLines(tf)), 2:5)

  tab1 <- read_csv_arrow(tf)
  expect_equal(is.na(tab1$a), is.na(df$a))
  expect_equal(is.na(tab1$b), is.na(df$b))

  unlink(tf) # Delete and write to the same file name again

  write.csv(df, tf, row.names=FALSE, na = "asdf")
  expect_equal(grep("asdf", readLines(tf)), 2:5)

  tab2 <- read_csv_arrow(tf, na = "asdf")
  expect_equal(is.na(tab2$a), is.na(df$a))
  expect_equal(is.na(tab2$b), is.na(df$b))
})

test_that("read_csv_arrow() respects col_select", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write.csv(tbl, tf, row.names = FALSE, quote = FALSE)

  tab <- read_csv_arrow(tf, col_select = ends_with("l"), as_data_frame = FALSE)
  expect_equal(tab, Table$create(example_data[, c("dbl", "lgl")]))

  tib <- read_csv_arrow(tf, col_select = ends_with("l"), as_data_frame = TRUE)
  expect_equal(tib, example_data[, c("dbl", "lgl")])
})

test_that("read_csv_arrow() can detect compression from file name", {
  skip_if_not_available("gzip")
  tf <- tempfile(fileext = ".csv.gz")
  on.exit(unlink(tf))

  write.csv(tbl, gzfile(tf), row.names = FALSE, quote = FALSE)
  tab1 <- read_csv_arrow(tf)
  expect_equivalent(tbl, tab1)
})
