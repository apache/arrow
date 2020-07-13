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

context("Parquet file reading/writing")

pq_file <- system.file("v0.7.1.parquet", package="arrow")

test_that("reading a known Parquet file to tibble", {
  skip_if_not_available("snappy")
  df <- read_parquet(pq_file)
  expect_true(tibble::is_tibble(df))
  expect_identical(dim(df), c(10L, 11L))
  # TODO: assert more about the contents
})

test_that("simple int column roundtrip", {
  df <- tibble::tibble(x = 1:5)
  pq_tmp_file <- tempfile() # You can specify the .parquet here but that's probably not necessary

  write_parquet(df, pq_tmp_file)
  df_read <- read_parquet(pq_tmp_file)
  expect_equivalent(df, df_read)
  # Make sure file connection is cleaned up
  expect_error(file.remove(pq_tmp_file), NA)
  expect_false(file.exists(pq_tmp_file))
})

test_that("read_parquet() supports col_select", {
  skip_if_not_available("snappy")
  df <- read_parquet(pq_file, col_select = c(x, y, z))
  expect_equal(names(df), c("x", "y", "z"))

  df <- read_parquet(pq_file, col_select = starts_with("c"))
  expect_equal(names(df), c("carat", "cut", "color", "clarity"))
})

test_that("read_parquet() with raw data", {
  skip_if_not_available("snappy")
  test_raw <- readBin(pq_file, what = "raw", n = 5000)
  df <- read_parquet(test_raw)
  expect_identical(dim(df), c(10L, 11L))
})

test_that("write_parquet() handles various compression= specs", {
  skip_if_not_available("snappy")
  tab <- Table$create(x1 = 1:5, x2 = 1:5, y = 1:5)

  expect_parquet_roundtrip(tab, compression = "snappy")
  expect_parquet_roundtrip(tab, compression = rep("snappy", 3L))
  expect_parquet_roundtrip(tab, compression = c(x1 = "snappy", x2 = "snappy"))
})

test_that("write_parquet() handles various compression_level= specs", {
  skip_if_not_available("gzip")
  tab <- Table$create(x1 = 1:5, x2 = 1:5, y = 1:5)

  expect_parquet_roundtrip(tab, compression = "gzip", compression_level = 4)
  expect_parquet_roundtrip(tab, compression = "gzip", compression_level = rep(4L, 3L))
  expect_parquet_roundtrip(tab, compression = "gzip", compression_level = c(x1 = 5L, x2 = 3L))
})

test_that("write_parquet() handles various use_dictionary= specs", {
  tab <- Table$create(x1 = 1:5, x2 = 1:5, y = 1:5)

  expect_parquet_roundtrip(tab, use_dictionary = TRUE)
  expect_parquet_roundtrip(tab, use_dictionary = c(TRUE, FALSE, TRUE))
  expect_parquet_roundtrip(tab, use_dictionary = c(x1 = TRUE, x2 = TRUE))
  expect_error(
    write_parquet(tab, tempfile(), use_dictionary = c(TRUE, FALSE)),
    "unsupported use_dictionary= specification"
  )
  expect_error(
    write_parquet(tab, tempfile(), use_dictionary = 12),
    "is.logical(use_dictionary) is not TRUE",
    fixed = TRUE
  )
})

test_that("write_parquet() handles various write_statistics= specs", {
  tab <- Table$create(x1 = 1:5, x2 = 1:5, y = 1:5)

  expect_parquet_roundtrip(tab, write_statistics = TRUE)
  expect_parquet_roundtrip(tab, write_statistics = c(TRUE, FALSE, TRUE))
  expect_parquet_roundtrip(tab, write_statistics = c(x1 = TRUE, x2 = TRUE))
})

test_that("write_parquet() can truncate timestamps", {
  tab <- Table$create(x1 = as.POSIXct("2020/06/03 18:00:00", tz = "UTC"))
  expect_type_equal(tab$x1, timestamp("us", "UTC"))

  tf <- tempfile()
  on.exit(unlink(tf))

  write_parquet(tab, tf, coerce_timestamps = "ms", allow_truncated_timestamps = TRUE)
  new <- read_parquet(tf, as_data_frame = FALSE)
  expect_type_equal(new$x1, timestamp("ms", "UTC"))
  expect_equivalent(as.data.frame(tab), as.data.frame(new))
})

test_that("make_valid_version()", {
  expect_equal(make_valid_version("1.0"), ParquetVersionType$PARQUET_1_0)
  expect_equal(make_valid_version("2.0"), ParquetVersionType$PARQUET_2_0)

  expect_equal(make_valid_version(1), ParquetVersionType$PARQUET_1_0)
  expect_equal(make_valid_version(2), ParquetVersionType$PARQUET_2_0)

  expect_equal(make_valid_version(1.0), ParquetVersionType$PARQUET_1_0)
  expect_equal(make_valid_version(2.0), ParquetVersionType$PARQUET_2_0)
})

test_that("write_parquet() defaults to snappy compression", {
  skip_if_not_available("snappy")
  tmp1 <- tempfile()
  tmp2 <- tempfile()
  write_parquet(mtcars, tmp1)
  write_parquet(mtcars, tmp2, compression = "snappy")
  expect_equal(file.size(tmp1), file.size(tmp2))
})

test_that("Factors are preserved when writing/reading from Parquet", {
  fct <- factor(c("a", "b"), levels = c("c", "a", "b"))
  ord <- factor(c("a", "b"), levels = c("c", "a", "b"), ordered = TRUE)
  chr <- c("a", "b")
  df <- tibble::tibble(fct = fct, ord = ord, chr = chr)

  pq_tmp_file <- tempfile()
  on.exit(unlink(pq_tmp_file))

  write_parquet(df, pq_tmp_file)
  df_read <- read_parquet(pq_tmp_file)
  expect_equivalent(df, df_read)
})

test_that("Lists are preserved when writing/reading from Parquet", {
  bool <- list(logical(0), NA, c(TRUE, FALSE))
  int <- list(integer(0), NA_integer_, 1:4)
  num <- list(numeric(0), NA_real_, c(1, 2))
  char <- list(character(0), NA_character_, c("itsy", "bitsy"))
  df <- tibble::tibble(bool = bool, int = int, num = num, char = char)

  pq_tmp_file <- tempfile()
  on.exit(unlink(pq_tmp_file))

  write_parquet(df, pq_tmp_file)
  df_read <- read_parquet(pq_tmp_file)
  expect_equivalent(df, df_read)
})

test_that("write_parquet() to stream", {
  df <- tibble::tibble(x = 1:5)
  tf <- tempfile()
  con <- FileOutputStream$create(tf)
  on.exit(unlink(tf))
  write_parquet(df, con)
  con$close()
  expect_equal(read_parquet(tf), df)
})

test_that("write_parquet() returns its input", {
  df <- tibble::tibble(x = 1:5)
  tf <- tempfile()
  on.exit(unlink(tf))
  df_out <- write_parquet(df, tf)
  expect_equivalent(df, df_out)
})
