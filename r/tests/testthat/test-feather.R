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

feather_file <- tempfile()
tib <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])

test_that("Write a feather file", {
  tib_out <- write_feather(tib, feather_file)
  expect_true(file.exists(feather_file))
  # Input is returned unmodified
  expect_identical(tib_out, tib)
})

test_that("write_ipc_file() returns its input", {
  tib_out <- write_ipc_file(tib, feather_file)
  expect_true(file.exists(feather_file))
  # Input is returned unmodified
  expect_identical(tib_out, tib)
})

expect_feather_roundtrip <- function(write_fun) {
  tf2 <- normalizePath(tempfile(), mustWork = FALSE)
  tf3 <- tempfile()
  on.exit({
    unlink(tf2)
    unlink(tf3)
  })

  # Write two ways. These are what varies with each run
  write_fun(tib, tf2)
  expect_true(file.exists(tf2))

  stream <- FileOutputStream$create(tf3)
  write_fun(tib, stream)
  stream$close()
  expect_true(file.exists(tf3))

  # Read both back
  tab2 <- read_feather(tf2)
  expect_s3_class(tab2, "data.frame")

  tab3 <- read_feather(tf3)
  expect_s3_class(tab3, "data.frame")

  # reading directly from arrow::io::MemoryMappedFile
  tab4 <- read_feather(mmap_open(tf3))
  expect_s3_class(tab4, "data.frame")

  # reading directly from arrow::io::ReadableFile
  tab5 <- read_feather(ReadableFile$create(tf3))
  expect_s3_class(tab5, "data.frame")

  expect_equal(tib, tab2)
  expect_equal(tib, tab3)
  expect_equal(tib, tab4)
  expect_equal(tib, tab5)
}

test_that("feather read/write round trip", {
  expect_feather_roundtrip(function(x, f) write_feather(x, f, version = 1))
  expect_feather_roundtrip(function(x, f) write_feather(x, f, version = 2))
  expect_feather_roundtrip(function(x, f) write_feather(x, f, version = 2, compression = TRUE))
  expect_feather_roundtrip(function(x, f) write_feather(x, f, version = 2, compression = "uncompressed"))
  expect_feather_roundtrip(function(x, f) write_feather(x, f, version = 2, compression = FALSE))
  expect_feather_roundtrip(function(x, f) write_ipc_file(x, f))
  expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, compression = TRUE))
  expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, compression = "uncompressed"))
  expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, compression = FALSE))
  expect_feather_roundtrip(function(x, f) write_feather(x, f, chunk_size = 32))
  expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, chunk_size = 32))
  if (codec_is_available("lz4")) {
    expect_feather_roundtrip(function(x, f) write_feather(x, f, compression = "lz4"))
    expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, compression = "lz4"))
  }
  if (codec_is_available("zstd")) {
    expect_feather_roundtrip(function(x, f) write_feather(x, f, compression = "zstd"))
    expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, compression = "zstd"))
    expect_feather_roundtrip(function(x, f) write_feather(x, f, compression = "zstd", compression_level = 3))
    expect_feather_roundtrip(function(x, f) write_ipc_file(x, f, compression = "zstd", compression_level = 3))
  }

  # Write from Arrow data structures
  expect_feather_roundtrip(function(x, f) write_feather(RecordBatch$create(x), f))
  expect_feather_roundtrip(function(x, f) write_ipc_file(RecordBatch$create(x), f))
  expect_feather_roundtrip(function(x, f) write_feather(Table$create(x), f))
  expect_feather_roundtrip(function(x, f) write_ipc_file(Table$create(x), f))
})

test_that("write_feather option error handling", {
  tf <- tempfile()
  expect_false(file.exists(tf))
  expect_error(
    write_feather(tib, tf, version = 1, chunk_size = 1024),
    "Feather version 1 does not support the 'chunk_size' option"
  )
  expect_error(
    write_feather(tib, tf, version = 1, compression = "lz4"),
    "Feather version 1 does not support the 'compression' option"
  )
  expect_error(
    write_feather(tib, tf, version = 1, compression_level = 1024),
    "Feather version 1 does not support the 'compression_level' option"
  )
  expect_error(
    write_feather(tib, tf, compression_level = 1024),
    "Can only specify a 'compression_level' when 'compression' is 'zstd'"
  )
  expect_match_arg_error(write_feather(tib, tf, compression = "bz2"))
  expect_false(file.exists(tf))
})

test_that("write_ipc_file option error handling", {
  tf <- tempfile()
  expect_false(file.exists(tf))
  expect_error(
    write_ipc_file(tib, tf, version = 1),
    "unused argument \\(version = 1\\)"
  )
  expect_error(
    write_ipc_file(tib, tf, compression_level = 1024),
    "Can only specify a 'compression_level' when 'compression' is 'zstd'"
  )
  expect_match_arg_error(write_ipc_file(tib, tf, compression = "bz2"))
  expect_false(file.exists(tf))
})

test_that("write_feather with invalid input type", {
  bad_input <- Array$create(1:5)
  expect_snapshot_error(write_feather(bad_input, feather_file))
})

test_that("read_feather supports col_select = <names>", {
  tab1 <- read_feather(feather_file, col_select = c("x", "y"))
  expect_s3_class(tab1, "data.frame")

  expect_equal(tib$x, tab1$x)
  expect_equal(tib$y, tab1$y)
})

test_that("feather handles col_select = <integer>", {
  tab1 <- read_feather(feather_file, col_select = 1:2)
  expect_s3_class(tab1, "data.frame")

  expect_equal(tib$x, tab1$x)
  expect_equal(tib$y, tab1$y)
})

test_that("feather handles col_select = <tidyselect helper>", {
  tab1 <- read_feather(feather_file, col_select = everything())
  expect_identical(tib, tab1)

  tab2 <- read_feather(feather_file, col_select = starts_with("x"))
  expect_identical(tab2, tib[, "x", drop = FALSE])

  tab3 <- read_feather(feather_file, col_select = c(starts_with("x"), contains("y")))
  expect_identical(tab3, tib[, c("x", "y"), drop = FALSE])

  tab4 <- read_feather(feather_file, col_select = -z)
  expect_identical(tab4, tib[, c("x", "y"), drop = FALSE])
})

test_that("feather read/write round trip", {
  tab1 <- read_feather(feather_file, as_data_frame = FALSE)
  expect_r6_class(tab1, "Table")

  expect_equal_data_frame(tib, tab1)
})

test_that("Read feather from raw vector", {
  test_raw <- readBin(feather_file, what = "raw", n = 5000)
  df <- read_feather(test_raw)
  expect_s3_class(df, "data.frame")
})

test_that("FeatherReader", {
  v1 <- tempfile()
  v2 <- tempfile()
  on.exit({
    unlink(v1)
    unlink(v2)
  })
  write_feather(tib, v1, version = 1)
  write_feather(tib, v2)
  f1 <- make_readable_file(v1)
  reader1 <- FeatherReader$create(f1)
  f1$close()
  expect_identical(reader1$version, 1L)
  f2 <- make_readable_file(v2)
  reader2 <- FeatherReader$create(f2)
  expect_identical(reader2$version, 2L)
  f2$close()
})

test_that("read_feather requires RandomAccessFile and errors nicely otherwise (ARROW-8615)", {
  skip_if_not_available("gzip")
  expect_error(
    read_feather(CompressedInputStream$create(feather_file)),
    'file must be a "RandomAccessFile"'
  )
})

test_that("write_feather() does not detect compression from filename", {
  # TODO(ARROW-17221): should this be supported?
  without <- tempfile(fileext = ".arrow")
  with_zst <- tempfile(fileext = ".arrow.zst")
  write_feather(mtcars, without)
  write_feather(mtcars, with_zst)
  expect_equal(file.size(without), file.size(with_zst))
})

test_that("read_feather() handles (ignores) compression in filename", {
  df <- tibble::tibble(x = 1:5)
  f <- tempfile(fileext = ".parquet.zst")
  write_feather(df, f)
  expect_equal(read_feather(f), df)
})

test_that("read_feather() and write_feather() accept connection objects", {
  skip_if_not(CanRunWithCapturedR())

  tf <- tempfile()
  on.exit(unlink(tf))

  # make this big enough that we might expose concurrency problems,
  # but not so big that it slows down the tests
  test_tbl <- tibble::tibble(
    x = 1:1e4,
    y = vapply(x, rlang::hash, character(1), USE.NAMES = FALSE),
    z = vapply(y, rlang::hash, character(1), USE.NAMES = FALSE)
  )

  write_feather(test_tbl, file(tf))
  expect_identical(read_feather(tf), test_tbl)
  expect_identical(read_feather(file(tf)), read_feather(tf))
})

test_that("read_feather closes connection to file", {
  tf <- tempfile()
  on.exit(unlink(tf))
  write_feather(tib, sink = tf)
  expect_true(file.exists(tf))
  read_feather(tf)
  expect_error(file.remove(tf), NA)
  expect_false(file.exists(tf))
})

test_that("Character vectors > 2GB can write to feather", {
  skip_on_cran()
  skip_if_not_running_large_memory_tests()
  df <- tibble::tibble(big = make_big_string())
  tf <- tempfile()
  on.exit(unlink(tf))
  write_feather(df, tf)
  expect_identical(read_feather(tf), df)
})

test_that("FeatherReader methods", {
  # Setup a feather file to use in the test
  feather_temp <- tempfile()
  on.exit({
    unlink(feather_temp)
  })
  write_feather(tib, feather_temp)
  feather_temp_RA <- make_readable_file(feather_temp)

  reader <- FeatherReader$create(feather_temp_RA)
  feather_temp_RA$close()

  # column_names
  expect_identical(
    reader$column_names,
    c("x", "y", "z")
  )

  # print method
  expect_identical(
    capture.output(print(reader)),
    # TODO: can we get rows/columns?
    c("FeatherReader:", "Schema", "x: int32", "y: double", "z: string")
  )
})

unlink(feather_file)

ft_file <- test_path("golden-files/data-arrow_2.0.0_lz4.feather")

test_that("Error messages are shown when the compression algorithm lz4 is not found", {
  msg <- paste0(
    ".*",
    "you will need to reinstall arrow with additional features enabled.\nSet one of ",
    "these environment variables before installing:",
    "\n\n \\* Sys\\.setenv\\(LIBARROW_MINIMAL = \"false\"\\) .*",
    "\\(for all optional features, including 'lz4'\\)",
    "\n \\* Sys\\.setenv\\(ARROW_WITH_LZ4 = \"ON\"\\) \\(for just 'lz4'\\)",
    "\n\nSee https://arrow.apache.org/docs/r/articles/install.html for details"
  )

  if (codec_is_available("lz4")) {
    d <- read_feather(ft_file)
    expect_s3_class(d, "data.frame")
  } else {
    expect_error(read_feather(ft_file), msg)
  }
})

test_that("Error is created when feather reads a parquet file", {
  expect_error(
    read_feather(system.file("v0.7.1.parquet", package = "arrow")),
    "Not a Feather V1 or Arrow IPC file"
  )
})

test_that("The read_ipc_file function is an alias of read_feather", {
  expect_identical(read_ipc_file, read_feather)
})

test_that("Can read Feather files from a URL", {
  skip_if_offline()
  skip_on_cran()
  feather_url <- "https://github.com/apache/arrow-testing/raw/master/data/arrow-ipc-stream/integration/1.0.0-littleendian/generated_datetime.arrow_file" # nolint
  fu <- read_feather(feather_url)
  expect_true(tibble::is_tibble(fu))
  expect_identical(dim(fu), c(17L, 15L))
})
