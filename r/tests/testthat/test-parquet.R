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

skip_if_not_available("parquet")

pq_file <- system.file("v0.7.1.parquet", package = "arrow")

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
  expect_equal(df, df_read)
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

test_that("write_parquet() accepts RecordBatch too", {
  batch <- RecordBatch$create(x1 = 1:5, x2 = 1:5, y = 1:5)
  tab <- parquet_roundtrip(batch)
  expect_equal(tab, Table$create(batch))
})

test_that("write_parquet() handles grouped_df", {
  library(dplyr, warn.conflicts = FALSE)
  df <- tibble::tibble(a = 1:4, b = 5) %>% group_by(b)
  # Since `df` is a "grouped_df", this test asserts that we get a grouped_df back
  expect_parquet_roundtrip(df, as_data_frame = TRUE)
})

test_that("write_parquet() with invalid input type", {
  bad_input <- Array$create(1:5)
  expect_snapshot_error(write_parquet(bad_input, tempfile()))
})

test_that("write_parquet() can truncate timestamps", {
  tab <- Table$create(x1 = as.POSIXct("2020/06/03 18:00:00", tz = "UTC"))
  expect_type_equal(tab$x1, timestamp("us", "UTC"))

  tf <- tempfile()
  on.exit(unlink(tf))

  write_parquet(tab, tf, coerce_timestamps = "ms", allow_truncated_timestamps = TRUE)
  new <- read_parquet(tf, as_data_frame = FALSE)
  expect_type_equal(new$x1, timestamp("ms", "UTC"))
  expect_equal(as.data.frame(tab), as.data.frame(new))
})

test_that("make_valid_parquet_version()", {
  expect_equal(
    make_valid_parquet_version("1.0"),
    ParquetVersionType$PARQUET_1_0
  )
  expect_deprecated(
    expect_equal(
      make_valid_parquet_version("2.0"),
      ParquetVersionType$PARQUET_2_0
    )
  )
  expect_equal(
    make_valid_parquet_version("2.4"),
    ParquetVersionType$PARQUET_2_4
  )
  expect_equal(
    make_valid_parquet_version("2.6"),
    ParquetVersionType$PARQUET_2_6
  )
  expect_equal(
    make_valid_parquet_version("latest"),
    ParquetVersionType$PARQUET_2_6
  )

  expect_equal(make_valid_parquet_version(1), ParquetVersionType$PARQUET_1_0)
  expect_deprecated(
    expect_equal(make_valid_parquet_version(2), ParquetVersionType$PARQUET_2_0)
  )
  expect_equal(make_valid_parquet_version(1.0), ParquetVersionType$PARQUET_1_0)
  expect_equal(make_valid_parquet_version(2.4), ParquetVersionType$PARQUET_2_4)
})

test_that("make_valid_parquet_version() input validation", {
  expect_error(
    make_valid_parquet_version("0.3.14"),
    "`version` must be one of"
  )
  expect_error(
    make_valid_parquet_version(NULL),
    "`version` must be one of"
  )
  expect_error(
    make_valid_parquet_version(c("2", "4")),
    "`version` must be one of"
  )
})

test_that("write_parquet() defaults to snappy compression", {
  skip_if_not_available("snappy")
  tmp1 <- tempfile()
  tmp2 <- tempfile()
  write_parquet(mtcars, tmp1)
  write_parquet(mtcars, tmp2, compression = "snappy")
  expect_equal(file.size(tmp1), file.size(tmp2))
})

test_that("write_parquet() does not detect compression from filename", {
  # TODO(ARROW-17221): should this be supported?
  without <- tempfile(fileext = ".parquet")
  with_gz <- tempfile(fileext = ".parquet.gz")
  write_parquet(mtcars, without)
  write_parquet(mtcars, with_gz)
  expect_equal(file.size(with_gz), file.size(without))
})

test_that("read_parquet() handles (ignores) compression in filename", {
  df <- tibble::tibble(x = 1:5)
  f <- tempfile(fileext = ".parquet.gz")
  write_parquet(df, f)
  expect_equal(read_parquet(f), df)
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
  expect_equal(df, df_read)
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
  expect_equal(df, df_read, ignore_attr = TRUE)
})

test_that("Maps are preserved when writing/reading from Parquet", {
  string_bool <- Array$create(list(data.frame(key = c("a", "b"), value = c(TRUE, FALSE), stringsAsFactors = FALSE)),
    type = map_of(utf8(), boolean())
  )
  int_struct <- Array$create(
    list(tibble::tibble(key = c(2, 4), value = data.frame(x = c(1, 2), y = c("a", "b"), stringsAsFactors = FALSE))),
    type = map_of(int64(), struct(x = int64(), y = utf8()))
  )

  df <- arrow_table(string_bool = string_bool, int_struct = int_struct)

  pq_tmp_file <- tempfile()
  on.exit(unlink(pq_tmp_file))

  write_parquet(df, pq_tmp_file)
  df_read <- read_parquet(pq_tmp_file, as_data_frame = FALSE)
  expect_equal(df, df_read, ignore_attr = TRUE)
})

test_that("read_parquet() and write_parquet() accept connection objects", {
  skip_if_not_available("snappy")

  tf <- tempfile()
  on.exit(unlink(tf))

  # make this big enough that we might expose concurrency problems,
  # but not so big that it slows down the tests
  test_tbl <- tibble::tibble(
    x = 1:1e4,
    y = vapply(x, rlang::hash, character(1), USE.NAMES = FALSE),
    z = vapply(y, rlang::hash, character(1), USE.NAMES = FALSE)
  )

  write_parquet(test_tbl, file(tf))
  expect_identical(read_parquet(tf), test_tbl)
  expect_identical(read_parquet(file(tf)), read_parquet(tf))
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
  expect_equal(df, df_out)
})

test_that("write_parquet() handles version argument", {
  df <- tibble::tibble(x = 1:5)
  tf <- tempfile()
  on.exit(unlink(tf))

  purrr::walk(list("1.0", "2.4", "2.6", "latest", 1.0, 2.4, 2.6, 1L), ~ {
    write_parquet(df, tf, version = .x)
    expect_identical(read_parquet(tf), df)
  })
  purrr::walk(list("3.0", 3.0, 3L, "A"), ~ {
    expect_error(write_parquet(df, tf, version = .x))
  })
})

test_that("ParquetFileReader raises an error for non-RandomAccessFile source", {
  skip_if_not_available("gzip")
  expect_error(
    ParquetFileReader$create(CompressedInputStream$create(pq_file)),
    'file must be a "RandomAccessFile"'
  )
})

test_that("ParquetFileWriter raises an error for non-OutputStream sink", {
  sch <- schema(a = float32())
  # ARROW-9946
  expect_error(
    ParquetFileWriter$create(schema = sch, sink = tempfile()),
    regexp = "OutputStream"
  )
})

test_that("ParquetFileReader $ReadRowGroup(s) methods", {
  tab <- Table$create(x = 1:100)
  tf <- tempfile()
  on.exit(unlink(tf))
  write_parquet(tab, tf, chunk_size = 10)

  reader <- ParquetFileReader$create(tf)
  expect_true(reader$ReadRowGroup(0) == Table$create(x = 1:10))
  expect_true(reader$ReadRowGroup(9) == Table$create(x = 91:100))
  expect_error(reader$ReadRowGroup(-1), "Some index in row_group_indices")
  expect_error(reader$ReadRowGroup(111), "Some index in row_group_indices")
  expect_error(reader$ReadRowGroup(c(1, 2)))
  expect_error(reader$ReadRowGroup("a"))

  expect_true(reader$ReadRowGroups(c(0, 1)) == Table$create(x = 1:20))
  expect_error(reader$ReadRowGroups(c(0, 1, -2))) # although it gives a weird error
  expect_error(reader$ReadRowGroups(c(0, 1, 31))) # ^^
  expect_error(reader$ReadRowGroups(c("a", "b")))

  ## -- with column_indices
  expect_true(reader$ReadRowGroup(0, 0) == Table$create(x = 1:10))
  expect_error(reader$ReadRowGroup(0, 1))

  expect_true(reader$ReadRowGroups(c(0, 1), 0) == Table$create(x = 1:20))
  expect_error(reader$ReadRowGroups(c(0, 1), 1))
})

test_that("Error messages are shown when the compression algorithm snappy is not found", {
  msg <- paste0(
    ".*",
    "you will need to reinstall arrow with additional features enabled.\nSet one of these ",
    "environment variables before installing:",
    "\n\n \\* Sys\\.setenv\\(LIBARROW_MINIMAL = \"false\"\\) ",
    "\\(for all optional features, including 'snappy'\\)",
    "\n \\* Sys\\.setenv\\(ARROW_WITH_SNAPPY = \"ON\"\\) \\(for just 'snappy')\n\n",
    "See https://arrow.apache.org/docs/r/articles/install.html for details"
  )

  if (codec_is_available("snappy")) {
    d <- read_parquet(pq_file)
    expect_s3_class(d, "data.frame")
  } else {
    expect_error(read_parquet(pq_file), msg)
  }
})

test_that("Error is created when parquet reads a feather file", {
  expect_error(
    read_parquet(test_path("golden-files/data-arrow_2.0.0_lz4.feather")),
    "Parquet magic bytes not found in footer"
  )
})

test_that("ParquetFileWrite chunk_size defaults", {
  tab <- Table$create(x = 1:101)
  tf <- tempfile()
  on.exit(unlink(tf))

  # we can alter our default cells per group
  withr::with_options(
    list(
      arrow.parquet_cells_per_group = 25
    ),
    {
      # this will be 4 chunks
      write_parquet(tab, tf)
      reader <- ParquetFileReader$create(tf)

      expect_true(reader$ReadRowGroup(0) == Table$create(x = 1:26))
      expect_true(reader$ReadRowGroup(3) == Table$create(x = 79:101))
      expect_error(reader$ReadRowGroup(4), "Some index in row_group_indices")
    }
  )

  # but we always have no more than max_chunks (even if cells_per_group is low!)
  # use a new tempfile so that windows doesn't complain about the file being over-written
  tf <- tempfile()
  on.exit(unlink(tf))

  withr::with_options(
    list(
      arrow.parquet_cells_per_group = 25,
      arrow.parquet_max_chunks = 2
    ),
    {
      # this will be 4 chunks
      write_parquet(tab, tf)
      reader <- ParquetFileReader$create(tf)

      expect_true(reader$ReadRowGroup(0) == Table$create(x = 1:51))
      expect_true(reader$ReadRowGroup(1) == Table$create(x = 52:101))
      expect_error(reader$ReadRowGroup(2), "Some index in row_group_indices")
    }
  )
})

test_that("ParquetFileWrite chunk_size calculation doesn't have integer overflow issues (ARROW-14894)", {
  expect_equal(calculate_chunk_size(31869547, 108, 2.5e8, 200), 2451504)

  # we can set the target cells per group, and it rounds appropriately
  expect_equal(calculate_chunk_size(100, 1, 25), 25)
  expect_equal(calculate_chunk_size(101, 1, 25), 26)

  # but our max_chunks is respected
  expect_equal(calculate_chunk_size(101, 1, 25, 2), 51)
})

test_that("deprecated int96 timestamp unit can be specified when reading Parquet files", {
  tf <- tempfile()
  on.exit(unlink(tf))

  table <- Table$create(
    some_datetime = as.POSIXct("2001-01-01 12:34:56.789")
  )

  write_parquet(
    table,
    tf,
    use_deprecated_int96_timestamps = TRUE
  )

  props <- ParquetArrowReaderProperties$create()
  props$set_coerce_int96_timestamp_unit(TimeUnit$MILLI)
  expect_identical(props$coerce_int96_timestamp_unit(), TimeUnit$MILLI)

  result <- read_parquet(
    tf,
    as_data_frame = FALSE,
    props = props
  )

  expect_identical(result$some_datetime$type$unit(), TimeUnit$MILLI)
  expect_true(result$some_datetime == table$some_datetime)
})

test_that("Can read parquet with nested lists and maps", {
  # Construct the path to the parquet-testing submodule. This will search:
  # * $ARROW_SOURCE_HOME/cpp/submodules/parquet-testing/data
  # * ../cpp/submodules/parquet-testing/data
  # ARROW_SOURCE_HOME is set in many of our CI setups, so that will find the files
  # the .. version should catch some (thought not all) ways of running tests locally
  base_path <- Sys.getenv("ARROW_SOURCE_HOME", "..")
  # make this a full path, at the root of the filesystem if we're using ARROW_SOURCE_HOME
  if (base_path != "..") {
    base_path <- file.path("", base_path)
  }
  parquet_test_data <- file.path(base_path, "cpp", "submodules", "parquet-testing", "data")
  skip_if_not(dir.exists(parquet_test_data) | force_tests(), "Parquet test data missing")

  pq <- read_parquet(paste0(parquet_test_data, "/nested_lists.snappy.parquet"), as_data_frame = FALSE)
  expect_type_equal(pq$a, list_of(field("element", list_of(field("element", list_of(field("element", utf8())))))))

  pq <- read_parquet(paste0(parquet_test_data, "/nested_maps.snappy.parquet"), as_data_frame = FALSE)
  expect_true(pq$a$type == map_of(utf8(), map_of(int32(), field("value", boolean(), nullable = FALSE))))
})
