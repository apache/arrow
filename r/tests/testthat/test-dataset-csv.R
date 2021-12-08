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

skip_if_not_available("dataset")

library(dplyr, warn.conflicts = FALSE)

csv_dir <- make_temp_dir()
tsv_dir <- make_temp_dir()

test_that("Setup (putting data in the dirs)", {
  dir.create(file.path(csv_dir, 5))
  dir.create(file.path(csv_dir, 6))
  write.csv(df1, file.path(csv_dir, 5, "file1.csv"), row.names = FALSE)
  write.csv(df2, file.path(csv_dir, 6, "file2.csv"), row.names = FALSE)
  expect_length(dir(csv_dir, recursive = TRUE), 2)

  # Now, tab-delimited
  dir.create(file.path(tsv_dir, 5))
  dir.create(file.path(tsv_dir, 6))
  write.table(df1, file.path(tsv_dir, 5, "file1.tsv"), row.names = FALSE, sep = "\t")
  write.table(df2, file.path(tsv_dir, 6, "file2.tsv"), row.names = FALSE, sep = "\t")
  expect_length(dir(tsv_dir, recursive = TRUE), 2)
})

test_that("CSV dataset", {
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  expect_r6_class(ds$format, "CsvFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_identical(names(ds), c(names(df1), "part"))
  if (getRversion() >= "4.0.0") {
    # CountRows segfaults on RTools35/R 3.6, so don't test it there
    expect_identical(dim(ds), c(20L, 7L))
  }
  expect_equal(
    ds %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 5) %>%
      collect() %>%
      summarize(mean = mean(as.numeric(integer))), # as.numeric bc they're being parsed as int64
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
  # Collecting virtual partition column works
  expect_equal(
    collect(ds) %>% arrange(part) %>% pull(part),
    c(rep(5, 10), rep(6, 10))
  )
})

test_that("CSV scan options", {
  options <- FragmentScanOptions$create("text")
  expect_equal(options$type, "csv")
  options <- FragmentScanOptions$create("csv",
    null_values = c("mynull"),
    strings_can_be_null = TRUE
  )
  expect_equal(options$type, "csv")

  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")
  df <- tibble(chr = c("foo", "mynull"))
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  ds <- open_dataset(dst_dir, format = "csv")
  expect_equal(ds %>% collect(), df)

  sb <- ds$NewScan()
  sb$FragmentScanOptions(options)

  tab <- sb$Finish()$ToTable()
  expect_equal(as.data.frame(tab), tibble(chr = c("foo", NA)))

  # Set default convert options in CsvFileFormat
  csv_format <- CsvFileFormat$create(
    null_values = c("mynull"),
    strings_can_be_null = TRUE
  )
  ds <- open_dataset(dst_dir, format = csv_format)
  expect_equal(ds %>% collect(), tibble(chr = c("foo", NA)))

  # Set both parse and convert options
  df <- tibble(chr = c("foo", "mynull"), chr2 = c("bar", "baz"))
  write.table(df, dst_file, row.names = FALSE, quote = FALSE, sep = "\t")
  ds <- open_dataset(dst_dir,
    format = "csv",
    delimiter = "\t",
    null_values = c("mynull"),
    strings_can_be_null = TRUE
  )
  expect_equal(ds %>% collect(), tibble(
    chr = c("foo", NA),
    chr2 = c("bar", "baz")
  ))
  expect_equal(
    ds %>%
      group_by(chr2) %>%
      summarize(na = all(is.na(chr))) %>%
      arrange(chr2) %>%
      collect(),
    tibble(
      chr2 = c("bar", "baz"),
      na = c(FALSE, TRUE)
    )
  )
})

test_that("compressed CSV dataset", {
  skip_if_not_available("gzip")
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv.gz")
  write.csv(df1, gzfile(dst_file), row.names = FALSE, quote = FALSE)
  format <- FileFormat$create("csv")
  ds <- open_dataset(dst_dir, format = format)
  expect_r6_class(ds$format, "CsvFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")

  expect_equal(
    ds %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6 & integer < 11) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
})

test_that("CSV dataset options", {
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")
  df <- tibble(chr = letters[1:10])
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  format <- FileFormat$create("csv", skip_rows = 1)
  ds <- open_dataset(dst_dir, format = format)

  expect_equal(
    ds %>%
      select(string = a) %>%
      collect(),
    df1[-1, ] %>%
      select(string = chr)
  )

  ds <- open_dataset(dst_dir, format = "csv", column_names = c("foo"))

  expect_equal(
    ds %>%
      select(string = foo) %>%
      collect(),
    tibble(string = c(c("chr"), letters[1:10]))
  )
})

test_that("Other text delimited dataset", {
  ds1 <- open_dataset(tsv_dir, partitioning = "part", format = "tsv")
  expect_equal(
    ds1 %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 5) %>%
      collect() %>%
      summarize(mean = mean(as.numeric(integer))), # as.numeric bc they're being parsed as int64
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )

  ds2 <- open_dataset(tsv_dir, partitioning = "part", format = "text", delimiter = "\t")
  expect_equal(
    ds2 %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 5) %>%
      collect() %>%
      summarize(mean = mean(as.numeric(integer))), # as.numeric bc they're being parsed as int64
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
})

test_that("readr parse options", {
  arrow_opts <- names(formals(CsvParseOptions$create))
  readr_opts <- names(formals(readr_to_csv_parse_options))

  # Arrow and readr parse options must be mutually exclusive, or else the code
  # in `csv_file_format_parse_options()` will error or behave incorrectly. A
  # failure of this test indicates that these two sets of option names are not
  # mutually exclusive.
  expect_equal(
    intersect(arrow_opts, readr_opts),
    character(0)
  )

  # With not yet supported readr parse options (ARROW-8631)
  expect_error(
    open_dataset(tsv_dir, partitioning = "part", delim = "\t", na = "\\N"),
    "supported"
  )

  # With unrecognized (garbage) parse options
  expect_error(
    open_dataset(
      tsv_dir,
      partitioning = "part",
      format = "text",
      asdfg = "\\"
    ),
    "Unrecognized"
  )

  # With both Arrow and readr parse options (disallowed)
  expect_error(
    open_dataset(
      tsv_dir,
      partitioning = "part",
      format = "text",
      quote = "\"",
      quoting = TRUE
    ),
    "either"
  )

  # With ambiguous partial option names (disallowed)
  expect_error(
    open_dataset(
      tsv_dir,
      partitioning = "part",
      format = "text",
      quo = "\"",
    ),
    "Ambiguous"
  )

  # With only readr parse options (and omitting format = "text")
  ds1 <- open_dataset(tsv_dir, partitioning = "part", delim = "\t")
  expect_equal(
    ds1 %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 5) %>%
      collect() %>%
      summarize(mean = mean(as.numeric(integer))), # as.numeric bc they're being parsed as int64
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
})

# see https://issues.apache.org/jira/browse/ARROW-12791
test_that("Error if no format specified and files are not parquet", {
  expect_error(
    open_dataset(csv_dir, partitioning = "part"),
    "Did you mean to specify a 'format' other than the default (parquet)?",
    fixed = TRUE
  )
  expect_error(
    open_dataset(csv_dir, partitioning = "part", format = "parquet"),
    "Parquet magic bytes not found"
  )
})

test_that("Column names inferred from schema for headerless CSVs (ARROW-14063)", {
  headerless_csv_dir <- make_temp_dir()
  tbl <- df1[, c("int", "dbl")]
  write.table(tbl, file.path(headerless_csv_dir, "file1.csv"), sep = ",", row.names = FALSE, col.names = FALSE)

  ds <- open_dataset(headerless_csv_dir, format = "csv", schema = schema(int = int32(), dbl = float64()))
  expect_equal(ds %>% collect(), tbl)
})

test_that("open_dataset() deals with BOMs (byte-order-marks) correctly", {
  temp_dir <- make_temp_dir()
  writeLines("\xef\xbb\xbfa,b\n1,2\n", con = file.path(temp_dir, "file1.csv"))
  writeLines("\xef\xbb\xbfa,b\n3,4\n", con = file.path(temp_dir, "file2.csv"))

  expect_equal(
    open_dataset(temp_dir, format = "csv") %>% collect(),
    tibble(a = c(1, 3), b = c(2, 4))
  )
})
