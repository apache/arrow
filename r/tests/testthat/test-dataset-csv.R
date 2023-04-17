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

# Data containing a header row
tbl <- df1[, c("int", "dbl")]
header_csv_dir <- make_temp_dir()
headerless_csv_dir <- make_temp_dir()

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

  write.table(tbl, file.path(header_csv_dir, "file1.csv"), sep = ",", row.names = FALSE)
  write.table(tbl, file.path(headerless_csv_dir, "file1.csv"), sep = ",", row.names = FALSE, col.names = FALSE)
})

test_that("CSV dataset", {
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  expect_r6_class(ds$format, "CsvFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_identical(names(ds), c(names(df1), "part"))
  expect_identical(dim(ds), c(20L, 7L))

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

  # With not yet supported readr parse options
  expect_error(
    open_dataset(tsv_dir, partitioning = "part", delim = "\t", quoted_na = TRUE),
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

test_that("Can set null string values", {
  dst_dir <- make_temp_dir()
  df <- tibble(x = c(1, NA, 3))
  write_dataset(df, dst_dir, null_string = "NULL_VALUE", format = "csv")

  csv_contents <- readLines(list.files(dst_dir, full.names = TRUE)[1])
  expect_equal(csv_contents, c("\"x\"", "1", "NULL_VALUE", "3"))

  back <- open_dataset(dst_dir, null_values = "NULL_VALUE", format = "csv") %>% collect()
  expect_equal(df, back)

  # Also works with `na` parameter
  dst_dir <- make_temp_dir()
  write_dataset(df, dst_dir, na = "another_null", format = "csv")

  csv_contents <- readLines(list.files(dst_dir, full.names = TRUE)[1])
  expect_equal(csv_contents, c("\"x\"", "1", "another_null", "3"))

  back <- open_dataset(dst_dir, null_values = "another_null", format = "csv") %>% collect()
  expect_equal(df, back)
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

test_that("Column names can be inferred from schema", {
  # First row must be skipped if file has header
  ds <- open_dataset(
    header_csv_dir,
    format = "csv",
    schema = schema(int = int32(), dbl = float64()),
    skip_rows = 1
  )
  expect_equal(collect(ds), tbl)

  # If first row isn't skipped, supply user-friendly error
  ds <- open_dataset(
    header_csv_dir,
    format = "csv",
    schema = schema(int = int32(), dbl = float64())
  )

  expect_error(
    collect(ds),
    regexp = paste0(
      "If you have supplied a schema and your data contains a ",
      "header row, you should supply the argument `skip = 1` to ",
      "prevent the header being read in as data."
    )
  )

  ds <- open_dataset(
    headerless_csv_dir,
    format = "csv",
    schema = schema(int = int32(), dbl = float64())
  )
  expect_equal(ds %>% collect(), tbl)
})

test_that("Can use col_names readr parameter", {
  expected_names <- c("my_int", "my_double")
  ds <- open_dataset(
    headerless_csv_dir,
    format = "csv",
    col_names = expected_names
  )
  expect_equal(names(ds), expected_names)
  expect_equal(ds %>% collect(), set_names(tbl, expected_names))

  # WITHOUT header, makes up names
  ds <- open_dataset(
    headerless_csv_dir,
    format = "csv",
    col_names = FALSE
  )
  expect_equal(names(ds), c("f0", "f1"))
  expect_equal(ds %>% collect(), set_names(tbl, c("f0", "f1")))

  # WITH header, gets names
  ds <- open_dataset(
    header_csv_dir,
    format = "csv",
    col_names = TRUE
  )
  expect_equal(names(ds), c("int", "dbl"))
  expect_equal(ds %>% collect(), tbl)

  ds <- open_dataset(
    header_csv_dir,
    format = "csv",
    col_names = FALSE,
    skip = 1
  )
  expect_equal(names(ds), c("f0", "f1"))
  expect_equal(ds %>% collect(), set_names(tbl, c("f0", "f1")))

  expect_error(
    open_dataset(headerless_csv_dir, format = "csv", col_names = c("my_int"))
  )
})

test_that("open_dataset() deals with BOMs (byte-order-marks) correctly", {
  temp_dir <- make_temp_dir()
  writeLines("\xef\xbb\xbfa,b\n1,2\n", con = file.path(temp_dir, "file1.csv"))
  writeLines("\xef\xbb\xbfa,b\n3,4\n", con = file.path(temp_dir, "file2.csv"))

  expect_equal(
    open_dataset(temp_dir, format = "csv") %>% collect() %>% arrange(b),
    tibble(a = c(1, 3), b = c(2, 4))
  )
})

test_that("Error if read_options$column_names and schema-names differ (ARROW-14744)", {
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "file.csv")
  df <- df1[, c("int", "dbl")]
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  schema <- schema(int = int32(), dbl = float64())

  # names in column_names but not in schema
  expect_error(
    open_dataset(csv_dir, format = "csv", schema = schema, column_names = c("int", "dbl", "lgl", "chr")),
    "`lgl` and `chr` not present in `schema`"
  )

  # names in schema but not in column_names
  expect_error(
    open_dataset(csv_dir, format = "csv", schema = schema, column_names = c("int")),
    "`dbl` not present in `column_names`"
  )

  # mismatches both ways
  expect_error(
    open_dataset(csv_dir, format = "csv", schema = schema, column_names = c("these", "wont", "match")),
    "`these`, `wont`, and `match` not present in `schema`.*`int` and `dbl` not present in `column_names`"
  )

  # correct names wrong order
  expect_error(
    open_dataset(csv_dir, format = "csv", schema = schema, column_names = c("dbl", "int")),
    "`column_names` and `schema` field names match but are not in the same order"
  )
})

test_that("skip argument in open_dataset", {
  tbl <- df1[, c("int", "dbl")]

  header_csv_dir <- make_temp_dir()
  write.table(tbl, file.path(header_csv_dir, "file1.csv"), sep = ",", row.names = FALSE)

  ds <- open_dataset(
    header_csv_dir,
    format = "csv",
    schema = schema(int = int32(), dbl = float64()),
    skip = 1
  )
  expect_equal(collect(ds), tbl)
})

test_that("error message if non-schema passed in as schema to open_dataset", {
  # passing in the schema function, not an actual schema
  expect_error(
    open_dataset(csv_dir, format = "csv", schema = schema),
    regexp = "`schema` must be an object of class 'Schema' not 'function'.",
    fixed = TRUE
  )
})

test_that("CSV reading/parsing/convert options can be passed in as lists", {
  tf <- tempfile()
  on.exit(unlink(tf))

  writeLines('"x"\n"y"\nNA\nNA\n"NULL"\n\n"foo"\n', tf)

  ds1 <- open_dataset(
    tf,
    format = "csv",
    convert_options = list(null_values = c("NA", "NULL"), strings_can_be_null = TRUE),
    read_options = list(skip_rows = 1L)
  ) %>%
    collect()

  ds2 <- open_dataset(
    tf,
    format = "csv",
    convert_options = CsvConvertOptions$create(null_values = c(NA, "NA", "NULL"), strings_can_be_null = TRUE),
    read_options = CsvReadOptions$create(skip_rows = 1L)
  ) %>%
    collect()

  expect_equal(ds1, ds2)
})

test_that("open_delim_dataset params passed through to open_dataset", {
  ds <- open_delim_dataset(csv_dir, delim = ",", partitioning = "part")
  expect_r6_class(ds$format, "CsvFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_identical(names(ds), c(names(df1), "part"))
  expect_identical(dim(ds), c(20L, 7L))

  # quote
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")

  df <- data.frame(a = c(1, 2), b = c("'abc'", "'def'"))
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  ds_quote <- open_csv_dataset(dst_dir, quote = "'") %>% collect()
  expect_equal(ds_quote$b, c("abc", "def"))

  # na
  ds <- open_csv_dataset(csv_dir, partitioning = "part", na = c("", "NA", "FALSE")) %>% collect()
  expect_identical(ds$lgl, c(
    TRUE, NA, NA, TRUE, NA, TRUE, NA, NA, TRUE, NA, TRUE, NA, NA,
    TRUE, NA, TRUE, NA, NA, TRUE, NA
  ))

  # col_names and skip
  ds <- open_csv_dataset(
    csv_dir,
    partitioning = "part",
    col_names = paste0("col_", 1:6),
    skip = 1
  ) %>% collect()

  expect_named(ds, c("col_1", "col_2", "col_3", "col_4", "col_5", "col_6", "part"))
  expect_equal(nrow(ds), 20)

  # col_types
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")

  df <- data.frame(a = c(1, NA, 2), b = c("'abc'", NA, "'def'"))
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  data_schema <- schema(a = string(), b = string())
  ds_strings <- open_csv_dataset(dst_dir, col_types = data_schema)
  expect_equal(ds_strings$schema, schema(a = string(), b = string()))

  # skip_empty_rows
  tf <- tempfile()
  writeLines('"x"\n"y"\nNA\nNA\n"NULL"\n\n\n', tf)

  ds <- open_csv_dataset(tf, skip_empty_rows = FALSE) %>% collect()
  expect_equal(nrow(ds), 7)

  # convert_options
  ds <- open_csv_dataset(
    csv_dir,
    convert_options = list(null_values = c("NA", "", "FALSE"), strings_can_be_null = TRUE)
  ) %>% collect()

  expect_equal(
    ds$lgl,
    c(TRUE, NA, NA, TRUE, NA, TRUE, NA, NA, TRUE, NA, TRUE, NA, NA, TRUE, NA, TRUE, NA, NA, TRUE, NA)
  )

  # read_options
  ds <- open_csv_dataset(
    csv_dir,
    read_options = list(column_names = paste0("col_", 1:6))
  ) %>% collect()

  expect_named(ds, c("col_1", "col_2", "col_3", "col_4", "col_5", "col_6"))

  # schema
  ds <- open_csv_dataset(
    csv_dir,
    schema = schema(
      int = int64(), dbl = int64(), lgl = bool(), chr = utf8(),
      fct = utf8(), ts = timestamp(unit = "s")
    ),
    skip = 1
  ) %>% collect()

  expect_named(ds, c("int", "dbl", "lgl", "chr", "fct", "ts"))

  # timestamp_parsers
  skip("GH-33708: timestamp_parsers don't appear to be working properly")

  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")

  df <- data.frame(time = "2023-01-16 19:47:57")
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  ds <- open_csv_dataset(dst_dir, timestamp_parsers = c(TimestampParser$create(format = "%d-%m-%y"))) %>% collect()

  expect_equal(ds$time, "16-01-2023")
})
