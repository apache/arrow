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

context("Dataset")

library(dplyr)

dataset_dir <- make_temp_dir()
hive_dir <- make_temp_dir()
ipc_dir <- make_temp_dir()
csv_dir <- make_temp_dir()
tsv_dir <- make_temp_dir()

skip_if_multithreading_disabled <- function() {
  is_32bit <- .Machine$sizeof.pointer < 8
  is_old_r <- getRversion() < "4.0.0"
  is_windows <- tolower(Sys.info()[["sysname"]]) == "windows"
  if (is_32bit && is_old_r && is_windows) {
    skip("Multithreading does not work properly on this system")
  }
}


first_date <- lubridate::ymd_hms("2015-04-29 03:12:39")
df1 <- tibble(
  int = 1:10,
  dbl = as.numeric(1:10),
  lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
  chr = letters[1:10],
  fct = factor(LETTERS[1:10]),
  ts = first_date + lubridate::days(1:10)
)

second_date <- lubridate::ymd_hms("2017-03-09 07:01:02")
df2 <- tibble(
  int = 101:110,
  dbl = c(as.numeric(51:59), NaN),
  lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
  chr = letters[10:1],
  fct = factor(LETTERS[10:1]),
  ts = second_date + lubridate::days(10:1)
)

test_that("Setup (putting data in the dir)", {
  if (arrow_with_parquet()) {
    dir.create(file.path(dataset_dir, 1))
    dir.create(file.path(dataset_dir, 2))
    write_parquet(df1, file.path(dataset_dir, 1, "file1.parquet"))
    write_parquet(df2, file.path(dataset_dir, 2, "file2.parquet"))
    expect_length(dir(dataset_dir, recursive = TRUE), 2)

    dir.create(file.path(hive_dir, "subdir", "group=1", "other=xxx"), recursive = TRUE)
    dir.create(file.path(hive_dir, "subdir", "group=2", "other=yyy"), recursive = TRUE)
    write_parquet(df1, file.path(hive_dir, "subdir", "group=1", "other=xxx", "file1.parquet"))
    write_parquet(df2, file.path(hive_dir, "subdir", "group=2", "other=yyy", "file2.parquet"))
    expect_length(dir(hive_dir, recursive = TRUE), 2)
  }

  # Now, an IPC format dataset
  dir.create(file.path(ipc_dir, 3))
  dir.create(file.path(ipc_dir, 4))
  write_feather(df1, file.path(ipc_dir, 3, "file1.arrow"))
  write_feather(df2, file.path(ipc_dir, 4, "file2.arrow"))
  expect_length(dir(ipc_dir, recursive = TRUE), 2)

  # Now, CSV
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

if (arrow_with_parquet()) {
  files <- c(
    file.path(dataset_dir, 1, "file1.parquet", fsep = "/"),
    file.path(dataset_dir, 2, "file2.parquet", fsep = "/")
  )
}

test_that("Simple interface for datasets", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_r6_class(ds$format, "ParquetFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_r6_class(ds, "Dataset")
  expect_equivalent(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>% # Testing the auto-casting of scalars
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )

  expect_equivalent(
    ds %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 1) %>% # 6 not 6L to test autocasting
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )

  # Collecting virtual partition column works
  expect_equal(
    collect(ds) %>% pull(part),
    c(rep(1, 10), rep(2, 10))
  )
})

test_that("dim method returns the correct number of rows and columns", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_identical(dim(ds), c(20L, 7L))
})


test_that("dim() correctly determine numbers of rows and columns on arrow_dplyr_query object", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))

  expect_identical(
    ds %>%
      filter(chr == 'a') %>%
      dim(),
    c(2L, 7L)
  )
  expect_equal(
    ds %>%
      select(chr, fct, int) %>%
      dim(),
    c(20L, 3L)
  )
  expect_identical(
    ds %>%
      select(chr, fct, int) %>%
      filter(chr == 'a') %>%
      dim(),
    c(2L, 3L)
  )
})

test_that("dataset from single local file path", {
  skip_on_os("windows")
  skip_if_not_available("parquet")
  ds <- open_dataset(files[1])
  expect_is(ds, "Dataset")
  expect_equivalent(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7) %>%
      collect() %>%
      arrange(dbl),
    df1[8:10, c("chr", "dbl")]
  )
})

test_that("dataset from vector of file paths", {
  skip_on_os("windows")
  skip_if_not_available("parquet")
  ds <- open_dataset(files)
  expect_is(ds, "Dataset")
  expect_equivalent(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )
})

test_that("dataset from directory URI", {
  skip_on_os("windows")
  skip_if_not_available("parquet")
  uri <- paste0("file://", dataset_dir)
  ds <- open_dataset(uri, partitioning = schema(part = uint8()))
  expect_r6_class(ds, "Dataset")
  expect_equivalent(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )
})

test_that("dataset from single file URI", {
  skip_on_os("windows")
  skip_if_not_available("parquet")
  uri <- paste0("file://", files[1])
  ds <- open_dataset(uri)
  expect_is(ds, "Dataset")
  expect_equivalent(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7) %>%
      collect() %>%
      arrange(dbl),
    df1[8:10, c("chr", "dbl")]
  )
})

test_that("dataset from vector of file URIs", {
  skip_on_os("windows")
  skip_if_not_available("parquet")
  uris <- paste0("file://", files)
  ds <- open_dataset(uris)
  expect_is(ds, "Dataset")
  expect_equivalent(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )
})

test_that("open_dataset errors on mixed paths and URIs", {
  skip_on_os("windows")
  skip_if_not_available("parquet")
  expect_error(
    open_dataset(c(files[1], paste0("file://", files[2]))),
    "Vectors of mixed paths and URIs are not supported"
  )
})

test_that("Simple interface for datasets (custom ParquetFileFormat)", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()),
                     format = FileFormat$create("parquet", dict_columns = c("chr")))
  expect_type_equal(ds$schema$GetFieldByName("chr")$type, dictionary())
})

test_that("Hive partitioning", {
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir, partitioning = hive_partition(other = utf8(), group = uint8()))
  expect_r6_class(ds, "Dataset")
  expect_equivalent(
    ds %>%
      filter(group == 2) %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53) %>%
      collect() %>%
      arrange(dbl),
    df2[1:2, c("chr", "dbl")]
  )
})

test_that("input validation", {
  skip_if_not_available("parquet")
  expect_error(
    open_dataset(hive_dir, hive_partition(other = utf8(), group = uint8()))
  )
})

test_that("Partitioning inference", {
  skip_if_not_available("parquet")
  # These are the same tests as above, just using the *PartitioningFactory
  ds1 <- open_dataset(dataset_dir, partitioning = "part")
  expect_identical(names(ds1), c(names(df1), "part"))
  expect_equivalent(
    ds1 %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 1) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )

  ds2 <- open_dataset(hive_dir)
  expect_identical(names(ds2), c(names(df1), "group", "other"))
  expect_equivalent(
    ds2 %>%
      filter(group == 2) %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53) %>%
      collect() %>%
      arrange(dbl),
    df2[1:2, c("chr", "dbl")]
  )
})

test_that("IPC/Feather format data", {
  ds <- open_dataset(ipc_dir, partitioning = "part", format = "feather")
  expect_r6_class(ds$format, "IpcFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_identical(names(ds), c(names(df1), "part"))
  expect_identical(dim(ds), c(20L, 7L))

  expect_equivalent(
    ds %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 3) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )

  # Collecting virtual partition column works
  expect_equal(
    collect(ds) %>% pull(part),
    c(rep(3, 10), rep(4, 10))
  )
})

test_that("CSV dataset", {
  skip_if_multithreading_disabled()
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  expect_r6_class(ds$format, "CsvFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_identical(names(ds), c(names(df1), "part"))
  if (getRversion() >= "4.0.0") {
    # CountRows segfaults on RTools35/R 3.6, so don't test it there
    expect_identical(dim(ds), c(20L, 7L))
  }
  expect_equivalent(
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
    collect(ds) %>% pull(part),
    c(rep(5, 10), rep(6, 10))
  )
})

test_that("CSV scan options", {
  skip_if_multithreading_disabled()
  options <- FragmentScanOptions$create("text")
  expect_equal(options$type, "csv")
  options <- FragmentScanOptions$create("csv",
                                        null_values = c("mynull"),
                                        strings_can_be_null = TRUE)
  expect_equal(options$type, "csv")

  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")
  df <- tibble(chr = c("foo", "mynull"))
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  ds <- open_dataset(dst_dir, format = "csv")
  expect_equivalent(ds %>% collect(), df)

  sb <- ds$NewScan()
  sb$FragmentScanOptions(options)

  tab <- sb$Finish()$ToTable()
  expect_equivalent(as.data.frame(tab), tibble(chr = c("foo", NA)))

  # Set default convert options in CsvFileFormat
  csv_format <- CsvFileFormat$create(null_values = c("mynull"),
                                     strings_can_be_null = TRUE)
  ds <- open_dataset(dst_dir, format = csv_format)
  expect_equivalent(ds %>% collect(), tibble(chr = c("foo", NA)))

  # Set both parse and convert options
  df <- tibble(chr = c("foo", "mynull"), chr2 = c("bar", "baz"))
  write.table(df, dst_file, row.names = FALSE, quote = FALSE, sep = "\t")
  ds <- open_dataset(dst_dir, format = "csv",
                     delimiter="\t",
                     null_values = c("mynull"),
                     strings_can_be_null = TRUE)
  expect_equivalent(ds %>% collect(), tibble(chr = c("foo", NA),
                                             chr2 = c("bar", "baz")))
})

test_that("compressed CSV dataset", {
  skip_if_multithreading_disabled()
  skip_if_not_available("gzip")
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv.gz")
  write.csv(df1, gzfile(dst_file), row.names = FALSE, quote = FALSE)
  format <- FileFormat$create("csv")
  ds <- open_dataset(dst_dir, format = format)
  expect_r6_class(ds$format, "CsvFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")

  expect_equivalent(
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
  skip_if_multithreading_disabled()
  dst_dir <- make_temp_dir()
  dst_file <- file.path(dst_dir, "data.csv")
  df <- tibble(chr = letters[1:10])
  write.csv(df, dst_file, row.names = FALSE, quote = FALSE)

  format <- FileFormat$create("csv", skip_rows = 1)
  ds <- open_dataset(dst_dir, format = format)

  expect_equivalent(
    ds %>%
      select(string = a) %>%
      collect(),
    df1[-1,] %>%
      select(string = chr)
  )

  ds <- open_dataset(dst_dir, format = "csv", column_names = c("foo"))

  expect_equivalent(
    ds %>%
      select(string = foo) %>%
      collect(),
    tibble(foo = c(c('chr'), letters[1:10]))
  )
})

test_that("Other text delimited dataset", {
  skip_if_multithreading_disabled()
  ds1 <- open_dataset(tsv_dir, partitioning = "part", format = "tsv")
  expect_equivalent(
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
  expect_equivalent(
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
  skip_if_multithreading_disabled()
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
  expect_equivalent(
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

test_that("Dataset with multiple file formats", {
  skip("https://issues.apache.org/jira/browse/ARROW-7653")
  skip_if_not_available("parquet")
  ds <- open_dataset(list(
    open_dataset(dataset_dir, format = "parquet", partitioning = "part"),
    open_dataset(ipc_dir, format = "arrow", partitioning = "part")
  ))
  expect_identical(names(ds), c(names(df1), "part"))
  expect_equivalent(
    ds %>%
      filter(int > 6 & part %in% c(1, 3)) %>%
      select(string = chr, integer = int) %>%
      collect(),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      rbind(., .) # Stack it twice
  )
})

test_that("Creating UnionDataset", {
  skip_if_not_available("parquet")
  ds1 <- open_dataset(file.path(dataset_dir, 1))
  ds2 <- open_dataset(file.path(dataset_dir, 2))
  union1 <- open_dataset(list(ds1, ds2))
  expect_r6_class(union1, "UnionDataset")
  expect_equivalent(
    union1 %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>% # Testing the auto-casting of scalars
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )

  # Now with the c() method
  union2 <- c(ds1, ds2)
  expect_r6_class(union2, "UnionDataset")
  expect_equivalent(
    union2 %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>% # Testing the auto-casting of scalars
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )

  # Confirm c() method error handling
  expect_error(c(ds1, 42), "character")
})

test_that("map_batches", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = "part")
  expect_equivalent(
    ds %>%
      filter(int > 5) %>%
      select(int, lgl) %>%
      map_batches(~summarize(., min_int = min(int))),
    tibble(min_int = c(6L, 101L))
  )
})

test_that("partitioning = NULL to ignore partition information (but why?)", {
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir, partitioning = NULL)
  expect_identical(names(ds), names(df1)) # i.e. not c(names(df1), "group", "other")
})

test_that("filter() with is.nan()", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_equivalent(
    ds %>%
      select(part, dbl) %>%
      filter(!is.nan(dbl), part == 2) %>%
      collect(),
    tibble(part = 2L, dbl = df2$dbl[!is.nan(df2$dbl)])
  )
})

test_that("filter() with %in%", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_equivalent(
    ds %>%
      select(int, part) %>%
      filter(int %in% c(6, 4, 3, 103, 107), part == 1) %>%
      collect(),
    tibble(int = df1$int[c(3, 4, 6)], part = 1)
  )

  # ARROW-9606: bug in %in% filter on partition column with >1 partition columns
  ds <- open_dataset(hive_dir)
  expect_equivalent(
    ds %>%
      filter(group %in% 2) %>%
      select(names(df2)) %>%
      collect(),
    df2
  )
})

test_that("filter() on timestamp columns", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_equivalent(
    ds %>%
      filter(ts >= lubridate::ymd_hms("2015-05-04 03:12:39")) %>%
      filter(part == 1) %>%
      select(ts) %>%
      collect(),
    df1[5:10, c("ts")],
  )

  # Now with Date
  expect_equivalent(
    ds %>%
      filter(ts >= as.Date("2015-05-04")) %>%
      filter(part == 1) %>%
      select(ts) %>%
      collect(),
    df1[5:10, c("ts")],
  )

  # Now with bare string date
  skip("Implement more aggressive implicit casting for scalars (ARROW-11402)")
  expect_equivalent(
    ds %>%
      filter(ts >= "2015-05-04") %>%
      filter(part == 1) %>%
      select(ts) %>%
      collect(),
    df1[5:10, c("ts")],
  )
})

test_that("filter() on date32 columns", {
  skip_if_not_available("parquet")
  tmp <- tempfile()
  dir.create(tmp)
  df <- data.frame(date = as.Date(c("2020-02-02", "2020-02-03")))
  write_parquet(df, file.path(tmp, "file.parquet"))

  expect_equal(
    open_dataset(tmp) %>%
      filter(date > as.Date("2020-02-02")) %>%
      collect() %>%
      nrow(),
    1L
  )

  # Also with timestamp scalar
  expect_equal(
    open_dataset(tmp) %>%
      filter(date > lubridate::ymd_hms("2020-02-02 00:00:00")) %>%
      collect() %>%
      nrow(),
    1L
  )
})


test_that("mutate()", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  mutated <- ds %>%
    select(chr, dbl, int) %>%
    filter(dbl * 2 > 14 & dbl - 50 < 3L) %>%
    mutate(twice = int * 2)
  expect_output(
    print(mutated),
"FileSystemDataset (query)
chr: string
dbl: double
int: int32
twice: double (multiply_checked(int, 2))

* Filter: ((multiply_checked(dbl, 2) > 14) and (subtract_checked(dbl, 50) < 3))
See $.data for the source Arrow object",
    fixed = TRUE
  )
  expect_equivalent(
    mutated %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl", "int")],
      df2[1:2, c("chr", "dbl", "int")]
    ) %>%
      mutate(
        twice = int * 2
      )
  )
})

test_that("mutate() features not yet implemented", {
  expect_error(
    ds %>%
      group_by(int) %>%
      mutate(avg = mean(int)),
    "mutate() on grouped data not supported in Arrow\nCall collect() first to pull data into R.",
    fixed = TRUE
  )
})

test_that("filter scalar validation doesn't crash (ARROW-7772)", {
  expect_error(
    ds %>%
      filter(int == "fff", part == 1) %>%
      collect(),
    "equal has no kernel matching input types .array.int32., scalar.string.."
  )
})

test_that("collect() on Dataset works (if fits in memory)", {
  skip_if_not_available("parquet")
  expect_equal(
    collect(open_dataset(dataset_dir)),
    rbind(df1, df2)
  )
})

test_that("count()", {
  skip_if_not_available("parquet")
  skip("count() is not a generic so we have to get here through summarize()")
  ds <- open_dataset(dataset_dir)
  df <- rbind(df1, df2)
  expect_equal(
    ds %>%
      filter(int > 6, int < 108) %>%
      count(chr),
    df %>%
      filter(int > 6, int < 108) %>%
      count(chr)
  )
})

test_that("arrange()", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  arranged <- ds %>%
    select(chr, dbl, int) %>%
    filter(dbl * 2 > 14 & dbl - 50 < 3L) %>%
    mutate(twice = int * 2) %>%
    arrange(chr, desc(twice), dbl + int)
  expect_output(
    print(arranged),
    "FileSystemDataset (query)
chr: string
dbl: double
int: int32
twice: double (multiply_checked(int, 2))

* Filter: ((multiply_checked(dbl, 2) > 14) and (subtract_checked(dbl, 50) < 3))
* Sorted by chr [asc], multiply_checked(int, 2) [desc], add_checked(dbl, int) [asc]
See $.data for the source Arrow object",
    fixed = TRUE
  )
  expect_equivalent(
    arranged %>%
      collect(),
    rbind(
      df1[8, c("chr", "dbl", "int")],
      df2[2, c("chr", "dbl", "int")],
      df1[9, c("chr", "dbl", "int")],
      df2[1, c("chr", "dbl", "int")],
      df1[10, c("chr", "dbl", "int")]
    ) %>%
      mutate(
        twice = int * 2
      )
  )
})

test_that("compute()/collect(as_data_frame=FALSE)", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir)

  tab1 <- ds %>% compute()
  expect_is(tab1, "Table")

  tab2 <- ds %>% collect(as_data_frame = FALSE)
  expect_is(tab2, "Table")

  tab3 <-  ds %>%
    mutate(negint = -int) %>%
    filter(negint > - 100) %>%
    arrange(chr) %>%
    select(negint) %>%
    compute()

  expect_is(tab3, "Table")

  expect_equal(
    tab3 %>% collect(),
    tibble(negint = -1:-10)
  )

  tab4 <-  ds %>%
    mutate(negint = -int) %>%
    filter(negint > - 100) %>%
    arrange(chr) %>%
    select(negint) %>%
    collect(as_data_frame = FALSE)

  expect_is(tab3, "Table")

  expect_equal(
    tab4 %>% collect(),
    tibble(negint = -1:-10)
  )

  tab5 <- ds %>%
    mutate(negint = -int) %>%
    group_by(fct) %>%
    compute()

  # the group_by() prevents compute() from returning a Table...
  expect_is(tab5, "arrow_dplyr_query")

  # ... but $.data is a Table (InMemoryDataset)...
  expect_r6_class(tab5$.data, "InMemoryDataset")
  # ... and the mutate() was evaluated
  expect_true("negint" %in% names(tab5$.data))

})

test_that("head/tail", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir)
  expect_equal(as.data.frame(head(ds)), head(df1))
  expect_equal(
    as.data.frame(head(ds, 12)),
    rbind(df1, df2[1:2,])
  )
  expect_equal(
    ds %>%
      filter(int > 6) %>%
      head() %>%
      as.data.frame(),
    rbind(df1[7:10,], df2[1:2,])
  )

  expect_equal(as.data.frame(tail(ds)), tail(df2))
  expect_equal(
    as.data.frame(tail(ds, 12)),
    rbind(df1[9:10,], df2)
  )
  expect_equal(
    ds %>%
      filter(int < 105) %>%
      tail() %>%
      as.data.frame(),
    rbind(df1[9:10,], df2[1:4,])
  )
})

test_that("Dataset [ (take by index)", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir)
  # Taking only from one file
  expect_equal(
    as.data.frame(ds[c(4, 5, 9), 3:4]),
    df1[c(4, 5, 9), 3:4]
  )
  # Taking from more than one
  expect_equal(
    as.data.frame(ds[c(4, 5, 9, 12, 13), 3:4]),
    rbind(df1[c(4, 5, 9), 3:4], df2[2:3, 3:4])
  )
  # Taking out of order
  expect_equal(
    as.data.frame(ds[c(4, 13, 9, 12, 5), ]),
    rbind(
      df1[4, ],
      df2[3, ],
      df1[9, ],
      df2[2, ],
      df1[5, ]
    )
  )

  # Take from a query
  ds2 <- ds %>%
    filter(int > 6) %>%
    select(int, lgl)
  expect_equal(
    as.data.frame(ds2[c(2, 5), ]),
    rbind(
      df1[8, c("int", "lgl")],
      df2[1, c("int", "lgl")]
    )
  )
})

test_that("dplyr method not implemented messages", {
  skip_if_not_available("parquet")
  ds <- open_dataset(dataset_dir)
  # This one is more nuanced
  expect_error(
    ds %>% filter(int > 6, dbl > max(dbl)),
    "Filter expression not supported for Arrow Datasets: dbl > max(dbl)\nCall collect() first to pull data into R.",
    fixed = TRUE
  )
  # One explicit test of the full message
  expect_error(
    ds %>% summarize(mean(int)),
    "summarize() is not currently implemented for Arrow Datasets. Call collect() first to pull data into R.",
    fixed = TRUE
  )
  # Helper for everything else
  expect_not_implemented <- function(x) {
    expect_error(x, "is not currently implemented for Arrow Datasets")
  }
  expect_not_implemented(ds %>% filter(int == 1) %>% summarize(n()))
})

test_that("Dataset and query print methods", {
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir)
  expect_output(
    print(ds),
    paste(
      "FileSystemDataset with 2 Parquet files",
      "int: int32",
      "dbl: double",
      "lgl: bool",
      "chr: string",
      "fct: dictionary<values=string, indices=int32>",
      "ts: timestamp[us, tz=UTC]",
      "group: int32",
      "other: string",
      sep = "\n"
    ),
    fixed = TRUE
  )
  expect_type(ds$metadata, "list")
  q <- select(ds, string = chr, lgl, integer = int)
  expect_output(
    print(q),
    paste(
      "Dataset (query)",
      "string: string",
      "lgl: bool",
      "integer: int32",
      "",
      "See $.data for the source Arrow object",
      sep = "\n"
    ),
    fixed = TRUE
  )
  expect_output(
    print(q %>% filter(integer == 6) %>% group_by(lgl)),
    paste(
      "Dataset (query)",
      "string: string",
      "lgl: bool",
      "integer: int32",
      "",
      "* Filter: (int == 6)",
      "* Grouped by lgl",
      "See $.data for the source Arrow object",
      sep = "\n"
    ),
    fixed = TRUE
  )
})

test_that("Scanner$ScanBatches", {
  ds <- open_dataset(ipc_dir, format = "feather")
  batches <- ds$NewScan()$Finish()$ScanBatches()
  table <- Table$create(!!!batches)
  expect_equivalent(as.data.frame(table), rbind(df1, df2))

  # use_async will always use the thread pool (even if it only uses
  # one thread) and RTools 3.5 on Windows doesn't support this
  skip_on_os("windows")
  batches <- ds$NewScan()$UseAsync(TRUE)$Finish()$ScanBatches()
  table <- Table$create(!!!batches)
  expect_equivalent(as.data.frame(table), rbind(df1, df2))
})

test_that("Scanner$ToRecordBatchReader()", {
  ds <- open_dataset(dataset_dir, partitioning = "part")
  scan <- ds %>%
    filter(part == 1) %>%
    select(int, lgl) %>%
    filter(int > 6) %>%
    Scanner$create()
  reader <- scan$ToRecordBatchReader()
  expect_r6_class(reader, "RecordBatchReader")
  expect_identical(
    as.data.frame(reader$read_table()),
    df1[df1$int > 6, c("int", "lgl")]
  )
})

expect_scan_result <- function(ds, schm) {
  sb <- ds$NewScan()
  expect_r6_class(sb, "ScannerBuilder")
  expect_equal(sb$schema, schm)

  sb$Project(c("chr", "lgl"))
  sb$Filter(Expression$field_ref("dbl") == 8)
  scn <- sb$Finish()
  expect_r6_class(scn, "Scanner")

  tab <- scn$ToTable()
  expect_r6_class(tab, "Table")

  expect_equivalent(
    as.data.frame(tab),
    df1[8, c("chr", "lgl")]
  )
}

test_that("Assembling a Dataset manually and getting a Table", {
  skip_if_not_available("parquet")
  fs <- LocalFileSystem$create()
  selector <- FileSelector$create(dataset_dir, recursive = TRUE)
  partitioning <- DirectoryPartitioning$create(schema(part = double()))

  fmt <- FileFormat$create("parquet")
  factory <- FileSystemDatasetFactory$create(fs, selector, NULL, fmt, partitioning = partitioning)
  expect_r6_class(factory, "FileSystemDatasetFactory")

  schm <- factory$Inspect()
  expect_r6_class(schm, "Schema")

  phys_schm <- ParquetFileReader$create(files[1])$GetSchema()
  expect_equal(names(phys_schm), names(df1))
  expect_equal(names(schm), c(names(phys_schm), "part"))

  child <- factory$Finish(schm)
  expect_r6_class(child, "FileSystemDataset")
  expect_r6_class(child$schema, "Schema")
  expect_r6_class(child$format, "ParquetFileFormat")
  expect_equal(names(schm), names(child$schema))
  expect_equivalent(child$files, files)

  ds <- Dataset$create(list(child), schm)
  expect_scan_result(ds, schm)
})

test_that("URI-decoding with directory partitioning", {
  root <- make_temp_dir()
  fmt <- FileFormat$create("feather")
  fs <- LocalFileSystem$create()
  selector <- FileSelector$create(root, recursive = TRUE)
  dir1 <- file.path(root, "2021-05-04 00%3A00%3A00", "%24")
  dir.create(dir1, recursive = TRUE)
  write_feather(df1, file.path(dir1, "data.feather"))

  partitioning <- DirectoryPartitioning$create(
    schema(date = timestamp(unit = "s"), string = utf8()))
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning = partitioning)
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_scan_result(ds, schm)

  partitioning <- DirectoryPartitioning$create(
    schema(date = timestamp(unit = "s"), string = utf8()),
    segment_encoding = "none")
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning = partitioning)
  schm <- factory$Inspect()
  expect_error(factory$Finish(schm), "Invalid: error parsing")

  partitioning_factory <- DirectoryPartitioningFactory$create(
    c("date", "string"))
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory)
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  # Can't directly inspect partition expressions, so do it implicitly via scan
  expect_equal(
    ds %>%
      filter(date == "2021-05-04 00:00:00", string == "$") %>%
      select(int) %>%
      collect(),
    df1 %>% select(int) %>% collect()
  )

  partitioning_factory <- DirectoryPartitioningFactory$create(
    c("date", "string"), segment_encoding = "none")
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory)
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_equal(
    ds %>%
      filter(date == "2021-05-04 00%3A00%3A00", string == "%24") %>%
      select(int) %>%
      collect(),
    df1 %>% select(int) %>% collect()
  )
})

test_that("URI-decoding with hive partitioning", {
  root <- make_temp_dir()
  fmt <- FileFormat$create("feather")
  fs <- LocalFileSystem$create()
  selector <- FileSelector$create(root, recursive = TRUE)
  dir1 <- file.path(root, "date=2021-05-04 00%3A00%3A00", "string=%24")
  dir.create(dir1, recursive = TRUE)
  write_feather(df1, file.path(dir1, "data.feather"))

  partitioning <- hive_partition(
    date = timestamp(unit = "s"), string = utf8())
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning = partitioning)
  ds <- factory$Finish(schm)
  expect_scan_result(ds, schm)

  partitioning <- hive_partition(
    date = timestamp(unit = "s"), string = utf8(), segment_encoding = "none")
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning = partitioning)
  expect_error(factory$Finish(schm), "Invalid: error parsing")

  partitioning_factory <- hive_partition()
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory)
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  # Can't directly inspect partition expressions, so do it implicitly via scan
  expect_equal(
    ds %>%
      filter(date == "2021-05-04 00:00:00", string == "$") %>%
      select(int) %>%
      collect(),
    df1 %>% select(int) %>% collect()
  )

  partitioning_factory <- hive_partition(segment_encoding = "none")
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory)
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_equal(
    ds %>%
      filter(date == "2021-05-04 00%3A00%3A00", string == "%24") %>%
      select(int) %>%
      collect(),
    df1 %>% select(int) %>% collect()
  )
})

test_that("Assembling multiple DatasetFactories with DatasetFactory", {
  skip_if_not_available("parquet")
  factory1 <- dataset_factory(file.path(dataset_dir, 1), format = "parquet")
  expect_r6_class(factory1, "FileSystemDatasetFactory")
  factory2 <- dataset_factory(file.path(dataset_dir, 2), format = "parquet")
  expect_r6_class(factory2, "FileSystemDatasetFactory")

  factory <- DatasetFactory$create(list(factory1, factory2))
  expect_r6_class(factory, "DatasetFactory")

  schm <- factory$Inspect()
  expect_r6_class(schm, "Schema")

  phys_schm <- ParquetFileReader$create(files[1])$GetSchema()
  expect_equal(names(phys_schm), names(df1))

  ds <- factory$Finish(schm)
  expect_r6_class(ds, "UnionDataset")
  expect_r6_class(ds$schema, "Schema")
  expect_equal(names(schm), names(ds$schema))
  expect_equivalent(map(ds$children, ~.$files), files)

  expect_scan_result(ds, schm)
})

test_that("Writing a dataset: CSV->IPC", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equivalent(
    new_ds %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6 & integer < 11) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )

  # Check whether "int" is present in the files or just in the dirs
  first <- read_feather(
    dir(dst_dir, pattern = ".feather$", recursive = TRUE, full.names = TRUE)[1],
    as_data_frame = FALSE
  )
  # It shouldn't be there
  expect_false("int" %in% names(first))
})

test_that("Writing a dataset: Parquet->IPC", {
  skip_if_not_available("parquet")
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(hive_dir)
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equivalent(
    new_ds %>%
      select(string = chr, integer = int, group) %>%
      filter(integer > 6 & group == 1) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
})

test_that("Writing a dataset: CSV->Parquet", {
  skip_if_not_available("parquet")
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "parquet", partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir)

  expect_equivalent(
    new_ds %>%
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

test_that("Writing a dataset: Parquet->Parquet (default)", {
  skip_if_not_available("parquet")
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(hive_dir)
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir)

  expect_equivalent(
    new_ds %>%
      select(string = chr, integer = int, group) %>%
      filter(integer > 6 & group == 1) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
})

test_that("Writing a dataset: no format specified", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  dst_dir <- make_temp_dir()
  write_dataset(mtcars, dst_dir)
  new_ds <- open_dataset(dst_dir)
  expect_equal(
    list.files(dst_dir, pattern = "parquet"),
    "part-0.parquet"
  )
  expect_true(
    inherits(new_ds$format, "ParquetFileFormat")
  )
  expect_equivalent(
    new_ds %>% collect(),
    mtcars
  )
})

test_that("Dataset writing: dplyr methods", {
  skip_if_not_available("parquet")
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(hive_dir)
  dst_dir <- tempfile()
  # Specify partition vars by group_by
  ds %>%
    group_by(int) %>%
    write_dataset(dst_dir, format = "feather")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  # select to specify schema (and rename)
  dst_dir2 <- tempfile()
  ds %>%
    group_by(int) %>%
    select(chr, dubs = dbl) %>%
    write_dataset(dst_dir2, format = "feather")
  new_ds <- open_dataset(dst_dir2, format = "feather")

  expect_equivalent(
    collect(new_ds) %>% arrange(int),
    rbind(df1[c("chr", "dbl", "int")], df2[c("chr", "dbl", "int")]) %>% rename(dubs = dbl)
  )

  # filter to restrict written rows
  dst_dir3 <- tempfile()
  ds %>%
    filter(int == 4) %>%
    write_dataset(dst_dir3, format = "feather")
  new_ds <- open_dataset(dst_dir3, format = "feather")

  expect_equivalent(
    new_ds %>% select(names(df1)) %>% collect(),
    df1 %>% filter(int == 4)
  )

  # mutate
  dst_dir3 <- tempfile()
  ds %>%
    filter(int == 4) %>%
    mutate(twice = int * 2) %>%
    write_dataset(dst_dir3, format = "feather")
  new_ds <- open_dataset(dst_dir3, format = "feather")

  expect_equivalent(
    new_ds %>% select(c(names(df1), "twice")) %>% collect(),
    df1 %>% filter(int == 4) %>% mutate(twice = int * 2)
  )
})

test_that("Dataset writing: non-hive", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir)
  dst_dir <- tempfile()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int", hive_style = FALSE)
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(as.character(c(1:10, 101:110))))
})

test_that("Dataset writing: no partitioning", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir)
  dst_dir <- tempfile()
  write_dataset(ds, dst_dir, format = "feather", partitioning = NULL)
  expect_true(dir.exists(dst_dir))
  expect_true(length(dir(dst_dir)) > 0)
})

test_that("Dataset writing: partition on null", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(hive_dir)

  dst_dir <- tempfile()
  partitioning = hive_partition(lgl = boolean())
  write_dataset(ds, dst_dir, partitioning = partitioning)
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), c("lgl=__HIVE_DEFAULT_PARTITION__", "lgl=false", "lgl=true"))

  dst_dir <- tempfile()
  partitioning = hive_partition(lgl = boolean(), null_fallback="xyz")
  write_dataset(ds, dst_dir, partitioning = partitioning)
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), c("lgl=false", "lgl=true", "lgl=xyz"))

  ds_readback <- open_dataset(dst_dir, partitioning = hive_partition(lgl = boolean(), null_fallback="xyz"))

  expect_identical(
    ds %>%
      select(int, lgl) %>%
      collect() %>%
      arrange(lgl, int),
    ds_readback %>%
      select(int, lgl) %>%
      collect() %>%
      arrange(lgl, int)
  )
})

test_that("Dataset writing: from data.frame", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  dst_dir <- tempfile()
  stacked <- rbind(df1, df2)
  stacked %>%
    group_by(int) %>%
    write_dataset(dst_dir, format = "feather")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equivalent(
    new_ds %>%
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

test_that("Dataset writing: from RecordBatch", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  dst_dir <- tempfile()
  stacked <- record_batch(rbind(df1, df2))
  stacked %>%
    group_by(int) %>%
    write_dataset(dst_dir, format = "feather")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equivalent(
    new_ds %>%
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

test_that("Writing a dataset: Ipc format options & compression", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()

  codec <- NULL
  if (codec_is_available("zstd")) {
    codec <- Codec$create("zstd")
  }

  write_dataset(ds, dst_dir, format = "feather", codec = codec)
  expect_true(dir.exists(dst_dir))

  new_ds <- open_dataset(dst_dir, format = "feather")
  expect_equivalent(
    new_ds %>%
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

test_that("Writing a dataset: Parquet format options", {
  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651
  skip_if_not_available("parquet")
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  dst_dir_no_truncated_timestamps <- make_temp_dir()

  # Use trace() to confirm that options are passed in
  trace(
    "parquet___ArrowWriterProperties___create",
    tracer = quote(warning("allow_truncated_timestamps == ", allow_truncated_timestamps)),
    print = FALSE,
    where = write_dataset
  )
  expect_warning(
    write_dataset(ds, dst_dir_no_truncated_timestamps, format = "parquet", partitioning = "int"),
    "allow_truncated_timestamps == FALSE"
  )
  expect_warning(
    write_dataset(ds, dst_dir, format = "parquet", partitioning = "int", allow_truncated_timestamps = TRUE),
    "allow_truncated_timestamps == TRUE"
  )
  untrace("parquet___ArrowWriterProperties___create", where = write_dataset)

  # Now confirm we can read back what we sent
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir)

  expect_equivalent(
    new_ds %>%
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

test_that("Writing a dataset: CSV format options", {
  skip_if_multithreading_disabled()
  df <- tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[1:10],
  )

  dst_dir <- make_temp_dir()
  write_dataset(df, dst_dir, format = "csv")
  expect_true(dir.exists(dst_dir))
  new_ds <- open_dataset(dst_dir, format = "csv")
  expect_equivalent(new_ds %>% collect(), df)

  dst_dir <- make_temp_dir()
  write_dataset(df, dst_dir, format = "csv", include_header = FALSE)
  expect_true(dir.exists(dst_dir))
  new_ds <- open_dataset(dst_dir, format = "csv",
                         column_names = c("int", "dbl", "lgl", "chr"))
  expect_equivalent(new_ds %>% collect(), df)
})

test_that("Dataset writing: unsupported features/input validation", {
  skip_if_not_available("parquet")
  expect_error(write_dataset(4), 'dataset must be a "Dataset"')

  ds <- open_dataset(hive_dir)
  expect_error(
    write_dataset(ds, partitioning = c("int", "NOTACOLUMN"), format = "ipc"),
    'Invalid field name: "NOTACOLUMN"'
  )
  expect_error(
    write_dataset(ds, tempfile(), basename_template = "something_without_i")
  )
  expect_error(
    write_dataset(ds, tempfile(), basename_template = NULL)
  )
})

# see https://issues.apache.org/jira/browse/ARROW-11328
test_that("Collecting zero columns from a dataset doesn't return entire dataset", {
  skip_if_not_available("parquet")
  tmp <- tempfile()
  write_dataset(mtcars, tmp, format = "parquet")
  expect_equal(
    open_dataset(tmp) %>% select() %>% collect() %>% dim(),
    c(32, 0)
  )
})

# see https://issues.apache.org/jira/browse/ARROW-12791
test_that("Error if no format specified and files are not parquet", {
  skip_if_not_available("parquet")
  expect_error(
    open_dataset(csv_dir, partitioning = "part"),
    "Did you mean to specify a 'format' other than the default (parquet)?",
    fixed = TRUE
  )
  expect_failure(
    expect_error(
      open_dataset(csv_dir, partitioning = "part", format = "parquet"),
      "Did you mean to specify a 'format'"
    )
  )
})
