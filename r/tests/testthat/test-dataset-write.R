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


hive_dir <- make_temp_dir()
csv_dir <- make_temp_dir()

test_that("Setup (putting data in the dirs)", {
  if (arrow_with_parquet()) {
    dir.create(file.path(hive_dir, "subdir", "group=1", "other=xxx"), recursive = TRUE)
    dir.create(file.path(hive_dir, "subdir", "group=2", "other=yyy"), recursive = TRUE)
    write_parquet(df1, file.path(hive_dir, "subdir", "group=1", "other=xxx", "file1.parquet"))
    write_parquet(df2, file.path(hive_dir, "subdir", "group=2", "other=yyy", "file2.parquet"))
    expect_length(dir(hive_dir, recursive = TRUE), 2)
  }

  # Now, CSV
  dir.create(file.path(csv_dir, 5))
  dir.create(file.path(csv_dir, 6))
  write.csv(df1, file.path(csv_dir, 5, "file1.csv"), row.names = FALSE)
  write.csv(df2, file.path(csv_dir, 6, "file2.csv"), row.names = FALSE)
  expect_length(dir(csv_dir, recursive = TRUE), 2)
})

test_that("Writing a dataset: CSV->IPC", {
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equal(
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
    dir(dst_dir, pattern = ".arrow$", recursive = TRUE, full.names = TRUE)[1],
    as_data_frame = FALSE
  )
  # It shouldn't be there
  expect_false("int" %in% names(first))
})

test_that("Writing a dataset: Parquet->IPC", {
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir)
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equal(
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
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "parquet", partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir)

  expect_equal(
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
  ds <- open_dataset(hive_dir)
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, partitioning = "int")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir)

  expect_equal(
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

test_that("Writing a dataset: `basename_template` default behavier", {
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")

  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "parquet", max_rows_per_file = 5L)
  expect_identical(
    dir(dst_dir, full.names = FALSE, recursive = TRUE),
    paste0("part-", 0:3, ".parquet")
  )
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "parquet", basename_template = "{i}.data", max_rows_per_file = 5L)
  expect_identical(
    dir(dst_dir, full.names = FALSE, recursive = TRUE),
    paste0(0:3, ".data")
  )
  dst_dir <- make_temp_dir()
  expect_error(
    write_dataset(ds, dst_dir, format = "parquet", basename_template = "part-i.parquet"),
    "basename_template did not contain '\\{i\\}'"
  )
  feather_dir <- make_temp_dir()
  write_dataset(ds, feather_dir, format = "feather", partitioning = "int")
  expect_identical(
    dir(feather_dir, full.names = FALSE, recursive = TRUE),
    sort(paste(paste("int", c(1:10, 101:110), sep = "="), "part-0.arrow", sep = "/"))
  )
  ipc_dir <- make_temp_dir()
  write_dataset(ds, ipc_dir, format = "ipc", partitioning = "int")
  expect_identical(
    dir(ipc_dir, full.names = FALSE, recursive = TRUE),
    sort(paste(paste("int", c(1:10, 101:110), sep = "="), "part-0.arrow", sep = "/"))
  )
})

test_that("Writing a dataset: existing data behavior", {
  # This test does not work on Windows because unlink does not immediately
  # delete the data.
  skip_on_os("windows")
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int")
  expect_true(dir.exists(dst_dir))

  check_dataset <- function() {
    new_ds <- open_dataset(dst_dir, format = "feather")

    expect_equal(
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
  }

  check_dataset()
  # By default we should overwrite
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int")
  check_dataset()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int", existing_data_behavior = "overwrite")
  check_dataset()
  expect_error(
    write_dataset(ds, dst_dir, format = "feather", partitioning = "int", existing_data_behavior = "error"),
    "directory is not empty"
  )
  unlink(dst_dir, recursive = TRUE)
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int", existing_data_behavior = "error")
  check_dataset()
})

test_that("Writing a dataset: no format specified", {
  dst_dir <- make_temp_dir()
  write_dataset(example_data, dst_dir)
  new_ds <- open_dataset(dst_dir)
  expect_equal(
    list.files(dst_dir, pattern = "parquet"),
    "part-0.parquet"
  )
  expect_true(
    inherits(new_ds$format, "ParquetFileFormat")
  )
  expect_equal(
    new_ds %>% collect(),
    example_data
  )
})

test_that("Dataset writing: dplyr methods", {
  skip_if_not_available("parquet")
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

  expect_equal(
    collect(new_ds) %>% arrange(int),
    rbind(df1[c("chr", "dbl", "int")], df2[c("chr", "dbl", "int")]) %>% rename(dubs = dbl)
  )

  # filter to restrict written rows
  dst_dir3 <- tempfile()
  ds %>%
    filter(int == 4) %>%
    write_dataset(dst_dir3, format = "feather")
  new_ds <- open_dataset(dst_dir3, format = "feather")

  expect_equal(
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

  expect_equal(
    new_ds %>% select(c(names(df1), "twice")) %>% collect(),
    df1 %>% filter(int == 4) %>% mutate(twice = int * 2)
  )

  # head
  dst_dir4 <- tempfile()
  ds %>%
    mutate(twice = int * 2) %>%
    arrange(int) %>%
    head(3) %>%
    write_dataset(dst_dir4, format = "feather")
  new_ds <- open_dataset(dst_dir4, format = "feather")

  expect_equal(
    new_ds %>%
      select(c(names(df1), "twice")) %>%
      collect(),
    df1 %>%
      mutate(twice = int * 2) %>%
      head(3)
  )
})

test_that("Dataset writing: non-hive", {
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir)
  dst_dir <- tempfile()
  write_dataset(ds, dst_dir, format = "feather", partitioning = "int", hive_style = FALSE)
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(as.character(c(1:10, 101:110))))
})

test_that("Dataset writing: no partitioning", {
  skip_if_not_available("parquet")
  ds <- open_dataset(hive_dir)
  dst_dir <- tempfile()
  write_dataset(ds, dst_dir, format = "feather", partitioning = NULL)
  expect_true(dir.exists(dst_dir))
  expect_true(length(dir(dst_dir)) > 0)
})

test_that("Dataset writing: partition on null", {
  ds <- open_dataset(hive_dir)

  dst_dir <- tempfile()
  partitioning <- hive_partition(lgl = boolean())
  write_dataset(ds, dst_dir, partitioning = partitioning)
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), c("lgl=__HIVE_DEFAULT_PARTITION__", "lgl=false", "lgl=true"))

  dst_dir <- tempfile()
  partitioning <- hive_partition(lgl = boolean(), null_fallback = "xyz")
  write_dataset(ds, dst_dir, partitioning = partitioning)
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), c("lgl=false", "lgl=true", "lgl=xyz"))

  ds_readback <- open_dataset(dst_dir, partitioning = hive_partition(lgl = boolean(), null_fallback = "xyz"))

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
  dst_dir <- tempfile()
  stacked <- rbind(df1, df2)
  stacked %>%
    group_by(int) %>%
    write_dataset(dst_dir, format = "feather")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equal(
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
  dst_dir <- tempfile()
  stacked <- record_batch(rbind(df1, df2))
  stacked %>%
    mutate(twice = int * 2) %>%
    group_by(int) %>%
    write_dataset(dst_dir, format = "feather")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equal(
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
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()

  codec <- NULL
  if (codec_is_available("zstd")) {
    codec <- Codec$create("zstd")
  }

  write_dataset(ds, dst_dir, format = "feather", codec = codec)
  expect_true(dir.exists(dst_dir))

  new_ds <- open_dataset(dst_dir, format = "feather")
  expect_equal(
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
  skip_if_not_available("parquet")
  ds <- open_dataset(csv_dir, partitioning = "part", format = "csv")
  dst_dir <- make_temp_dir()
  dst_dir_no_truncated_timestamps <- make_temp_dir()

  # Use trace() to confirm that options are passed in
  suppressMessages(trace(
    "parquet___ArrowWriterProperties___create",
    tracer = quote(warning("allow_truncated_timestamps == ", allow_truncated_timestamps)),
    print = FALSE,
    where = write_dataset
  ))
  expect_warning(
    write_dataset(ds, dst_dir_no_truncated_timestamps, format = "parquet", partitioning = "int"),
    "allow_truncated_timestamps == FALSE"
  )
  expect_warning(
    write_dataset(ds, dst_dir, format = "parquet", partitioning = "int", allow_truncated_timestamps = TRUE),
    "allow_truncated_timestamps == TRUE"
  )
  suppressMessages(untrace(
    "parquet___ArrowWriterProperties___create",
    where = write_dataset
  ))

  # Now confirm we can read back what we sent
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir)

  expect_equal(
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
  expect_equal(new_ds %>% collect(), df)

  dst_dir <- make_temp_dir()
  write_dataset(df, dst_dir, format = "csv", include_header = FALSE)
  expect_true(dir.exists(dst_dir))
  new_ds <- open_dataset(dst_dir,
    format = "csv",
    column_names = c("int", "dbl", "lgl", "chr")
  )
  expect_equal(new_ds %>% collect(), df)
})

test_that("Dataset writing: unsupported features/input validation", {
  skip_if_not_available("parquet")
  expect_error(write_dataset(4), "You must supply a")
  expect_error(
    write_dataset(data.frame(x = 1, x = 2, check.names = FALSE)),
    "Field names must be unique"
  )

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

# see https://issues.apache.org/jira/browse/ARROW-12315
test_that("Max partitions fails with non-integer values and less than required partitions values", {
  skip_if_not_available("parquet")
  df <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[1:10],
  )
  dst_dir <- make_temp_dir()

  # max_partitions = 10 => pass
  expect_silent(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = 10)
  )

  # max_partitions < 10 => error
  expect_error(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = 5),
    "Fragment would be written into 10 partitions. This exceeds the maximum of 5"
  )

  # negative max_partitions => error
  expect_error(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = -3),
    "max_partitions must be a positive, non-missing integer"
  )

  # round(max_partitions, 0) != max_partitions  => error
  expect_error(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = 3.5),
    "max_partitions must be a positive, non-missing integer"
  )

  # max_partitions = NULL => fail
  expect_error(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = NULL),
    "max_partitions must be a positive, non-missing integer"
  )

  # max_partitions = NA => fail
  expect_error(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = NA_integer_),
    "max_partitions must be a positive, non-missing integer"
  )

  # max_partitions = chr => error
  expect_error(
    write_dataset(df, dst_dir, partitioning = "int", max_partitions = "foobar"),
    "max_partitions must be a positive, non-missing integer"
  )
})

test_that("max_rows_per_group is adjusted if at odds with max_rows_per_file", {
  skip_if_not_available("parquet")
  df <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[1:10],
  )
  dst_dir <- make_temp_dir()

  # max_rows_per_group unset adjust silently
  expect_silent(
    write_dataset(df, dst_dir, max_rows_per_file = 5)
  )
})


test_that("write_dataset checks for format-specific arguments", {
  df <- tibble::tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[1:10],
  )
  dst_dir <- make_temp_dir()
  expect_snapshot(
    write_dataset(df, dst_dir, format = "feather", compression = "snappy"),
    error = TRUE
  )
  expect_snapshot(
    write_dataset(df, dst_dir, format = "feather", nonsensical_arg = "blah-blah"),
    error = TRUE
  )
  expect_snapshot(
    write_dataset(df, dst_dir, format = "arrow", nonsensical_arg = "blah-blah"),
    error = TRUE
  )
  expect_snapshot(
    write_dataset(df, dst_dir, format = "ipc", nonsensical_arg = "blah-blah"),
    error = TRUE
  )
  expect_snapshot(
    write_dataset(df, dst_dir, format = "csv", nonsensical_arg = "blah-blah"),
    error = TRUE
  )
  expect_snapshot(
    write_dataset(df, dst_dir, format = "parquet", nonsensical_arg = "blah-blah"),
    error = TRUE
  )
})

get_num_of_files <- function(dir, format) {
  files <- list.files(dir, pattern = paste(".", format, sep = ""), recursive = TRUE, full.names = TRUE)
  length(files)
}

test_that("Dataset write max open files", {
  skip_if_not_available("parquet")
  # test default partitioning
  dst_dir <- make_temp_dir()
  file_format <- "parquet"
  partitioning <- "c2"
  num_of_unique_c2_groups <- 5

  record_batch_1 <- record_batch(
    c1 = c(1, 2, 3, 4, 0, 10),
    c2 = c("a", "b", "c", "d", "e", "a")
  )
  record_batch_2 <- record_batch(
    c1 = c(5, 6, 7, 8, 0, 1),
    c2 = c("a", "b", "c", "d", "e", "c")
  )
  record_batch_3 <- record_batch(
    c1 = c(9, 10, 11, 12, 0, 1),
    c2 = c("a", "b", "c", "d", "e", "d")
  )
  record_batch_4 <- record_batch(
    c1 = c(13, 14, 15, 16, 0, 1),
    c2 = c("a", "b", "c", "d", "e", "b")
  )

  table <- Table$create(
    d1 = record_batch_1, d2 = record_batch_2,
    d3 = record_batch_3, d4 = record_batch_4
  )

  write_dataset(table, path = dst_dir, format = file_format, partitioning = partitioning)

  # reduce 1 from the length of list of directories, since it list the search path)
  expect_equal(length(list.dirs(dst_dir)) - 1, num_of_unique_c2_groups)

  max_open_files <- 3
  dst_dir <- make_temp_dir()
  write_dataset(
    table,
    path = dst_dir,
    format = file_format,
    partitioning = partitioning,
    max_open_files = max_open_files
  )

  expect_gt(get_num_of_files(dst_dir, file_format), max_open_files)
})


test_that("Dataset write max rows per files", {
  skip_if_not_available("parquet")
  num_of_records <- 35
  df <- tibble::tibble(
    int = 1:num_of_records,
    dbl = as.numeric(1:num_of_records),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 7),
    chr = rep(letters[1:7], 5),
  )
  table <- Table$create(df)
  max_rows_per_file <- 10
  max_rows_per_group <- 10
  dst_dir <- make_temp_dir()
  file_format <- "parquet"

  write_dataset(
    table,
    path = dst_dir,
    format = file_format,
    max_rows_per_file = max_rows_per_file,
    max_rows_per_group = max_rows_per_group
  )

  expected_partitions <- num_of_records %/% max_rows_per_file + 1
  written_files <- list.files(dst_dir)
  result_partitions <- length(written_files)

  expect_equal(expected_partitions, result_partitions)
  total_records <- 0
  for (file in written_files) {
    file_path <- paste(dst_dir, file, sep = "/")
    ds <- read_parquet(file_path)
    cur_records <- nrow(ds)
    expect_lte(cur_records, max_rows_per_file)
    total_records <- total_records + cur_records
  }
  expect_equal(total_records, num_of_records)
})

test_that("Dataset min_rows_per_group", {
  skip_if_not(CanRunWithCapturedR())
  skip_if_not_available("parquet")

  rb1 <- record_batch(
    c1 = c(1, 2, 3, 4),
    c2 = c("a", "b", "e", "a")
  )
  rb2 <- record_batch(
    c1 = c(5, 6, 7, 8, 9),
    c2 = c("a", "b", "c", "d", "h")
  )
  rb3 <- record_batch(
    c1 = c(10, 11),
    c2 = c("a", "b")
  )

  dataset <- Table$create(d1 = rb1, d2 = rb2, d3 = rb3)

  dst_dir <- make_temp_dir()
  min_rows_per_group <- 4
  max_rows_per_group <- 5

  write_dataset(
    dataset,
    min_rows_per_group = min_rows_per_group,
    max_rows_per_group = max_rows_per_group,
    path = dst_dir
  )

  ds <- open_dataset(dst_dir)

  row_group_sizes <- ds %>%
    map_batches(~ record_batch(nrows = .$num_rows)) %>%
    pull(nrows) %>%
    as.vector()
  index <- 1

  # We expect there to be 3 row groups since 11/5 = 2.2 and 11/4 = 2.75
  expect_length(row_group_sizes, 3L)

  # We have all the rows
  expect_equal(sum(row_group_sizes), nrow(ds))

  # We expect that 2 of those will be between the two bounds
  in_bounds <- row_group_sizes >= min_rows_per_group & row_group_sizes <= max_rows_per_group
  expect_equal(sum(in_bounds), 2)
  # and the last one that is not is less than the max:
  expect_lte(row_group_sizes[!in_bounds], max_rows_per_group)
})

test_that("Dataset write max rows per group", {
  skip_if_not(CanRunWithCapturedR())
  skip_if_not_available("parquet")

  num_of_records <- 30
  max_rows_per_group <- 18
  df <- tibble::tibble(
    int = 1:num_of_records,
    dbl = as.numeric(1:num_of_records),
  )
  table <- Table$create(df)
  dst_dir <- make_temp_dir()
  file_format <- "parquet"

  write_dataset(table, path = dst_dir, format = file_format, max_rows_per_group = max_rows_per_group)

  written_files <- list.files(dst_dir)
  record_combination <- list()

  # writes only to a single file with multiple groups
  file_path <- paste(dst_dir, written_files[[1]], sep = "/")
  ds <- open_dataset(file_path)
  row_group_sizes <- ds %>%
    map_batches(~ record_batch(nrows = .$num_rows)) %>%
    pull(nrows) %>%
    as.vector() %>%
    sort()

  expect_equal(row_group_sizes, c(12, 18))
})

test_that("Can delete filesystem dataset after write_dataset", {
  # While this test should pass on all platforms, this is primarily
  # a test for Windows because that platform won't allow open files
  # to be deleted.
  dataset_dir2 <- tempfile()
  ds0 <- open_dataset(hive_dir)
  write_dataset(ds0, dataset_dir2)

  dataset_dir3 <- tempfile()
  on.exit(unlink(dataset_dir3, recursive = TRUE))

  ds <- open_dataset(dataset_dir2)
  write_dataset(ds, dataset_dir3)

  unlink(dataset_dir2, recursive = TRUE)
  expect_false(dir.exists(dataset_dir2))
})
