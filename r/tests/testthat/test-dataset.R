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

dataset_dir <- make_temp_dir()
hive_dir <- make_temp_dir()
ipc_dir <- make_temp_dir()

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
})

test_that("IPC/Feather format data", {
  ds <- open_dataset(ipc_dir, partitioning = "part", format = "feather")
  expect_r6_class(ds$format, "IpcFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_identical(names(ds), c(names(df1), "part"))
  expect_identical(dim(ds), c(20L, 7L))

  expect_equal(
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
    ds %>% arrange(part) %>% pull(part) %>% as.vector(),
    c(rep(3, 10), rep(4, 10))
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

  expect_equal(
    as.data.frame(tab),
    df1[8, c("chr", "lgl")]
  )
}

test_that("URI-decoding with directory partitioning", {
  root <- make_temp_dir()
  fmt <- FileFormat$create("feather")
  fs <- LocalFileSystem$create()
  selector <- FileSelector$create(root, recursive = TRUE)
  dir1 <- file.path(root, "2021-05-04 00%3A00%3A00", "%24")
  dir.create(dir1, recursive = TRUE)
  write_feather(df1, file.path(dir1, "data.feather"))

  partitioning <- DirectoryPartitioning$create(
    schema(date = timestamp(unit = "s"), string = utf8())
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt,
    partitioning = partitioning
  )
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_scan_result(ds, schm)

  partitioning <- DirectoryPartitioning$create(
    schema(date = timestamp(unit = "s"), string = utf8()),
    segment_encoding = "none"
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt,
    partitioning = partitioning
  )
  schm <- factory$Inspect()
  expect_error(factory$Finish(schm), "Invalid: error parsing")

  partitioning_factory <- DirectoryPartitioningFactory$create(
    c("date", "string")
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory
  )
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
    c("date", "string"),
    segment_encoding = "none"
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory
  )
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
    date = timestamp(unit = "s"), string = utf8()
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt,
    partitioning = partitioning
  )
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_scan_result(ds, schm)

  partitioning <- hive_partition(
    date = timestamp(unit = "s"), string = utf8(), segment_encoding = "none"
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt,
    partitioning = partitioning
  )
  expect_error(factory$Finish(schm), "Invalid: error parsing")

  partitioning_factory <- hive_partition()
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory
  )
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
    fs, selector, NULL, fmt, partitioning_factory
  )
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

test_that("URI-decoding with hive partitioning with key encoded", {
  root <- make_temp_dir()
  fmt <- FileFormat$create("feather")
  fs <- LocalFileSystem$create()
  selector <- FileSelector$create(root, recursive = TRUE)
  dir1 <- file.path(root, "test%20key=2021-05-04 00%3A00%3A00", "test%20key1=%24")
  dir.create(dir1, recursive = TRUE)
  write_feather(df1, file.path(dir1, "data.feather"))

  partitioning <- hive_partition(
    `test key` = timestamp(unit = "s"), `test key1` = utf8(), segment_encoding = "uri"
  )
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt,
    partitioning = partitioning
  )
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_scan_result(ds, schm)

  # segment encoding for both key and values
  partitioning_factory <- hive_partition(segment_encoding = "uri")
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory
  )
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_equal(
    ds %>%
      filter(`test key` == "2021-05-04 00:00:00", `test key1` == "$") %>%
      select(int) %>%
      collect(),
    df1 %>% select(int) %>% collect()
  )

  # no segment encoding
  partitioning_factory <- hive_partition(segment_encoding = "none")
  factory <- FileSystemDatasetFactory$create(
    fs, selector, NULL, fmt, partitioning_factory
  )
  schm <- factory$Inspect()
  ds <- factory$Finish(schm)
  expect_equal(
    ds %>%
      filter(`test%20key` == "2021-05-04 00%3A00%3A00", `test%20key1` == "%24") %>%
      select(int) %>%
      collect(),
    df1 %>% select(int) %>% collect()
  )
})

# Everything else below here is using parquet files
skip_if_not_available("parquet")

files <- c(
  file.path(dataset_dir, 1, "file1.parquet", fsep = "/"),
  file.path(dataset_dir, 2, "file2.parquet", fsep = "/")
)

test_that("Simple interface for datasets", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_r6_class(ds$format, "ParquetFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_r6_class(ds, "Dataset")
  expect_equal(
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

  expect_equal(
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
    ds %>% arrange(part) %>% pull(part) %>% as.vector(),
    c(rep(1, 10), rep(2, 10))
  )
})

test_that("Can set schema on dataset", {
  ds <- open_dataset(dataset_dir)
  expected_schema <- schema(x = int32(), y = utf8())
  expect_false(ds$schema == expected_schema)

  ds_new <- ds$WithSchema(expected_schema)
  expect_equal(ds_new$schema, expected_schema)
  expect_false(ds$schema == expected_schema)

  ds$schema <- expected_schema
  expect_equal(ds$schema, expected_schema)
})

test_that("as.data.frame.Dataset", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_identical(dim(as.data.frame(ds)), c(20L, 7L))
})

test_that("dim method returns the correct number of rows and columns", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_identical(dim(ds), c(20L, 7L))
})


test_that("dim() correctly determine numbers of rows and columns on arrow_dplyr_query object", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))

  expect_identical(
    ds %>%
      filter(chr == "a") %>%
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
      filter(chr == "a") %>%
      dim(),
    c(2L, 3L)
  )
})

test_that("Simple interface for datasets (custom ParquetFileFormat)", {
  ds <- open_dataset(dataset_dir,
    partitioning = schema(part = uint8()),
    format = FileFormat$create("parquet", dict_columns = c("chr"))
  )
  expect_type_equal(ds$schema$GetFieldByName("chr")$type, dictionary())
})

test_that("Hive partitioning", {
  ds <- open_dataset(hive_dir, partitioning = hive_partition(other = utf8(), group = uint8()))
  expect_r6_class(ds, "Dataset")
  expect_equal(
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
  expect_error(
    open_dataset(hive_dir, hive_partition(other = utf8(), group = uint8()))
  )
})

test_that("Partitioning inference", {
  # These are the same tests as above, just using the *PartitioningFactory
  ds1 <- open_dataset(dataset_dir, partitioning = "part")
  expect_identical(names(ds1), c(names(df1), "part"))
  expect_equal(
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
  expect_equal(
    ds2 %>%
      filter(group == 2) %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53) %>%
      collect() %>%
      arrange(dbl),
    df2[1:2, c("chr", "dbl")]
  )
})

test_that("Specifying partitioning when hive_style", {
  expected_schema <- open_dataset(hive_dir)$schema

  # If string and names match hive partition names, it's accepted silently
  ds_with_chr <- open_dataset(hive_dir, partitioning = c("group", "other"))
  expect_equal(ds_with_chr$schema, expected_schema)

  # If they don't match, we get an error
  expect_error(
    open_dataset(hive_dir, partitioning = c("asdf", "zxcv")),
    paste(
      '"partitioning" does not match the detected Hive-style partitions:',
      'c\\("group", "other"\\).*after opening the dataset'
    )
  )

  # If schema and names match, the schema is used to specify the types
  ds_with_sch <- open_dataset(
    hive_dir,
    partitioning = schema(group = int32(), other = utf8())
  )
  expect_equal(ds_with_sch$schema, expected_schema)

  ds_with_int8 <- open_dataset(
    hive_dir,
    partitioning = schema(group = int8(), other = utf8())
  )
  expect_equal(ds_with_int8$schema[["group"]]$type, int8())

  # If they don't match, we get an error
  expect_error(
    open_dataset(hive_dir, partitioning = schema(a = int32(), b = utf8())),
    paste(
      '"partitioning" does not match the detected Hive-style partitions:',
      'c\\("group", "other"\\).*after opening the dataset'
    )
  )

  # This can be disabled with hive_style = FALSE
  ds_not_hive <- open_dataset(
    hive_dir,
    partitioning = c("group", "other"),
    hive_style = FALSE
  )
  # Since it's DirectoryPartitioning, the column values are all strings
  # like "group=1"
  expect_equal(ds_not_hive$schema[["group"]]$type, utf8())

  # And if no partitioning is specified and hive_style = FALSE, we don't parse at all
  ds_not_hive <- open_dataset(
    hive_dir,
    hive_style = FALSE
  )
  expect_null(ds_not_hive$schema[["group"]])
  expect_null(ds_not_hive$schema[["other"]])
})

test_that("Including partition columns in schema, hive style", {
  expected_schema <- open_dataset(hive_dir)$schema
  # Specify a different type than what is autodetected
  expected_schema$group <- float32()

  ds <- open_dataset(hive_dir, schema = expected_schema)
  expect_equal(ds$schema, expected_schema)

  # Now also with specifying `partitioning`
  ds2 <- open_dataset(hive_dir, schema = expected_schema, partitioning = c("group", "other"))
  expect_equal(ds2$schema, expected_schema)
})

test_that("Including partition columns in schema and partitioning, hive style CSV (ARROW-14743)", {
  mtcars_dir <- tempfile()
  on.exit(unlink(mtcars_dir))

  tab <- Table$create(mtcars)
  # Writing is hive-style by default
  write_dataset(tab, mtcars_dir, format = "csv", partitioning = "cyl")

  mtcars_ds <- open_dataset(
    mtcars_dir,
    schema = tab$schema,
    format = "csv",
    partitioning = "cyl"
  )
  expect_equal(mtcars_ds$schema, tab$schema)
})

test_that("partitioning = NULL to ignore partition information (but why?)", {
  ds <- open_dataset(hive_dir, partitioning = NULL)
  expect_identical(names(ds), names(df1)) # i.e. not c(names(df1), "group", "other")
})

test_that("Dataset with multiple file formats", {
  skip("https://issues.apache.org/jira/browse/ARROW-7653")
  ds <- open_dataset(list(
    open_dataset(dataset_dir, format = "parquet", partitioning = "part"),
    open_dataset(ipc_dir, format = "arrow", partitioning = "part")
  ))
  expect_identical(names(ds), c(names(df1), "part"))
  expect_equal(
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
  ds1 <- open_dataset(file.path(dataset_dir, 1))
  ds2 <- open_dataset(file.path(dataset_dir, 2))
  union1 <- open_dataset(list(ds1, ds2))
  expect_r6_class(union1, "UnionDataset")
  expect_equal(
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
  expect_equal(
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

test_that("UnionDataset can merge schemas", {
  sub_df1 <- Table$create(
    x = Array$create(c(1, 2, 3)),
    y = Array$create(c("a", "b", "c"))
  )
  sub_df2 <- Table$create(
    x = Array$create(c(4, 5)),
    z = Array$create(c("d", "e"))
  )

  path1 <- make_temp_dir()
  path2 <- make_temp_dir()
  write_dataset(sub_df1, path1, format = "parquet")
  write_dataset(sub_df2, path2, format = "parquet")

  ds1 <- open_dataset(path1, format = "parquet")
  ds2 <- open_dataset(path2, format = "parquet")

  ds <- c(ds1, ds2)
  actual <- ds %>%
    collect() %>%
    arrange(x)
  expect_equal(colnames(actual), c("x", "y", "z"))

  union_all_common <- function(...) {
    common <- vctrs::vec_cast_common(...)
    rlang::inject(union_all(!!!common))
  }

  expected <- union_all_common(
    as_tibble(sub_df1),
    as_tibble(sub_df2)
  )
  expect_equal(actual, expected)

  # without unifying schemas, takes the first schema and discards any columns
  # in the second which aren't in the first
  ds <- open_dataset(list(ds1, ds2), unify_schemas = FALSE)
  expected <- union_all_common(
    as_tibble(sub_df1),
    as_tibble(sub_df2) %>% select(x)
  )
  actual <- ds %>%
    collect() %>%
    arrange(x)
  expect_equal(colnames(actual), c("x", "y"))
  expect_equal(actual, expected)
})

test_that("UnionDataset handles InMemoryDatasets", {
  sub_df1 <- Table$create(
    x = Array$create(c(1, 2, 3)),
    y = Array$create(c("a", "b", "c"))
  )
  sub_df2 <- Table$create(
    x = Array$create(c(4, 5)),
    z = Array$create(c("d", "e"))
  )

  ds1 <- InMemoryDataset$create(sub_df1)
  ds2 <- InMemoryDataset$create(sub_df2)
  ds <- c(ds1, ds2)
  actual <- ds %>%
    arrange(x) %>%
    compute()
  expected <- concat_tables(sub_df1, sub_df2)
  expect_equal(actual, expected)
})

test_that("scalar aggregates with many batches (ARROW-16904)", {
  tf <- tempfile()
  write_parquet(data.frame(x = 1:100), tf, chunk_size = 20)

  ds <- open_dataset(tf)
  replicate(100, ds %>% summarize(min(x)) %>% pull())

  expect_true(
    all(
      replicate(100, ds %>% summarize(min(x)) %>% pull() %>% as.vector()) == 1
    )
  )
  expect_true(
    all(
      replicate(100, ds %>% summarize(max(x)) %>% pull() %>% as.vector()) == 100
    )
  )
})

test_that("streaming map_batches into an ExecPlan", {
  skip_if_not(CanRunWithCapturedR())

  ds <- open_dataset(dataset_dir, partitioning = "part")

  # summarize returns arrow_dplyr_query, which gets collected into a tibble
  expect_equal(
    ds %>%
      filter(int > 5) %>%
      select(int, lgl) %>%
      map_batches(~ summarize(., min_int = min(int))) %>%
      arrange(min_int) %>%
      collect(),
    tibble(min_int = c(6L, 101L))
  )

  # $num_rows returns integer vector, so we need to wrap it in a RecordBatch
  expect_equal(
    ds %>%
      filter(int > 5) %>%
      select(int, lgl) %>%
      map_batches(~ record_batch(nrows = .$num_rows)) %>%
      pull(nrows) %>%
      as.vector() %>%
      sort(),
    c(5, 10)
  )

  # Can take a raw dataset as X argument
  expect_equal(
    ds %>%
      map_batches(~ count(., part)) %>%
      arrange(part) %>%
      collect(),
    tibble(part = c(1, 2), n = c(10, 10))
  )

  # $Take returns RecordBatch
  expect_equal(
    ds %>%
      filter(int > 5) %>%
      select(int, lgl) %>%
      map_batches(~ .$Take(0)) %>%
      arrange(int) %>%
      collect(),
    tibble(int = c(6, 101), lgl = c(TRUE, TRUE))
  )

  # Do things in R and put back into Arrow
  expect_equal(
    ds %>%
      filter(int < 5) %>%
      select(int) %>%
      map_batches(
        # as_mapper() can't handle %>%?
        ~ mutate(as.data.frame(.), lets = letters[int])
      ) %>%
      arrange(int) %>%
      collect(),
    tibble(int = 1:4, lets = letters[1:4])
  )
})

test_that("map_batches with explicit schema", {
  fun_with_dots <- function(batch, first_col, first_col_val) {
    record_batch(
      !!first_col := first_col_val,
      b = batch$a$cast(float64())
    )
  }

  empty_reader <- RecordBatchReader$create(
    batches = list(),
    schema = schema(a = int32())
  )
  expect_equal(
    map_batches(
      empty_reader,
      fun_with_dots,
      "first_col_name",
      "first_col_value",
      .schema = schema(first_col_name = string(), b = float64())
    )$read_table(),
    arrow_table(first_col_name = character(), b = double())
  )

  reader <- RecordBatchReader$create(
    batches = list(
      record_batch(a = 1, b = "two"),
      record_batch(a = 2, b = "three")
    )
  )
  expect_equal(
    map_batches(
      reader,
      fun_with_dots,
      "first_col_name",
      "first_col_value",
      .schema = schema(first_col_name = string(), b = float64())
    )$read_table(),
    arrow_table(
      first_col_name = c("first_col_value", "first_col_value"),
      b = as.numeric(1:2)
    )
  )
})

test_that("map_batches without explicit schema", {
  fun_with_dots <- function(batch, first_col, first_col_val) {
    record_batch(
      !!first_col := first_col_val,
      b = batch$a$cast(float64())
    )
  }

  empty_reader <- RecordBatchReader$create(
    batches = list(),
    schema = schema(a = int32())
  )
  expect_error(
    map_batches(
      empty_reader,
      fun_with_dots,
      "first_col_name",
      "first_col_value"
    )$read_table(),
    "Can't infer schema"
  )

  reader <- RecordBatchReader$create(
    batches = list(
      record_batch(a = 1, b = "two"),
      record_batch(a = 2, b = "three")
    )
  )
  expect_equal(
    map_batches(
      reader,
      fun_with_dots,
      "first_col_name",
      "first_col_value"
    )$read_table(),
    arrow_table(
      first_col_name = c("first_col_value", "first_col_value"),
      b = as.numeric(1:2)
    )
  )
})

test_that("head/tail", {
  # head/tail with no query are still deterministic order
  ds <- open_dataset(dataset_dir)
  big_df <- rbind(df1, df2)

  # No n provided (default is 6, all from one batch)
  expect_equal(as.data.frame(head(ds)), head(df1))
  expect_equal(as.data.frame(tail(ds)), tail(df2))

  # n = 0: have to drop `fct` because factor levels don't come through from
  # arrow when there are 0 rows
  zero_df <- big_df[FALSE, names(big_df) != "fct"]
  expect_equal(as.data.frame(head(ds, 0))[, names(ds) != "fct"], zero_df)
  expect_equal(as.data.frame(tail(ds, 0))[, names(ds) != "fct"], zero_df)

  # Two more cases: more than 1 batch, and more than nrow
  for (n in c(12, 1000)) {
    expect_equal(as.data.frame(head(ds, n)), head(big_df, n))
    expect_equal(as.data.frame(tail(ds, n)), tail(big_df, n))
  }
  expect_error(head(ds, -1)) # Not yet implemented
  expect_error(tail(ds, -1)) # Not yet implemented
})


test_that("unique()", {
  ds <- open_dataset(dataset_dir)
  in_r_mem <- rbind(df1, df2)
  at <- arrow_table(in_r_mem)
  rbr <- as_record_batch_reader(in_r_mem)

  expect_s3_class(unique(ds), "arrow_dplyr_query")
  expect_s3_class(unique(at), "arrow_dplyr_query")
  expect_s3_class(unique(rbr), "arrow_dplyr_query")

  # on a arrow_dplyr_query
  adq_eg <- ds %>%
    select(fct) %>%
    unique()
  expect_s3_class(adq_eg, "arrow_dplyr_query")

  # order not set by distinct so some sorting required
  expect_equal(sort(collect(unique(ds))$int), sort(unique(in_r_mem)$int))

  # on a arrow table
  expect_equal(
    at %>%
      unique() %>%
      collect(),
    unique(in_r_mem)
  )
  expect_equal(
    rbr %>%
      unique() %>%
      collect(),
    unique(in_r_mem)
  )
  expect_snapshot_error(unique(arrow_table(in_r_mem), incomparables = TRUE))
})


test_that("Dataset [ (take by index)", {
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

test_that("Dataset and query print methods", {
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

test_that("Can delete filesystem dataset files after collection", {
  # While this test should pass on all platforms, this is primarily
  # a test for Windows because that platform won't allow open files
  # to be deleted.
  dataset_dir2 <- tempfile()
  ds0 <- open_dataset(dataset_dir)
  write_dataset(ds0, dataset_dir2)

  ds <- open_dataset(dataset_dir2)
  collected <- ds %>% arrange(int) %>% collect()
  unlink(dataset_dir2, recursive = TRUE)
  expect_false(dir.exists(dataset_dir2))

  expect_identical(
    collected,
    ds0 %>% arrange(int) %>% collect()
  )

  # Also try with head(), since this creates a nested query whose interior
  # components should also be cleaned up to allow deleting the original
  # dataset
  write_dataset(ds0, dataset_dir2)
  ds <- open_dataset(dataset_dir2)
  collected <- ds %>% arrange(int) %>% head() %>% arrange(int) %>% collect()
  unlink(dataset_dir2, recursive = TRUE)
  expect_false(dir.exists(dataset_dir2))

  expect_identical(
    collected,
    ds0 %>% arrange(int) %>% head() %>% arrange(int) %>% collect()
  )
})

test_that("Scanner$ScanBatches", {
  ds <- open_dataset(ipc_dir, format = "feather")
  batches <- ds$NewScan()$Finish()$ScanBatches()
  table <- Table$create(!!!batches)
  expect_equal(as.data.frame(table), rbind(df1, df2))

  batches <- ds$NewScan()$Finish()$ScanBatches()
  table <- Table$create(!!!batches)
  expect_equal(as.data.frame(table), rbind(df1, df2))
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

test_that("Scanner$create() filter/projection pushdown", {
  ds <- open_dataset(dataset_dir, partitioning = "part")

  # the standard to compare all Scanner$create()s against
  scan_one <- ds %>%
    filter(int > 7 & dbl < 57) %>%
    select(int, dbl, lgl) %>%
    mutate(int_plus = int + 1, dbl_minus = dbl - 1) %>%
    Scanner$create()

  # select a column in projection
  scan_two <- ds %>%
    filter(int > 7 & dbl < 57) %>%
    # select an extra column, since we are going to
    select(int, dbl, lgl, chr) %>%
    mutate(int_plus = int + 1, dbl_minus = dbl - 1) %>%
    Scanner$create(projection = c("int", "dbl", "lgl", "int_plus", "dbl_minus"))
  expect_identical(
    as.data.frame(scan_one$ToRecordBatchReader()$read_table()),
    as.data.frame(scan_two$ToRecordBatchReader()$read_table())
  )

  # adding filters to Scanner$create
  scan_three <- ds %>%
    filter(int > 7) %>%
    select(int, dbl, lgl) %>%
    mutate(int_plus = int + 1, dbl_minus = dbl - 1) %>%
    Scanner$create(
      filter = Expression$create("less", Expression$field_ref("dbl"), Expression$scalar(57))
    )
  expect_identical(
    as.data.frame(scan_one$ToRecordBatchReader()$read_table()),
    as.data.frame(scan_three$ToRecordBatchReader()$read_table())
  )

  expect_error(
    ds %>%
      select(int, dbl, lgl) %>%
      Scanner$create(projection = "not_a_col"),
    # Full message is "attempting to project with unknown columns" >= 4.0.0, but
    # prior versions have a less nice "all(projection %in% names(proj)) is not TRUE"
    "project"
  )

  expect_error(
    ds %>%
      select(int, dbl, lgl) %>%
      Scanner$create(filter = list("foo", "bar")),
    "filter expressions must be either an expression or a list of expressions"
  )
})

test_that("Assembling a Dataset manually and getting a Table", {
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
  expect_equal(child$files, files)

  ds <- Dataset$create(list(child), schm)
  expect_scan_result(ds, schm)
})

test_that("Assembling multiple DatasetFactories with DatasetFactory", {
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
  expect_equal(unlist(map(ds$children, ~ .$files)), files)

  expect_scan_result(ds, schm)
})

# By default, snappy encoding will be used, and
# Snappy has a UBSan issue: https://github.com/google/snappy/pull/148
skip_on_linux_devel()

# see https://issues.apache.org/jira/browse/ARROW-11328
test_that("Collecting zero columns from a dataset doesn't return entire dataset", {
  tmp <- tempfile()
  write_dataset(mtcars, tmp, format = "parquet")
  expect_equal(
    open_dataset(tmp) %>% select() %>% collect() %>% dim(),
    c(32, 0)
  )
})

test_that("dataset RecordBatchReader to C-interface to arrow_dplyr_query", {
  ds <- open_dataset(hive_dir)

  # export the RecordBatchReader via the C-interface
  stream_ptr <- allocate_arrow_array_stream()
  scan <- Scanner$create(ds)
  reader <- scan$ToRecordBatchReader()
  reader$export_to_c(stream_ptr)

  expect_equal(
    RecordBatchStreamReader$import_from_c(stream_ptr) %>%
      filter(int < 8 | int > 55) %>%
      mutate(part_plus = group + 6) %>%
      arrange(dbl) %>%
      collect(),
    ds %>%
      filter(int < 8 | int > 55) %>%
      mutate(part_plus = group + 6) %>%
      arrange(dbl) %>%
      collect()
  )

  # must clean up the pointer or we leak
  delete_arrow_array_stream(stream_ptr)
})

test_that("dataset to C-interface to arrow_dplyr_query with proj/filter", {
  ds <- open_dataset(hive_dir)

  # filter the dataset
  ds <- ds %>%
    filter(int > 2)

  # export the RecordBatchReader via the C-interface
  stream_ptr <- allocate_arrow_array_stream()
  scan <- Scanner$create(
    ds,
    projection = names(ds),
    filter = Expression$create("less", Expression$field_ref("int"), Expression$scalar(8L))
  )
  reader <- scan$ToRecordBatchReader()
  reader$export_to_c(stream_ptr)

  # then import it and check that the roundtripped value is the same
  circle <- RecordBatchStreamReader$import_from_c(stream_ptr)

  # create an arrow_dplyr_query() from the recordbatch reader
  reader_adq <- arrow_dplyr_query(circle)

  expect_equal(
    reader_adq %>%
      mutate(part_plus = group + 6) %>%
      arrange(dbl) %>%
      collect(),
    ds %>%
      filter(int < 8, int > 2) %>%
      mutate(part_plus = group + 6) %>%
      arrange(dbl) %>%
      collect()
  )

  # must clean up the pointer or we leak
  delete_arrow_array_stream(stream_ptr)
})

test_that("Filter parquet dataset with is.na ARROW-15312", {
  ds_path <- make_temp_dir()

  df <- tibble(x = 1:3, y = c(0L, 0L, NA_integer_), z = c(0L, 1L, NA_integer_))
  write_dataset(df, ds_path)

  # OK: Collect then filter: returns row 3, as expected
  expect_identical(
    open_dataset(ds_path) %>% collect() %>% filter(is.na(y)),
    df %>% collect() %>% filter(is.na(y))
  )

  # Before the fix: Filter then collect on y returned a 0-row tibble
  expect_identical(
    open_dataset(ds_path) %>% filter(is.na(y)) %>% collect(),
    df %>% filter(is.na(y)) %>% collect()
  )

  # OK: Filter then collect (on z) returns row 3, as expected
  expect_identical(
    open_dataset(ds_path) %>% filter(is.na(z)) %>% collect(),
    df %>% filter(is.na(z)) %>% collect()
  )
})

test_that("FileSystemFactoryOptions with DirectoryPartitioning", {
  parent_path <- make_temp_dir()
  ds_path <- file.path(parent_path, "a_subdir")
  write_dataset(mtcars, ds_path, partitioning = "cyl", hive = FALSE)
  expect_equal(dir(parent_path), "a_subdir")
  expect_equal(dir(ds_path), c("4", "6", "8"))
  ds <- open_dataset(
    parent_path,
    partitioning = "cyl",
    factory_options = list(partition_base_dir = ds_path)
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  # Add an invalid file
  file.create(file.path(ds_path, "ASDFnotdata.pq"))
  ds <- open_dataset(
    parent_path,
    partitioning = "cyl",
    factory_options = list(
      partition_base_dir = ds_path,
      # open_dataset() fails if we don't exclude invalid
      exclude_invalid_files = TRUE
    )
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  ds <- open_dataset(
    parent_path,
    partitioning = "cyl",
    factory_options = list(
      partition_base_dir = ds_path,
      # We can also ignore by prefix
      selector_ignore_prefixes = "ASDF"
    )
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  # Add a _$folder$ file (which Hadoop may write)
  file.create(file.path(ds_path, "cyl_$folder$"))
  ds <- open_dataset(
    parent_path,
    partitioning = "cyl",
    factory_options = list(
      partition_base_dir = ds_path,
      # we can't ignore suffixes but we can exclude invalid
      exclude_invalid_files = TRUE
    )
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  # Now with a list of files
  ds <- open_dataset(
    dir(ds_path, recursive = TRUE, full.names = TRUE),
    factory_options = list(
      exclude_invalid_files = TRUE
    )
  )
  expect_equal(
    ds %>%
      summarize(sum(gear)) %>%
      collect() %>%
      as.data.frame(),
    mtcars %>%
      summarize(sum(gear))
  )
})

test_that("FileSystemFactoryOptions with HivePartitioning", {
  parent_path <- make_temp_dir()
  ds_path <- file.path(parent_path, "a_subdir")
  write_dataset(mtcars, ds_path, partitioning = "cyl")
  expect_equal(dir(parent_path), "a_subdir")
  expect_equal(dir(ds_path), c("cyl=4", "cyl=6", "cyl=8"))
  ds <- open_dataset(parent_path)

  # With Hive partitioning, partition_base_dir isn't needed
  expect_setequal(names(ds), names(mtcars))
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  # Add an invalid file
  file.create(file.path(ds_path, "ASDFnotdata.pq"))
  ds <- open_dataset(
    parent_path,
    factory_options = list(
      # open_dataset() fails if we don't exclude invalid
      exclude_invalid_files = TRUE
    )
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  ds <- open_dataset(
    parent_path,
    factory_options = list(
      # We can also ignore by prefix
      selector_ignore_prefixes = "ASDF"
    )
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )

  # Add a _$folder$ file (which Hadoop may write)
  file.create(file.path(ds_path, "cyl_$folder$"))
  ds <- open_dataset(
    parent_path,
    factory_options = list(
      # we can't ignore suffixes but we can exclude invalid
      exclude_invalid_files = TRUE
    )
  )
  expect_equal(
    ds %>%
      arrange(cyl) %>%
      pull(cyl) %>%
      as.vector(),
    sort(mtcars$cyl)
  )
})

test_that("FileSystemFactoryOptions input validation", {
  expect_error(
    open_dataset(dataset_dir, factory_options = list(other = TRUE)),
    'Invalid factory_options: "other"'
  )
  expect_error(
    open_dataset(
      dataset_dir,
      partitioning = "part",
      factory_options = list(partition_base_dir = 42)
    ),
    "factory_options$partition_base_dir is not a string",
    fixed = TRUE
  )
  expect_error(
    open_dataset(dataset_dir, factory_options = list(selector_ignore_prefixes = 42)),
    "factory_options$selector_ignore_prefixes must be a character vector",
    fixed = TRUE
  )
  expect_error(
    open_dataset(dataset_dir, factory_options = list(exclude_invalid_files = 42)),
    "factory_options$exclude_invalid_files must be TRUE/FALSE",
    fixed = TRUE
  )

  expect_warning(
    open_dataset(hive_dir, factory_options = list(partition_base_dir = hive_dir)),
    "factory_options$partition_base_dir is not meaningful for Hive partitioning",
    fixed = TRUE
  )

  files <- dir(dataset_dir, full.names = TRUE, recursive = TRUE)
  expect_error(
    open_dataset(files, factory_options = list(selector_ignore_prefixes = "__")),
    paste(
      "Invalid factory_options for creating a Dataset from a vector",
      'of file paths: "selector_ignore_prefixes"'
    ),
    fixed = TRUE
  )
})

test_that("can add in augmented fields", {
  ds <- open_dataset(hive_dir)

  observed <- ds %>%
    mutate(file_name = add_filename()) %>%
    collect()

  expect_named(
    observed,
    c("int", "dbl", "lgl", "chr", "fct", "ts", "group", "other", "file_name")
  )

  expect_equal(
    sort(unique(observed$file_name)),
    list.files(hive_dir, full.names = TRUE, recursive = TRUE)
  )

  error_regex <- paste(
    "`add_filename()` or use of the `__filename` augmented field can only",
    "be used with with Dataset objects, and can only be added before doing",
    "an aggregation or a join."
  )

  # errors appropriately with ArrowTabular objects
  expect_error(
    arrow_table(mtcars) %>%
      mutate(file = add_filename()) %>%
      collect(),
    regexp = error_regex,
    fixed = TRUE
  )

  # errors appropriately with aggregation
  expect_error(
    ds %>%
      summarise(max_int = max(int)) %>%
      mutate(file_name = add_filename()) %>%
      collect(),
    regexp = error_regex,
    fixed = TRUE
  )

  # joins to tables
  another_table <- select(example_data, int, dbl2)
  expect_error(
    ds %>%
      left_join(another_table, by = "int") %>%
      mutate(file = add_filename()) %>%
      collect(),
    regexp = error_regex,
    fixed = TRUE
  )

  # and on joins to datasets
  another_dataset_dir <- tempfile()
  on.exit(unlink(another_dataset_dir, recursive = TRUE))
  another_dataset <- write_dataset(another_table, another_dataset_dir)

  expect_error(
    ds %>%
      left_join(open_dataset(another_dataset_dir), by = "int") %>%
      mutate(file = add_filename()) %>%
      collect(),
    regexp = error_regex,
    fixed = TRUE
  )

  # this hits the implicit_schema path by joining afterwards
  join_after <- ds %>%
    mutate(file = add_filename()) %>%
    left_join(open_dataset(another_dataset_dir), by = "int") %>%
    collect()

  expect_named(
    join_after,
    c("int", "dbl", "lgl", "chr", "fct", "ts", "group", "other", "file", "dbl2")
  )

  expect_equal(
    sort(unique(join_after$file)),
    list.files(hive_dir, full.names = TRUE, recursive = TRUE)
  )

  # another test on the explicit_schema path
  summarise_after <- ds %>%
    mutate(file = add_filename()) %>%
    group_by(file) %>%
    summarise(max_int = max(int)) %>%
    collect()

  expect_equal(
    sort(summarise_after$file),
    list.files(hive_dir, full.names = TRUE, recursive = TRUE)
  )
})
