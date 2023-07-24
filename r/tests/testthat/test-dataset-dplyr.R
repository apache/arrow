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
skip_if_not_available("parquet")

library(dplyr, warn.conflicts = FALSE)

dataset_dir <- make_temp_dir()
hive_dir <- make_temp_dir()

test_that("Setup (putting data in the dir)", {
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
})

test_that("filter() with is.nan()", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_equal(
    ds %>%
      select(part, dbl) %>%
      filter(!is.nan(dbl), part == 2) %>%
      collect(),
    tibble(part = 2L, dbl = df2$dbl[!is.nan(df2$dbl)])
  )
})

test_that("filter() with %in%", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_equal(
    ds %>%
      select(int, part) %>%
      filter(int %in% c(6, 4, 3, 103, 107), part == 1) %>%
      collect(),
    tibble(int = df1$int[c(3, 4, 6)], part = 1)
  )

  # ARROW-9606: bug in %in% filter on partition column with >1 partition columns
  ds <- open_dataset(hive_dir)
  expect_equal(
    ds %>%
      filter(group %in% 2) %>%
      select(names(df2)) %>%
      collect(),
    df2
  )
})

test_that("filter() on timestamp columns", {
  skip_if_not_available("re2")

  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_equal(
    ds %>%
      filter(ts >= lubridate::ymd_hms("2015-05-04 03:12:39")) %>%
      filter(part == 1) %>%
      select(ts) %>%
      collect(),
    df1[5:10, c("ts")],
  )

  # Now with Date
  expect_equal(
    ds %>%
      filter(ts >= as.Date("2015-05-04")) %>%
      filter(part == 1) %>%
      select(ts) %>%
      collect(),
    df1[5:10, c("ts")],
  )

  # Now with bare string date
  skip("Implement more aggressive implicit casting for scalars (ARROW-11402)")
  expect_equal(
    ds %>%
      filter(ts >= "2015-05-04") %>%
      filter(part == 1) %>%
      select(ts) %>%
      collect(),
    df1[5:10, c("ts")],
  )
})

test_that("filter() on date32 columns", {
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

  skip_if_not_available("re2")

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
twice: int32 (multiply_checked(int, 2))

* Filter: ((multiply_checked(dbl, 2) > 14) and (subtract_checked(dbl, 50) < 3))
See $.data for the source Arrow object",
    fixed = TRUE
  )
  expect_equal(
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
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_error(
    ds %>%
      group_by(int) %>%
      mutate(avg = mean(int)),
    "window functions not currently supported in Arrow\nCall collect() first to pull data into R.",
    fixed = TRUE
  )
})

test_that("filter scalar validation doesn't crash (ARROW-7772)", {
  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))
  expect_error(
    ds %>%
      filter(int == Expression$scalar("fff"), part == 1) %>%
      collect(),
    "'equal' has no kernel matching input types .int32, string."
  )
})

test_that("collect() on Dataset works (if fits in memory)", {
  expect_equal(
    collect(open_dataset(dataset_dir)) %>% arrange(int),
    rbind(df1, df2)
  )
})

test_that("count()", {
  ds <- open_dataset(dataset_dir)
  df <- rbind(df1, df2)
  expect_equal(
    ds %>%
      filter(int > 6, int < 108) %>%
      count(chr) %>%
      arrange(chr) %>%
      collect(),
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
twice: int32 (multiply_checked(int, 2))

* Filter: ((multiply_checked(dbl, 2) > 14) and (subtract_checked(dbl, 50) < 3))
* Sorted by chr [asc], multiply_checked(int, 2) [desc], add_checked(dbl, int) [asc]
See $.data for the source Arrow object",
    fixed = TRUE
  )
  expect_equal(
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
  ds <- open_dataset(dataset_dir)

  tab1 <- ds %>% compute()
  expect_r6_class(tab1, "Table")

  tab2 <- ds %>% collect(as_data_frame = FALSE)
  expect_r6_class(tab2, "Table")

  tab3 <- ds %>%
    mutate(negint = -int) %>%
    filter(negint > -100) %>%
    arrange(chr) %>%
    select(negint) %>%
    compute()

  expect_r6_class(tab3, "Table")

  expect_equal(
    tab3 %>% collect(),
    tibble(negint = -1:-10)
  )

  tab4 <- ds %>%
    mutate(negint = -int) %>%
    filter(negint > -100) %>%
    arrange(chr) %>%
    select(negint) %>%
    collect(as_data_frame = FALSE)

  expect_r6_class(tab3, "Table")

  expect_equal(
    tab4 %>% collect(),
    tibble(negint = -1:-10)
  )

  tab5 <- ds %>%
    mutate(negint = -int) %>%
    group_by(fct) %>%
    compute()

  expect_r6_class(tab5, "Table")
  # mutate() was evaluated
  expect_true("negint" %in% names(tab5))
})

test_that("head/tail on query on dataset", {
  # head/tail on arrow_dplyr_query does not have deterministic order,
  # so without sorting we can only assert the correct number of rows
  ds <- open_dataset(dataset_dir)

  expect_identical(
    ds %>%
      filter(int > 6) %>%
      head(5) %>%
      compute() %>%
      nrow(),
    5L
  )

  expect_equal(
    ds %>%
      filter(int > 6) %>%
      arrange(int) %>%
      head() %>%
      collect(),
    rbind(df1[7:10, ], df2[1:2, ])
  )

  expect_equal(
    ds %>%
      filter(int < 105) %>%
      tail(4) %>%
      compute() %>%
      nrow(),
    4L
  )

  expect_equal(
    ds %>%
      filter(int < 105) %>%
      arrange(int) %>%
      tail() %>%
      collect(),
    rbind(df1[9:10, ], df2[1:4, ])
  )
})

test_that("dplyr method not implemented messages", {
  ds <- open_dataset(dataset_dir)
  # This one is more nuanced
  expect_error(
    ds %>% filter(int > 6, dbl > max(dbl)),
    "Filter expression not supported for Arrow Datasets: dbl > max(dbl)\nCall collect() first to pull data into R.",
    fixed = TRUE
  )
})

test_that("show_exec_plan(), show_query() and explain() with datasets", {
  # show_query() and explain() are wrappers around show_exec_plan() and are not
  # tested separately

  ds <- open_dataset(dataset_dir, partitioning = schema(part = uint8()))

  # minimal test
  expect_output(
    ds %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "ProjectNode.*", # output columns
      "SourceNode" # entry point
    )
  )

  # filter and select
  expect_output(
    ds %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6L & part == 1) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "ProjectNode.*", # output columns
      "FilterNode.*", # filter node
      "int > 6.*", # filtering expressions
      "SourceNode" # entry point
    )
  )

  # group_by and summarise
  expect_output(
    ds %>%
      group_by(part) %>%
      summarise(avg = mean(int)) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "GroupByNode.*", # group by node
      "keys=.*part.*", # key for aggregations
      "aggregates=.*hash_mean.*", # aggregations
      "ProjectNode.*", # input columns
      "SourceNode" # entry point
    )
  )

  # arrange and head
  expect_output(
    ds %>%
      filter(lgl) %>%
      arrange(chr) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "OrderByNode.*chr.*ASC.*", # arrange goes via the OrderBy node
      "ProjectNode.*", # output columns
      "FilterNode.*", # filter node
      "filter=lgl.*", # filtering expression
      "SourceNode" # entry point
    )
  )
})
