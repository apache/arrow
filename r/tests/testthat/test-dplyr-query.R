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

library(dplyr, warn.conflicts = FALSE)
library(stringr)

skip_if_not_available("acero")

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
tbl$another_chr <- tail(letters, 10)

test_that("basic select/filter/collect", {
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5)

  expect_s3_class(b2, "arrow_dplyr_query")
  t2 <- collect(b2)
  expect_equal(t2, tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")])
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})

test_that("dim() on query", {
  compare_dplyr_binding(
    .input %>%
      filter(int > 5) %>%
      select(int, chr) %>%
      dim(),
    tbl
  )
})

test_that("Print method", {
  expect_output(
    record_batch(tbl) %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      select(chr, int, lgl) %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      print(),
    'RecordBatch (query)
int: int32
chr: string

* Filter: (((dbl > 2) and ((chr == "d") or (chr == "f"))) and (int < 5))
See $.data for the source Arrow object',
    fixed = TRUE
  )
})

test_that("pull", {
  compare_dplyr_binding(
    .input %>% pull() %>% as.vector(),
    tbl
  )
  compare_dplyr_binding(
    .input %>% pull(1) %>% as.vector(),
    tbl
  )
  compare_dplyr_binding(
    .input %>% pull(chr) %>% as.vector(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      filter(int > 4) %>%
      rename(strng = chr) %>%
      pull(strng) %>%
      as.vector(),
    tbl
  )
})

test_that("pull() shows a deprecation warning if the option isn't set", {
  expect_warning(
    vec <- tbl %>%
      arrow_table() %>%
      pull(as_vector = NULL),
    "Current behavior of returning an R vector is deprecated"
  )
  # And the default is the old behavior, an R vector
  expect_identical(vec, pull(tbl))
})

test_that("collect(as_data_frame=FALSE)", {
  batch <- record_batch(tbl)

  b1 <- batch %>% collect(as_data_frame = FALSE)

  expect_r6_class(b1, "RecordBatch")

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    collect(as_data_frame = FALSE)

  # collect(as_data_frame = FALSE) always returns Table now
  expect_r6_class(b2, "Table")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")]
  expect_equal_data_frame(b2, expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    collect(as_data_frame = FALSE)
  expect_r6_class(b3, "Table")
  expect_equal_data_frame(b3, set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    collect(as_data_frame = FALSE)
  expect_r6_class(b4, "Table")
  expect_equal_data_frame(
    b4,
    expected %>%
      rename(strng = chr) %>%
      group_by(int)
  )
})

test_that("compute()", {
  batch <- record_batch(tbl)

  b1 <- batch %>% compute()

  expect_r6_class(b1, "RecordBatch")

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    compute()

  expect_r6_class(b2, "Table")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")]
  expect_equal_data_frame(b2, expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    compute()
  expect_r6_class(b3, "Table")
  expect_equal_data_frame(b3, set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    compute()
  expect_r6_class(b4, "Table")
  expect_equal_data_frame(
    b4,
    expected %>%
      rename(strng = chr) %>%
      group_by(int)
  )
})

test_that("head", {
  compare_dplyr_binding(
    .input %>%
      select(int, strng = chr) %>%
      filter(int > 5) %>%
      group_by(int) %>%
      head(2) %>%
      collect(),
    tbl
  )

  # This would fail if we evaluated head() after filter()
  compare_dplyr_binding(
    .input %>%
      select(int, strng = chr) %>%
      arrange(int) %>%
      head(2) %>%
      filter(int > 5) %>%
      mutate(twice = int * 2) %>%
      collect(),
    tbl
  )
})

test_that("arrange then head returns the right data (ARROW-14162)", {
  compare_dplyr_binding(
    .input %>%
      # mpg has ties so we need to sort by two things to get deterministic order
      arrange(mpg, disp) %>%
      head(4) %>%
      collect(),
    tibble::as_tibble(mtcars)
  )
})

test_that("arrange then tail returns the right data", {
  compare_dplyr_binding(
    .input %>%
      # mpg has ties so we need to sort by two things to get deterministic order
      arrange(mpg, disp) %>%
      tail(4) %>%
      collect(),
    tibble::as_tibble(mtcars)
  )
})

test_that("tail", {
  # With sorting
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      filter(int < 5) %>%
      arrange(int) %>%
      tail(2) %>%
      collect(),
    tbl
  )
  # Without sorting: table order is implicit, and we can compute the filter
  # row length, so the query can use Fetch with offset
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      filter(int < 5) %>%
      tail(2) %>%
      collect(),
    tbl
  )
})

test_that("No duplicate field names are allowed in an arrow_dplyr_query", {
  expect_error(
    Table$create(tbl, tbl) %>%
      filter(int > 0),
    regexp = paste0(
      'The following field names were found more than once in the data: "int", "dbl", ',
      '"dbl2", "lgl", "false", "chr", "fct", "verses", "padded_strings"'
    )
  )
})

test_that("all_sources() finds all data sources in a query", {
  skip_if_not_available("dataset")
  tab <- Table$create(a = 1)
  ds <- InMemoryDataset$create(tab)
  expect_equal(all_sources(tab), list(tab))
  expect_equal(
    tab %>%
      filter(a > 0) %>%
      summarize(a = sum(a)) %>%
      arrange(desc(a)) %>%
      all_sources(),
    list(tab)
  )
  expect_equal(
    tab %>%
      filter(a > 0) %>%
      union_all(ds) %>%
      all_sources(),
    list(tab, ds)
  )

  expect_equal(
    tab %>%
      filter(a > 0) %>%
      union_all(ds) %>%
      left_join(tab) %>%
      all_sources(),
    list(tab, ds, tab)
  )
  expect_equal(
    tab %>%
      filter(a > 0) %>%
      union_all(left_join(ds, tab)) %>%
      left_join(tab) %>%
      all_sources(),
    list(tab, ds, tab, tab)
  )
})

test_that("query_on_dataset() looks at all data sources in a query", {
  skip_if_not_available("dataset")
  tab <- Table$create(a = 1)
  ds <- InMemoryDataset$create(tab)
  expect_false(query_on_dataset(tab))
  expect_true(query_on_dataset(ds))
  expect_false(
    tab %>%
      filter(a > 0) %>%
      summarize(a = sum(a)) %>%
      arrange(desc(a)) %>%
      query_on_dataset()
  )
  expect_true(
    tab %>%
      filter(a > 0) %>%
      union_all(ds) %>%
      query_on_dataset()
  )

  expect_true(
    tab %>%
      filter(a > 0) %>%
      union_all(left_join(ds, tab)) %>%
      left_join(tab) %>%
      query_on_dataset()
  )
  expect_false(
    tab %>%
      filter(a > 0) %>%
      union_all(left_join(tab, tab)) %>%
      left_join(tab) %>%
      query_on_dataset()
  )
})

test_that("query_can_stream()", {
  skip_if_not_available("dataset")
  tab <- Table$create(a = 1)
  ds <- InMemoryDataset$create(tab)
  expect_true(query_can_stream(tab))
  expect_true(query_can_stream(ds))
  expect_true(query_can_stream(NULL))
  expect_true(
    ds %>%
      filter(a > 0) %>%
      query_can_stream()
  )
  expect_false(
    tab %>%
      filter(a > 0) %>%
      arrange(desc(a)) %>%
      query_can_stream()
  )
  expect_false(
    tab %>%
      filter(a > 0) %>%
      summarize(a = sum(a)) %>%
      query_can_stream()
  )
  expect_true(
    tab %>%
      filter(a > 0) %>%
      union_all(ds) %>%
      query_can_stream()
  )
  expect_false(
    tab %>%
      filter(a > 0) %>%
      union_all(summarize(ds, a = sum(a))) %>%
      query_can_stream()
  )

  expect_true(
    tab %>%
      filter(a > 0) %>%
      union_all(left_join(ds, tab)) %>%
      left_join(tab) %>%
      query_can_stream()
  )
  expect_true(
    tab %>%
      filter(a > 0) %>%
      union_all(left_join(tab, tab)) %>%
      left_join(tab) %>%
      query_can_stream()
  )
  expect_false(
    tab %>%
      filter(a > 0) %>%
      union_all(left_join(tab, tab)) %>%
      left_join(ds) %>%
      query_can_stream()
  )
  expect_false(
    tab %>%
      filter(a > 0) %>%
      arrange(a) %>%
      union_all(left_join(tab, tab)) %>%
      left_join(tab) %>%
      query_can_stream()
  )
})

test_that("show_exec_plan(), show_query() and explain()", {
  # show_query() and explain() are wrappers around show_exec_plan() and are not
  # tested separately

  # minimal test - this fails if we don't coerce the input to `show_exec_plan()`
  # to be an `arrow_dplyr_query`
  expect_output(
    mtcars %>%
      arrow_table() %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with 2 nodes:.*", # boiler plate for ExecPlan
      "SinkNode.*", # output
      "TableSourceNode" # entry point
    )
  )

  # arrow_table and mutate
  expect_output(
    tbl %>%
      arrow_table() %>%
      filter(dbl > 2, chr != "e") %>%
      select(chr, int, lgl) %>%
      mutate(int_plus_ten = int + 10) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "chr, int, lgl, \"int_plus_ten\".*", # selected columns
      "FilterNode.*", # filter node
      "(dbl > 2).*", # filter expressions
      "chr != \"e\".*",
      "TableSourceNode" # entry point
    )
  )

  # record_batch and mutate
  expect_output(
    tbl %>%
      record_batch() %>%
      filter(dbl > 2, chr != "e") %>%
      select(chr, int, lgl) %>%
      mutate(int_plus_ten = int + 10) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "chr, int, lgl, \"int_plus_ten\".*", # selected columns
      "(dbl > 2).*", # the filter expressions
      "chr != \"e\".*",
      "TableSourceNode" # the entry point"
    )
  )

  # test with group_by and summarise
  expect_output(
    tbl %>%
      arrow_table() %>%
      group_by(lgl) %>%
      summarise(avg = mean(dbl, na.rm = TRUE)) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "GroupByNode.*", # the group_by statement
      "keys=.*lgl.*", # the key for the aggregations
      "aggregates=.*hash_mean.*avg.*", # the aggregations
      "ProjectNode.*", # the input columns
      "TableSourceNode" # the entry point
    )
  )

  # test with join
  expect_output(
    tbl %>%
      arrow_table() %>%
      left_join(
        example_data %>%
          arrow_table() %>%
          mutate(doubled_dbl = dbl * 2) %>%
          select(int, doubled_dbl),
        by = "int"
      ) %>%
      select(int, verses, doubled_dbl) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "ProjectNode.*", # output columns
      "HashJoinNode.*", # the join
      "ProjectNode.*", # input columns for the second table
      "\"doubled_dbl\"\\: multiply_checked\\(dbl, 2\\).*", # mutate
      "TableSourceNode.*", # second table
      "TableSourceNode" # first table
    )
  )

  expect_output(
    mtcars %>%
      arrow_table() %>%
      filter(mpg > 20) %>%
      arrange(desc(wt)) %>%
      show_exec_plan(),
    regexp = paste0(
      "ExecPlan with .* nodes:.*", # boiler plate for ExecPlan
      "OrderBy.*wt.*DESC.*", # arrange goes via the OrderBy node
      "FilterNode.*", # filter node
      "TableSourceNode.*" # entry point
    )
  )

  # printing the ExecPlan for a nested query would currently force the
  # evaluation of the inner one(s), which we want to avoid => no output
  expect_output(
    mtcars %>%
      arrow_table() %>%
      filter(mpg > 20) %>%
      head(3) %>%
      show_exec_plan(),
    paste0(
      "ExecPlan with 4 nodes:.*",
      "3:SinkNode.*",
      "2:FetchNode.offset=0 count=3.*",
      "1:FilterNode.filter=.mpg > 20.*",
      "0:TableSourceNode.*"
    )
  )
})

test_that("needs_projection unit tests", {
  tab <- Table$create(tbl)
  # Wrapper to simplify tests
  query_needs_projection <- function(query) {
    needs_projection(query$selected_columns, tab$schema)
  }
  expect_false(query_needs_projection(as_adq(tab)))
  expect_false(query_needs_projection(
    tab %>% collapse() %>% collapse()
  ))
  expect_true(query_needs_projection(
    tab %>% mutate(int = int + 2)
  ))
  expect_true(query_needs_projection(
    tab %>% select(int, chr)
  ))
  expect_true(query_needs_projection(
    tab %>% rename(int2 = int)
  ))
  expect_true(query_needs_projection(
    tab %>% relocate(lgl)
  ))
})

test_that("compute() on a grouped query returns a Table with groups in metadata", {
  tab1 <- tbl %>%
    arrow_table() %>%
    group_by(int) %>%
    compute()
  expect_r6_class(tab1, "Table")
  expect_equal_data_frame(
    tab1,
    tbl %>%
      group_by(int)
  )
  expect_equal(
    collect(tab1),
    tbl %>%
      group_by(int)
  )
})

test_that("collect() is identical to compute() %>% collect()", {
  tab1 <- tbl %>%
    arrow_table()
  adq1 <- tab1 %>%
    group_by(int)

  expect_equal(
    tab1 %>%
      compute() %>%
      collect(),
    tab1 %>%
      collect()
  )
  expect_equal(
    adq1 %>%
      compute() %>%
      collect(),
    adq1 %>%
      collect()
  )
})

test_that("Scalars in expressions match the type of the field, if possible", {
  tbl_with_datetime <- tbl
  tbl_with_datetime$dates <- as.Date("2022-08-28") + 1:10
  tbl_with_datetime$times <- lubridate::ymd_hms("2018-10-07 19:04:05") + 1:10
  tab <- Table$create(tbl_with_datetime)

  # 5 is double in R but is properly interpreted as int, no cast is added
  expect_output(
    tab %>%
      filter(int == 5) %>%
      show_exec_plan(),
    "int == 5"
  )

  # Because 5.2 can't cast to int32 without truncation, we pass as is
  # and Acero will cast int to float64
  expect_output(
    tab %>%
      filter(int == 5.2) %>%
      show_exec_plan(),
    "filter=(cast(int, {to_type=double",
    fixed = TRUE
  )
  expect_equal(
    tab %>%
      filter(int == 5.2) %>%
      nrow(),
    0
  )

  # int == string, errors starting in dplyr 1.1.0
  expect_snapshot_warning(
    tab %>% filter(int == "5")
  )

  # Strings automatically parsed to date/timestamp
  expect_output(
    tab %>%
      filter(dates > "2022-09-01") %>%
      show_exec_plan(),
    "dates > 2022-09-01"
  )
  compare_dplyr_binding(
    .input %>%
      filter(dates > "2022-09-01") %>%
      collect(),
    tbl_with_datetime
  )

  # ARROW-18401: These will error if the system timezone is not valid. A PR was
  # submitted to fix this docker image upstream; this skip can be removed after
  # it merges.
  # https://github.com/r-hub/rhub-linux-builders/pull/65
  skip_if(identical(Sys.timezone(), "/UTC"))

  expect_output(
    tab %>%
      filter(times > "2018-10-07 19:04:05") %>%
      show_exec_plan(),
    "times > 2018-10-0. ..:..:05"
  )
  compare_dplyr_binding(
    .input %>%
      filter(times > "2018-10-07 19:04:05") %>%
      collect(),
    tbl_with_datetime
  )

  tab_with_decimal <- tab %>%
    mutate(dec = cast(dbl, decimal(15, 2))) %>%
    compute()

  # This reproduces the issue on ARROW-17601, found in the TPC-H query 1
  # In ARROW-17462, we chose not to auto-cast to decimal to avoid that issue
  result <- tab_with_decimal %>%
    summarize(
      tpc_h_1 = sum(dec * (1 - dec) * (1 + dec), na.rm = TRUE),
      as_dbl = sum(dbl * (1 - dbl) * (1 + dbl), na.rm = TRUE)
    ) %>%
    collect()
  expect_equal(result$tpc_h_1, result$as_dbl)
})

test_that("Can use nested field refs", {
  nested_data <- tibble(int = 1:5, df_col = tibble(a = 6:10, b = 11:15))

  compare_dplyr_binding(
    .input %>%
      mutate(
        nested = df_col$a,
        times2 = df_col$a * 2
      ) %>%
      filter(nested > 7) %>%
      collect(),
    nested_data
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        nested = df_col$a,
        times2 = df_col$a * 2
      ) %>%
      filter(nested > 7) %>%
      summarize(sum(times2)) %>%
      collect(),
    nested_data
  )
})

test_that("Can use nested field refs with Dataset", {
  skip_if_not_available("dataset")
  # Now with Dataset: make sure column pushdown in ScanNode works
  nested_data <- tibble(int = 1:5, df_col = tibble(a = 6:10, b = 11:15))
  tf <- tempfile()
  dir.create(tf)
  write_dataset(nested_data, tf)
  ds <- open_dataset(tf)

  expect_equal(
    ds %>%
      mutate(
        nested = df_col$a,
        times2 = df_col$a * 2
      ) %>%
      filter(nested > 7) %>%
      collect(),
    nested_data %>%
      mutate(
        nested = df_col$a,
        times2 = df_col$a * 2
      ) %>%
      filter(nested > 7)
  )
  # Issue #34519: error when projecting same name, but only on file dataset
  expect_equal(
    ds %>%
      mutate(int = as.numeric(int)) %>%
      collect(),
    nested_data %>%
      mutate(int = as.numeric(int)) %>%
      collect()
  )
})

test_that("Use struct_field for $ on non-field-ref", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        df_col = tibble(i = int, d = dbl)
      ) %>%
      transmute(
        int2 = df_col$i,
        dbl2 = df_col$d
      ) %>%
      collect(),
    example_data
  )
})

test_that("nested field ref error handling", {
  expect_error(
    example_data %>%
      arrow_table() %>%
      mutate(x = int$nested) %>%
      compute(),
    "No match"
  )
})
