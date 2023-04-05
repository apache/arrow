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
skip_on_cran()
# DuckDB 0.7.1-1 may have errors with R<4.0
skip_on_r_older_than("4.0")

# this test needs to be the first one since all other test blocks are skipped
# if duckdb is not installed
test_that("meaningful error message when duckdb is not installed", {
  # skipping if duckdb is installed since we're testing the to_duckdb function's
  # complaint when a user tries to call it, but duckdb isn't available
  skip_if(requireNamespace("duckdb", quietly = TRUE))
  ds <- InMemoryDataset$create(example_data)
  expect_error(
    to_duckdb(ds),
    regexp = "Please install the `duckdb` package to pass data with `to_duckdb()`.",
    fixed = TRUE
  )
})

skip_if_not_installed("duckdb", minimum_version = "0.3.1")
skip_if_not_installed("dbplyr")

library(duckdb, quietly = TRUE)
library(dplyr, warn.conflicts = FALSE)

test_that("to_duckdb", {
  ds <- InMemoryDataset$create(example_data)

  expect_identical(
    ds %>%
      to_duckdb() %>%
      collect() %>%
      # factors don't roundtrip https://github.com/duckdb/duckdb/issues/1879
      select(!fct) %>%
      arrange(int),
    example_data %>%
      select(!fct) %>%
      arrange(int)
  )

  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      to_duckdb() %>%
      group_by(lgl) %>%
      summarise(mean_int = mean(int, na.rm = TRUE), mean_dbl = mean(dbl, na.rm = TRUE)) %>%
      collect() %>%
      arrange(mean_int),
    tibble::tibble(
      lgl = c(TRUE, NA, FALSE),
      mean_int = c(3, 6.25, 8.5),
      mean_dbl = c(3.1, 6.35, 6.1)
    )
  )

  # can group_by before the to_duckdb
  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      group_by(lgl) %>%
      to_duckdb() %>%
      summarise(mean_int = mean(int, na.rm = TRUE), mean_dbl = mean(dbl, na.rm = TRUE)) %>%
      collect() %>%
      arrange(mean_int),
    tibble::tibble(
      lgl = c(TRUE, NA, FALSE),
      mean_int = c(3, 6.25, 8.5),
      mean_dbl = c(3.1, 6.35, 6.1)
    )
  )
})

test_that("to_duckdb then to_arrow", {
  ds <- InMemoryDataset$create(example_data)

  ds_rt <- ds %>%
    to_duckdb() %>%
    # factors don't roundtrip https://github.com/duckdb/duckdb/issues/1879
    select(-fct) %>%
    to_arrow()

  expect_identical(
    collect(ds_rt),
    ds %>%
      select(-fct) %>%
      collect()
  )

  # And we can continue the pipeline
  ds_rt <- ds %>%
    to_duckdb() %>%
    # factors don't roundtrip https://github.com/duckdb/duckdb/issues/1879
    select(-fct) %>%
    to_arrow() %>%
    filter(int > 5)

  expect_identical(
    ds_rt %>%
      collect() %>%
      arrange(int),
    ds %>%
      select(-fct) %>%
      filter(int > 5) %>%
      collect() %>%
      arrange(int)
  )

  # Now check errors
  ds_rt <- ds %>%
    to_duckdb() %>%
    # factors don't roundtrip https://github.com/duckdb/duckdb/issues/1879
    select(-fct)

  # alter the class of ds_rt's connection to simulate some other database
  class(ds_rt$src$con) <- "some_other_connection"

  expect_error(
    to_arrow(ds_rt),
    "to_arrow\\(\\) currently only supports Arrow tables, Arrow datasets,"
  )
})

test_that("to_arrow roundtrip, with dataset", {
  # these will continue to error until 0.3.2 is released
  # https://github.com/duckdb/duckdb/pull/2957
  skip_if_not_installed("duckdb", minimum_version = "0.3.2")
  # With a multi-part dataset
  tf <- tempfile()
  new_ds <- rbind(
    cbind(example_data, part = 1),
    cbind(example_data, part = 2),
    cbind(mutate(example_data, dbl = dbl * 3, dbl2 = dbl2 * 3), part = 3),
    cbind(mutate(example_data, dbl = dbl * 4, dbl2 = dbl2 * 4), part = 4)
  )
  write_dataset(new_ds, tf, partitioning = "part")

  ds <- open_dataset(tf)

  expect_identical(
    ds %>%
      to_duckdb() %>%
      select(-fct) %>%
      mutate(dbl_plus = dbl + 1) %>%
      to_arrow() %>%
      filter(int > 5 & part > 1) %>%
      collect() %>%
      arrange(part, int) %>%
      as.data.frame(),
    ds %>%
      select(-fct) %>%
      filter(int > 5 & part > 1) %>%
      mutate(dbl_plus = dbl + 1) %>%
      collect() %>%
      arrange(part, int)
  )
})

test_that("to_arrow roundtrip, with dataset (without wrapping)", {
  # these will continue to error until 0.3.2 is released
  # https://github.com/duckdb/duckdb/pull/2957
  skip_if_not_installed("duckdb", minimum_version = "0.3.2")
  # With a multi-part dataset
  tf <- tempfile()
  new_ds <- rbind(
    cbind(example_data, part = 1),
    cbind(example_data, part = 2),
    cbind(mutate(example_data, dbl = dbl * 3, dbl2 = dbl2 * 3), part = 3),
    cbind(mutate(example_data, dbl = dbl * 4, dbl2 = dbl2 * 4), part = 4)
  )
  write_dataset(new_ds, tf, partitioning = "part")

  out <- open_dataset(tf) %>%
    to_duckdb() %>%
    select(-fct) %>%
    mutate(dbl_plus = dbl + 1) %>%
    to_arrow()

  expect_r6_class(out, "RecordBatchReader")
})

# The next set of tests use an already-extant connection to test features of
# persistence and querying against the table without using the `tbl` itself, so
# we need to create a connection separate from the ephemeral one that is made
# with arrow_duck_connection()
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "PRAGMA threads=2")
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

test_that("Joining, auto-cleanup enabled", {
  # ARROW-17643, ARROW-17818: A change in duckdb 0.5.0 caused this test to fail
  # TODO: ARROW-17809 Follow up with the latest duckdb release to solve the issue
  skip("ARROW-17818: Latest DuckDB causes this test to fail")

  ds <- InMemoryDataset$create(example_data)

  table_one_name <- "my_arrow_table_1"
  table_one <- to_duckdb(ds, con = con, table_name = table_one_name)
  table_two_name <- "my_arrow_table_2"
  table_two <- to_duckdb(ds, con = con, table_name = table_two_name)

  res <- dbGetQuery(
    con,
    paste0(
      "SELECT * FROM ", table_one_name,
      " INNER JOIN ", table_two_name,
      " ON ", table_one_name, ".int = ", table_two_name, ".int"
    )
  )
  expect_identical(dim(res), c(9L, 14L))

  # clean up cleans up the tables
  expect_true(all(c(table_one_name, table_two_name) %in% duckdb::duckdb_list_arrow(con)))
  rm(table_one, table_two)
  gc()
  expect_false(any(c(table_one_name, table_two_name) %in% duckdb::duckdb_list_arrow(con)))
})

test_that("Joining, auto-cleanup disabled", {
  ds <- InMemoryDataset$create(example_data)

  table_three_name <- "my_arrow_table_3"
  table_three <- to_duckdb(ds, con = con, table_name = table_three_name, auto_disconnect = FALSE)

  # clean up does *not* clean these tables
  expect_true(table_three_name %in% duckdb::duckdb_list_arrow(con))
  rm(table_three)
  gc()
  # but because we aren't auto_disconnecting then we still have this table.
  expect_true(table_three_name %in% duckdb::duckdb_list_arrow(con))
})

test_that("to_duckdb with a table", {
  tab <- Table$create(example_data)

  expect_identical(
    tab %>%
      to_duckdb() %>%
      group_by(int > 4) %>%
      summarise(
        int_mean = mean(int, na.rm = TRUE),
        dbl_mean = mean(dbl, na.rm = TRUE)
      ) %>%
      collect(),
    tibble::tibble(
      "int > 4" = c(FALSE, NA, TRUE),
      int_mean = c(2, NA, 7.5),
      dbl_mean = c(2.1, 4.1, 7.3)
    )
  )
})

test_that("to_duckdb passing a connection", {
  ds <- InMemoryDataset$create(example_data)

  con_separate <- dbConnect(duckdb::duckdb())
  # we always want to test in parallel
  dbExecute(con_separate, "PRAGMA threads=2")
  on.exit(dbDisconnect(con_separate, shutdown = TRUE), add = TRUE)

  # create a table to join to that we know is in our con_separate
  new_df <- data.frame(
    int = 1:10,
    char = letters[26:17],
    stringsAsFactors = FALSE
  )
  DBI::dbWriteTable(con_separate, "separate_join_table", new_df)

  table_four <- ds %>%
    select(int, lgl, dbl) %>%
    to_duckdb(con = con_separate, auto_disconnect = FALSE)
  # dbplyr 2.2.0 renames this internal attribute to lazy_query
  table_four_name <- table_four$ops$x %||% table_four$lazy_query$x

  result <- DBI::dbGetQuery(
    con_separate,
    paste0(
      "SELECT * FROM ", table_four_name,
      " INNER JOIN separate_join_table ",
      "ON separate_join_table.int = ", table_four_name, ".int"
    )
  )

  expect_identical(dim(result), c(9L, 5L))
  expect_identical(result$char, new_df[new_df$int != 4, ]$char)
})
