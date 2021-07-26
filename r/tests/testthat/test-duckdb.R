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

skip_if_not_installed("duckdb", minimum_version = "0.2.8")
skip_if_not_installed("dbplyr")
library(duckdb)
library(dplyr)

test_that("to_duckdb", {
  ds <- InMemoryDataset$create(example_data)

  expect_identical(
    ds %>%
      to_duckdb() %>%
      collect() %>%
      # factors don't roundtrip
      select(!fct),
    select(example_data, !fct)
  )

  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      to_duckdb() %>%
      group_by(lgl) %>%
      summarise(mean_int = mean(int, na.rm = TRUE), mean_dbl = mean(dbl, na.rm = TRUE)) %>%
      collect(),
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
      collect(),
    tibble::tibble(
      lgl = c(TRUE, NA, FALSE),
      mean_int = c(3, 6.25, 8.5),
      mean_dbl = c(3.1, 6.35, 6.1)
    )
  )
})

test_that("summarise(..., .engine)", {
  ds <- InMemoryDataset$create(example_data)
  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      group_by(lgl) %>%
      summarise(
        mean_int = mean(int, na.rm = TRUE),
        mean_dbl = mean(dbl, na.rm = TRUE),
        .engine = "duckdb"
      ) %>%
      collect(),
    tibble::tibble(
      lgl = c(TRUE, NA, FALSE),
      mean_int = c(3, 6.25, 8.5),
      mean_dbl = c(3.1, 6.35, 6.1)
    )
  )
})

# The next set of tests use an already-extant connection to test features of
# persistence and querying against the table without using the `tbl` itself, so
# we need to create a connection separate from the ephemeral one that is made
# with arrow_duck_connection()
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "PRAGMA threads=2")
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

# write one table to the connection so it is kept open
DBI::dbWriteTable(con, "mtcars", mtcars)

test_that("Joining, auto-cleanup", {
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
  expect_true(all(c(table_one_name, table_two_name) %in% DBI::dbListTables(con)))
  rm(table_one, table_two)
  gc()
  expect_false(any(c(table_one_name, table_two_name) %in% DBI::dbListTables(con)))
})

test_that("Joining, auto-cleanup disabling", {
  ds <- InMemoryDataset$create(example_data)

  table_three_name <- "my_arrow_table_3"
  table_three <- to_duckdb(ds, con = con, table_name = table_three_name, auto_disconnect = FALSE)

  # clean up does *not* clean these tables
  expect_true(table_three_name %in% DBI::dbListTables(con))
  rm(table_three)
  gc()
  # but because we aren't auto_disconnecting then we still have this table.
  expect_true(table_three_name %in% DBI::dbListTables(con))
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
    char = letters[26:17]
  )
  DBI::dbWriteTable(con_separate, "separate_join_table", new_df)

  table_four <- ds %>%
    select(int, lgl, dbl) %>%
    to_duckdb(con = con_separate, auto_disconnect = FALSE)
  table_four_name <- table_four$ops$x

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
