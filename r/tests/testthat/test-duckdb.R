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

skip_if_not_installed("duckdb")
library(duckdb)
library(dplyr)

test_that("basic integration", {
  rb <- record_batch(example_data)

  con <- dbConnect(duckdb::duckdb())
  # we always want to test in parallel
  dbExecute(con, "PRAGMA threads=2")

  duckdb_register_arrow(con, "my_recordbatch", rb)
  expect_identical(dbGetQuery(con, "SELECT count(*) FROM my_recordbatch")$`count_star()`, 10)

  dbDisconnect(con, shutdown=TRUE)
})

test_that("alchemize_to_duckdb", {
  ds <- InMemoryDataset$create(example_data)

  expect_identical(
    ds %>%
      alchemize_to_duckdb() %>%
      collect() %>%
      # factors don't roundtrip
      select(!fct),
    select(example_data, !fct)
  )

  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      alchemize_to_duckdb() %>%
      group_by(lgl) %>%
      summarise(mean_int = mean(int, na.rm = TRUE), mean_dbl = mean(dbl, na.rm = TRUE)) %>%
      collect(),
    tibble::tibble(
      lgl = c(TRUE, NA, FALSE),
      mean_int = c(3, 6.25, 8.5),
      mean_dbl = c(3.1, 6.35, 6.1)
    )
  )

  # can group_by before the alchemize_to_duckdb
  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      group_by(lgl) %>%
      alchemize_to_duckdb() %>%
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

test_that("Joining, auto-cleanup", {
  ds <- InMemoryDataset$create(example_data)

  con <- dbConnect(duckdb::duckdb())
  # we always want to test in parallel
  dbExecute(con, "PRAGMA threads=2")
  # add a table to keep the connection available
  dbWriteTable(con, "mtcars", mtcars)

  table_one <- alchemize_to_duckdb(ds, con = con)
  table_one_name <- as.character(table_one$ops$x)
  table_two <- alchemize_to_duckdb(ds, con = con)
  table_two_name <- as.character(table_two$ops$x)

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
  expect_setequal(DBI::dbListTables(con), c("mtcars", table_one_name, table_two_name))
  rm(table_one, table_two)
  gc()
  expect_equal(DBI::dbListTables(con), "mtcars")

  dbDisconnect(con, shutdown=TRUE)
})

test_that("Joining, auto-cleanup disabling", {
  ds <- InMemoryDataset$create(example_data)

  con <- dbConnect(duckdb::duckdb())
  table_one <- alchemize_to_duckdb(ds, con = con, auto_disconnect = FALSE)
  table_one_name <- as.character(table_one$ops$x)

  # clean up does *not* clean these tables
  expect_equal(DBI::dbListTables(con), table_one_name)
  rm(table_one)
  gc()
  # but because we aren't auto_disconnecting then we still have this table.
  expect_equal(DBI::dbListTables(con), table_one_name)

  dbDisconnect(con, shutdown=TRUE)
})


test_that("alchemize_to_duckdb passing a connection", {
  ds <- InMemoryDataset$create(example_data)

  con <- dbConnect(duckdb::duckdb())
  # we always want to test in parallel
  dbExecute(con, "PRAGMA threads=2")

  # mock out the dbConnect method to error
  # TODO: use mockr for this? As it stands, if there is an error, the unmocking
  # won't be called (and on.exit only works at the end of the file it appears)
  old_arrow_duck_connection <- arrow_duck_connection
  new_arrow_duck_connection <- function(...) stop("A new connection was attempted")
  assignInNamespace("arrow_duck_connection", new_arrow_duck_connection, "arrow")

  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      alchemize_to_duckdb(con = con) %>%
      group_by(lgl) %>%
      summarise(mean_int = mean(int, na.rm = TRUE), mean_dbl = mean(dbl, na.rm = TRUE)) %>%
      collect(),
    tibble::tibble(
      lgl = c(TRUE, NA, FALSE),
      mean_int = c(3, 6.25, 8.5),
      mean_dbl = c(3.1, 6.35, 6.1)
    )
  )

  assignInNamespace("arrow_duck_connection", old_arrow_duck_connection, "arrow")
  dbDisconnect(con, shutdown=TRUE)
})
