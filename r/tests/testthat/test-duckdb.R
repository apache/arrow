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

con <- dbConnect(duckdb::duckdb())
# we always want to test in parallel
dbExecute(con, "PRAGMA threads=2")
on.exit(dbDisconnect(con))

test_that("basic integration", {
  rb <- record_batch(example_data)

  duckdb_register_arrow(con, "my_recordbatch", rb)
  expect_identical(dbGetQuery(con, "SELECT count(*) FROM my_recordbatch")$`count_star()`, 10)
})

test_that("alchemize", {
  ds <- InMemoryDataset$create(example_data)

  expect_identical(
    ds %>%
      alchemize() %>%
      collect() %>%
      # factors don't roundtrip
      select(!fct),
    select(example_data, !fct)
  )

  expect_identical(
    ds %>%
      select(int, lgl, dbl) %>%
      alchemize() %>%
      group_by(lgl) %>%
      summarise(mean_int = mean(int, na.rm = TRUE), mean_dbl = mean(dbl, na.rm = TRUE)) %>%
      collect(),
    tibble::tibble(
      lgl = c(FALSE, TRUE, NA),
      mean_int = c(8.5, 3, 6.25),
      mean_dbl = c(6.1, 3.1, 6.35)
    )
  )
})
