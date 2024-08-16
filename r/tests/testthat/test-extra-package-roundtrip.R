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

skip_on_cran()

# Any additional package that we test here that is not already in DESCRIPTION should be
# added to dev/tasks/r/github.linux.extra.packages.yml in the r-lib/actions/setup-r-dependencies@v2
# step so that they are installed + available in that CI job.

# So that we can force these in CI
load_or_skip <- function(pkg) {
  if (identical(tolower(Sys.getenv("ARROW_R_FORCE_EXTRA_PACKAGE_TESTS")), "true")) {
    # because of this indirection on the package name we also avoid a CHECK note and 
    # we don't otherwise need to Suggest this
    requireNamespace(pkg, quietly = TRUE)
  } else {
    skip_if(!requireNamespace(pkg, quietly = TRUE))
  }
  attachNamespace(pkg)
}

library(dplyr)

test_that("readr read csvs roundtrip", {
  load_or_skip("readr")

  tbl <- example_data[, c("dbl", "lgl", "false", "chr")]

  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  # we should still be able to turn this into a table
  new_df <- read_csv(tf, show_col_types = FALSE)
  expect_equal(new_df, as_tibble(arrow_table(new_df)))    

  # we should still be able to turn this into a table
  new_df <- read_csv(tf, show_col_types = FALSE, lazy = TRUE)
  expect_equal(new_df, as_tibble(arrow_table(new_df)))    

  # and can roundtrip to a parquet file
  pq_tmp_file <- tempfile()
  write_parquet(new_df, pq_tmp_file)
  new_df_read <- read_parquet(pq_tmp_file)

  # we should still be able to turn this into a table
  expect_equal(new_df, new_df_read)
})

test_that("data.table objects roundtrip", {
  load_or_skip("data.table")

  # https://github.com/Rdatatable/data.table/blob/83fd2c05ce2d8555ceb8ba417833956b1b574f7e/R/cedta.R#L25-L27
  .datatable.aware=TRUE

  DT <- as.data.table(example_data)

  # Table -> collect which is what writing + reading to parquet uses under the hood to roundtrip
  tab <- as_arrow_table(DT)
  DT_read <- collect(tab)

  # we should still be able to turn this into a table
  # the .internal.selfref attribute is automatically ignored by testthat/waldo
  expect_equal(DT, DT_read)

  # and we can set keys + indices + create new columns
  setkey(DT, chr)
  setindex(DT, dbl)
  DT[, dblshift := data.table::shift(dbl, 1)]

  # Table -> collect
  tab <- as_arrow_table(DT)
  DT_read <- collect(tab)

  # we should still be able to turn this into a table
  expect_equal(DT, DT_read)
})

test_that("units roundtrip", {
  load_or_skip("units")

  tbl <- example_data
  units(tbl$dbl) <- "s"

   # Table -> collect which is what writing + reading to parquet uses under the hood to roundtrip
  tab <- as_arrow_table(tbl)
  tbl_read <- collect(tab)

  # we should still be able to turn this into a table
  expect_equal(tbl, tbl_read)
})
