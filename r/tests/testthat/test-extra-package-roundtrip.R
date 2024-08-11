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

library(tibble)

test_that("readr read csvs roundtrip", {
  load_or_skip("readr")

  tbl <- example_data[, c("dbl", "lgl", "false", "chr")]

  tf <- tempfile()
  on.exit(unlink(tf))
  write.csv(tbl, tf, row.names = FALSE)

  # we should still be able to turn this into a table
  new_df <- read_csv(tf, show_col_types = FALSE)
  expect_equal(tbl, as_tibble(arrow_table(new_df)))    

  # we should still be able to turn this into a table
  new_df <- read_csv(tf, show_col_types = FALSE, lazy = TRUE)
  expect_equal(tbl, as_tibble(arrow_table(new_df)))    
})

test_that("data.table objects roundtrip", {
  # indirection to avoid CHECK note and we don't otherwise need to Suggest this
  load_or_skip("data.table")

  DT <- as.data.table(example_data)

  # we should still be able to turn this into a table
  expect_equal(example_data, as_tibble(arrow_table(DT)))    

  # TODO: parquet write?  
})
