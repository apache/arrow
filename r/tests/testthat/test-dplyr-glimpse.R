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

# The glimpse output for tests with `example_data` is different on R < 3.6
# because the `lgl` column is generated with `sample()` and the RNG
# algorithm is different in older R versions.
skip_on_r_older_than("3.6")

library(dplyr, warn.conflicts = FALSE)

test_that("glimpse() Table/ChunkedArray", {
  tab <- Table$create(example_data)
  expect_snapshot(glimpse(tab))
  expect_snapshot(glimpse(tab$chr))
})

test_that("glimpse() RecordBatch/Array", {
  batch <- RecordBatch$create(example_data)
  expect_snapshot(glimpse(batch))
  expect_snapshot(glimpse(batch$int))
})

test_that("glimpse() with VctrsExtensionType", {
  haven <- Table$create(haven_data)
  expect_snapshot(glimpse(haven))
  expect_snapshot(glimpse(haven[[3]]))
})

test_that("glimpse prints message about schema if there are complex types", {
  dictionary_but_no_metadata <- Table$create(a = 1:5, b = factor(1:5))
  expect_snapshot(glimpse(dictionary_but_no_metadata))
  # No message here
  expect_snapshot(glimpse(Table$create(a = 1)))
})

test_that("glimpse() calls print() instead of showing data for RBR", {
  expect_snapshot({
    example_data %>%
      as_record_batch_reader() %>%
      glimpse()
  })
  expect_snapshot({
    example_data %>%
      as_record_batch_reader() %>%
      select(int) %>%
      glimpse()
  })
})

skip_if_not_available("dataset")
big_df <- rbind(
  cbind(df1, group = 1),
  cbind(df2, group = 2)
)
ds_dir <- make_temp_dir()
write_dataset(big_df, ds_dir, partitioning = "group")

ds <- open_dataset(ds_dir)

test_that("glimpse() on Dataset", {
  expect_snapshot(glimpse(ds))
})

test_that("glimpse() on Dataset query only shows data for streaming eval", {
  # Because dataset scan row order is not deterministic, we can't snapshot
  # the whole output. Instead check for an indication that glimpse method ran
  # instead of the regular print() method that is the fallback
  expect_output(
    ds %>%
      select(int, chr) %>%
      filter(int > 2) %>%
      mutate(twice = int * 2) %>%
      glimpse(),
    "Call `print()` for query details",
    fixed = TRUE
  )

  # This doesn't show the data and falls back to print()
  expect_snapshot({
    ds %>%
      summarize(max(int)) %>%
      glimpse()
  })
})

test_that("glimpse() on in-memory query shows data even if aggregating", {
  expect_snapshot({
    example_data %>%
      arrow_table() %>%
      summarize(sum(int, na.rm = TRUE)) %>%
      glimpse()
  })
})
