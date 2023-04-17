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

skip_if_not_available("acero")

tbl <- example_data

test_that("slice_head/tail, ungrouped", {
  # head/tail are not deterministic in Arrow because data is unordered
  # so we can't assert identical to dplyr, just assert right number of rows
  tab <- arrow_table(tbl)
  expect_equal(
    tab %>%
      slice_head(n = 5) %>%
      nrow(),
    5
  )
  expect_equal(
    tab %>%
      slice_tail(n = 5) %>%
      nrow(),
    5
  )

  expect_equal(
    tab %>%
      slice_head(prop = .25) %>%
      nrow(),
    2
  )
  expect_equal(
    tab %>%
      slice_tail(prop = .25) %>%
      nrow(),
    2
  )
})

test_that("slice_min/max, ungrouped", {
  # with_ties must be FALSE
  tab <- arrow_table(tbl)
  expect_error(
    tab %>% slice_max(int, n = 5),
    "with_ties = TRUE"
  )
  expect_error(
    tab %>% slice_min(int, n = 5),
    "with_ties = TRUE"
  )
  compare_dplyr_binding(
    .input %>%
      slice_max(int, n = 4, with_ties = FALSE) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      slice_min(int, n = 4, with_ties = FALSE) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      slice_max(int, prop = .25, with_ties = FALSE) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      slice_min(int, prop = .25, with_ties = FALSE) %>%
      collect(),
    tbl
  )
})

test_that("slice_sample, ungrouped", {
  skip_if_not(CanRunWithCapturedR())

  tab <- arrow_table(tbl)
  expect_error(
    tab %>% slice_sample(replace = TRUE),
    "Sampling with replacement"
  )
  expect_error(
    tab %>% slice_sample(weight_by = dbl),
    "weight_by"
  )

  # Let's not take any chances on random failures
  skip_on_cran()
  # Because this is random (and we only have 10 rows), try several times
  for (i in 1:50) {
    sampled_prop <- tab %>%
      slice_sample(prop = .2) %>%
      collect() %>%
      nrow()
    if (sampled_prop == 2) break
  }
  expect_equal(sampled_prop, 2)

  # Test that slice_sample(n) returns n rows
  # With a larger dataset, we would be more confident to get exactly n
  # but with this dataset, we should at least not get >n rows
  sampled_n <- tab %>%
    slice_sample(n = 2) %>%
    collect() %>%
    nrow()
  expect_lte(sampled_n, 2)

  # Test with dataset, which matters for the UDF HACK
  skip_if_not_available("dataset")
  sampled_n <- tab %>%
    InMemoryDataset$create() %>%
    slice_sample(n = 2) %>%
    collect() %>%
    nrow()
  expect_lte(sampled_n, 2)
})

test_that("slice_* not supported with groups", {
  grouped <- tbl %>%
    arrow_table() %>%
    group_by(lgl)
  expect_error(
    slice_head(grouped, n = 5),
    "Slicing grouped data not supported in Arrow"
  )
  expect_error(
    slice_tail(grouped, n = 5),
    "Slicing grouped data not supported in Arrow"
  )
  expect_error(
    slice_min(grouped, int, n = 5),
    "Slicing grouped data not supported in Arrow"
  )
  expect_error(
    slice_max(grouped, int, n = 5),
    "Slicing grouped data not supported in Arrow"
  )
  expect_error(
    slice_sample(grouped, n = 5),
    "Slicing grouped data not supported in Arrow"
  )
})

test_that("input validation", {
  tab <- arrow_table(tbl)
  for (p in list("a", -1, 2, c(.01, .02), NA_real_)) {
    expect_error(
      slice_head(tab, prop = !!p),
      "`prop` must be a single numeric value between 0 and 1",
      fixed = TRUE
    )
  }

  expect_error(
    tab %>% slice_tail(n = 3, with_ties = FALSE),
    "`...` must be empty"
  )
})

test_that("n <-> prop conversion when nrow is not known", {
  joined <- tbl %>%
    arrow_table() %>%
    full_join(tbl, by = "int")
  expect_true(is.na(nrow(joined)))

  expect_error(
    joined %>%
      slice_min(int, prop = .25, with_ties = FALSE),
    "Slicing with `prop` when"
  )

  expect_error(
    joined %>%
      slice_sample(n = 5),
    "slice_sample() with `n` when",
    fixed = TRUE
  )
})

# TODO: handle edge case where prop = 1, do nothing?
