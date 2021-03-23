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

library(dplyr)

tbl <- example_data_for_sorting

test_that("arrange", {
  expect_dplyr_equal(
    input %>%
      arrange(int, chr) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      arrange(dttm, int) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      arrange(int, desc(dbl)) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      arrange(int, desc(desc(dbl))) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      arrange(int) %>%
      arrange(desc(dbl)) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      arrange(int + dbl, chr) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      mutate(zzz = int + dbl,) %>%
      arrange(zzz, chr) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      mutate(zzz = int + dbl) %>%
      arrange(int + dbl, chr) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      mutate(int + dbl) %>%
      arrange(int + dbl, chr) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp) %>%
      arrange(int, dbl) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp) %>%
      arrange(int, dbl, .by_group = TRUE) %>%
      collect(),
    tbl %>%
      slice_sample(prop = 1L)
  )
  expect_dplyr_equal(
    input %>%
      group_by(grp, grp2) %>%
      arrange(int, dbl, .by_group = TRUE) %>%
      collect(),
    tbl %>%
      mutate(grp2 = ifelse(is.na(lgl), 1L, as.integer(lgl))) %>%
      slice_sample(prop = 1L)
  )
  expect_warning(
    expect_equal(
      tbl %>%
        slice_sample(prop = 1L) %>%
        Table$create() %>%
        arrange(abs(int), dbl) %>%
        collect(),
      tbl %>%
        slice_sample(prop = 1L) %>%
        arrange(abs(int), dbl) %>%
        collect()
    ),
    "not supported in Arrow",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      arrange(1),
    "does not contain any field names",
    fixed = TRUE
  )
})
