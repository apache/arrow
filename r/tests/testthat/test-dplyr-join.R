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

library(dplyr, warn.conflicts = FALSE)

left <- example_data
left$some_grouping <- rep(c(1, 2), 5)

left_tab <- Table$create(left)

to_join <- tibble::tibble(
  some_grouping = c(1, 2),
  capital_letters = c("A", "B"),
  another_column = TRUE
)
to_join_tab <- Table$create(to_join)


test_that("left_join", {
  expect_message(
    compare_dplyr_binding(
      .input %>%
        left_join(to_join) %>%
        collect(),
      left
    ),
    'Joining, by = "some_grouping"'
  )
})

test_that("left_join `by` args", {
  compare_dplyr_binding(
    .input %>%
      left_join(to_join, by = "some_grouping") %>%
      collect(),
    left
  )
  compare_dplyr_binding(
    .input %>%
      left_join(
        to_join %>%
          rename(the_grouping = some_grouping),
        by = c(some_grouping = "the_grouping")
      ) %>%
      collect(),
    left
  )

  compare_dplyr_binding(
    .input %>%
      rename(the_grouping = some_grouping) %>%
      left_join(
        to_join,
        by = c(the_grouping = "some_grouping")
      ) %>%
      collect(),
    left
  )
})

test_that("join two tables", {
  expect_identical(
    left_tab %>%
      left_join(to_join_tab, by = "some_grouping") %>%
      collect(),
    left %>%
      left_join(to_join, by = "some_grouping") %>%
      collect()
  )
})

test_that("Error handling", {
  expect_error(
    left_tab %>%
      left_join(to_join, by = "not_a_col") %>%
      collect(),
    "all(names(by) %in% names(x)) is not TRUE",
    fixed = TRUE
  )
})

# TODO: test duplicate col names
# TODO: casting: int and float columns?

test_that("right_join", {
  compare_dplyr_binding(
    .input %>%
      right_join(to_join, by = "some_grouping") %>%
      collect(),
    left
  )
})

test_that("inner_join", {
  compare_dplyr_binding(
    .input %>%
      inner_join(to_join, by = "some_grouping") %>%
      collect(),
    left
  )
})

test_that("full_join", {
  compare_dplyr_binding(
    .input %>%
      full_join(to_join, by = "some_grouping") %>%
      collect(),
    left
  )
})

test_that("semi_join", {
  compare_dplyr_binding(
    .input %>%
      semi_join(to_join, by = "some_grouping") %>%
      collect(),
    left
  )
})

test_that("anti_join", {
  compare_dplyr_binding(
    .input %>%
      # Factor levels when there are no rows in the data don't match
      # TODO: use better anti_join test data
      select(-fct) %>%
      anti_join(to_join, by = "some_grouping") %>%
      collect(),
    left
  )
})

test_that("mutate then join", {
  left <- Table$create(
    one = c("a", "b"),
    two = 1:2
  )
  right <- Table$create(
    three = TRUE,
    dos = 2L
  )

  expect_equal(
    left %>%
      rename(dos = two) %>%
      mutate(one = toupper(one)) %>%
      left_join(
        right %>%
          mutate(three = !three)
      ) %>%
      arrange(dos) %>%
      collect(),
    tibble(
      one = c("A", "B"),
      dos = 1:2,
      three = c(NA, FALSE)
    )
  )
})
