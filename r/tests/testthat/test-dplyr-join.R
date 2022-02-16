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
    "Join columns must be present in data"
  )

  # we print both message_x and message_y with an unnamed `by` vector
  expect_snapshot(
    left_join(
      arrow_table(example_data),
      arrow_table(example_data),
      by = "made_up_colname"
    ),
    error = TRUE
  )

  # we only print message_y as `int` is a column of x
  expect_snapshot(
    left_join(
      arrow_table(example_data),
      arrow_table(example_data),
      by = c("int" = "made_up_colname")
    ),
    error = TRUE
  )

  # we only print message_x as `int` is a column of y
  expect_snapshot(
    left_join(
      arrow_table(example_data),
      arrow_table(example_data),
      by = c("made_up_colname" = "int")
    ),
    error = TRUE
  )

  # we print both message_x and message_y
  expect_snapshot(
    left_join(
      arrow_table(example_data),
      arrow_table(example_data),
      by = c("made_up_colname1", "made_up_colname2")
    ),
    error = TRUE
  )

  expect_snapshot(
    left_join(
      arrow_table(example_data),
      arrow_table(example_data),
      by = c("made_up_colname1" = "made_up_colname2")
    ),
    error = TRUE
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

test_that("arrow dplyr query correctly mutates then joins", {
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

test_that("arrow dplyr query correctly filters then joins", {
  left <- Table$create(
    one = c("a", "b", "c"),
    two = 1:3
  )
  right <- Table$create(
    three = c(FALSE, TRUE, NA),
    dos = c(2L, 3L, 4L)
  )

  expect_equal(
    left %>%
      rename(dos = two) %>%
      filter(one %in% letters[1:2]) %>%
      left_join(
        right %>%
          filter(!is.na(three))
      ) %>%
      arrange(dos) %>%
      collect(),
    tibble(
      one = c("a", "b"),
      dos = 1:2,
      three = c(NA, FALSE)
    )
  )
})


test_that("arrow dplyr query can join with tibble", {
  # ARROW-14908
  dir_out <- tempdir()
  write_dataset(iris, file.path(dir_out, "iris"))
  species_codes <- data.frame(
    Species = c("setosa", "versicolor", "virginica"),
    code = c("SET", "VER", "VIR")
  )

  withr::with_options(
    list(arrow.use_threads = FALSE),
    {
      iris <- open_dataset(file.path(dir_out, "iris"))
      res <- left_join(iris, species_codes) %>% collect() # We should not segfault here.
      expect_equal(nrow(res), 150)
    }
  )
})
