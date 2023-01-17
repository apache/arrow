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

left <- example_data
left$some_grouping <- rep(c(1, 2), 5)

to_join <- tibble::tibble(
  some_grouping = c(1, 2, 3),
  capital_letters = c("A", "B", "C"),
  another_column = TRUE
)

test_that("left_join with automatic grouping", {
  expect_identical(
    as_record_batch(left) %>%
      left_join(to_join) %>%
      collect(),
    left %>%
      left_join(to_join, by = "some_grouping") %>%
      collect()
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

test_that("left_join with join_by", {
  # only run this test in newer versions of dplyr that include `join_by()`
  skip_if_not(packageVersion("dplyr") >= "1.0.99.9000")

  compare_dplyr_binding(
    .input %>%
      left_join(to_join, join_by(some_grouping)) %>%
      collect(),
    left
  )
  compare_dplyr_binding(
    .input %>%
      left_join(
        to_join %>%
          rename(the_grouping = some_grouping),
        join_by(some_grouping == the_grouping)
      ) %>%
      collect(),
    left
  )

  compare_dplyr_binding(
    .input %>%
      rename(the_grouping = some_grouping) %>%
      left_join(
        to_join,
        join_by(the_grouping == some_grouping)
      ) %>%
      collect(),
    left
  )
})

test_that("join two tables", {
  expect_identical(
    arrow_table(left) %>%
      left_join(arrow_table(to_join), by = "some_grouping") %>%
      collect(),
    left %>%
      left_join(to_join, by = "some_grouping") %>%
      collect()
  )
})

test_that("Error handling", {
  expect_error(
    arrow_table(left) %>%
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

test_that("Error handling for unsupported expressions in join_by", {
  # only run this test in newer versions of dplyr that include `join_by()`
  skip_if_not(packageVersion("dplyr") >= "1.0.99.9000")

  expect_error(
    arrow_table(left) %>%
      left_join(to_join, join_by(some_grouping >= some_grouping)),
    "not supported"
  )

  expect_error(
    arrow_table(left) %>%
      left_join(to_join, join_by(closest(some_grouping >= some_grouping))),
    "not supported"
  )
})

# TODO: test duplicate col names
# TODO: casting: int and float columns?

test_that("right_join", {
  compare_dplyr_binding(
    .input %>%
      right_join(to_join, by = "some_grouping", keep = TRUE) %>%
      collect(),
    left
  )

  compare_dplyr_binding(
    .input %>%
      right_join(to_join, by = "some_grouping", keep = FALSE) %>%
      collect(),
    left
  )
})

test_that("inner_join", {
  compare_dplyr_binding(
    .input %>%
      inner_join(to_join, by = "some_grouping", keep = TRUE) %>%
      collect(),
    left
  )

  compare_dplyr_binding(
    .input %>%
      inner_join(to_join, by = "some_grouping", keep = FALSE) %>%
      collect(),
    left
  )
})

test_that("full_join", {
  compare_dplyr_binding(
    .input %>%
      full_join(to_join, by = "some_grouping", keep = TRUE) %>%
      collect(),
    left
  )

  compare_dplyr_binding(
    .input %>%
      full_join(to_join, by = "some_grouping", keep = FALSE) %>%
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
      # Use the ASCII version so we don't need utf8proc for this test
      mutate(one = arrow_ascii_upper(one)) %>%
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

test_that("suffix", {
  left_suf <- Table$create(
    key = c(1, 2),
    left_unique = c(2.1, 3.1),
    shared = c(10.1, 10.3)
  )

  right_suf <- Table$create(
    key = c(1, 2, 3, 10, 20),
    right_unique = c(1.1, 1.2, 3.1, 4.1, 4.3),
    shared = c(20.1, 30, 40, 50, 60)
  )

  join_op <- inner_join(left_suf, right_suf, by = "key", suffix = c("_left", "_right"))
  output <- collect(join_op)
  res_col_names <- names(output)
  expected_col_names <- c("key", "left_unique", "shared_left", "right_unique", "shared_right")
  expect_equal(expected_col_names, res_col_names)
})

test_that("suffix and implicit schema", {
  left_suf <- Table$create(
    key = c(1, 2),
    left_unique = c(2.1, 3.1),
    shared = c(10.1, 10.3)
  )

  right_suf <- Table$create(
    key = c(1, 2, 3, 10, 20),
    right_unique = c(1.1, 1.2, 3.1, 4.1, 4.3),
    shared = c(20.1, 30, 40, 50, 60)
  )

  join_op <- inner_join(left_suf, right_suf, by = "key", suffix = c("_left", "_right"))
  output <- collect(join_op)
  impl_schema <- implicit_schema(join_op)
  expect_equal(names(output), names(implicit_schema(join_op)))
})

test_that("summarize and join", {
  left_suf <- Table$create(
    key = c(1, 2, 1, 2),
    left_unique = c(2.1, 3.1, 4.1, 6.1),
    shared = c(10.1, 10.3, 10.2, 10.4)
  )

  right_suf <- Table$create(
    key = c(1, 2, 3, 10, 20),
    right_unique = c(1.1, 1.2, 3.1, 4.1, 4.3),
    shared = c(20.1, 30, 40, 50, 60)
  )

  joined <- left_suf %>%
    group_by(key) %>%
    summarize(left_unique = mean(left_unique), shared = mean(shared)) %>%
    inner_join(right_suf, by = "key", suffix = c("_left", "_right"))

  output <- collect(joined)
  res_col_names <- names(output)
  expected_col_names <- c("key", "left_unique", "shared_left", "right_unique", "shared_right")
  expect_equal(expected_col_names, res_col_names)
})

test_that("arrow dplyr query can join two datasets", {
  # ARROW-14908 and ARROW-15718
  skip_if_not_available("dataset")

  # By default, snappy encoding will be used, and
  # Snappy has a UBSan issue: https://github.com/google/snappy/pull/148
  skip_on_linux_devel()

  dir_out <- tempdir()

  quakes %>%
    select(stations, lat, long) %>%
    group_by(stations) %>%
    write_dataset(file.path(dir_out, "ds1"))

  quakes %>%
    select(stations, mag, depth) %>%
    group_by(stations) %>%
    write_dataset(file.path(dir_out, "ds2"))

  withr::with_options(
    list(arrow.use_threads = FALSE),
    {
      res <- open_dataset(file.path(dir_out, "ds1")) %>%
        left_join(open_dataset(file.path(dir_out, "ds2")), by = "stations") %>%
        collect() # We should not segfault here.
      expect_equal(nrow(res), 21872)
    }
  )
})

test_that("full joins handle keep", {
  full_data_df <- tibble::tibble(
    x = rep(c("a", "b"), each = 5),
    y = rep(1:5, 2),
    z = rep("zzz", 10),
    index = 1:10
  )
  small_dataset_df <- tibble::tibble(
    value = c(0.1, 0.2, 0.3, 0.4, 0.5),
    x = c(rep("a", 3), rep("b", 2)),
    y = 1:5,
    z = 6:10
  )

  compare_dplyr_binding(
    .input %>%
      full_join(full_data_df, by = c("y", "x"), keep = TRUE) %>%
      arrange(index) %>%
      collect(),
    small_dataset_df
  )

  compare_dplyr_binding(
    .input %>%
      full_join(full_data_df, by = c("y", "x"), keep = FALSE) %>%
      arrange(index) %>%
      collect(),
    small_dataset_df
  )
})
