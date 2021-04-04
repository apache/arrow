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
library(stringr)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2*(1:10)+1, side = "both")
tbl$some_negative <- tbl$int * (-1)^(1:nrow(tbl))

test_that("filter() on is.na()", {
  expect_dplyr_equal(
    input %>%
      filter(is.na(lgl)) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("filter() with NAs in selection", {
  expect_dplyr_equal(
    input %>%
      filter(lgl) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("Filter returning an empty Table should not segfault (ARROW-8354)", {
  expect_dplyr_equal(
    input %>%
      filter(false) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("filtering with expression", {
  char_sym <- "b"
  expect_dplyr_equal(
    input %>%
      filter(chr == char_sym) %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
})

test_that("filtering with arithmetic", {
  expect_dplyr_equal(
    input %>%
      filter(dbl + 1 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(dbl / 2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(dbl / 2L > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(int / 2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(int / 2L > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(dbl %/% 2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )
})

test_that("filtering with expression + autocasting", {
  expect_dplyr_equal(
    input %>%
      filter(dbl + 1 > 3L) %>% # test autocasting with comparison to 3L
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(int + 1 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )
})

test_that("More complex select/filter", {
  expect_dplyr_equal(
    input %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      select(chr, int, lgl) %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      collect(),
    tbl
  )
})

test_that("filter() with %in%", {
  expect_dplyr_equal(
    input %>%
      filter(dbl > 2, chr %in% c("d", "f")) %>%
      collect(),
    tbl
  )
})

test_that("Negative scalar values", {
  expect_dplyr_equal(
    input %>%
      filter(some_negative > -2) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      filter(some_negative %in% -1) %>%
      collect(),
    tbl
    )
  expect_dplyr_equal(
    input %>%
      filter(int == -some_negative) %>%
      collect(),
    tbl
  )
})


test_that("filter() with between()", {
  expect_dplyr_equal(
    input %>%
      filter(between(dbl, 1, 2)) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(between(dbl, 0.5, 2)) %>%
      collect(),
    tbl
  )

  expect_identical(
    tbl %>%
      record_batch() %>%
      filter(between(dbl, int, dbl2)) %>%
      collect(),
    tbl %>%
      filter(dbl >= int, dbl <= dbl2)
    )

  expect_error(
    tbl %>%
      record_batch() %>%
      filter(between(dbl, 1, "2")) %>%
      collect()
  )

  expect_error(
    tbl %>%
      record_batch() %>%
      filter(between(dbl, 1, NA)) %>%
      collect()
  )

  expect_error(
    tbl %>%
      record_batch() %>%
      filter(between(chr, 1, 2)) %>%
      collect()
  )

})

test_that("filter() with string ops", {
  skip_if_not_available("utf8proc")
  skip_if(getRversion() < "3.4.0", "R < 3.4")
  # Extra instrumentation to ensure that we're calling Arrow compute here
  # because many base R string functions implicitly call as.character,
  # which means they still work on Arrays but actually force data into R
  # 1) wrapper that raises a warning if as.character is called. Can't wrap
  #    the whole test because as.character apparently gets called in other
  #    (presumably legitimate) places
  # 2) Wrap the test in expect_warning(expr, NA) to catch the warning
  with_no_as_character <- function(expr) {
    trace(
      "as.character",
      tracer = quote(warning("as.character was called")),
      print = FALSE,
      where = toupper
    )
    on.exit(untrace("as.character", where = toupper))
    force(expr)
  }

  expect_warning(
    expect_dplyr_equal(
      input %>%
        filter(dbl > 2, with_no_as_character(toupper(chr)) %in% c("D", "F")) %>%
        collect(),
      tbl
    ),
  NA)

  expect_dplyr_equal(
    input %>%
      filter(dbl > 2, str_length(verses) > 25) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(dbl > 2, str_length(str_trim(padded_strings, "left")) > 5) %>%
      collect(),
    tbl
  )
})

test_that("filter environment scope", {
  # "object 'b_var' not found"
  expect_dplyr_error(input %>% filter(batch, chr == b_var))

  b_var <- "b"
  expect_dplyr_equal(
    input %>%
      filter(chr == b_var) %>%
      collect(),
    tbl
  )
  # Also for functions
  # 'could not find function "isEqualTo"' because we haven't defined it yet
  expect_dplyr_error(filter(batch, isEqualTo(int, 4)))

  skip("Need to substitute in user defined function too")
  # TODO: fix this: this isEqualTo function is eagerly evaluating; it should
  # instead yield array_expressions. Probably bc the parent env of the function
  # has the Ops.Array methods defined; we need to move it so that the parent
  # env is the data mask we use in the dplyr eval
  isEqualTo <- function(x, y) x == y & !is.na(x)
  expect_dplyr_equal(
    input %>%
      select(-fct) %>% # factor levels aren't identical
      filter(isEqualTo(int, 4)) %>%
      collect(),
    tbl
  )
})

test_that("Filtering on a column that doesn't exist errors correctly", {
  with_language("fr", {
    # expect_warning(., NA) because the usual behavior when it hits a filter
    # that it can't evaluate is to raise a warning, collect() to R, and retry
    # the filter. But we want this to error the first time because it's
    # a user error, not solvable by retrying in R
    expect_warning(
      expect_error(
        tbl %>% record_batch() %>% filter(not_a_col == 42) %>% collect(),
        "objet 'not_a_col' introuvable"
      ),
      NA
    )
  })
  with_language("en", {
    expect_warning(
      expect_error(
        tbl %>% record_batch() %>% filter(not_a_col == 42) %>% collect(),
        "object 'not_a_col' not found"
      ),
      NA
    )
  })
})

test_that("Filtering with a function that doesn't have an Array/expr method still works", {
  expect_warning(
    expect_dplyr_equal(
      input %>%
        filter(int > 2, pnorm(dbl) > .99) %>%
        collect(),
      tbl
    ),
    'Filter expression not implemented in Arrow: pnorm(dbl) > 0.99; pulling data into R',
    fixed = TRUE
  )
})

test_that("Calling Arrow compute functions 'directly'", {
  expect_equal(
    tbl %>%
      record_batch() %>%
      filter(arrow_add(dbl, 1) > 3L) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl %>%
      filter(dbl + 1 > 3L) %>%
      select(string = chr, int, dbl)
  )

  expect_dplyr_equal(
    tbl %>%
      record_batch() %>%
      filter(arrow_greater(arrow_add(dbl, 1), 3L)) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl %>%
      filter(dbl + 1 > 3L) %>%
      select(string = chr, int, dbl)
  )
})

test_that("filter() with .data pronoun", {
  expect_dplyr_equal(
    input %>%
      filter(.data$dbl > 4) %>%
      select(.data$chr, .data$int, .data$lgl) %>%
      collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      filter(is.na(.data$lgl)) %>%
      select(.data$chr, .data$int, .data$lgl) %>%
      collect(),
    tbl
  )

  # and the .env pronoun too!
  chr <- 4
  expect_dplyr_equal(
    input %>%
      filter(.data$dbl > .env$chr) %>%
      select(.data$chr, .data$int, .data$lgl) %>%
      collect(),
    tbl
  )

  # but there is an error if we don't override the masking with `.env`
  expect_dplyr_error(
    tbl %>%
      filter(.data$dbl > chr) %>%
      select(.data$chr, .data$int, .data$lgl) %>%
      collect()
  )
})
