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
library(stringr)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
tbl$some_negative <- tbl$int * (-1)^(1:nrow(tbl)) # nolint

test_that("filter() on is.na()", {
  compare_dplyr_binding(
    .input %>%
      filter(is.na(lgl)) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("filter() with NAs in selection", {
  compare_dplyr_binding(
    .input %>%
      filter(lgl) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("Filter returning an empty Table should not segfault (ARROW-8354)", {
  compare_dplyr_binding(
    .input %>%
      filter(false) %>%
      select(chr, int, lgl) %>%
      collect(),
    tbl
  )
})

test_that("filtering with expression", {
  char_sym <- "b"
  compare_dplyr_binding(
    .input %>%
      filter(chr == char_sym) %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
})

test_that("filtering with arithmetic", {
  compare_dplyr_binding(
    .input %>%
      filter(dbl + 1 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl / 2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl / 2L > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(int / 2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(int / 2L > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl %/% 2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl^2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )
})

test_that("filtering with expression + autocasting", {
  compare_dplyr_binding(
    .input %>%
      filter(dbl + 1 > 3L) %>% # test autocasting with comparison to 3L
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(int + 1 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(int^2 > 3) %>%
      select(string = chr, int, dbl) %>%
      collect(),
    tbl
  )
})

test_that("More complex select/filter", {
  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      select(chr, int, lgl) %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      collect(),
    tbl
  )
})

test_that("filter() with %in%", {
  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, chr %in% c("d", "f")) %>%
      collect(),
    tbl
  )
})

test_that("Negative scalar values", {
  compare_dplyr_binding(
    .input %>%
      filter(some_negative > -2) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      filter(some_negative %in% -1) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      filter(int == -some_negative) %>%
      collect(),
    tbl
  )
})

test_that("filter() with between()", {
  compare_dplyr_binding(
    .input %>%
      filter(between(dbl, 1, 2)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
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

  compare_dplyr_binding(
    .input %>%
      filter(between(dbl, 1, NA)) %>%
      collect(),
    tbl
  )
})

test_that("filter() with string ops", {
  skip_if_not_available("utf8proc")
  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, str_length(verses) > 25) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, str_length(str_trim(padded_strings, "left")) > 5) %>%
      collect(),
    tbl
  )
})

test_that("filter environment scope", {
  # "object 'b_var' not found"
  compare_dplyr_error(.input %>% filter(chr == b_var), tbl)

  b_var <- "b"
  compare_dplyr_binding(
    .input %>%
      filter(chr == b_var) %>%
      collect(),
    tbl
  )
  # Also for functions
  # 'could not find function "isEqualTo"' because we haven't defined it yet
  compare_dplyr_error(.input %>% filter(isEqualTo(int, 4)), tbl)

  # This works but only because there are S3 methods for those operations
  isEqualTo <- function(x, y) x == y & !is.na(x)
  compare_dplyr_binding(
    .input %>%
      select(-fct) %>% # factor levels aren't identical
      filter(isEqualTo(int, 4)) %>%
      collect(),
    tbl
  )
  # Try something that needs to call another nse_func
  compare_dplyr_binding(
    .input %>%
      select(-fct) %>%
      filter(nchar(padded_strings) < 10) %>%
      collect(),
    tbl
  )
  isShortString <- function(x) nchar(x) < 10
  skip("TODO: ARROW-14071")
  compare_dplyr_binding(
    .input %>%
      select(-fct) %>%
      filter(isShortString(padded_strings)) %>%
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

test_that("Filtering with unsupported functions", {
  compare_dplyr_binding(
    .input %>%
      filter(int > 2, pnorm(dbl) > .99) %>%
      collect(),
    tbl,
    warning = "Expression pnorm\\(dbl\\) > 0.99 not supported in Arrow; pulling data into R"
  )
  compare_dplyr_binding(
    .input %>%
      filter(
        nchar(chr, type = "bytes", allowNA = TRUE) == 1, # bad, Arrow msg
        int > 2, # good
        pnorm(dbl) > .99 # bad, opaque
      ) %>%
      collect(),
    tbl,
    warning = '\\* In nchar\\(chr, type = "bytes", allowNA = TRUE\\) == 1, allowNA = TRUE not supported in Arrow
\\* Expression pnorm\\(dbl\\) > 0.99 not supported in Arrow
pulling data into R'
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

  compare_dplyr_binding(
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
  compare_dplyr_binding(
    .input %>%
      filter(.data$dbl > 4) %>%
      # use "quoted" strings instead of .data pronoun where tidyselect is used
      # .data pronoun deprecated in select in tidyselect 1.2
      select("chr", "int", "lgl") %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(is.na(.data$lgl)) %>%
      select("chr", "int", "lgl") %>%
      collect(),
    tbl
  )

  # and the .env pronoun too!
  chr <- 4
  compare_dplyr_binding(
    .input %>%
      filter(.data$dbl > .env$chr) %>%
      select("chr", "int", "lgl") %>%
      collect(),
    tbl
  )
})

test_that("filter() with namespaced functions", {
  compare_dplyr_binding(
    .input %>%
      filter(dplyr::between(dbl, 1, 2)) %>%
      collect(),
    tbl
  )

  skip_if_not_available("utf8proc")
  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2, stringr::str_length(verses) > 25) %>%
      collect(),
    tbl
  )
})

test_that("filter() with across()", {
  compare_dplyr_binding(
    .input %>%
      filter(if_any(ends_with("l"), ~ is.na(.))) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(
        false == FALSE,
        if_all(everything(), ~ !is.na(.)),
        int > 2
      ) %>%
      collect(),
    tbl
  )
})
