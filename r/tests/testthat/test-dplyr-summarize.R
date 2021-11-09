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

withr::local_options(list(arrow.summarise.sort = TRUE))

library(dplyr, warn.conflicts = FALSE)
library(stringr)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
tbl$some_grouping <- rep(c(1, 2), 5)

test_that("summarize() doesn't evaluate eagerly", {
  expect_s3_class(
    Table$create(tbl) %>%
      summarize(total = sum(int)),
    "arrow_dplyr_query"
  )
  expect_r6_class(
    Table$create(tbl) %>%
      summarize(total = sum(int)) %>%
      compute(),
    "ArrowTabular"
  )
})

test_that("Can aggregate in Arrow", {
  compare_dplyr_binding(
    .input %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      summarize(total = sum(int)) %>%
      collect(),
    tbl
  )
})

test_that("Group by sum on dataset", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int * 4, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int)) %>%
      collect(),
    tbl,
  )
})

test_that("Group by mean on dataset", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(mean = mean(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(mean = mean(int, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("Group by sd on dataset", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(sd = sd(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(sd = sd(int, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("Group by var on dataset", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(var = var(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(var = var(int, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("n()", {
  compare_dplyr_binding(
    .input %>%
      summarize(counts = n()) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(counts = n()) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("Group by any/all", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(all(lgl, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(all(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(has_words = nchar(verses) < 0) %>%
      group_by(some_grouping) %>%
      summarize(any(has_words, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      mutate(has_words = nchar(verses) < 0) %>%
      group_by(some_grouping) %>%
      summarize(all(has_words, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(has_words = all(nchar(verses) < 0, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("n_distinct() on dataset", {
  # With groupby
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(distinct = n_distinct(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(distinct = n_distinct(lgl, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  # Without groupby
  compare_dplyr_binding(
    .input %>%
      summarize(distinct = n_distinct(lgl, na.rm = FALSE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      summarize(distinct = n_distinct(lgl, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      summarize(distinct = n_distinct(int, lgl)) %>%
      collect(),
    tbl,
    warning = "Multiple arguments"
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(distinct = n_distinct(int, lgl)) %>%
      collect(),
    tbl,
    warning = "Multiple arguments"
  )
})

test_that("Functions that take ... but we only accept a single arg", {
  compare_dplyr_binding(
    .input %>%
      summarize(distinct = n_distinct()) %>%
      collect(),
    tbl,
    warning = "0 arguments"
  )
  compare_dplyr_binding(
    .input %>%
      summarize(distinct = n_distinct(int, lgl)) %>%
      collect(),
    tbl,
    warning = "Multiple arguments"
  )
  # Now that we've demonstrated that the whole machinery works, let's test
  # the agg_funcs directly
  expect_error(agg_funcs$n_distinct(), "n_distinct() with 0 arguments", fixed = TRUE)
  expect_error(agg_funcs$sum(), "sum() with 0 arguments", fixed = TRUE)
  expect_error(agg_funcs$any(), "any() with 0 arguments", fixed = TRUE)
  expect_error(agg_funcs$all(), "all() with 0 arguments", fixed = TRUE)
  expect_error(agg_funcs$min(), "min() with 0 arguments", fixed = TRUE)
  expect_error(agg_funcs$max(), "max() with 0 arguments", fixed = TRUE)
  expect_error(agg_funcs$n_distinct(1, 2), "Multiple arguments to n_distinct()")
  expect_error(agg_funcs$sum(1, 2), "Multiple arguments to sum")
  expect_error(agg_funcs$any(1, 2), "Multiple arguments to any()")
  expect_error(agg_funcs$all(1, 2), "Multiple arguments to all()")
  expect_error(agg_funcs$min(1, 2), "Multiple arguments to min()")
  expect_error(agg_funcs$max(1, 2), "Multiple arguments to max()")
})

test_that("median()", {
  # When medians are integer-valued, stats::median() sometimes returns output of
  # type integer, whereas whereas the Arrow approx_median kernels always return
  # output of type float64. The calls to median(int, ...) in the tests below
  # are enclosed in as.double() to work around this known difference.

  # Use old testthat behavior here so we don't have to assert the same warning
  # over and over
  local_edition(2)

  # with groups
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        med_dbl = median(dbl),
        med_int = as.double(median(int)),
        med_dbl_narmf = median(dbl, FALSE),
        med_int_narmf = as.double(median(int, na.rm = FALSE)),
        med_dbl_narmt = median(dbl, na.rm = TRUE),
        med_int_narmt = as.double(median(int, TRUE))
      ) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl,
    warning = "median\\(\\) currently returns an approximate median in Arrow"
  )
  # without groups, with na.rm = TRUE
  compare_dplyr_binding(
    .input %>%
      summarize(
        med_dbl_narmt = median(dbl, na.rm = TRUE),
        med_int_narmt = as.double(median(int, TRUE))
      ) %>%
      collect(),
    tbl,
    warning = "median\\(\\) currently returns an approximate median in Arrow"
  )
  # without groups, with na.rm = FALSE (the default)
  compare_dplyr_binding(
    .input %>%
      summarize(
        med_dbl = median(dbl),
        med_int = as.double(median(int)),
        med_dbl_narmf = median(dbl, FALSE),
        med_int_narmf = as.double(median(int, na.rm = FALSE))
      ) %>%
      collect(),
    tbl,
    warning = "median\\(\\) currently returns an approximate median in Arrow"
  )
  local_edition(3)
})

test_that("quantile()", {
  # The default method for stats::quantile() throws an error when na.rm = FALSE
  # and the input contains NA or NaN, whereas the Arrow tdigest kernels return
  # null in this situation. To work around this known difference, the tests
  # below always use na.rm = TRUE when the data contains NA or NaN.

  # The default method for stats::quantile() has an argument `names` that
  # controls whether the result has a names attribute. It defaults to
  # names = TRUE. With Arrow, it is not possible to give the result a names
  # attribute, so the quantile() binding in Arrow does not accept a `names`
  # argument. Differences in this names attribute cause compare_dplyr_binding() to
  # report that the objects are not equal, so we do not use compare_dplyr_binding()
  # in the tests below.

  # The tests below all use probs = 0.5 because other values cause differences
  # between the exact quantiles returned by R and the approximate quantiles
  # returned by Arrow.

  # When quantiles are integer-valued, stats::quantile() sometimes returns
  # output of type integer, whereas whereas the Arrow tdigest kernels always
  # return output of type float64. The calls to quantile(int, ...) in the tests
  # below are enclosed in as.double() to work around this known difference.

  local_edition(2)
  # with groups
  expect_warning(
    expect_equal(
      tbl %>%
        group_by(some_grouping) %>%
        summarize(
          q_dbl = quantile(dbl, probs = 0.5, na.rm = TRUE, names = FALSE),
          q_int = as.double(
            quantile(int, probs = 0.5, na.rm = TRUE, names = FALSE)
          )
        ) %>%
        arrange(some_grouping),
      Table$create(tbl) %>%
        group_by(some_grouping) %>%
        summarize(
          q_dbl = quantile(dbl, probs = 0.5, na.rm = TRUE),
          q_int = as.double(quantile(int, probs = 0.5, na.rm = TRUE))
        ) %>%
        arrange(some_grouping) %>%
        collect()
    ),
    "quantile() currently returns an approximate quantile in Arrow",
    fixed = TRUE
  )

  # without groups
  expect_warning(
    expect_equal(
      tbl %>%
        summarize(
          q_dbl = quantile(dbl, probs = 0.5, na.rm = TRUE, names = FALSE),
          q_int = as.double(
            quantile(int, probs = 0.5, na.rm = TRUE, names = FALSE)
          )
        ),
      Table$create(tbl) %>%
        summarize(
          q_dbl = quantile(dbl, probs = 0.5, na.rm = TRUE),
          q_int = as.double(quantile(int, probs = 0.5, na.rm = TRUE))
        ) %>%
        collect()
    ),
    "quantile() currently returns an approximate quantile in Arrow",
    fixed = TRUE
  )

  # with missing values and na.rm = FALSE
  expect_warning(
    expect_equal(
      tibble(
        q_dbl = NA_real_,
        q_int = NA_real_
      ),
      Table$create(tbl) %>%
        summarize(
          q_dbl = quantile(dbl, probs = 0.5, na.rm = FALSE),
          q_int = as.double(quantile(int, probs = 0.5, na.rm = FALSE))
        ) %>%
        collect()
    ),
    "quantile() currently returns an approximate quantile in Arrow",
    fixed = TRUE
  )
  local_edition(3)

  # with a vector of 2+ probs
  expect_warning(
    Table$create(tbl) %>%
      summarize(q = quantile(dbl, probs = c(0.2, 0.8), na.rm = TRUE)),
    "quantile() with length(probs) != 1 not supported in Arrow",
    fixed = TRUE
  )
})

test_that("summarize() with min() and max()", {
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      filter(int > 5) %>% # this filters out the NAs in `int`
      summarize(min_int = min(int), max_int = max(int)) %>%
      collect(),
    tbl,
  )
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      filter(int > 5) %>% # this filters out the NAs in `int`
      summarize(
        min_int = min(int + 4) / 2,
        max_int = 3 / max(42 - int)
      ) %>%
      collect(),
    tbl,
  )
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      summarize(min_int = min(int), max_int = max(int)) %>%
      collect(),
    tbl,
  )
  compare_dplyr_binding(
    .input %>%
      select(int) %>%
      summarize(
        min_int = min(int, na.rm = TRUE),
        max_int = max(int, na.rm = TRUE)
      ) %>%
      collect(),
    tbl,
  )
  compare_dplyr_binding(
    .input %>%
      select(dbl, int) %>%
      summarize(
        min_int = -min(log(ceiling(dbl)), na.rm = TRUE),
        max_int = log(max(as.double(int), na.rm = TRUE))
      ) %>%
      collect(),
    tbl,
  )

  # multiple dots arguments to min(), max() not supported
  compare_dplyr_binding(
    .input %>%
      summarize(min_mult = min(dbl, int)) %>%
      collect(),
    tbl,
    warning = "Multiple arguments to min\\(\\) not supported in Arrow"
  )
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, dbl2) %>%
      summarize(max_mult = max(int, dbl, dbl2)) %>%
      collect(),
    tbl,
    warning = "Multiple arguments to max\\(\\) not supported in Arrow"
  )

  # min(logical) or max(logical) yields integer in R
  # min(Boolean) or max(Boolean) yields Boolean in Arrow
  compare_dplyr_binding(
    .input %>%
      select(lgl) %>%
      summarize(
        max_lgl = as.logical(max(lgl, na.rm = TRUE)),
        min_lgl = as.logical(min(lgl, na.rm = TRUE))
      ) %>%
      collect(),
    tbl,
  )
})

test_that("min() and max() on character strings", {
  compare_dplyr_binding(
    .input %>%
      summarize(
        min_chr = min(chr, na.rm = TRUE),
        max_chr = max(chr, na.rm = TRUE)
      ) %>%
      collect(),
    tbl,
  )
  skip("Strings not supported by hash_min_max (ARROW-13988)")
  compare_dplyr_binding(
    .input %>%
      group_by(fct) %>%
      summarize(
        min_chr = min(chr, na.rm = TRUE),
        max_chr = max(chr, na.rm = TRUE)
      ) %>%
      collect(),
    tbl,
  )
})

test_that("summarise() with !!sym()", {
  test_chr_col <- "int"
  test_dbl_col <- "dbl"
  test_lgl_col <- "lgl"
  compare_dplyr_binding(
    .input %>%
      group_by(false) %>%
      summarise(
        sum = sum(!!sym(test_dbl_col)),
        any = any(!!sym(test_lgl_col)),
        all = all(!!sym(test_lgl_col)),
        mean = mean(!!sym(test_dbl_col)),
        sd = sd(!!sym(test_dbl_col)),
        var = var(!!sym(test_dbl_col)),
        n_distinct = n_distinct(!!sym(test_chr_col)),
        min = min(!!sym(test_dbl_col)),
        max = max(!!sym(test_dbl_col))
      ) %>%
      collect(),
    tbl
  )
})

test_that("Filter and aggregate", {
  compare_dplyr_binding(
    .input %>%
      filter(some_grouping == 2) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(int > 5) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(some_grouping == 2) %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(int > 5) %>%
      group_by(some_grouping) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("Group by edge cases", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping * 2) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      group_by(alt = some_grouping * 2) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("Do things after summarize", {
  group2_sum <- tbl %>%
    group_by(some_grouping) %>%
    filter(int > 5) %>%
    summarize(total = sum(int, na.rm = TRUE)) %>%
    pull() %>%
    tail(1)

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      filter(int > 5) %>%
      summarize(total = sum(int, na.rm = TRUE)) %>%
      filter(total == group2_sum) %>%
      mutate(extra = total * 5) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      filter(dbl > 2) %>%
      select(chr, int, lgl) %>%
      mutate(twice = int * 2L) %>%
      group_by(lgl) %>%
      summarize(
        count = n(),
        total = sum(twice, na.rm = TRUE)
      ) %>%
      mutate(mean = total / count) %>%
      collect(),
    tbl
  )
})

test_that("Expressions on aggregations", {
  # This is what it effectively is
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        any = any(lgl),
        all = all(lgl)
      ) %>%
      ungroup() %>% # TODO: loosen the restriction on mutate after group_by
      mutate(some = any & !all) %>%
      select(some_grouping, some) %>%
      collect(),
    tbl
  )
  # More concisely:
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(any(lgl) & !all(lgl)) %>%
      collect(),
    tbl
  )

  # Save one of the aggregates first
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        any_lgl = any(lgl),
        some = any_lgl & !all(lgl)
      ) %>%
      collect(),
    tbl
  )

  # Make sure order of columns in result is correct
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        any_lgl = any(lgl),
        some = any_lgl & !all(lgl),
        n()
      ) %>%
      collect(),
    tbl
  )

  # Aggregate on an aggregate (trivial but dplyr allows)
  skip("Aggregate on an aggregate not supported")
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        any_lgl = any(any(lgl))
      ) %>%
      collect(),
    tbl
  )
})

test_that("Summarize with 0 arguments", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize() %>%
      collect(),
    tbl
  )
})

test_that("Not (yet) supported: implicit join", {
  withr::local_options(list(arrow.debug = TRUE))
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        sum((dbl - mean(dbl))^2)
      ) %>%
      collect(),
    tbl,
    warning = "Expression sum\\(\\(dbl - mean\\(dbl\\)\\)\\^2\\) not supported in Arrow; pulling data into R"
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        sum(dbl - mean(dbl))
      ) %>%
      collect(),
    tbl,
    warning = "Expression sum\\(dbl - mean\\(dbl\\)\\) not supported in Arrow; pulling data into R"
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        sqrt(sum((dbl - mean(dbl))^2) / (n() - 1L))
      ) %>%
      collect(),
    tbl,
    warning = "Expression sum\\(\\(dbl - mean\\(dbl\\)\\)\\^2\\) not supported in Arrow; pulling data into R"
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        dbl - mean(dbl)
      ) %>%
      collect(),
    tbl,
    warning = "Expression dbl - mean\\(dbl\\) not supported in Arrow; pulling data into R"
  )

  # This one could possibly be supported--in mutate()
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        dbl - int
      ) %>%
      collect(),
    tbl,
    warning = "Expression dbl - int not supported in Arrow; pulling data into R"
  )
})

test_that(".groups argument", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n()) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "drop_last") %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "keep") %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "drop") %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping, int < 6) %>%
      summarize(count = n(), .groups = "rowwise") %>%
      collect(),
    tbl,
    warning = TRUE
  )

  # abandon_ship() raises the warning, then dplyr itself errors
  # This isn't ideal but it's fine and won't be an issue on Datasets
  expect_error(
    expect_warning(
      Table$create(tbl) %>%
        group_by(some_grouping, int < 6) %>%
        summarize(count = n(), .groups = "NOTVALID"),
      "Invalid .groups argument"
    ),
    "NOTVALID"
  )
})

test_that("summarize() handles group_by .drop", {
  # Error: Type error: Sorting not supported for type dictionary<values=string, indices=int8, ordered=0>
  withr::local_options(list(arrow.summarise.sort = FALSE))

  tbl <- tibble(
    x = 1:10,
    y = factor(rep(c("a", "c"), each = 5), levels = c("a", "b", "c"))
  )
  compare_dplyr_binding(
    .input %>%
      group_by(y) %>%
      count() %>%
      collect() %>%
      arrange(y),
    tbl
  )
  # Not supported: check message
  compare_dplyr_binding(
    .input %>%
      group_by(y, .drop = FALSE) %>%
      count() %>%
      collect() %>%
      # Because it's not supported, we have to filter out the (empty) row
      # that dplyr keeps, just so we test equal (otherwise)
      filter(y != "b") %>%
      arrange(y),
    tbl,
    warning = ".drop = FALSE currently not supported in Arrow aggregation"
  )

  # But this is ok because there is no factor group
  compare_dplyr_binding(
    .input %>%
      group_by(y, .drop = FALSE) %>%
      count() %>%
      collect() %>%
      arrange(y),
    tibble(
      x = 1:10,
      y = rep(c("a", "c"), each = 5)
    )
  )
})

test_that("summarise() passes through type information for temporary columns", {
  # applies to ifelse and case_when(), in which argument types are checked
  # within a translated function (previously this failed because the appropriate
  # schema was not available for n() > 1, mean(y), and mean(z))
  compare_dplyr_binding(
    .input %>%
      group_by(x) %>%
      summarise(r = if_else(n() > 1, mean(y), mean(z))) %>%
      collect(),
    tibble(
      x = c(0, 1, 1),
      y = c(2, 3, 5),
      z = c(8, 13, 21)
    )
  )
})
