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

withr::local_options(list(
  arrow.summarise.sort = TRUE,
  rlib_warning_verbosity = "verbose",
  # This prevents the warning in `summarize()` about having grouped output without
  # also specifying what to do with `.groups`
  dplyr.summarise.inform = FALSE
))

library(dplyr, warn.conflicts = FALSE)
library(stringr)

skip_if_not_available("acero")

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
      summarize(
        mean = mean(int, na.rm = FALSE),
        mean2 = base::mean(int, na.rm = TRUE)
      ) %>%
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
      summarize(
        sd = sd(int, na.rm = FALSE),
        sd2 = stats::sd(int, na.rm = TRUE)
      ) %>%
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
      summarize(
        var = var(int, na.rm = FALSE),
        var2 = stats::var(int, na.rm = TRUE)
      ) %>%
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
      summarize(
        counts = n(),
        counts2 = dplyr::n()
      ) %>%
      arrange(some_grouping) %>%
      collect(),
    tbl
  )
})

test_that("Group by any/all", {
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        any(lgl, na.rm = TRUE),
        base::any(lgl, na.rm = TRUE)
      ) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        all(lgl, na.rm = TRUE),
        base::all(lgl, na.rm = TRUE)
      ) %>%
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

test_that("n_distinct() with many batches", {
  skip_if_not_available("parquet")

  tf <- tempfile()
  write_parquet(dplyr::starwars, tf, chunk_size = 20)

  ds <- open_dataset(tf)
  expect_equal(
    ds %>% summarise(n_distinct(sex, na.rm = FALSE)) %>% collect(),
    ds %>% collect() %>% summarise(n_distinct(sex, na.rm = FALSE))
  )
})

test_that("n_distinct() on dataset", {
  # With group_by
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
      summarize(
        distinct = n_distinct(lgl, na.rm = TRUE),
        distinct2 = dplyr::n_distinct(lgl, na.rm = TRUE)
      ) %>%
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
  # With zero arguments, n_distinct() will error in dplyr 1.1.0 too,
  # so use a Dataset to avoid the "pulling data into R" step that would
  # trigger a dplyr error
  skip_if_not_available("dataset")
  expect_snapshot(
    InMemoryDataset$create(tbl) %>%
      summarize(distinct = n_distinct()),
    error = TRUE
  )

  expect_snapshot_warning(
    as_record_batch(tbl) %>%
      summarize(distinct = n_distinct(int, lgl))
  )

  # Now that we've demonstrated that the whole machinery works, let's test
  # the agg_funcs directly
  expect_error(call_binding_agg("n_distinct"), "n_distinct() with 0 arguments", fixed = TRUE)
  expect_error(call_binding_agg("sum"), "sum() with 0 arguments", fixed = TRUE)
  expect_error(call_binding_agg("any"), "any() with 0 arguments", fixed = TRUE)
  expect_error(call_binding_agg("all"), "all() with 0 arguments", fixed = TRUE)
  expect_error(call_binding_agg("min"), "min() with 0 arguments", fixed = TRUE)
  expect_error(call_binding_agg("max"), "max() with 0 arguments", fixed = TRUE)
  expect_error(call_binding_agg("n_distinct", 1, 2), "Multiple arguments to n_distinct()")
  expect_error(call_binding_agg("sum", 1, 2), "Multiple arguments to sum")
  expect_error(call_binding_agg("any", 1, 2), "Multiple arguments to any()")
  expect_error(call_binding_agg("all", 1, 2), "Multiple arguments to all()")
  expect_error(call_binding_agg("min", 1, 2), "Multiple arguments to min()")
  expect_error(call_binding_agg("max", 1, 2), "Multiple arguments to max()")
})

test_that("median()", {
  # When medians are integer-valued, stats::median() sometimes returns output of
  # type integer, whereas whereas the Arrow approx_median kernels always return
  # output of type float64. The calls to median(int, ...) in the tests below
  # are enclosed in as.double() to work around this known difference.

  # with groups
  suppressWarnings(
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
    ),
    classes = "arrow.median.approximate"
  )
  # without groups, with na.rm = TRUE
  suppressWarnings(
    compare_dplyr_binding(
      .input %>%
        summarize(
          med_dbl_narmt = median(dbl, na.rm = TRUE),
          med_int_narmt = as.double(median(int, TRUE))
        ) %>%
        collect(),
      tbl,
      warning = "median\\(\\) currently returns an approximate median in Arrow"
    ),
    classes = "arrow.median.approximate"
  )
  # without groups, with na.rm = FALSE (the default)
  suppressWarnings(
    compare_dplyr_binding(
      .input %>%
        summarize(
          med_dbl = median(dbl),
          med_int = as.double(median(int)),
          med_dbl2 = stats::median(dbl),
          med_int2 = base::as.double(stats::median(int)),
          med_dbl_narmf = median(dbl, FALSE),
          med_int_narmf = as.double(median(int, na.rm = FALSE))
        ) %>%
        collect(),
      tbl,
      warning = "median\\(\\) currently returns an approximate median in Arrow"
    ),
    classes = "arrow.median.approximate"
  )
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

  # with groups
  suppressWarnings(
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
    ),
    classes = "arrow.quantile.approximate"
  )

  # without groups
  suppressWarnings(
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
    ),
    classes = "arrow.quantile.approximate"
  )

  # with missing values and na.rm = FALSE
  suppressWarnings(
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
    ),
    classes = "arrow.quantile.approximate"
  )

  # with a vector of 2+ probs
  expect_warning(
    Table$create(tbl) %>%
      summarize(q = quantile(dbl, probs = c(0.2, 0.8), na.rm = TRUE)),
    "quantile() with length(probs) != 1 not supported in Arrow",
    fixed = TRUE
  )
})

test_that("quantile() with namespacing", {
  suppressWarnings(
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
            q_dbl = stats::quantile(dbl, probs = 0.5, na.rm = TRUE),
            q_int = as.double(quantile(int, probs = 0.5, na.rm = TRUE))
          ) %>%
          arrange(some_grouping) %>%
          collect()
      ),
      "quantile() currently returns an approximate quantile in Arrow",
      fixed = TRUE
    ),
    classes = "arrow.quantile.approximate"
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
        max_int = max(int, na.rm = TRUE),
        min_int2 = base::min(int, na.rm = TRUE),
        max_int2 = base::max(int, na.rm = TRUE)
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
  withr::with_options(list(arrow.summarise.sort = FALSE), {
    # TODO(#29887 / ARROW-14313) sorting on dictionary columns not supported
    # so turn off arrow.summarise.sort so that we don't order_by fct after summarize
    compare_dplyr_binding(
      .input %>%
        group_by(fct) %>%
        summarize(
          min_chr = min(chr, na.rm = TRUE),
          max_chr = max(chr, na.rm = TRUE)
        ) %>%
        arrange(min_chr) %>%
        collect(),
      tbl,
    )
  })
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

test_that("Non-field variable references in aggregations", {
  tab <- arrow_table(x = 1:5)
  scale_factor <- 10
  expect_identical(
    tab %>%
      summarize(value = sum(x) / scale_factor) %>%
      collect(),
    tab %>%
      summarize(value = sum(x) / 10) %>%
      collect()
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

  # Aggregates on aggregates are not supported
  expect_warning(
    record_batch(tbl) %>% summarise(any(any(lgl))),
    paste(
      "Aggregate within aggregate expression",
      "any\\(any\\(lgl\\)\\) not supported in Arrow"
    )
  )

  # Check aggregates on aggeregates with more complex calls
  expect_warning(
    record_batch(tbl) %>% summarise(any(any(!lgl))),
    paste(
      "Aggregate within aggregate expression",
      "any\\(any\\(!lgl\\)\\) not supported in Arrow"
    )
  )
  expect_warning(
    record_batch(tbl) %>% summarise(!any(any(lgl))),
    paste(
      "Aggregate within aggregate expression",
      "any\\(any\\(lgl\\)\\) not supported in Arrow"
    )
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
    warning = paste(
      "Aggregate within aggregate expression sum\\(\\(dbl - mean\\(dbl\\)\\)\\^2\\)",
      "not supported in Arrow; pulling data into R"
    )
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        sum(dbl - mean(dbl))
      ) %>%
      collect(),
    tbl,
    warning = paste(
      "Aggregate within aggregate expression sum\\(dbl - mean\\(dbl\\)\\)",
      "not supported in Arrow; pulling data into R"
    )
  )
  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        sqrt(sum((dbl - mean(dbl))^2) / (n() - 1L))
      ) %>%
      collect(),
    tbl,
    warning = paste(
      "Aggregate within aggregate expression sum\\(\\(dbl - mean\\(dbl\\)\\)\\^2\\)",
      "not supported in Arrow; pulling data into R"
    )
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        dbl - mean(dbl)
      ) %>%
      collect(),
    tbl,
    warning = paste(
      "Expression dbl - mean\\(dbl\\) is not an aggregate expression",
      "or is not supported in Arrow; pulling data into R"
    )
  )

  compare_dplyr_binding(
    .input %>%
      group_by(some_grouping) %>%
      summarize(
        dbl
      ) %>%
      collect(),
    tbl,
    warning = paste(
      "Expression dbl is not an aggregate expression",
      "or is not supported in Arrow; pulling data into R"
    )
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
    warning = paste(
      "Expression dbl - int is not an aggregate expression",
      "or is not supported in Arrow; pulling data into R"
    )
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

test_that("summarise() can handle scalars and literal values", {
  some_scalar_value <- 2L

  compare_dplyr_binding(
    .input %>% summarise(y = 1L) %>% collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>% summarise(y = some_scalar_value) %>% collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>% summarise(y = !!some_scalar_value) %>% collect(),
    tbl
  )

  expect_identical(
    record_batch(tbl) %>% summarise(y = 1L) %>% collect(),
    tibble(y = 1L)
  )

  expect_identical(
    record_batch(tbl) %>% summarise(y = Expression$scalar(1L)) %>% collect(),
    tibble(y = 1L)
  )

  expect_identical(
    record_batch(tbl) %>% summarise(y = Scalar$create(1L)) %>% collect(),
    tibble(y = 1L)
  )

  expect_identical(
    record_batch(tbl) %>% summarise(y = some_scalar_value) %>% collect(),
    tibble(y = 2L)
  )

  expect_identical(
    record_batch(tbl) %>% summarise(y = !!some_scalar_value) %>% collect(),
    tibble(y = 2L)
  )
})

test_that("summarise() supports namespacing", {
  compare_dplyr_binding(
    .input %>%
      summarize(total = base::sum(int, na.rm = TRUE)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      summarise(
        log_total = sum(base::log(int) + 1, na.rm = TRUE)
      ) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      summarise(
        log_total = base::round(base::sum(base::log(int) + dbl, na.rm = TRUE))
      ) %>%
      collect(),
    tbl
  )
})

test_that("We don't add unnecessary ProjectNodes when aggregating", {
  tab <- Table$create(tbl)

  # Wrapper to simplify the tests
  expect_project_nodes <- function(query, n) {
    plan <- capture.output(query %>% show_query())
    expect_length(grep("ProjectNode", plan), n)
  }

  # 1 Projection: select int as `mean(int)` before aggregation
  expect_project_nodes(
    tab %>% summarize(mean(int)),
    1
  )

  # 0 Projections if
  # (a) input only contains the col you're aggregating, and
  # (b) the output col name is the same as the input name, and
  # (c) no grouping
  expect_project_nodes(
    tab[, "int"] %>% summarize(int = mean(int, na.rm = TRUE)),
    0
  )

  # 0 Projections if
  # (a) only nullary functions in summarize()
  # (b) no grouping
  expect_project_nodes(
    tab[, "int"] %>% summarize(n()),
    0
  )

  # Still just 1 projection
  expect_project_nodes(
    tab %>% group_by(lgl) %>% summarize(mean(int)),
    1
  )
  expect_project_nodes(
    tab %>% count(lgl),
    1
  )
})

test_that("Can use across() within summarise()", {
  compare_dplyr_binding(
    .input %>%
      group_by(lgl) %>%
      summarise(across(starts_with("dbl"), sum, .names = "sum_{.col}")) %>%
      arrange(lgl) %>%
      collect(),
    example_data
  )

  # across() doesn't work in summarise when input expressions evaluate to bare field references
  expect_warning(
    example_data %>%
      arrow_table() %>%
      group_by(lgl) %>%
      summarise(across(everything())) %>%
      collect(),
    regexp = "Expression int is not an aggregate expression or is not supported in Arrow; pulling data into R"
  )
})

test_that("across() does not select grouping variables within summarise()", {
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, chr) %>%
      group_by(chr) %>%
      summarise(across(everything(), sum)) %>%
      arrange(chr) %>%
      collect(),
    example_data
  )

  expect_error(
    example_data %>%
      select(int, dbl) %>%
      arrow_table() %>%
      group_by(int) %>%
      summarise(across(int, sum)),
    "Column `int` doesn't exist"
  )
})
