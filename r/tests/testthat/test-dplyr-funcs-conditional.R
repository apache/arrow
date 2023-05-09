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
suppressPackageStartupMessages(library(bit64))

skip_if_not_available("acero")

tbl <- example_data
tbl$verses <- verses[[1]]
tbl$another_chr <- tail(letters, 10)

test_that("if_else and ifelse", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, 1, 0),
        y2 = dplyr::if_else(int > 6, 1, 0)
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, int, 0L)
      ) %>%
      collect(),
    tbl
  )

  expect_error(
    Table$create(tbl) %>%
      mutate(
        y = if_else(int > 5, 1, FALSE)
      ) %>%
      collect(),
    "NotImplemented: Function 'if_else' has no kernel matching input types"
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, 1, NA_real_)
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = ifelse(int > 5, 1, 0),
        y2 = base::ifelse(int > 6, 1, 0)
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(dbl > 5, TRUE, FALSE)
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(chr %in% letters[1:3], 1L, 3L)
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, "one", "zero")
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, chr, another_chr)
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, "true", chr, missing = "MISSING")
      ) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(int > 5, fct, factor("a"))
      ) %>%
      collect() %>%
      # Arrow if_else() kernel does not preserve unused factor levels,
      # so reset the levels of all the factor columns to make the test pass
      # (ARROW-14649)
      transmute(across(
        where(is.factor),
        ~ factor(.x, levels = c("a", "b", "c", "d", "g", "h", "i", "j"))
      )),
    tbl
  )

  # detecting NA and NaN works just fine
  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(is.na(dbl), chr, "false", missing = "MISSING")
      ) %>%
      collect(),
    example_data_for_sorting
  )

  # However, currently comparisons with NaNs return false and not NaNs or NAs
  skip("ARROW-13364")
  compare_dplyr_binding(
    .input %>%
      mutate(
        y = if_else(dbl > 5, chr, another_chr, missing = "MISSING")
      ) %>%
      collect(),
    example_data_for_sorting
  )

  skip("TODO: could? should? we support the autocasting in ifelse")
  compare_dplyr_binding(
    .input %>%
      mutate(y = ifelse(int > 5, 1, FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("case_when()", {
  compare_dplyr_binding(
    .input %>%
      transmute(cw = case_when(lgl ~ dbl, !false ~ dbl + dbl2)) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      mutate(cw = case_when(int > 5 ~ 1, TRUE ~ 0)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(cw = case_when(int > 5 ~ 1, .default = 0)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      transmute(cw = case_when(chr %in% letters[1:3] ~ 1L) + 41L) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      filter(case_when(
        dbl + int - 1.1 == dbl2 ~ TRUE,
        NA ~ NA,
        TRUE ~ FALSE
      ) & !is.na(dbl2)) %>%
      collect(),
    tbl
  )

  # with namespacing
  compare_dplyr_binding(
    .input %>%
      filter(dplyr::case_when(
        dbl + int - 1.1 == dbl2 ~ TRUE,
        NA ~ NA,
        TRUE ~ FALSE
      ) & !is.na(dbl2)) %>%
      collect(),
    tbl
  )

  # dplyr::case_when() errors if values on right side of formulas do not have
  # exactly the same type, but the Arrow case_when kernel allows compatible types
  expect_equal(
    tbl %>%
      mutate(i64 = as.integer64(1e10)) %>%
      Table$create() %>%
      transmute(cw = case_when(
        is.na(fct) ~ int,
        is.na(chr) ~ dbl,
        TRUE ~ i64
      )) %>%
      collect(),
    tbl %>%
      transmute(
        cw = ifelse(is.na(fct), int, ifelse(is.na(chr), dbl, 1e10))
      )
  )

  # expected errors (which are caught by abandon_ship() and changed to warnings)
  # TODO: Find a way to test these directly without abandon_ship() interfering
  expect_error(
    # no cases
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when()),
      "case_when"
    )
  )
  expect_error(
    # argument not a formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(TRUE ~ FALSE, TRUE)),
      "case_when"
    )
  )
  expect_error(
    # non-logical R scalar on left side of formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(0L ~ FALSE, TRUE ~ FALSE)),
      "case_when"
    )
  )
  expect_error(
    # non-logical Arrow column reference on left side of formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(int ~ FALSE)),
      "case_when"
    )
  )
  expect_error(
    # non-logical Arrow expression on left side of formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(dbl + 3.14159 ~ TRUE)),
      "case_when"
    )
  )

  expect_error(
    expect_warning(
      tbl %>%
        arrow_table() %>%
        mutate(cw = case_when(int > 5 ~ 1, .default = c(0, 1)))
    ),
    "`.default` must have size"
  )

  expect_warning(
    tbl %>%
      arrow_table() %>%
      mutate(cw = case_when(int > 5 ~ 1, .ptype = integer())),
    "not supported in Arrow"
  )

  expect_warning(
    tbl %>%
      arrow_table() %>%
      mutate(cw = case_when(int > 5 ~ 1, .size = 10)),
    "not supported in Arrow"
  )

  compare_dplyr_binding(
    .input %>%
      transmute(cw = case_when(lgl ~ "abc")) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      transmute(cw = case_when(lgl ~ verses, !false ~ paste(chr, chr))) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        cw = case_when(!(!(!(lgl))) ~ factor(chr), TRUE ~ fct)
      ) %>%
      collect(),
    tbl,
    warning = TRUE
  )
})

test_that("coalesce()", {
  # character
  df <- tibble(
    w = c(NA_character_, NA_character_, NA_character_),
    x = c(NA_character_, NA_character_, "c"),
    y = c(NA_character_, "b", "c"),
    z = c("a", "b", "c")
  )
  compare_dplyr_binding(
    .input %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect(),
    df
  )

  # with namespacing
  compare_dplyr_binding(
    .input %>%
      mutate(
        cw = dplyr::coalesce(w),
        cz = dplyr::coalesce(z),
        cwx = dplyr::coalesce(w, x),
        cwxy = dplyr::coalesce(w, x, y),
        cwxyz = dplyr::coalesce(w, x, y, z)
      ) %>%
      collect(),
    df
  )

  # factor
  df_fct <- df %>%
    transmute(across(everything(), ~ factor(.x, levels = c("a", "b", "c"))))
  compare_dplyr_binding(
    .input %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect() %>%
      # Arrow coalesce() kernel does not preserve unused factor levels,
      # so reset the levels of all the factor columns to make the test pass
      # (ARROW-14649)
      transmute(across(where(is.factor), ~ factor(.x, levels = c("a", "b", "c")))),
    df_fct
  )

  # integer
  df <- tibble(
    w = c(NA_integer_, NA_integer_, NA_integer_),
    x = c(NA_integer_, NA_integer_, 3L),
    y = c(NA_integer_, 2L, 3L),
    z = 1:3
  )
  compare_dplyr_binding(
    .input %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect(),
    df
  )

  # double with NaNs
  df <- tibble(
    w = c(NA_real_, NaN, NA_real_),
    x = c(NA_real_, NaN, 3.3),
    y = c(NA_real_, 2.2, 3.3),
    z = c(1.1, 2.2, 3.3)
  )

  # we can't use compare_dplyr_binding here as dplyr silently converts NaN to NA in coalesce()
  # see https://github.com/tidyverse/dplyr/issues/6833
  expect_identical(
    arrow_table(df) %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect(),
    mutate(
      df,
      cw = c(NA, NaN, NA),
      cz = c(1.1, 2.2, 3.3),
      cwx = c(NA, NaN, 3.3),
      cwxy = c(NA, 2.2, 3.3),
      cwxyz = c(1.1, 2.2, 3.3)
    )
  )

  # NaNs stay NaN and are not converted to NA in the results
  # (testing this requires expect_identical())
  expect_identical(
    df %>% Table$create() %>% mutate(cwx = coalesce(w, x)) %>% collect(),
    df %>% mutate(cwx = c(NA, NaN, 3.3))
  )
  expect_identical(
    df %>% Table$create() %>% transmute(cw = coalesce(w)) %>% collect(),
    df %>% transmute(cw = w)
  )
  expect_identical(
    df %>% Table$create() %>% transmute(cn = coalesce(NaN)) %>% collect(),
    df %>% transmute(cn = NaN)
  )
  # singles stay single
  expect_equal(
    (df %>%
      Table$create(schema = schema(
        w = float32(),
        x = float32(),
        y = float32(),
        z = float32()
      )) %>%
      transmute(c = coalesce(w, x, y, z)) %>%
      compute()
    )$schema[[1]]$type,
    float32()
  )
  # with R literal values
  expect_identical(
    arrow_table(df) %>%
      mutate(
        c1 = coalesce(4.4),
        c2 = coalesce(NA_real_),
        c3 = coalesce(NaN),
        c4 = coalesce(w, x, y, 5.5),
        c5 = coalesce(w, x, y, NA_real_),
        c6 = coalesce(w, x, y, NaN)
      ) %>%
      collect(),
    mutate(
      df,
      c1 = 4.4,
      c2 = NA_real_,
      c3 = NaN,
      c4 = c(5.5, 2.2, 3.3),
      c5 = c(NA, 2.2, 3.3),
      c6 = c(NaN, 2.2, 3.3)
    )
  )

  # no arguments
  expect_error(
    call_binding("coalesce"),
    "At least one argument must be supplied to coalesce()",
    fixed = TRUE
  )
})
