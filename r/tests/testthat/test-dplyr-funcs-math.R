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

test_that("abs()", {
  df <- tibble(x = c(-127, -10, -1, -0, 0, 1, 10, 127, NA))

  compare_dplyr_binding(
    .input %>%
      transmute(
        abs = abs(x),
        abs2 = base::abs(x)
      ) %>%
      collect(),
    df
  )
})

test_that("sign()", {
  df <- tibble(x = c(-127, -10, -1, -0, 0, 1, 10, 127, NA))

  compare_dplyr_binding(
    .input %>%
      transmute(
        sign = sign(x),
        sign2 = base::sign(x)
      ) %>%
      collect(),
    df
  )
})

test_that("ceiling(), floor(), trunc(), round()", {
  df <- tibble(x = c(-1, -0.55, -0.5, -0.1, 0, 0.1, 0.5, 0.55, 1, NA, NaN))

  compare_dplyr_binding(
    .input %>%
      mutate(
        c = ceiling(x),
        f = floor(x),
        t = trunc(x),
        r = round(x),
        c2 = base::ceiling(x),
        f2 = base::floor(x),
        t2 = base::trunc(x),
        r2 = base::round(x)
      ) %>%
      collect(),
    df
  )

  # with digits set to 1
  compare_dplyr_binding(
    .input %>%
      filter(x %% 0.5 == 0) %>% # filter out indeterminate cases (see below)
      mutate(r = round(x, 1)) %>%
      collect(),
    df
  )

  # with digits set to -1
  compare_dplyr_binding(
    .input %>%
      mutate(
        rd = round(floor(x * 111), -1), # double
        y = ifelse(is.nan(x), NA_integer_, x),
        ri = round(as.integer(y * 111), -1) # integer (with the NaN removed)
      ) %>%
      collect(),
    df
  )

  # round(x, -2) is equivalent to round_to_multiple(x, 100)
  expect_equal(
    Table$create(x = 1111.1) %>%
      mutate(r = round(x, -2)) %>%
      collect(),
    Table$create(x = 1111.1) %>%
      mutate(r = arrow_round_to_multiple(x, options = list(multiple = 100))) %>%
      collect()
  )

  # For consistency with base R, the binding for round() uses the Arrow
  # library's HALF_TO_EVEN round mode, but the expectations *above* would pass
  # even if another round mode were used. The expectations *below* should fail
  # with other round modes. However, some decimal numbers cannot be represented
  # exactly as floating point numbers, and for the ones that also end in 5 (such
  # as 0.55), R's rounding behavior is indeterminate: it will vary depending on
  # the OS. In practice, this seems to affect Windows, so we skip these tests
  # on Windows and on CRAN.

  skip_on_cran()
  skip_on_os("windows")

  compare_dplyr_binding(
    .input %>%
      mutate(r = round(x, 1)) %>%
      collect(),
    df
  )

  # Verify that round mode HALF_TO_EVEN, which is what the round() binding uses,
  # yields results consistent with R...
  expect_equal(
    as.vector(
      call_function(
        "round",
        Array$create(df$x),
        options = list(ndigits = 1L, round_mode = RoundMode$HALF_TO_EVEN)
      )
    ),
    round(df$x, 1)
  )
  # ...but that the round mode HALF_TOWARDS_ZERO does not. If the expectation
  # below fails, it means that the expectation above is not effectively testing
  # that Arrow is using the HALF_TO_EVEN mode.
  expect_false(
    isTRUE(all.equal(
      as.vector(
        call_function(
          "round",
          Array$create(df$x),
          options = list(ndigits = 1L, round_mode = RoundMode$HALF_TOWARDS_ZERO)
        )
      ),
      round(df$x, 1)
    ))
  )
})

test_that("log functions", {
  df <- tibble(x = c(1:10, NA, NA))

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = log(x),
        y2 = base::log(x)
      ) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = log(x, base = exp(1))) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = log(x, base = 2)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = log(x, base = 10)) %>%
      collect(),
    df
  )

  # test log(, base = (length == 1))
  compare_dplyr_binding(
    .input %>%
      mutate(y = log(x, base = 5)) %>%
      collect(),
    df
  )

  # test log(, base = (length != 1))
  expect_error(
    call_binding("log", 10, base = 5:6),
    "base must be a column or a length-1 numeric; other values not supported in Arrow",
    fixed = TRUE
  )

  # test log(x = (length != 1))
  expect_error(
    call_binding("log", 10:11),
    "x must be a column or a length-1 numeric; other values not supported in Arrow",
    fixed = TRUE
  )

  # test log(, base = Expression)
  compare_dplyr_binding(
    .input %>%
      # test cases where base = 1 below
      filter(x != 1) %>%
      mutate(
        y = log(x, base = x),
        z = log(2, base = x)
      ) %>%
      collect(),
    df
  )

  # log(1, base = 1) is NaN in both R and Arrow
  # suppress the R warning because R warns but Arrow does not
  suppressWarnings(
    compare_dplyr_binding(
      .input %>%
        mutate(y = log(x, base = y)) %>%
        collect(),
      tibble(x = 1, y = 1)
    )
  )

  # log(n != 1, base = 1) is Inf in R and Arrow
  compare_dplyr_binding(
    .input %>%
      mutate(y = log(x, base = y)) %>%
      collect(),
    tibble(x = 10, y = 1)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = logb(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = log1p(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = log2(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = log10(x)) %>%
      collect(),
    df
  )

  # with namespacing
  compare_dplyr_binding(
    .input %>%
      mutate(
        a = base::logb(x),
        b = base::log1p(x),
        c = base::log2(x),
        d = base::log10(x)
      ) %>%
      collect(),
    df
  )
})

test_that("trig functions", {
  df <- tibble(x = c(seq(from = 0, to = 1, by = 0.1), NA))

  compare_dplyr_binding(
    .input %>%
      mutate(y = sin(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = cos(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = tan(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = asin(x)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = acos(x)) %>%
      collect(),
    df
  )

  # with namespacing
  compare_dplyr_binding(
    .input %>%
      mutate(
        a = base::sin(x),
        b = base::cos(x),
        c = base::tan(x),
        d = base::asin(x),
        e = base::acos(x)
      ) %>%
      collect(),
    df
  )
})

test_that("arith functions ", {
  df <- tibble(x = c(1:5, NA))

  compare_dplyr_binding(
    .input %>%
      transmute(
        int_div = x %/% 2,
        addition = x + 1,
        multiplication = x * 3,
        subtraction = x - 5,
        division = x / 2,
        power = x^3,
        modulo = x %% 3
      ) %>%
      collect(),
    df
  )
})

test_that("floor division maintains type consistency with R", {
  df <- tibble(
    integers = c(1:4, NA_integer_),
    doubles = c(as.numeric(1:4), NA_real_)
  )

  compare_dplyr_binding(
    .input %>%
      transmute(
        int_div_dbl = integers %/% 2,
        int_div_int = integers %/% 2L,
        int_div_zero_int = integers %/% 0L,
        int_div_zero_dbl = integers %/% 0,
        dbl_div_dbl = doubles %/% 2,
        dbl_div_int = doubles %/% 2L,
        dbl_div_zero_int = doubles %/% 0L,
        dbl_div_zero_dbl = doubles %/% 0
      ) %>%
      collect(),
    df
  )
})

test_that("exp()", {
  df <- tibble(x = c(1:5, NA))

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = exp(x),
        y2 = base::exp(x)
      ) %>%
      collect(),
    df
  )
})

test_that("sqrt()", {
  df <- tibble(x = c(1:5, NA))

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = sqrt(x),
        y2 = base::sqrt(x)
      ) %>%
      collect(),
    df
  )
})
