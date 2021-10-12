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


test_that("abs()", {
  df <- tibble(x = c(-127, -10, -1, -0, 0, 1, 10, 127, NA))

  expect_dplyr_equal(
    input %>%
      transmute(abs = abs(x)) %>%
      collect(),
    df
  )
})

test_that("sign()", {
  df <- tibble(x = c(-127, -10, -1, -0, 0, 1, 10, 127, NA))

  expect_dplyr_equal(
    input %>%
      transmute(sign = sign(x)) %>%
      collect(),
    df
  )
})

test_that("ceiling(), floor(), trunc(), round()", {
  df <- tibble(x = c(-1, -0.55, -0.5, -0.1, 0, 0.1, 0.5, 0.55, 1, NA, NaN))

  expect_dplyr_equal(
    input %>%
      mutate(
        c = ceiling(x),
        f = floor(x),
        t = trunc(x),
        r = round(x)
      ) %>%
      collect(),
    df
  )

  # with digits set to 1
  expect_dplyr_equal(
    input %>%
      filter(x %% 0.5 == 0) %>% # filter out indeterminate cases (see below)
      mutate(r = round(x, 1)) %>%
      collect(),
    df
  )

  # with digits set to -1
  expect_dplyr_equal(
    input %>%
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

  expect_dplyr_equal(
    input %>%
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

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = exp(1))) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = 2)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = 10)) %>%
      collect(),
    df
  )

  # test log(, base = (length == 1))
  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = 5)) %>%
      collect(),
    df
  )

  # test log(, base = (length != 1))
  expect_error(
    nse_funcs$log(10, base = 5:6),
    "base must be a column or a length-1 numeric; other values not supported by Arrow",
    fixed = TRUE
  )

  # test log(x = (length != 1))
  expect_error(
    nse_funcs$log(10:11),
    "x must be a column or a length-1 numeric; other values not supported by Arrow",
    fixed = TRUE
  )

  # test log(, base = Expression)
  expect_dplyr_equal(
    input %>%
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
    expect_dplyr_equal(
      input %>%
        mutate(y = log(x, base = y)) %>%
        collect(),
      tibble(x = 1, y = 1)
    )
  )

  # log(n != 1, base = 1) is Inf in R and Arrow
  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = y)) %>%
      collect(),
    tibble(x = 10, y = 1)
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = logb(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log1p(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log2(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log10(x)) %>%
      collect(),
    df
  )
})

test_that("trig functions", {
  df <- tibble(x = c(seq(from = 0, to = 1, by = 0.1), NA))

  expect_dplyr_equal(
    input %>%
      mutate(y = sin(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = cos(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = tan(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = asin(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = acos(x)) %>%
      collect(),
    df
  )
})

test_that("arith functions ", {
  df <- tibble(x = c(1:5, NA))

  expect_dplyr_equal(
    input %>%
      transmute(
        int_div = x %/% 2,
        addition = x + 1,
        multiplication = x * 3,
        subtraction = x - 5,
        division = x / 2,
        power = x ^ 3,
        modulo = x %% 3
      ) %>%
      collect(),
    df
  )
})
