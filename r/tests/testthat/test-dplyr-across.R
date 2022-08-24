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

test_that("Can use across() within mutate()", {
  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), round)) %>%
      collect(),
    example_data
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        dbl2 = dbl * 2,
        across(c(dbl, dbl2), round),
        int2 = int * 2,
        dbl = dbl + 3
      ) %>%
      collect(),
    example_data
  )

  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), list(exp, sqrt))) %>%
      collect(),
    example_data
  )

  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), list("fun1" = round, "fun2" = sqrt))) %>%
      collect(),
    example_data
  )

  # this is valid is neither R nor Arrow
  expect_error(
    expect_warning(
      compare_dplyr_binding(
        .input %>%
          arrow_table() %>%
          mutate(across(c(dbl, dbl2), list("fun1" = round(sqrt(dbl))))) %>%
          collect(),
        example_data,
        warning = TRUE
      )
    )
  )

  # across() arguments not in default order
  compare_dplyr_binding(
    .input %>%
      mutate(across(.fns = round, c(dbl, dbl2))) %>%
      collect(),
    example_data
  )

  # across() with no columns named
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, dbl2) %>%
      mutate(across(.fns = round)) %>%
      collect(),
    example_data
  )

  # dynamic variable name
  int <- c("dbl", "dbl2")
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, dbl2) %>%
      mutate(across(all_of(int), sqrt)) %>%
      collect(),
    example_data
  )

  # .names argument
  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), round, .names = "{.col}.{.fn}")) %>%
      collect(),
    example_data
  )

  compare_dplyr_binding(
    .input %>%
      mutate(across(c(dbl, dbl2), round, .names = "round_{.col}")) %>%
      collect(),
    example_data
  )

  # ellipses (...) are a deprecated argument
  expect_error(
    example_data %>%
      arrow_table() %>%
      mutate(across(c(dbl, dbl2), round, digits = -1)) %>%
      collect(),
    regexp = "`...` argument to `across()` is deprecated in dplyr and not supported in Arrow",
    fixed = TRUE
  )

  # alternative ways of specifying .fns - as a list
  compare_dplyr_binding(
    .input %>%
      mutate(across(1:dbl2, list(round))) %>%
      collect(),
    example_data
  )

  # supply .fns as a one-item vector
  compare_dplyr_binding(
    .input %>%
      mutate(across(1:dbl2, c(round))) %>%
      collect(),
    example_data
  )

  # ARROW-17366: purrr-style lambda functions not yet supported
  expect_error(
    compare_dplyr_binding(
      .input %>%
        mutate(across(1:dbl2, ~ round(.x, digits = -1))) %>%
        collect(),
      example_data
    ),
    regexp = "purrr-style lambda functions as `.fns` argument to `across()` not yet supported in Arrow",
    fixed = TRUE
  )

  expect_error(
    compare_dplyr_binding(
      .input %>%
        mutate(across(1:dbl2, list(~ round(.x, digits = -1), ~ sqrt(.x)))) %>%
        collect(),
      example_data
    ),
    regexp = "purrr-style lambda functions as `.fns` argument to `across()` not yet supported in Arrow",
    fixed = TRUE
  )

  # .fns = NULL, the default
  compare_dplyr_binding(
    .input %>%
      mutate(across(1:dbl2, NULL)) %>%
      collect(),
    example_data
  )

  # ARROW-12778 - `where()` is not yet supported
  expect_error(
    compare_dplyr_binding(
      .input %>%
        mutate(across(where(is.double))) %>%
        collect(),
      example_data
    ),
    "Unsupported selection helper"
  )

  # gives the right error with window functions
  expect_warning(
    arrow_table(example_data) %>%
      mutate(
        x = int + 2,
        across(c("int", "dbl"), list(mean = mean, sd = sd, round)),
        exp(dbl2)
      ) %>%
      collect(),
    "window functions not currently supported in Arrow; pulling data into R",
    fixed = TRUE
  )
})
