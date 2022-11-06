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

test_that("expand_across correctly expands quosures", {

  # single unnamed function
  expect_across_equal(
    quos(across(c(dbl, dbl2), round)),
    quos(
      dbl = round(dbl),
      dbl2 = round(dbl2)
    ),
    example_data
  )

  # multiple unnamed functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), list(exp, sqrt))),
    quos(
      dbl_1 = exp(dbl),
      dbl_2 = sqrt(dbl),
      dbl2_1 = exp(dbl2),
      dbl2_2 = sqrt(dbl2)
    ),
    example_data
  )

  # single named function
  expect_across_equal(
    quos(across(c(dbl, dbl2), list("fun1" = round))),
    quos(
      dbl_fun1 = round(dbl),
      dbl2_fun1 = round(dbl2)
    ),
    example_data
  )

  # multiple named functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), list("fun1" = round, "fun2" = sqrt))),
    quos(
      dbl_fun1 = round(dbl),
      dbl_fun2 = sqrt(dbl),
      dbl2_fun1 = round(dbl2),
      dbl2_fun2 = sqrt(dbl2)
    ),
    example_data
  )

  # mix of named and unnamed functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), list(round, "fun2" = sqrt))),
    quos(
      dbl_1 = round(dbl),
      dbl_fun2 = sqrt(dbl),
      dbl2_1 = round(dbl2),
      dbl2_fun2 = sqrt(dbl2)
    ),
    example_data
  )

  # across() with no functions returns columns unchanged
  expect_across_equal(
    quos(across(starts_with("dbl"))),
    quos(
      dbl = dbl,
      dbl2 = dbl2
    ),
    example_data
  )

  # across() arguments not in default order
  expect_across_equal(
    quos(across(.fns = round, c(dbl, dbl2))),
    quos(
      dbl = round(dbl),
      dbl2 = round(dbl2)
    ),
    example_data
  )

  # across() with no columns named
  expect_across_equal(
    quos(across(.fns = round)),
    quos(
      int = round(int),
      dbl = round(dbl),
      dbl2 = round(dbl2)
    ),
    example_data %>% select(int, dbl, dbl2)
  )

  # column selection via dynamic variable name
  int <- c("dbl", "dbl2")
  expect_across_equal(
    quos(across(all_of(int), sqrt)),
    quos(
      dbl = sqrt(dbl),
      dbl2 = sqrt(dbl2)
    ),
    example_data
  )

  # ellipses (...) are a deprecated argument
  expect_error(
    expand_across(
      example_data,
      quos(across(c(dbl, dbl2), round, digits = -1))
    ),
    regexp = "`...` argument to `across()` is deprecated in dplyr and not supported in Arrow",
    fixed = TRUE
  )

  # alternative ways of specifying .fns - as a list
  expect_across_equal(
    quos(across(1:dbl2, list(round))),
    quos(
      int_1 = round(int),
      dbl_1 = round(dbl),
      dbl2_1 = round(dbl2)
    ),
    example_data
  )

  # supply .fns as a one-item vector
  expect_across_equal(
    quos(across(1:dbl2, c(round))),
    quos(
      int_1 = round(int),
      dbl_1 = round(dbl),
      dbl2_1 = round(dbl2)
    ),
    example_data
  )

  # .names argument
  expect_across_equal(
    quos(across(c(dbl, dbl2), round, .names = "{.col}.{.fn}")),
    quos(
      dbl.1 = round(dbl),
      dbl2.1 = round(dbl2)
    ),
    example_data
  )

  # names argument with custom text
  expect_across_equal(
    quos(across(c(dbl, dbl2), round, .names = "round_{.col}")),
    quos(
      round_dbl = round(dbl),
      round_dbl2 = round(dbl2)
    ),
    example_data
  )

  # names argument supplied but no functions
  expect_across_equal(
    quos(across(starts_with("dbl"), .names = "new_{.col}")),
    quos(
      new_dbl = dbl,
      new_dbl2 = dbl2
    ),
    example_data
  )

  # .names argument and functions named
  expect_across_equal(
    quos(across(c(dbl, dbl2), list("my_round" = round, "my_exp" = exp), .names = "{.col}.{.fn}")),
    quos(
      dbl.my_round = round(dbl),
      dbl.my_exp = exp(dbl),
      dbl2.my_round = round(dbl2),
      dbl2.my_exp = exp(dbl2)
    ),
    example_data
  )

  # .names argument and mix of named and unnamed functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), list(round, "my_exp" = exp), .names = "{.col}.{.fn}")),
    quos(
      dbl.1 = round(dbl),
      dbl.my_exp = exp(dbl),
      dbl2.1 = round(dbl2),
      dbl2.my_exp = exp(dbl2)
    ),
    example_data
  )

  # dodgy .names specification
  expect_error(
    expand_across(
      example_data,
      quos(across(c(dbl, dbl2), list(round, "my_exp" = exp), .names = "zarg"))
    ),
    regexp = "`.names` specification must produce (number of columns * number of functions) names.",
    fixed = TRUE
  )

  # Using package name prefix (ARROW-17724)
  expect_across_equal(
    quos(across(c(dbl, dbl2), base::round)),
    quos(
      dbl = base::round(dbl),
      dbl2 = base::round(dbl2)
    ),
    example_data
  )

  expect_across_equal(
    quos(across(c(dbl, dbl2), c(base::round, base::sqrt))),
    quos(
      dbl_1 = base::round(dbl),
      dbl_2 = base::sqrt(dbl),
      dbl2_1 = base::round(dbl2),
      dbl2_2 = base::sqrt(dbl2)
    ),
    example_data
  )
})

test_that("purrr-style lambda functions are supported", {

  # using `.x` inside lambda functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), ~ round(.x, digits = 0))),
    quos(
      dbl = round(dbl, digits = 0),
      dbl2 = round(dbl2, digits = 0)
    ),
    example_data
  )

  # using `.` inside lambda functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), ~ round(., digits = 0))),
    quos(
      dbl = round(dbl, digits = 0),
      dbl2 = round(dbl2, digits = 0)
    ),
    example_data
  )

  # testing both `.` and `.x` in lambda functions
  expect_across_equal(
    quos(across(c(dbl, dbl2), c(~ round(.x, digits = 0), ~ . * 2))),
    quos(
      dbl_1 = round(dbl, digits = 0),
      dbl_2 = dbl * 2,
      dbl2_1 = round(dbl2, digits = 0),
      dbl2_2 = dbl2 * 2
    ),
    example_data
  )

  # internal function for lambda functions
  expect_identical(
    arrow:::expr_substitute(
      quote(~ round(.x * 2, digits = 0)),
      sym(".x"), sym("dbl2")
    ),
    quote(~ round(dbl2 * 2, digits = 0))
  )
})

test_that("ARROW-14071 - function(x)-style lambda functions are not supported", {
  expect_error(
    expand_across(as_adq(example_data), quos(across(.cols = c(dbl, dbl2), list(function(x) {
      head(x, 1)
    }, function(x) {
      head(x, 1)
    })))),
    regexp = "Anonymous functions are not yet supported in Arrow"
  )

  expect_error(
    expand_across(
      as_adq(example_data),
      quos(across(.cols = c(dbl, dbl2), function(x) {
        head(x, 1)
      }))
    ),
    regexp = "Anonymous functions are not yet supported in Arrow"
  )
})

test_that("if_all() and if_any() are supported", {

  expect_across_equal(
    quos(if_any(everything(), ~is.na(.x))),
    quos(is.na(int) | is.na(dbl) | is.na(dbl2) | is.na(lgl) | is.na(false) | is.na(chr) | is.na(fct)),
    example_data
  )

  expect_across_equal(
    quos(if_all(everything(), ~is.na(.x))),
    quos(is.na(int) & is.na(dbl) & is.na(dbl2) & is.na(lgl) & is.na(false) & is.na(chr) & is.na(fct)),
    example_data
  )

})
