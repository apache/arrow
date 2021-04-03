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

test_that("sub and gsub", {
  df <- tibble(x = c("Foo", "bar"))

  for(fun in list(quote(sub), quote(gsub))) {

    for(fixed in c(TRUE, FALSE)) {

      expect_dplyr_equal(
        input %>%
          transmute(x = eval(fun)("Foo", "baz", x, fixed = fixed)) %>%
          collect(),
        df
      )
      expect_dplyr_equal(
        input %>%
          transmute(x = eval(fun)("Foo", "baz", x, fixed = fixed)) %>%
          collect(),
        df
      )
      expect_dplyr_equal(
        input %>%
          transmute(x = eval(fun)("^B.+", "baz", x, ignore.case = FALSE, fixed = fixed)) %>%
          collect(),
        df
      )
      expect_dplyr_equal(
        input %>%
          transmute(x = eval(fun)("Foo", "baz", x, ignore.case = FALSE, fixed = fixed)) %>%
          collect(),
        df
      )

      # the tests below all use ignore.case = TRUE
      # but base::sub and base::gsub ignore ignore.case = TRUE with a warning when fixed = TRUE
      # so we can't use expect_dplyr_equal() for the tests below
      expect_equal(
        df %>%
          Table$create() %>%
          transmute(x = eval(fun)("Foo", "baz", x, ignore.case = TRUE, fixed = fixed)) %>%
          collect(),
        tibble(x = c("baz", "bar"))
      )
      expect_equal(
        df %>%
          Table$create() %>%
          transmute(x = eval(fun)("o", "u", x, ignore.case = TRUE, fixed = fixed)) %>%
          collect(),
        if (fun == quote(sub)) tibble(x = c("Fuo", "bar")) else tibble(x = c("Fuu", "bar"))
      )
      expect_equal(
        df %>%
          Table$create() %>%
          transmute(x = eval(fun)("^B.+", "baz", x, ignore.case = TRUE, fixed = fixed)) %>%
          collect(),
        if (fixed) tibble(x = c("Foo", "bar")) else tibble(x = c("Foo", "baz"))
      )

    }

  }
})

test_that("str_replace and str_replace_all", {
  df <- tibble(x = c("Foo", "bar"))

  library(stringr)

  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace_all(x, regex("^F"), "baz")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace_all(x, fixed("o"), "u")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace(x, fixed("o"), "u")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace(x, fixed("O"), "u")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace(x, fixed("O", ignore_case = TRUE), "u")) %>%
      collect(),
    df
  )

  # TODO: add more tests of str_replace and str_replace_all
})
