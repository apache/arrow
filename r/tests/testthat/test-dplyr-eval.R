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

test_that("user-defined function evaluation with mutate()", {
  nchar2 <- function(x) 1 + nchar(x)

  # simple expression
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  tibble::tibble(my_string = "1234") %>%
    arrow_table() %>%
    mutate(
      var1 = nchar(my_string),
      var2 = nchar2(my_string)) %>%
    collect()

  # a slightly more complicated expression
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar3 <- function(x) 2 + nchar(x)

  # multiple unknown calls in the same expression (to test the iteration)
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = nchar2(my_string) + nchar3(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  # user function defined using namespacing
  nchar4 <- function(x) 3 + base::nchar(x)

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar4(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar5 <- function(x) 1 + nchar(x)
  nchar6 <- function(x) 1 + nchar5(x)

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var6 = nchar6(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar7 <- function(x) 1 + nchar(x)
  nchar8 <- function(x) 3 + base::nchar(x)
  nchar9 <- function(x) 4 + nchar7(x) + nchar8(x)

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var9 = nchar9(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar10 <- function(x) 1 + nchar(x)
  nchar11 <- function(x) 1 + nchar10(x)
  nchar12 <- function(x) 6 + nchar11(x)

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var12 = nchar12(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  # TODO add test for a function that isn't namespaced and not present in the
  # caller environment
})

test_that("user-defined function evaluation with filter()", {
  # standard data frame for testing strings
  tbl <- example_data
  tbl$verses <- verses[[1]]
  tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
  tbl$some_grouping <- rep(c(1, 2), 5)

  isShortString <- function(x) nchar(x) < 10
  compare_dplyr_binding(
    .input %>%
      select(-fct) %>%
      filter(isShortString(padded_strings)) %>%
      collect(),
    tbl
  )
  nchar2 <- function(x) 1 + nchar(x)
  isShortString2 <- function(x) nchar2(x) < 10
  compare_dplyr_binding(
    .input %>%
      select(-fct) %>%
      filter(isShortString2(padded_strings)) %>%
      collect(),
    tbl
  )
})
