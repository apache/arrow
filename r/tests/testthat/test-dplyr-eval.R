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

test_that("binding translation works", {
  nchar2 <- function(x) {
    1 + nchar(x)
  }

  # simple expression
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  # a slightly more complicated expression
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar3 <- function(x) {
    2 + nchar(x)
  }
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
  nchar4 <- function(x) {
    3 + base::nchar(x)
  }

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar4(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar5 <- function(x) {
    4 + nchar2(x)
  }

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var5 = nchar5(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar6 <- function(x) {
    4 + nchar2(x) + nchar4(x)
  }

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var6 = nchar6(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )
})
