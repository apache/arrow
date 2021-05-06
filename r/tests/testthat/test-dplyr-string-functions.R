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

skip_if_not_available("utf8proc")

library(dplyr)
library(stringr)

test_that("grepl with ignore.case = FALSE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar"))
  expect_dplyr_equal(
    input %>%
      filter(grepl("o", x, fixed = TRUE)) %>%
      collect(),
    df
  )
})

test_that("sub and gsub with ignore.case = FALSE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar"))
  expect_dplyr_equal(
    input %>%
      transmute(x = sub("Foo", "baz", x, fixed = TRUE)) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = gsub("o", "u", x, fixed = TRUE)) %>%
      collect(),
    df
  )
})

# many of the remainder of these tests require RE2
skip_if_not_available("re2")

test_that("grepl", {
  df <- tibble(x = c("Foo", "bar"))

  for(fixed in c(TRUE, FALSE)) {

    expect_dplyr_equal(
      input %>%
        filter(grepl("Foo", x, fixed = fixed)) %>%
        collect(),
      df
    )
    expect_dplyr_equal(
      input %>%
        transmute(x = grepl("^B.+", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )
    expect_dplyr_equal(
      input %>%
        filter(grepl("Foo", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )

  }

})

test_that("grepl with ignore.case = TRUE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar"))

  # base::grepl() ignores ignore.case = TRUE with a warning when fixed = TRUE,
  # so we can't use expect_dplyr_equal() for these tests
  expect_equal(
    df %>%
      Table$create() %>%
      filter(grepl("O", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = "Foo")
  )
  expect_equal(
    df %>%
      Table$create() %>%
      filter(x = grepl("^B.+", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = character(0))
  )

})

test_that("str_detect", {
  df <- tibble(x = c("Foo", "bar"))

  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, regex("^F"))) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_detect(x, regex("^f[A-Z]{2}", ignore_case = TRUE))) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_detect(x, regex("^f[A-Z]{2}", ignore_case = TRUE), negate = TRUE)) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, fixed("o"))) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, fixed("O"))) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, fixed("O", ignore_case = TRUE))) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, fixed("O", ignore_case = TRUE), negate = TRUE)) %>%
      collect(),
    df
  )

})

test_that("sub and gsub", {
  df <- tibble(x = c("Foo", "bar"))

  for(fixed in c(TRUE, FALSE)) {

    expect_dplyr_equal(
      input %>%
        transmute(x = sub("Foo", "baz", x, fixed = fixed)) %>%
        collect(),
      df
    )
    expect_dplyr_equal(
      input %>%
        transmute(x = sub("^B.+", "baz", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )
    expect_dplyr_equal(
      input %>%
        transmute(x = sub("Foo", "baz", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )

  }
})

test_that("sub and gsub with ignore.case = TRUE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar"))

  # base::sub() and base::gsub() ignore ignore.case = TRUE with a warning when
  # fixed = TRUE, so we can't use expect_dplyr_equal() for these tests
  expect_equal(
    df %>%
      Table$create() %>%
      transmute(x = sub("O", "u", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = c("Fuo", "bar"))
  )
  expect_equal(
    df %>%
      Table$create() %>%
      transmute(x = gsub("o", "u", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = c("Fuu", "bar"))
  )
  expect_equal(
    df %>%
      Table$create() %>%
      transmute(x = sub("^B.+", "baz", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    df # unchanged
  )

})

test_that("str_replace and str_replace_all", {
  df <- tibble(x = c("Foo", "bar"))

  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace_all(x, regex("^F"), "baz")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace(x, regex("^f[A-Z]{2}", ignore_case = TRUE), "baz")) %>%
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

})

test_that("backreferences in pattern", {
  skip("RE2 does not support backreferences in pattern (https://github.com/google/re2/issues/101)")
  df <- tibble(x = c("Foo", "bar"))

  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, regex("F([aeiou])\\1"))) %>%
      collect(),
    df
  )
})

test_that("backreferences (substitutions) in replacement", {
  df <- tibble(x = c("Foo", "bar"))

  expect_dplyr_equal(
    input %>%
      transmute(desc = sub(
        "(?:https?|ftp)://([^/\r\n]+)(/[^\r\n]*)?",
        "path `\\2` on server `\\1`",
        url
        )
      ) %>%
      collect(),
    tibble(url = "https://arrow.apache.org/docs/r/")
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace(x, regex("^(\\w)o(.*)", ignore_case = TRUE), "\\1\\2p")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace(x, regex("^(\\w)o(.*)", ignore_case = TRUE), "\\1\\2p")) %>%
      collect(),
    df
  )
})

test_that("edge cases", {

  # in case-insensitive fixed match/replace, test that "\\E" in the search
  # string and backslashes in the replacement string are interpreted literally.
  # this test does not use expect_dplyr_equal() because base::sub() and
  # base::grepl() do not support ignore.case = TRUE when fixed = TRUE.
  expect_equal(
    tibble(x = c("\\Q\\e\\D")) %>%
      Table$create() %>%
      filter(grepl("\\E", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = c("\\Q\\e\\D"))
  )
  expect_equal(
    tibble(x = c("\\Q\\e\\D")) %>%
      Table$create() %>%
      transmute(x = sub("\\E", "\\L", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = c("\\Q\\L\\D"))
  )

  # test that a user's "(?i)" prefix does not break the "(?i)" prefix that's
  # added in case-insensitive regex match/replace
  expect_dplyr_equal(
    input %>%
      filter(grepl("(?i)^[abc]{3}$", x, ignore.case = TRUE, fixed = FALSE)) %>%
      collect(),
    tibble(x = c("ABC"))
  )
  expect_dplyr_equal(
    input %>%
      transmute(x = sub("(?i)^[abc]{3}$", "123", x, ignore.case = TRUE, fixed = FALSE)) %>%
      collect(),
    tibble(x = c("ABC"))
  )

})

test_that("errors and warnings", {
  df <- tibble(x = c("Foo", "bar"))

  # These conditions generate an error, but abandon_ship() catches the error,
  # issues a warning, and pulls the data into R
  expect_warning(
    df %>%
      Table$create() %>%
      filter(str_detect(x, boundary(type = "character"))) %>%
      collect(),
    "not implemented"
  )
  expect_warning(
    df %>%
      Table$create() %>%
      mutate(x = str_replace_all(x, coll("o", locale = "en"), "ó")) %>%
      collect(),
    "not supported"
  )

  # This condition generates a warning
  expect_warning(
    df %>%
      Table$create() %>%
      transmute(x = str_replace_all(x, regex("o", multiline = TRUE), "u")),
    "Ignoring pattern modifier argument not supported in Arrow: \"multiline\""
  )
})
