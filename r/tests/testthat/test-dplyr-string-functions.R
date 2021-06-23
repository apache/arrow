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

  for (fixed in c(TRUE, FALSE)) {

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

  for (fixed in c(TRUE, FALSE)) {

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
      transmute(x = str_replace_all(x, "^F", "baz")) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      transmute(x = str_replace_all(x, regex("^F"), "baz")) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(x = str_replace(x, "^F[a-z]{2}", "baz")) %>%
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

test_that("strsplit and str_split", {

  df <- tibble(x = c("Foo and bar", "baz and qux and quux"))

  expect_dplyr_equal(
    input %>%
      mutate(x = strsplit(x, "and")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = strsplit(x, "and.*", fixed = TRUE)) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = strsplit(x, " +and +")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = str_split(x, "and")) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = str_split(x, "and", n = 2)) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = str_split(x, fixed("and"), n = 2)) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = str_split(x, regex("and"), n = 2)) %>%
      collect(),
    df
  )
  expect_dplyr_equal(
    input %>%
      mutate(x = str_split(x, "Foo|bar", n = 2)) %>%
      collect(),
    df
  )
})

test_that("arrow_*_split_whitespace functions", {

  # use only ASCII whitespace characters
  df_ascii <- tibble(x = c("Foo\nand bar", "baz\tand qux and quux"))

  # use only non-ASCII whitespace characters
  df_utf8 <- tibble(x = c("Foo\u00A0and\u2000bar", "baz\u2006and\u1680qux\u3000and\u2008quux"))

  df_split <- tibble(x = list(c("Foo", "and", "bar"), c("baz", "and", "qux", "and", "quux")))

  # use default option values
  expect_equivalent(
    df_ascii %>%
      Table$create() %>%
      mutate(x = arrow_ascii_split_whitespace(x)) %>%
      collect(),
    df_split
  )
  expect_equivalent(
    df_utf8 %>%
      Table$create() %>%
      mutate(x = arrow_utf8_split_whitespace(x)) %>%
      collect(),
    df_split
  )

  # specify non-default option values
  expect_equivalent(
    df_ascii %>%
      Table$create() %>%
      mutate(
        x = arrow_ascii_split_whitespace(x, options = list(max_splits = 1, reverse = TRUE))
      ) %>%
      collect(),
    tibble(x = list(c("Foo\nand", "bar"), c("baz\tand qux and", "quux")))
  )
  expect_equivalent(
    df_utf8 %>%
      Table$create() %>%
      mutate(
        x = arrow_utf8_split_whitespace(x, options = list(max_splits = 1, reverse = TRUE))
      ) %>%
      collect(),
    tibble(x = list(c("Foo\u00A0and", "bar"), c("baz\u2006and\u1680qux\u3000and", "quux")))
  )
})

test_that("errors and warnings in string splitting", {
  # These conditions generate an error, but abandon_ship() catches the error,
  # issues a warning, and pulls the data into R (if computing on InMemoryDataset)
  # Elsewhere we test that abandon_ship() works,
  # so here we can just call the functions directly

  x <- Expression$field_ref("x")
  expect_error(
    nse_funcs$str_split(x, fixed("and", ignore_case = TRUE)),
    "Case-insensitive string splitting not supported by Arrow"
  )
  expect_error(
    nse_funcs$str_split(x, coll("and.?")),
    "Pattern modifier `coll()` not supported by Arrow",
    fixed = TRUE
  )
  expect_error(
    nse_funcs$str_split(x, boundary(type = "word")),
    "Pattern modifier `boundary()` not supported by Arrow",
    fixed = TRUE
  )
  expect_error(
    nse_funcs$str_split(x, "and", n = 0),
    "Splitting strings into zero parts not supported by Arrow"
  )

  # This condition generates a warning
  expect_warning(
    nse_funcs$str_split(x, fixed("and"), simplify = TRUE),
    "Argument 'simplify = TRUE' will be ignored"
  )
})

test_that("errors and warnings in string detection and replacement", {
  x <- Expression$field_ref("x")

  expect_error(
    nse_funcs$str_detect(x, boundary(type = "character")),
    "Pattern modifier `boundary()` not supported by Arrow",
    fixed = TRUE
  )
  expect_error(
    nse_funcs$str_replace_all(x, coll("o", locale = "en"), "ó"),
    "Pattern modifier `coll()` not supported by Arrow",
    fixed = TRUE
  )

  # This condition generates a warning
  expect_warning(
    nse_funcs$str_replace_all(x, regex("o", multiline = TRUE), "u"),
    "Ignoring pattern modifier argument not supported in Arrow: \"multiline\""
  )

})

test_that("backreferences in pattern in string detection", {
  skip("RE2 does not support backreferences in pattern (https://github.com/google/re2/issues/101)")
  df <- tibble(x = c("Foo", "bar"))

  expect_dplyr_equal(
    input %>%
      filter(str_detect(x, regex("F([aeiou])\\1"))) %>%
      collect(),
    df
  )
})

test_that("backreferences (substitutions) in string replacement", {
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
      transmute(x = str_replace(x, "^(\\w)o(.*)", "\\1\\2p")) %>%
      collect(),
    df
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

test_that("edge cases in string detection and replacement", {

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

test_that("strptime", {

  t_string <- tibble(x = c("2018-10-07 19:04:05", NA))
  t_stamp <- tibble(x = c(lubridate::ymd_hms("2018-10-07 19:04:05"), NA))

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x)
      ) %>%
      collect(),
    t_stamp,
    check.tzone = FALSE
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S")
      ) %>%
      collect(),
    t_stamp,
    check.tzone = FALSE
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", unit = "ns")
      ) %>%
      collect(),
    t_stamp,
    check.tzone = FALSE
  )

  expect_equal(
    t_string %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%Y-%m-%d %H:%M:%S", unit = "s")
      ) %>%
      collect(),
    t_stamp,
    check.tzone = FALSE
  )

  tstring <- tibble(x = c("08-05-2008", NA))
  tstamp <- tibble(x = c(strptime("08-05-2008", format = "%m-%d-%Y"), NA))

  expect_equal(
    tstring %>%
      Table$create() %>%
      mutate(
        x = strptime(x, format = "%m-%d-%Y")
      ) %>%
      collect(),
    tstamp,
    check.tzone = FALSE
  )

})

test_that("errors in strptime", {
  # Error when tz is passed

  x <- Expression$field_ref("x")
  expect_error(
    nse_funcs$strptime(x, tz = "PDT"),
    'Time zone argument not supported by Arrow'
  )
})
