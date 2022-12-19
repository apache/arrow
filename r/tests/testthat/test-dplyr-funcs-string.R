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

library(dplyr, warn.conflicts = FALSE)
library(lubridate)
library(stringr)
library(stringi)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")
tbl$some_grouping <- rep(c(1, 2), 5)

test_that("paste, paste0, and str_c", {
  df <- tibble(
    v = c("A", "B", "C"),
    w = c("a", "b", "c"),
    x = c("d", NA_character_, "f"),
    y = c(NA_character_, "h", "i"),
    z = c(1.1, 2.2, NA)
  )
  x <- Expression$field_ref("x")
  y <- Expression$field_ref("y")

  # no NAs in data
  compare_dplyr_binding(
    .input %>%
      transmute(
        a = paste(v, w),
        a2 = base::paste(v, w)
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(paste(v, w, sep = "-")) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        a = paste0(v, w),
        a2 = base::paste0(v, w)
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        a = str_c(v, w),
        a2 = stringr::str_c(v, w)
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(str_c(v, w, sep = "+")) %>%
      collect(),
    df
  )

  # NAs in data
  compare_dplyr_binding(
    .input %>%
      transmute(paste(x, y)) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(paste(x, y, sep = "-")) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(str_c(x, y)) %>%
      collect(),
    df
  )

  # non-character column in dots
  compare_dplyr_binding(
    .input %>%
      transmute(paste0(x, y, z)) %>%
      collect(),
    df
  )

  # literal string in dots
  compare_dplyr_binding(
    .input %>%
      transmute(paste(x, "foo", y)) %>%
      collect(),
    df
  )

  # literal NA in dots
  compare_dplyr_binding(
    .input %>%
      transmute(paste(x, NA, y)) %>%
      collect(),
    df
  )

  # expressions in dots
  compare_dplyr_binding(
    .input %>%
      transmute(paste0(x, toupper(y), as.character(z))) %>%
      collect(),
    df
  )

  # sep is literal NA
  # errors in paste() (consistent with base::paste())
  expect_error(
    call_binding("paste", x, y, sep = NA_character_),
    "Invalid separator"
  )
  # In next release of stringr (late 2022), str_c also errors
  expect_error(
    call_binding("str_c", x, y, sep = NA_character_),
    "`sep` must be a single string, not `NA`."
  )

  # sep passed in dots to paste0 (which doesn't take a sep argument)
  compare_dplyr_binding(
    .input %>%
      transmute(paste0(x, y, sep = "-")) %>%
      collect(),
    df
  )

  # known differences

  # arrow allows the separator to be an array
  expect_equal(
    df %>%
      Table$create() %>%
      transmute(result = paste(x, y, sep = w)) %>%
      collect(),
    df %>%
      transmute(result = paste(x, w, y, sep = ""))
  )

  # expected errors

  # collapse argument not supported
  expect_error(
    call_binding("paste", x, y, collapse = ""),
    "collapse"
  )
  expect_error(
    call_binding("paste0", x, y, collapse = ""),
    "collapse"
  )
  expect_error(
    call_binding("str_c", x, y, collapse = ""),
    "collapse"
  )

  # literal vectors of length != 1 not supported
  expect_error(
    call_binding("paste", x, character(0), y),
    "Literal vectors of length != 1 not supported in string concatenation"
  )
  expect_error(
    call_binding("paste", x, c(",", ";"), y),
    "Literal vectors of length != 1 not supported in string concatenation"
  )
})

test_that("grepl with ignore.case = FALSE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar", NA_character_))
  compare_dplyr_binding(
    .input %>%
      filter(grepl("o", x, fixed = TRUE)) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      mutate(x = grepl("o", x, fixed = TRUE)) %>%
      collect(),
    df
  )
})

test_that("sub and gsub with ignore.case = FALSE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar"))
  compare_dplyr_binding(
    .input %>%
      transmute(x = sub("Foo", "baz", x, fixed = TRUE)) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = gsub("o", "u", x, fixed = TRUE)) %>%
      collect(),
    df
  )
})

# many of the remainder of these tests require RE2
skip_if_not_available("re2")

test_that("grepl", {
  df <- tibble(x = c("Foo", "bar", NA_character_))

  for (fixed in c(TRUE, FALSE)) {
    compare_dplyr_binding(
      .input %>%
        filter(grepl("Foo", x, fixed = fixed)) %>%
        collect(),
      df
    )
    compare_dplyr_binding(
      .input %>%
        transmute(x = grepl("^B.+", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )
    compare_dplyr_binding(
      .input %>%
        filter(grepl("Foo", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )
    # with namespacing
    compare_dplyr_binding(
      .input %>%
        filter(base::grepl("Foo", x, fixed = fixed)) %>%
        collect(),
      df
    )
  }
})

test_that("grepl with ignore.case = TRUE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar", NA_character_))

  # base::grepl() ignores ignore.case = TRUE with a warning when fixed = TRUE,
  # so we can't use compare_dplyr_binding() for these tests
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
      filter(grepl("^B.+", x, ignore.case = TRUE, fixed = TRUE)) %>%
      collect(),
    tibble(x = character(0))
  )
  expect_equal(
    df %>%
      Table$create() %>%
      mutate(
        a = grepl("O", x, ignore.case = TRUE, fixed = TRUE)
      ) %>%
      collect(),
    tibble(
      x = c("Foo", "bar", NA_character_),
      a = c(TRUE, FALSE, FALSE)
    )
  )
})

test_that("str_detect", {
  df <- tibble(x = c("Foo", "bar", NA_character_))

  compare_dplyr_binding(
    .input %>%
      filter(str_detect(x, regex("^F"))) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        a = str_detect(x, regex("^f[A-Z]{2}", ignore_case = TRUE)),
        a2 = stringr::str_detect(x, regex("^f[A-Z]{2}", ignore_case = TRUE))
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = str_detect(x, regex("^f[A-Z]{2}", ignore_case = TRUE), negate = TRUE)) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      filter(str_detect(x, fixed("o"))) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      filter(str_detect(x, fixed("O"))) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      filter(str_detect(x, fixed("O", ignore_case = TRUE))) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      filter(str_detect(x, fixed("O", ignore_case = TRUE), negate = TRUE)) %>%
      collect(),
    df
  )
})

test_that("sub and gsub", {
  df <- tibble(x = c("Foo", "bar"))

  for (fixed in c(TRUE, FALSE)) {
    compare_dplyr_binding(
      .input %>%
        transmute(x = sub("Foo", "baz", x, fixed = fixed)) %>%
        collect(),
      df
    )
    compare_dplyr_binding(
      .input %>%
        transmute(x = sub("^B.+", "baz", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )
    compare_dplyr_binding(
      .input %>%
        transmute(x = sub("Foo", "baz", x, ignore.case = FALSE, fixed = fixed)) %>%
        collect(),
      df
    )
  }
})

test_that("sub and gsub with ignore.case = TRUE and fixed = TRUE", {
  df <- tibble(x = c("Foo", "bar"))

  # base::sub() and base::gsub() ignore ignore.case = TRUE with a warning when
  # fixed = TRUE, so we can't use compare_dplyr_binding() for these tests
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

test_that("sub and gsub with namespacing", {
  compare_dplyr_binding(
    .input %>%
      mutate(verses_new = base::gsub("o", "u", verses, fixed = TRUE)) %>%
      collect(),
    tbl
  )

  compare_dplyr_binding(
    .input %>%
      mutate(verses_new = base::sub("o", "u", verses, fixed = TRUE)) %>%
      collect(),
    tbl
  )
})

test_that("str_replace and str_replace_all", {
  df <- tibble(x = c("Foo", "bar"))

  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace_all(x, "^F", "baz")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace_all(x, regex("^F"), "baz")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_replace(x, "^F[a-z]{2}", "baz")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace(x, regex("^f[A-Z]{2}", ignore_case = TRUE), "baz")) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        x = str_replace_all(x, fixed("o"), "u"),
        x2 = stringr::str_replace_all(x, fixed("o"), "u")
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        x = str_replace(x, fixed("O"), "u"),
        x2 = stringr::str_replace(x, fixed("O"), "u")
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace(x, fixed("O", ignore_case = TRUE), "u")) %>%
      collect(),
    df
  )
})

test_that("strsplit and str_split", {
  df <- tibble(x = c("Foo and bar", "baz and qux and quux"))

  compare_dplyr_binding(
    .input %>%
      mutate(x = strsplit(x, "and")) %>%
      collect(),
    df,
    # `ignore_attr = TRUE` because the vctr coming back from arrow (ListArray)
    # has type information in it, but it's just a bare list from R/dplyr.
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(x = strsplit(x, "and.*", fixed = TRUE)) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(
        a = strsplit(x, " +and +"),
        a2 = base::strsplit(x, " +and +")
      ) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(
        a = str_split(x, "and"),
        a2 = stringr::str_split(x, "and")
      ) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_split(x, "and", n = 2)) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_split(x, fixed("and"), n = 2)) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_split(x, regex("and"), n = 2)) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_split(x, "Foo|bar", n = 2)) %>%
      collect(),
    df,
    ignore_attr = TRUE
  )
})

test_that("strrep and str_dup", {
  df <- tibble(x = c("foo1", " \tB a R\n", "!apACHe aRroW!"))
  for (times in 0:8) {
    compare_dplyr_binding(
      .input %>%
        mutate(x = strrep(x, times)) %>%
        collect(),
      df
    )

    compare_dplyr_binding(
      .input %>%
        mutate(x = str_dup(x, times)) %>%
        collect(),
      df
    )
  }
})

test_that("str_to_lower, str_to_upper, and str_to_title", {
  df <- tibble(x = c("foo1", " \tB a R\n", "!apACHe aRroW!"))
  compare_dplyr_binding(
    .input %>%
      transmute(
        x_lower = str_to_lower(x),
        x_upper = str_to_upper(x),
        x_title = str_to_title(x),
        x_lower_nmspc = stringr::str_to_lower(x),
        x_upper_nmspc = stringr::str_to_upper(x),
        x_title_nmspc = stringr::str_to_title(x)
      ) %>%
      collect(),
    df
  )

  # Error checking a single function because they all use the same code path.
  expect_error(
    call_binding("str_to_lower", "Apache Arrow", locale = "sp"),
    "Providing a value for 'locale' other than the default ('en') is not supported in Arrow",
    fixed = TRUE
  )
})

test_that("arrow_*_split_whitespace functions", {
  # use only ASCII whitespace characters
  df_ascii <- tibble(x = c("Foo\nand bar", "baz\tand qux and quux"))

  # use only non-ASCII whitespace characters
  df_utf8 <- tibble(x = c("Foo\u00A0and\u2000bar", "baz\u2006and\u1680qux\u3000and\u2008quux"))

  df_split <- tibble(x = list(c("Foo", "and", "bar"), c("baz", "and", "qux", "and", "quux")))

  # use default option values
  expect_equal(
    df_ascii %>%
      Table$create() %>%
      mutate(x = arrow_ascii_split_whitespace(x)) %>%
      collect(),
    df_split,
    ignore_attr = TRUE
  )
  expect_equal(
    df_utf8 %>%
      Table$create() %>%
      mutate(x = arrow_utf8_split_whitespace(x)) %>%
      collect(),
    df_split,
    ignore_attr = TRUE
  )

  # specify non-default option values
  expect_equal(
    df_ascii %>%
      Table$create() %>%
      mutate(
        x = arrow_ascii_split_whitespace(x, options = list(max_splits = 1, reverse = TRUE))
      ) %>%
      collect(),
    tibble(x = list(c("Foo\nand", "bar"), c("baz\tand qux and", "quux"))),
    ignore_attr = TRUE
  )
  expect_equal(
    df_utf8 %>%
      Table$create() %>%
      mutate(
        x = arrow_utf8_split_whitespace(x, options = list(max_splits = 1, reverse = TRUE))
      ) %>%
      collect(),
    tibble(x = list(c("Foo\u00A0and", "bar"), c("baz\u2006and\u1680qux\u3000and", "quux"))),
    ignore_attr = TRUE
  )
})

test_that("errors and warnings in string splitting", {
  # These conditions generate an error, but abandon_ship() catches the error,
  # issues a warning, and pulls the data into R (if computing on InMemoryDataset)
  # Elsewhere we test that abandon_ship() works,
  # so here we can just call the functions directly

  x <- Expression$field_ref("x")
  expect_error(
    call_binding("str_split", x, fixed("and", ignore_case = TRUE)),
    "Case-insensitive string splitting not supported in Arrow"
  )
  expect_error(
    call_binding("str_split", x, coll("and.?")),
    "Pattern modifier `coll()` not supported in Arrow",
    fixed = TRUE
  )
  expect_error(
    call_binding("str_split", x, boundary(type = "word")),
    "Pattern modifier `boundary()` not supported in Arrow",
    fixed = TRUE
  )
  expect_error(
    call_binding("str_split", x, "and", n = 0),
    "Splitting strings into zero parts not supported in Arrow"
  )

  # This condition generates a warning
  expect_warning(
    call_binding("str_split", x, fixed("and"), simplify = TRUE),
    "Argument 'simplify = TRUE' will be ignored"
  )
})

test_that("errors and warnings in string detection and replacement", {
  x <- Expression$field_ref("x")

  expect_error(
    call_binding("str_detect", x, boundary(type = "character")),
    "Pattern modifier `boundary()` not supported in Arrow",
    fixed = TRUE
  )
  expect_error(
    call_binding("str_replace_all", x, coll("o", locale = "en"), "ó"),
    "Pattern modifier `coll()` not supported in Arrow",
    fixed = TRUE
  )

  # This condition generates a warning
  expect_warning(
    call_binding("str_replace_all", x, regex("o", multiline = TRUE), "u"),
    "Ignoring pattern modifier argument not supported in Arrow: \"multiline\""
  )
})

test_that("backreferences in pattern in string detection", {
  skip("RE2 does not support backreferences in pattern (https://github.com/google/re2/issues/101)")
  df <- tibble(x = c("Foo", "bar"))

  compare_dplyr_binding(
    .input %>%
      filter(str_detect(x, regex("F([aeiou])\\1"))) %>%
      collect(),
    df
  )
})

test_that("backreferences (substitutions) in string replacement", {
  df <- tibble(x = c("Foo", "bar"))

  compare_dplyr_binding(
    .input %>%
      transmute(desc = sub(
        "(?:https?|ftp)://([^/\r\n]+)(/[^\r\n]*)?",
        "path `\\2` on server `\\1`",
        url
      )) %>%
      collect(),
    tibble(url = "https://arrow.apache.org/docs/r/")
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace(x, "^(\\w)o(.*)", "\\1\\2p")) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace(x, regex("^(\\w)o(.*)", ignore_case = TRUE), "\\1\\2p")) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = str_replace(x, regex("^(\\w)o(.*)", ignore_case = TRUE), "\\1\\2p")) %>%
      collect(),
    df
  )
})

test_that("edge cases in string detection and replacement", {
  # in case-insensitive fixed match/replace, test that "\\E" in the search
  # string and backslashes in the replacement string are interpreted literally.
  # this test does not use compare_dplyr_binding() because base::sub() and
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
  compare_dplyr_binding(
    .input %>%
      filter(grepl("(?i)^[abc]{3}$", x, ignore.case = TRUE, fixed = FALSE)) %>%
      collect(),
    tibble(x = c("ABC"))
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = sub("(?i)^[abc]{3}$", "123", x, ignore.case = TRUE, fixed = FALSE)) %>%
      collect(),
    tibble(x = c("ABC"))
  )
})

test_that("arrow_find_substring and arrow_find_substring_regex", {
  df <- tibble(x = c("Foo and Bar", "baz and qux and quux"))

  expect_equal(
    df %>%
      Table$create() %>%
      mutate(x = arrow_find_substring(x, options = list(pattern = "b"))) %>%
      collect(),
    tibble(x = c(-1, 0))
  )
  expect_equal(
    df %>%
      Table$create() %>%
      mutate(x = arrow_find_substring(
        x,
        options = list(pattern = "b", ignore_case = TRUE)
      )) %>%
      collect(),
    tibble(x = c(8, 0))
  )
  expect_equal(
    df %>%
      Table$create() %>%
      mutate(x = arrow_find_substring_regex(
        x,
        options = list(pattern = "^[fb]")
      )) %>%
      collect(),
    tibble(x = c(-1, 0))
  )
  expect_equal(
    df %>%
      Table$create() %>%
      mutate(x = arrow_find_substring_regex(
        x,
        options = list(pattern = "[AEIOU]", ignore_case = TRUE)
      )) %>%
      collect(),
    tibble(x = c(1, 1))
  )
})

test_that("stri_reverse and arrow_ascii_reverse functions", {
  df_ascii <- tibble(x = c("Foo\nand bar", "baz\tand qux and quux"))

  df_utf8 <- tibble(x = c("Foo\u00A0\u0061nd\u00A0bar", "\u0062az\u00A0and\u00A0qux\u3000and\u00A0quux"))

  compare_dplyr_binding(
    .input %>%
      mutate(x = stri_reverse(x)) %>%
      collect(),
    df_utf8
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = stri_reverse(x)) %>%
      collect(),
    df_ascii
  )

  expect_equal(
    df_ascii %>%
      Table$create() %>%
      mutate(x = arrow_ascii_reverse(x)) %>%
      collect(),
    tibble(x = c("rab dna\nooF", "xuuq dna xuq dna\tzab"))
  )

  expect_error(
    df_utf8 %>%
      Table$create() %>%
      mutate(x = arrow_ascii_reverse(x)) %>%
      collect(),
    "Invalid: Non-ASCII sequence in input"
  )
})

test_that("str_like", {
  df <- tibble(x = c("Foo and bar", "baz and qux and quux"))

  # No match - entire string
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_like(x, "baz")) %>%
      collect(),
    df
  )
  # with namespacing
  compare_dplyr_binding(
    .input %>%
      mutate(x = stringr::str_like(x, "baz")) %>%
      collect(),
    df
  )

  # Match - entire string
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_like(x, "Foo and bar")) %>%
      collect(),
    df
  )

  # Wildcard
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_like(x, "f%", ignore_case = TRUE)) %>%
      collect(),
    df
  )

  # Ignore case
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_like(x, "f%", ignore_case = FALSE)) %>%
      collect(),
    df
  )

  # Single character
  compare_dplyr_binding(
    .input %>%
      mutate(x = str_like(x, "_a%")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_like(x, "%baz%")) %>%
      collect(),
    df
  )
})

test_that("str_pad", {
  df <- tibble(x = c("Foo and bar", "baz and qux and quux"))

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_pad(x, width = 31)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_pad(x, width = 30, side = "right")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_pad(x, width = 31, side = "left", pad = "+")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_pad(x, width = 10, side = "left", pad = "+")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        a = str_pad(x, width = 31, side = "both"),
        a2 = stringr::str_pad(x, width = 31, side = "both")
      ) %>%
      collect(),
    df
  )
})

test_that("substr with string()", {
  df <- tibble(x = "Apache Arrow")

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, 1, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, 0, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, -1, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, 6, 1)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, -1, -2)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, 9, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, 1, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = substr(x, 8, 12)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = substr(x, -5, -1),
        y2 = base::substr(x, -5, -1)
      ) %>%
      collect(),
    df
  )

  expect_error(
    call_binding("substr", "Apache Arrow", c(1, 2), 3),
    "`start` must be length 1 - other lengths are not supported in Arrow"
  )

  expect_error(
    call_binding("substr", "Apache Arrow", 1, c(2, 3)),
    "`stop` must be length 1 - other lengths are not supported in Arrow"
  )
})

test_that("substr with binary()", {
  batch <- record_batch(x = list(charToRaw("Apache Arrow")))

  # Check a field reference input
  expect_identical(
    batch %>%
      transmute(y = substr(x, 1, 3)) %>%
      collect() %>%
      # because of the arrow_binary class
      mutate(y = unclass(y)),
    tibble::tibble(y = list(charToRaw("Apa")))
  )

  # Check a Scalar input
  scalar <- Scalar$create(batch$x)
  expect_identical(
    batch %>%
      transmute(y = substr(scalar, 1, 3)) %>%
      collect() %>%
      # because of the arrow_binary class
      mutate(y = unclass(y)),
    tibble::tibble(y = list(charToRaw("Apa")))
  )
})

test_that("substring", {
  # binding for substring just calls call_binding("substr", ...),
  # tested extensively above
  df <- tibble(x = "Apache Arrow")

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = substring(x, 1, 6),
        y2 = base::substring(x, 1, 6)
      ) %>%
      collect(),
    df
  )
})

test_that("str_sub", {
  df <- tibble(x = "Apache Arrow")

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, 1, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, 0, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, -1, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, 6, 1)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, -1, -2)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, -1, 3)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, 9, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, 1, 6)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(y = str_sub(x, 8, 12)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        y = str_sub(x, -5, -1),
        y2 = stringr::str_sub(x, -5, -1)
      ) %>%
      collect(),
    df
  )

  expect_error(
    call_binding("str_sub", "Apache Arrow", c(1, 2), 3),
    "`start` must be length 1 - other lengths are not supported in Arrow"
  )

  expect_error(
    call_binding("str_sub", "Apache Arrow", 1, c(2, 3)),
    "`end` must be length 1 - other lengths are not supported in Arrow"
  )
})

test_that("str_starts, str_ends, startsWith, endsWith", {
  df <- tibble(x = c("Foo", "bar", "baz", "qux", NA_character_))

  compare_dplyr_binding(
    .input %>%
      filter(str_starts(x, "b.*")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_starts(x, "b.*", negate = TRUE)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_starts(x, fixed("b.*"))) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_starts(x, fixed("b"))) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(
        a = str_starts(x, "b.*"),
        a2 = stringr::str_starts(x, "b.*"),
        b = str_starts(x, "b.*", negate = TRUE),
        c = str_starts(x, fixed("b")),
        d = str_starts(x, fixed("b"), negate = TRUE)
      ) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_ends(x, "r")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_ends(x, "r", negate = TRUE)) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_ends(x, fixed("r$"))) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(str_ends(x, fixed("r"))) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(
        a = str_ends(x, "r"),
        a2 = stringr::str_ends(x, "r"),
        b = str_ends(x, "r", negate = TRUE),
        c = str_ends(x, fixed("r")),
        d = str_ends(x, fixed("r"), negate = TRUE)
      ) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(startsWith(x, "b")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(endsWith(x, "r")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(startsWith(x, "b.*")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      filter(endsWith(x, "r$")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(
        a = startsWith(x, "b"),
        b = endsWith(x, "r"),
        a2 = base::startsWith(x, "b"),
        b2 = base::endsWith(x, "r")
      ) %>%
      collect(),
    df
  )
})

test_that("str_count", {
  df <- tibble(
    cities = c("Kolkata", "Dar es Salaam", "Tel Aviv", "San Antonio", "Cluj Napoca", "Bern", "Bogota"),
    dots = c("a.", "...", ".a.a", "a..a.", "ab...", "dse....", ".f..d..")
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        a_count = str_count(cities, pattern = "a"),
        a_count_nmspc = stringr::str_count(cities, pattern = "a")
      ) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(p_count = str_count(cities, pattern = "d")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(p_count = str_count(cities,
        pattern = regex("d", ignore_case = TRUE)
      )) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(e_count = str_count(cities, pattern = "u")) %>%
      collect(),
    df
  )

  # call_binding("str_count", ) is not vectorised over pattern
  compare_dplyr_binding(
    .input %>%
      mutate(let_count = str_count(cities, pattern = c("a", "b", "e", "g", "p", "n", "s"))) %>%
      collect(),
    df,
    warning = TRUE
  )

  compare_dplyr_binding(
    .input %>%
      mutate(dots_count = str_count(dots, ".")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(dots_count = str_count(dots, fixed("."))) %>%
      collect(),
    df
  )
})

test_that("base::tolower and base::toupper", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        verse_to_upper = toupper(verses),
        verse_to_lower = tolower(verses),
        verse_to_upper_nmspc = base::toupper(verses),
        verse_to_lower_nmspc = base::tolower(verses)
      ) %>%
      collect(),
    tbl
  )
})

test_that("namespaced unary and binary string functions", {
  # str_length and stringi::stri_reverse
  compare_dplyr_binding(
    .input %>%
      mutate(
        verse_length = stringr::str_length(verses),
        reverses_verse = stringi::stri_reverse(verses)
      ) %>%
      collect(),
    tbl
  )

  # stringr::str_dup and base::strrep
  df <- tibble(x = c("foo1", " \tB a R\n", "!apACHe aRroW!"))
  for (times in 0:8) {
    compare_dplyr_binding(
      .input %>%
        mutate(x = base::strrep(x, times)) %>%
        collect(),
      df
    )

    compare_dplyr_binding(
      .input %>%
        mutate(x = stringr::str_dup(x, times)) %>%
        collect(),
      df
    )
  }
})

test_that("nchar with namespacing", {
  compare_dplyr_binding(
    .input %>%
      mutate(verses_nchar = base::nchar(verses)) %>%
      collect(),
    tbl
  )
})

test_that("str_trim()", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        left_trim_padded_string = str_trim(padded_strings, "left"),
        right_trim_padded_string = str_trim(padded_strings, "right"),
        both_trim_padded_string = str_trim(padded_strings, "both"),
        left_trim_padded_string_nmspc = stringr::str_trim(padded_strings, "left"),
        right_trim_padded_string_nmspc = stringr::str_trim(padded_strings, "right"),
        both_trim_padded_string_nmspc = stringr::str_trim(padded_strings, "both")
      ) %>%
      collect(),
    tbl
  )
})

test_that("str_remove and str_remove_all", {
  df <- tibble(x = c("Foo", "bar"))

  compare_dplyr_binding(
    .input %>%
      transmute(x = str_remove_all(x, "^F")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(x = str_remove_all(x, regex("^F"))) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = str_remove(x, "^F[a-z]{2}")) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(x = str_remove(x, regex("^f[A-Z]{2}", ignore_case = TRUE))) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        x = str_remove_all(x, fixed("o")),
        x2 = stringr::str_remove_all(x, fixed("o"))
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        x = str_remove(x, fixed("O")),
        x2 = stringr::str_remove(x, fixed("O"))
      ) %>%
      collect(),
    df
  )
  compare_dplyr_binding(
    .input %>%
      transmute(x = str_remove(x, fixed("O", ignore_case = TRUE))) %>%
      collect(),
    df
  )
})
