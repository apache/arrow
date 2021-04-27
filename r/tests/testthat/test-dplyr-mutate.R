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

# because expect_dplyr_equal involves no assertion (wrong initial thought)
# we need to "translate" dplyr tests as:
#
# dplyr:
# expect_equal(
#   mutate(data.frame(x = 1, y = 1), z = 1, x = NULL, y = NULL),
#   data.frame(z = 1)
# )
#
# arrow:
# expect_dplyr_equal(
#   input %>% mutate(z = 1, x = NULL, y = NULL) %>% collect(),
#   data.frame(x = 1, y = 1)
# )

library(dplyr)
library(stringr)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2*(1:10)+1, side = "both")

test_that("mutate() is lazy", {
  expect_s3_class(
    tbl %>% record_batch() %>% mutate(int = int + 6L),
    "arrow_dplyr_query"
  )
})

test_that("basic mutate", {
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      mutate(int = int + 6L) %>%
      collect(),
    tbl
  )
})

test_that("transmute", {
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      transmute(int = int + 6L) %>%
      collect(),
    tbl
  )
})

test_that("transmute() with NULL inputs", {
  expect_dplyr_equal(
    input %>%
      transmute(int = NULL) %>%
      collect(),
    tbl
  )
})

test_that("empty transmute()", {
  expect_dplyr_equal(
    input %>%
      transmute() %>%
      collect(),
    tbl
  )
})

test_that("mutate and refer to previous mutants", {
  expect_dplyr_equal(
    input %>%
      select(int, verses) %>%
      mutate(
        line_lengths = nchar(verses),
        longer = line_lengths * 10
      ) %>%
      filter(line_lengths > 15) %>%
      collect(),
    tbl
  )
})

test_that("nchar() arguments", {
  expect_dplyr_equal(
    input %>%
      select(int, verses) %>%
      mutate(
        line_lengths = nchar(verses, type = "bytes"),
        longer = line_lengths * 10
      ) %>%
      filter(line_lengths > 15) %>%
      collect(),
    tbl
  )
  expect_warning(
    expect_dplyr_equal(
      input %>%
        select(int, verses) %>%
        mutate(
          line_lengths = nchar(verses, type = "bytes", allowNA = TRUE),
          longer = line_lengths * 10
        ) %>%
        filter(line_lengths > 15) %>%
        collect(),
      tbl
    ),
    "not supported"
  )
})

test_that("mutate with .data pronoun", {
  expect_dplyr_equal(
    input %>%
      select(int, verses) %>%
      mutate(
        line_lengths = str_length(verses),
        longer = .data$line_lengths * 10
      ) %>%
      filter(line_lengths > 15) %>%
      collect(),
    tbl
  )
})

test_that("mutate with unnamed expressions", {
  expect_dplyr_equal(
    input %>%
      select(int, padded_strings) %>%
      mutate(
        int,                   # bare column name
        nchar(padded_strings)  # expression
      ) %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )
})

test_that("mutate with reassigning same name", {
  expect_dplyr_equal(
    input %>%
      transmute(
        new = lgl,
        new = chr
      ) %>%
      collect(),
    tbl
  )
})

test_that("mutate with single value for recycling", {
  skip("Not implemented (ARROW-11705")
  expect_dplyr_equal(
    input %>%
      select(int, padded_strings) %>%
      mutate(
        dr_bronner = 1 # ALL ONE!
      ) %>%
      collect(),
    tbl
  )
})

test_that("dplyr::mutate's examples", {
  # Newly created variables are available immediately
  expect_dplyr_equal(
    input %>%
      select(name, mass) %>%
      mutate(
        mass2 = mass * 2,
        mass2_squared = mass2 * mass2
      ) %>%
      collect(),
    starwars # this is a test tibble that ships with dplyr
  )

  # As well as adding new variables, you can use mutate() to
  # remove variables and modify existing variables.
  expect_dplyr_equal(
    input %>%
      select(name, height, mass, homeworld) %>%
      mutate(
        mass = NULL,
        height = height * 0.0328084 # convert to feet
      ) %>%
      collect(),
    starwars
  )

  # Examples we don't support should succeed
  # but warn that they're pulling data into R to do so

  # across + autosplicing: ARROW-11699
  expect_warning(
    expect_dplyr_equal(
      input %>%
        select(name, homeworld, species) %>%
        mutate(across(!name, as.factor)) %>%
        collect(),
      starwars
    ),
    "Expression across.*not supported in Arrow"
  )

  # group_by then mutate
  expect_warning(
    expect_dplyr_equal(
      input %>%
        select(name, mass, homeworld) %>%
        group_by(homeworld) %>%
        mutate(rank = min_rank(desc(mass))) %>%
        collect(),
      starwars
    ),
    "not supported in Arrow"
  )

  # `.before` and `.after` experimental args: ARROW-11701
  df <- tibble(x = 1, y = 2)
  expect_dplyr_equal(
    input %>% mutate(z = x + y) %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       x     y     z
  #>   <dbl> <dbl> <dbl>
  #> 1     1     2     3
  expect_dplyr_equal(
    input %>% mutate(z = x + y, .before = 1) %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       z     x     y
  #>   <dbl> <dbl> <dbl>
  #> 1     3     1     2
  expect_dplyr_equal(
    input %>% mutate(z = x + y, .after = x) %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       x     z     y
  #>   <dbl> <dbl> <dbl>
  #> 1     1     3     2

  # By default, mutate() keeps all columns from the input data.
  # Experimental: You can override with `.keep`
  df <- tibble(x = 1, y = 2, a = "a", b = "b")
  expect_dplyr_equal(
    input %>% mutate(z = x + y, .keep = "all") %>% collect(), # the default
    df
  )
  #> # A tibble: 1 x 5
  #>       x     y a     b         z
  #>   <dbl> <dbl> <chr> <chr> <dbl>
  #> 1     1     2 a     b         3
  expect_dplyr_equal(
    input %>% mutate(z = x + y, .keep = "used") %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       x     y     z
  #>   <dbl> <dbl> <dbl>
  #> 1     1     2     3
  expect_dplyr_equal(
    input %>% mutate(z = x + y, .keep = "unused") %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>   a     b         z
  #>   <chr> <chr> <dbl>
  #> 1 a     b         3
  expect_dplyr_equal(
    input %>% mutate(z = x + y, .keep = "none") %>% collect(), # same as transmute()
    df
  )
  #> # A tibble: 1 x 1
  #>       z
  #>   <dbl>
  #> 1     3

  # Grouping ----------------------------------------
  # The mutate operation may yield different results on grouped
  # tibbles because the expressions are computed within groups.
  # The following normalises `mass` by the global average:
  # TODO(ARROW-11702)
  expect_warning(
    expect_dplyr_equal(
      input %>%
        select(name, mass, species) %>%
        mutate(mass_norm = mass / mean(mass, na.rm = TRUE)) %>%
        collect(),
      starwars
    ),
    "not supported in Arrow"
  )
})

test_that("handle bad expressions", {
  # TODO: search for functions other than mean() (see above test)
  # that need to be forced to fail because they error ambiguously

  with_language("fr", {
    # expect_warning(., NA) because the usual behavior when it hits a filter
    # that it can't evaluate is to raise a warning, collect() to R, and retry
    # the filter. But we want this to error the first time because it's
    # a user error, not solvable by retrying in R
    expect_warning(
      expect_error(
        Table$create(tbl) %>% mutate(newvar = NOTAVAR + 2),
        "objet 'NOTAVAR' introuvable"
      ),
      NA
    )
  })
})

test_that("print a mutated table", {
  expect_output(
    Table$create(tbl) %>%
      select(int) %>%
      mutate(twice = int * 2) %>%
      print(),
'Table (query)
int: int32
twice: expr

See $.data for the source Arrow object',
  fixed = TRUE)

  # Handling non-expressions/edge cases
  expect_output(
    Table$create(tbl) %>%
      select(int) %>%
      mutate(again = 1:10) %>%
      print(),
'Table (query)
int: int32
again: expr

See $.data for the source Arrow object',
  fixed = TRUE)
})

test_that("mutate and write_dataset", {
  skip_if_not_available("dataset")
  # See related test in test-dataset.R

  skip_on_os("windows") # https://issues.apache.org/jira/browse/ARROW-9651

  first_date <- lubridate::ymd_hms("2015-04-29 03:12:39")
  df1 <- tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[1:10],
    fct = factor(LETTERS[1:10]),
    ts = first_date + lubridate::days(1:10)
  )

  second_date <- lubridate::ymd_hms("2017-03-09 07:01:02")
  df2 <- tibble(
    int = 101:110,
    dbl = c(as.numeric(51:59), NaN),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[10:1],
    fct = factor(LETTERS[10:1]),
    ts = second_date + lubridate::days(10:1)
  )

  dst_dir <- tempfile()
  stacked <- record_batch(rbind(df1, df2))
  stacked %>%
    mutate(twice = int * 2) %>%
    group_by(int) %>%
    write_dataset(dst_dir, format = "feather")
  expect_true(dir.exists(dst_dir))
  expect_identical(dir(dst_dir), sort(paste("int", c(1:10, 101:110), sep = "=")))

  new_ds <- open_dataset(dst_dir, format = "feather")

  expect_equivalent(
    new_ds %>%
      select(string = chr, integer = int, twice) %>%
      filter(integer > 6 & integer < 11) %>%
      collect() %>%
      summarize(mean = mean(integer)),
    df1 %>%
      select(string = chr, integer = int) %>%
      mutate(twice = integer * 2) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
})

# PACHA ADDITIONS ----
# READ THIS CAREFULLY PLEASE, IT'S MY 1ST DAY WRITING THIS KIND OF SENSITIVE TESTS

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L1-L10
# the rest of that test belongs in L55-62 here
test_that("empty mutate returns input", {
  # dbl2 = 5, so I'm grouping by a constant
  gtbl <- group_by(tbl, dbl2)

  expect_dplyr_equal(input %>% mutate() %>% collect(), tbl)
  expect_dplyr_equal(input %>% mutate(!!!list()) %>% collect(), tbl)
  expect_dplyr_equal(input %>% mutate() %>% collect(), gtbl)
  expect_dplyr_equal(input %>% mutate(!!!list()) %>% collect(), gtbl)
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L12-L6
test_that("rownames preserved", {
  skip("Row names are not preserved")
  df <- data.frame(x = c(1, 2), row.names = c("a", "b"))
  expect_dplyr_equal(input %>% mutate(y = c(3, 4)) %>% collect() %>% rownames(), df)
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L18-L29
test_that("mutations applied progressively", {
  df <- tibble(x = 1)

  expect_dplyr_equal(
    input %>% mutate(y = x + 1, z = y + 1) %>% collect(),
    df
  )
  expect_dplyr_equal(
    input %>% mutate(x = x + 1, x = x + 1) %>% collect(),
    df
  )
  expect_dplyr_equal(
    input %>% mutate(y = x + 1, z = y + 1) %>% collect(),
    df
  )

  df <- data.frame(x = 1, y = 2)
  expect_equal(
    df %>% Table$create() %>% mutate(x2 = x, x3 = x2 + 1) %>% collect(),
    df %>% Table$create() %>% mutate(x2 = x + 0, x3 = x2 + 1) %>% collect()
  )
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L37-L54
test_that("can remove variables with NULL (dplyr #462)", {
  df <- tibble(x = 1:3, y = 1:3)
  gf <- group_by(df, x)

  expect_dplyr_equal(input %>% mutate(y = NULL) %>% collect(), df)
  expect_dplyr_equal(input %>% mutate(y = NULL) %>% collect(), gf)

  # even if it doesn't exist
  expect_dplyr_equal(input %>% mutate(z = NULL) %>% collect(), df)
  # or was just created
  expect_dplyr_equal(input %>% mutate(z = rep(1, nrow(input)), z = NULL) %>% collect(), df)

  # regression test for https://github.com/tidyverse/dplyr/issues/4974
  expect_dplyr_equal(
    input %>% mutate(z = 1, x = NULL, y = NULL) %>% collect(),
    data.frame(x = 1, y = 1)
  )
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L71-L75
# test_that("assignments don't overwrite variables (dplyr #315)", {
#   expect_dplyr_equal(
#     tibble(x = 1, y = 2) %>% mutate(z = {x <- 10; x}) %>% collect(),
#     tibble(x = 1, y = 2, z = 10)
#   )
# })
# NOT SURE ABOUT THIS!
test_that("assignments don't overwrite variables (dplyr #315)", {
  expect_dplyr_equal(
    input %>% mutate(z = {x <- 10; x}) %>% collect(),
    tibble(x = 1, y = 2)
  )
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L77-L81
test_that("can mutate a data frame with zero columns and `NULL` column names", {
  df <- vctrs::new_data_frame(n = 2L)
  colnames(df) <- NULL
  expect_dplyr_equal(
    input %>% mutate(x = c(1,2)) %>% collect(),
    df
  )
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L102-L106
test_that("mutate disambiguates NA and NaN (#1448)", {
  expect_dplyr_equal(
    input %>% mutate(y = x * 1) %>% select(y) %>% collect(),
    tibble(x = c(1, NA, NaN))
  )
})

# similar to https://github.com/tidyverse/dplyr/blob/master/tests/testthat/test-mutate.r#L102-L106
# this is somewhat "contained" in the previous test
test_that("mutate handles data frame columns", {
  expect_dplyr_equal(
    input %>% mutate(new_col = data.frame(x = 1:3)) %>% select(new_col) %>% collect(),
    data.frame(x = 1:3)
  )

  # mutate() on grouped data not supported in Arrow; this will be pulling data into R
  expect_warning(expect_dplyr_equal(
    input %>%
      group_by(x) %>%
      mutate(new_col = x) %>%
      ungroup() %>%
      select(new_col) %>%
      collect(),
    data.frame(x = 1:3)
  ))

  skip("rowwise() is not (yet) implemented in Arrow")
  expect_dplyr_equal(
    input %>%
      rowwise(x) %>%
      mutate(new_col = x) %>%
      ungroup() %>%
      select(new_col) %>%
      collect(),
    data.frame(x = 1:3)
  )
})
