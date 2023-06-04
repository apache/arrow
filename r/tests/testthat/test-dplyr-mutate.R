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
library(stringr)

skip_if_not_available("acero")

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2 * (1:10) + 1, side = "both")

test_that("mutate() is lazy", {
  expect_s3_class(
    tbl %>% record_batch() %>% mutate(int = int + 6L),
    "arrow_dplyr_query"
  )
})

test_that("basic mutate", {
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      mutate(int = int + 6L) %>%
      collect(),
    tbl
  )
})

test_that("mutate() with NULL inputs", {
  compare_dplyr_binding(
    .input %>%
      mutate(int = NULL) %>%
      collect(),
    tbl
  )
})

test_that("empty mutate()", {
  compare_dplyr_binding(
    .input %>%
      mutate() %>%
      collect(),
    tbl
  )
})

test_that("transmute", {
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      transmute(int = int + 6L) %>%
      collect(),
    tbl
  )
})

test_that("transmute after group_by", {
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, chr) %>%
      group_by(chr, int) %>%
      transmute(dbl + 1) %>%
      collect(),
    tbl
  )
})

test_that("transmute respect bespoke dplyr implementation", {
  ## see: https://github.com/tidyverse/dplyr/issues/6086
  compare_dplyr_binding(
    .input %>%
      transmute(dbl, int = int + 6L) %>%
      collect(),
    tbl
  )
})

test_that("transmute() with NULL inputs", {
  compare_dplyr_binding(
    .input %>%
      transmute(int = NULL) %>%
      collect(),
    tbl
  )
})

test_that("empty transmute()", {
  compare_dplyr_binding(
    .input %>%
      transmute() %>%
      collect(),
    tbl
  )
})

test_that("transmute with unnamed expressions", {
  compare_dplyr_binding(
    .input %>%
      select(int, padded_strings) %>%
      transmute(
        int, # bare column name
        nchar(padded_strings) # expression
      ) %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )
})

test_that("transmute() with unsupported arguments", {
  expect_error(
    tbl %>%
      Table$create() %>%
      transmute(int = int + 42L, .keep = "all"),
    "`transmute()` does not support the `.keep` argument",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      transmute(int = int + 42L, .before = lgl),
    "`transmute()` does not support the `.before` argument",
    fixed = TRUE
  )
  expect_error(
    tbl %>%
      Table$create() %>%
      transmute(int = int + 42L, .after = chr),
    "`transmute()` does not support the `.after` argument",
    fixed = TRUE
  )
})

test_that("transmute() defuses dots arguments (ARROW-13262)", {
  expect_warning(
    tbl %>%
      Table$create() %>%
      transmute(
        a = stringr::str_c(padded_strings, padded_strings),
        b = stringr::str_squish(a)
      ) %>%
      collect(),
    "Expression stringr::str_squish(a) not supported in Arrow; pulling data into R",
    fixed = TRUE
  )
})

test_that("mutate and refer to previous mutants", {
  compare_dplyr_binding(
    .input %>%
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
  compare_dplyr_binding(
    .input %>%
      select(int, verses) %>%
      mutate(
        line_lengths = nchar(verses, type = "bytes"),
        longer = line_lengths * 10
      ) %>%
      filter(line_lengths > 15) %>%
      collect(),
    tbl
  )
  # This tests the whole abandon_ship() machinery
  compare_dplyr_binding(
    .input %>%
      select(int, verses) %>%
      mutate(
        line_lengths = nchar(verses, type = "bytes", allowNA = TRUE),
        longer = line_lengths * 10
      ) %>%
      filter(line_lengths > 15) %>%
      collect(),
    tbl,
    warning = paste0(
      "In nchar\\(verses, type = \"bytes\", allowNA = TRUE\\), ",
      "allowNA = TRUE not supported in Arrow; pulling data into R"
    )
  )
})

test_that("mutate with .data pronoun", {
  compare_dplyr_binding(
    .input %>%
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
  compare_dplyr_binding(
    .input %>%
      select(int, padded_strings) %>%
      mutate(
        int, # bare column name
        nchar(padded_strings) # expression
      ) %>%
      filter(int > 5) %>%
      collect(),
    tbl
  )
})

test_that("mutate with reassigning same name", {
  compare_dplyr_binding(
    .input %>%
      transmute(
        new = lgl,
        new = chr
      ) %>%
      collect(),
    tbl
  )
})

test_that("mutate with single value for recycling", {
  compare_dplyr_binding(
    .input %>%
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
  compare_dplyr_binding(
    .input %>%
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
  compare_dplyr_binding(
    .input %>%
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

  # test modified from version in dplyr::mutate due to ARROW-12632
  compare_dplyr_binding(
    .input %>%
      select(name, height, mass) %>%
      mutate(across(!name, as.character)) %>%
      collect(),
    starwars,
  )

  # group_by then mutate
  compare_dplyr_binding(
    .input %>%
      select(name, mass, homeworld) %>%
      group_by(homeworld) %>%
      mutate(rank = min_rank(desc(mass))) %>%
      collect(),
    starwars,
    warning = TRUE
  )

  # `.before` and `.after` experimental args: ARROW-11701
  df <- tibble(x = 1, y = 2)
  compare_dplyr_binding(
    .input %>% mutate(z = x + y) %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       x     y     z
  #>   <dbl> <dbl> <dbl>
  #> 1     1     2     3

  compare_dplyr_binding(
    .input %>% mutate(z = x + y, .before = 1) %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       z     x     y
  #>   <dbl> <dbl> <dbl>
  #> 1     3     1     2
  compare_dplyr_binding(
    .input %>% mutate(z = x + y, .after = x) %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       x     z     y
  #>   <dbl> <dbl> <dbl>
  #> 1     1     3     2

  # By default, mutate() keeps all columns from the input data.
  # Experimental: You can override with `.keep`
  df <- tibble(x = 1, y = 2, a = "a", b = "b")
  compare_dplyr_binding(
    .input %>% mutate(z = x + y, .keep = "all") %>% collect(), # the default
    df
  )
  #> # A tibble: 1 x 5
  #>       x     y a     b         z
  #>   <dbl> <dbl> <chr> <chr> <dbl>
  #> 1     1     2 a     b         3
  compare_dplyr_binding(
    .input %>% mutate(z = x + y, .keep = "used") %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>       x     y     z
  #>   <dbl> <dbl> <dbl>
  #> 1     1     2     3
  compare_dplyr_binding(
    .input %>% mutate(z = x + y, .keep = "unused") %>% collect(),
    df
  )
  #> # A tibble: 1 x 3
  #>   a     b         z
  #>   <chr> <chr> <dbl>
  #> 1 a     b         3
  compare_dplyr_binding(
    .input %>% mutate(z = x + y, x, .keep = "none") %>% collect(),
    df
  )
  #> # A tibble: 1 × 2
  #>       x     z
  #>   <dbl> <dbl>
  #> 1     1     3

  # Grouping ----------------------------------------
  # The mutate operation may yield different results on grouped
  # tibbles because the expressions are computed within groups.
  # The following normalises `mass` by the global average:
  # TODO(ARROW-13926): support window functions
  compare_dplyr_binding(
    .input %>%
      select(name, mass, species) %>%
      mutate(mass_norm = mass / mean(mass, na.rm = TRUE)) %>%
      collect(),
    starwars,
    warning = "window function"
  )
})

test_that("Can mutate after group_by as long as there are no aggregations", {
  compare_dplyr_binding(
    .input %>%
      select(int, chr) %>%
      group_by(chr) %>%
      mutate(int = int + 6L) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      select(mean = int, chr) %>%
      # rename `int` to `mean` and use `mean` in `mutate()` to test that
      # `all_funs()` does not incorrectly identify it as an aggregate function
      group_by(chr) %>%
      mutate(mean = mean + 6L) %>%
      collect(),
    tbl
  )
  # Check the column order when .keep = "none"
  compare_dplyr_binding(
    .input %>%
      select(chr, int) %>%
      group_by(chr) %>%
      mutate(int + 1, .keep = "none") %>%
      collect(),
    tbl
  )
  expect_warning(
    tbl %>%
      Table$create() %>%
      select(int, chr) %>%
      group_by(chr) %>%
      mutate(avg_int = mean(int)) %>%
      collect(),
    "window functions not currently supported in Arrow; pulling data into R",
    fixed = TRUE
  )
  expect_warning(
    tbl %>%
      Table$create() %>%
      select(mean = int, chr) %>%
      # rename `int` to `mean` and use `mean(mean)` in `mutate()` to test that
      # `all_funs()` detects `mean()` despite the collision with a column name
      group_by(chr) %>%
      mutate(avg_int = mean(mean)) %>%
      collect(),
    "window functions not currently supported in Arrow; pulling data into R",
    fixed = TRUE
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

test_that("Can't just add a vector column with mutate()", {
  expect_warning(
    expect_equal(
      Table$create(tbl) %>%
        select(int) %>%
        mutate(again = 1:10),
      tibble::tibble(int = tbl$int, again = 1:10)
    ),
    "In again = 1:10, only values of size one are recycled; pulling data into R"
  )
})

test_that("print a mutated table", {
  expect_output(
    Table$create(tbl) %>%
      select(int) %>%
      mutate(twice = int * 2) %>%
      print(),
    "Table (query)
int: int32
twice: int32 (multiply_checked(int, 2))

See $.data for the source Arrow object",
    fixed = TRUE
  )
})

test_that("mutate and write_dataset", {
  skip_if_not_available("dataset")
  # See related test in test-dataset.R

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

  expect_equal(
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

test_that("mutate and pmin/pmax", {
  df <- tibble(
    city = c("Chillan", "Valdivia", "Osorno"),
    val1 = c(200, 300, NA),
    val2 = c(100, NA, NA),
    val3 = c(0, NA, NA)
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        max_val_1 = pmax(val1, val2, val3),
        max_val_2 = pmax(val1, val2, val3, na.rm = TRUE),
        min_val_1 = pmin(val1, val2, val3),
        min_val_2 = pmin(val1, val2, val3, na.rm = TRUE),
        max_val_1_nmspc = base::pmax(val1, val2, val3),
        max_val_2_nmspc = base::pmax(val1, val2, val3, na.rm = TRUE),
        min_val_1_nmspc = base::pmin(val1, val2, val3),
        min_val_2_nmspc = base::pmin(val1, val2, val3, na.rm = TRUE)
      ) %>%
      collect(),
    df
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        max_val_1 = pmax(val1 - 100, 200, val1 * 100, na.rm = TRUE),
        min_val_1 = pmin(val1 - 100, 100, val1 * 100, na.rm = TRUE),
      ) %>%
      collect(),
    df
  )
})

test_that("mutate() and transmute() with namespaced functions", {
  compare_dplyr_binding(
    .input %>%
      mutate(
        a = base::round(dbl) + base::log(int)
      ) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        a = base::round(dbl) + base::log(int)
      ) %>%
      collect(),
    tbl
  )

  # str_detect binding depends on RE2
  skip_if_not_available("re2")
  compare_dplyr_binding(
    .input %>%
      mutate(
        b = stringr::str_detect(verses, "ur")
      ) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        b = stringr::str_detect(verses, "ur")
      ) %>%
      collect(),
    tbl
  )
})

test_that("Can use across() within mutate()", {

  # expressions work in the right order
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

  compare_dplyr_binding(
    .input %>%
      mutate(across(where(is.double))) %>%
      collect(),
    example_data
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

test_that("Can use across() within transmute()", {
  compare_dplyr_binding(
    .input %>%
      transmute(
        dbl2 = dbl * 2,
        across(c(dbl, dbl2), round),
        int2 = int * 2,
        dbl = dbl + 3
      ) %>%
      collect(),
    example_data
  )
})

test_that("across() does not select grouping variables within mutate()", {
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, chr) %>%
      group_by(chr) %>%
      mutate(across(everything(), round)) %>%
      collect(),
    example_data
  )

  expect_error(
    example_data %>%
      arrow_table() %>%
      group_by(chr) %>%
      mutate(across(chr, as.character)),
    "Column `chr` doesn't exist"
  )
})

test_that("across() does not select grouping variables within transmute()", {
  compare_dplyr_binding(
    .input %>%
      select(int, dbl, chr) %>%
      group_by(chr) %>%
      transmute(across(everything(), round)) %>%
      collect(),
    example_data
  )

  expect_error(
    example_data %>%
      arrow_table() %>%
      group_by(chr) %>%
      transmute(across(chr, as.character)),
    "Column `chr` doesn't exist"
  )
})
