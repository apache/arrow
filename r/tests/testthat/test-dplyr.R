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

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2*(1:10)+1, side = "both")

test_that("basic select/filter/collect", {
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5)

  expect_is(b2, "arrow_dplyr_query")
  t2 <- collect(b2)
  expect_equal(t2, tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")])
  # Test that the original object is not affected
  expect_identical(collect(batch), tbl)
})

test_that("dim() on query", {
  expect_dplyr_equal(
    input %>%
      filter(int > 5) %>%
      select(int, chr) %>%
      dim(),
    tbl
  )
})

test_that("Print method", {
  expect_output(
    record_batch(tbl) %>%
      filter(dbl > 2, chr == "d" | chr == "f") %>%
      select(chr, int, lgl) %>%
      filter(int < 5) %>%
      select(int, chr) %>%
      print(),
'RecordBatch (query)
int: int32
chr: string

* Filter: and(and(greater(dbl, 2), or(equal(chr, "d"), equal(chr, "f"))), less(int, 5))
See $.data for the source Arrow object',
  fixed = TRUE
  )
})

test_that("summarize", {
  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int)),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      select(int, chr) %>%
      filter(int > 5) %>%
      summarize(min_int = min(int) / 2),
    tbl
  )
})

test_that("Empty select returns no columns", {
  expect_dplyr_equal(
    input %>% select() %>% collect(),
    tbl,
    skip_table = "Table with 0 cols doesn't know how many rows it should have"
  )
})
test_that("Empty select still includes the group_by columns", {
  expect_dplyr_equal(
    input %>% group_by(chr) %>% select() %>% collect(),
    tbl
  )
})

test_that("select/rename", {
  expect_dplyr_equal(
    input %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      rename(string = chr) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      rename(strng = chr) %>%
      rename(other = strng) %>%
      collect(),
    tbl
  )
})

test_that("select/rename with selection helpers", {

  # TODO: add some passing tests here

  expect_error(
    expect_dplyr_equal(
      input %>%
        select(where(is.numeric)) %>%
        collect(),
      tbl
    ),
    "Unsupported selection helper"
  )
})

test_that("filtering with rename", {
  expect_dplyr_equal(
    input %>%
      filter(chr == "b") %>%
      select(string = chr, int) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      select(string = chr, int) %>%
      filter(string == "b") %>%
      collect(),
    tbl
  )
})

test_that("pull", {
  expect_dplyr_equal(
    input %>% pull(),
    tbl
  )
  expect_dplyr_equal(
    input %>% pull(1),
    tbl
  )
  expect_dplyr_equal(
    input %>% pull(chr),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      filter(int > 4) %>%
      rename(strng = chr) %>%
      pull(strng),
    tbl
  )
})

test_that("collect(as_data_frame=FALSE)", {
  batch <- record_batch(tbl)

  b1 <- batch %>% collect(as_data_frame = FALSE)

  expect_is(b1, "RecordBatch")

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    collect(as_data_frame = FALSE)

  expect_is(b2, "RecordBatch")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")]
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    collect(as_data_frame = FALSE)
  expect_is(b3, "RecordBatch")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    collect(as_data_frame = FALSE)
  expect_is(b4, "arrow_dplyr_query")
  expect_equal(
    as.data.frame(b4),
    expected %>%
      rename(strng = chr) %>%
      group_by(int)
    )
})

test_that("compute()", {
  batch <- record_batch(tbl)

  b1 <- batch %>% compute()

  expect_is(b1, "RecordBatch")

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    compute()

  expect_is(b2, "RecordBatch")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")]
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    compute()
  expect_is(b3, "RecordBatch")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    compute()
  expect_is(b4, "arrow_dplyr_query")
  expect_equal(
    as.data.frame(b4),
    expected %>%
      rename(strng = chr) %>%
      group_by(int)
  )
})

test_that("head", {
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    head(2)

  expect_is(b2, "RecordBatch")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")][1:2, ]
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    head(2)
  expect_is(b3, "RecordBatch")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    head(2)
  expect_is(b4, "arrow_dplyr_query")
  expect_equal(
    as.data.frame(b4),
    expected %>%
      rename(strng = chr) %>%
      group_by(int)
    )
})

test_that("tail", {
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    tail(2)

  expect_is(b2, "RecordBatch")
  expected <- tail(tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")], 2)
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    tail(2)
  expect_is(b3, "RecordBatch")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    tail(2)
  expect_is(b4, "arrow_dplyr_query")
  expect_equal(
    as.data.frame(b4),
    expected %>%
      rename(strng = chr) %>%
      group_by(int)
    )
})

test_that("relocate", {
  df <- tibble(a = 1, b = 1, c = 1, d = "a", e = "a", f = "a")
  expect_dplyr_equal(
    input %>% relocate(f) %>% collect(),
    df,
  )
  expect_dplyr_equal(
    input %>% relocate(a, .after = c) %>% collect(),
    df,
  )
  expect_dplyr_equal(
    input %>% relocate(f, .before = b) %>% collect(),
    df,
  )
  expect_dplyr_equal(
    input %>% relocate(a, .after = last_col()) %>% collect(),
    df,
  )
  expect_dplyr_equal(
    input %>% relocate(ff = f) %>% collect(),
    df,
  )
})

test_that("relocate with selection helpers", {
  expect_dplyr_equal(
    input %>% relocate(any_of(c("a", "e", "i", "o", "u"))) %>% collect(),
    df
  )
  expect_error(
    df %>% Table$create() %>% relocate(where(is.character)),
    "Unsupported selection helper"
  )
  expect_error(
    df %>% Table$create() %>% relocate(a, b, c, .after = where(is.character)),
    "Unsupported selection helper"
  )
  expect_error(
    df %>% Table$create() %>% relocate(d, e, f, .before = where(is.numeric)),
    "Unsupported selection helper"
  )
})

test_that("explicit type conversions with cast()", {
  num_int32 <- 12L
  num_int64 <- bit64::as.integer64(10)

  int_types <- c(int8(), int16(), int32(), int64())
  uint_types <- c(uint8(), uint16(), uint32(), uint64())
  float_types <- c(float32(), float64())

  types <- c(
    int_types,
    uint_types,
    float_types,
    double(), # not actually a type, a base R function but should be alias for float64
    string()
  )

  for (type in types) {
    expect_type_equal(
      {
        t1 <- Table$create(x = num_int32) %>%
          transmute(x = cast(x, type)) %>%
          compute()
        t1$schema[[1]]$type
      },
      as_type(type)
    )
    expect_type_equal(
      {
        t1 <- Table$create(x = num_int64) %>%
          transmute(x = cast(x, type)) %>%
          compute()
        t1$schema[[1]]$type
      },
      as_type(type)
    )
  }

  # Arrow errors when truncating floats...
  expect_error(
    expect_type_equal(
      {
        t1 <- Table$create(pi = pi) %>%
          transmute(three = cast(pi, int32())) %>%
          compute()
        t1$schema[[1]]$type
      },
      int32()
    ),
    "truncated"
  )

  # ... unless safe = FALSE (or allow_float_truncate = TRUE)
  expect_type_equal(
    {
      t1 <- Table$create(pi = pi) %>%
        transmute(three = cast(pi, int32(), safe = FALSE)) %>%
        compute()
      t1$schema[[1]]$type
    },
    int32()
  )
})

test_that("explicit type conversions with as.*()", {
  library(bit64)
  expect_dplyr_equal(
    input %>%
      transmute(
        int2chr = as.character(int),
        int2dbl = as.double(int),
        int2int = as.integer(int),
        int2num = as.numeric(int),
        dbl2chr = as.character(dbl),
        dbl2dbl = as.double(dbl),
        dbl2int = as.integer(dbl),
        dbl2num = as.numeric(dbl),
      ) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      transmute(
        chr2chr = as.character(chr),
        chr2dbl = as.double(chr),
        chr2int = as.integer(chr),
        chr2num = as.numeric(chr)
      ) %>%
      collect(),
    tibble(chr = c("1", "2", "3"))
  )
  expect_dplyr_equal(
    input %>%
      transmute(
        chr2i64 = as.integer64(chr),
        dbl2i64 = as.integer64(dbl),
        i642i64 = as.integer64(i64),
      ) %>%
      collect(),
    tibble(chr = "10000000000", dbl = 10000000000, i64 = as.integer64(1e10))
  )
  expect_dplyr_equal(
    input %>%
      transmute(
        chr2lgl = as.logical(chr),
        dbl2lgl = as.logical(dbl),
        int2lgl = as.logical(int)
      ) %>%
      collect(),
    tibble(
      chr = c("TRUE", "FALSE", "true", "false"),
      dbl = c(1, 0, -99, 0),
      int = c(1L, 0L, -99L, 0L)
    )
  )
  expect_dplyr_equal(
    input %>%
      transmute(
        dbl2chr = as.character(dbl),
        dbl2dbl = as.double(dbl),
        dbl2int = as.integer(dbl),
        dbl2lgl = as.logical(dbl),
        int2chr = as.character(int),
        int2dbl = as.double(int),
        int2int = as.integer(int),
        int2lgl = as.logical(int),
        lgl2chr = as.character(lgl), # Arrow returns "true", "false" here ...
        lgl2dbl = as.double(lgl),
        lgl2int = as.integer(lgl),
        lgl2lgl = as.logical(lgl)
      ) %>%
      collect() %>%
      # need to use toupper() *after* collect() or else skip if utf8proc not available
      mutate(lgl2chr = toupper(lgl2chr)), # ... but we need "TRUE", "FALSE"
    tibble(
      dbl = c(1, 0, NA_real_),
      int = c(1L, 0L, NA_integer_),
      lgl = c(TRUE, FALSE, NA)
    )
  )
})

test_that("as.factor()/dictionary_encode()", {
  df1 <- tibble(x = c("C", "D", "B", NA, "D", "B", "S", "A", "B", "Z", "B"))
  df2 <- tibble(x = c(5, 5, 5, NA, 2, 3, 6, 8))

  expect_dplyr_equal(
    input %>%
      transmute(x = as.factor(x)) %>%
      collect(),
    df1
  )

  expect_warning(
    expect_dplyr_equal(
      input %>%
        transmute(x = as.factor(x)) %>%
        collect(),
      df2
    ),
    "Coercing dictionary values to R character factor levels"
  )

  # dictionary values with default null encoding behavior ("mask") omits
  # nulls from the dictionary values
  expect_equal(
    {
      rb1 <- df1 %>%
        record_batch() %>%
        transmute(x = dictionary_encode(x)) %>%
        compute()
      dict <- rb1$x$dictionary()
      as.vector(dict$Take(dict$SortIndices()))
    },
    sort(unique(df1$x), na.last = NA)
  )

  # dictionary values with "encode" null encoding behavior includes nulls in
  # the dictionary values
  expect_equal(
    {
      rb1 <- df1 %>%
        record_batch() %>%
        transmute(x = dictionary_encode(x, null_encoding_behavior = "encode")) %>%
        compute()
      dict <- rb1$x$dictionary()
      as.vector(dict$Take(dict$SortIndices()))
    },
    sort(unique(df1$x), na.last = TRUE)
  )

})

test_that("bad explicit type conversions with as.*()", {

  # Arrow returns lowercase "true", "false" (instead of "TRUE", "FALSE" like R)
  expect_error(
    expect_dplyr_equal(
      input %>%
        transmute(lgl2chr = as.character(lgl)) %>%
        collect(),
      tibble(lgl = c(TRUE, FALSE, NA)
      )
    )
  )

  # Arrow fails to parse these strings as numbers (instead of returning NAs with
  # a warning like R does)
  expect_error(
    expect_warning(
      expect_dplyr_equal(
        input %>%
          transmute(chr2num = as.numeric(chr)) %>%
          collect(),
        tibble(chr = c("l.O", "S.S", ""))
      )
    )
  )

  # Arrow fails to parse these strings as Booleans (instead of returning NAs
  # like R does)
  expect_error(
    expect_dplyr_equal(
      input %>%
        transmute(chr2lgl = as.logical(chr)) %>%
        collect(),
      tibble(chr = c("TRU", "FAX", ""))
    )
  )

})
