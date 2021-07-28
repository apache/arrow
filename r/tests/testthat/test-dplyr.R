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

library(dplyr)
library(stringr)

tbl <- example_data
# Add some better string data
tbl$verses <- verses[[1]]
# c(" a ", "  b  ", "   c   ", ...) increasing padding
# nchar =   3  5  7  9 11 13 15 17 19 21
tbl$padded_strings <- stringr::str_pad(letters[1:10], width = 2*(1:10) + 1, side = "both")

test_that("basic select/filter/collect", {
  batch <- record_batch(tbl)

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5)

  expect_s3_class(b2, "arrow_dplyr_query")
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
'InMemoryDataset (query)
int: int32
chr: string

* Filter: (((dbl > 2) and ((chr == "d") or (chr == "f"))) and (int < 5))
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

  # collect(as_data_frame = FALSE) always returns Table now
  expect_r6_class(b2, "Table")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")]
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    collect(as_data_frame = FALSE)
  expect_r6_class(b3, "Table")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    collect(as_data_frame = FALSE)
  expect_s3_class(b4, "arrow_dplyr_query")
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

  expect_r6_class(b1, "RecordBatch")

  b2 <- batch %>%
    select(int, chr) %>%
    filter(int > 5) %>%
    compute()

  expect_r6_class(b2, "Table")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")]
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    compute()
  expect_r6_class(b3, "Table")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    compute()
  expect_s3_class(b4, "arrow_dplyr_query")
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

  expect_r6_class(b2, "Table")
  expected <- tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")][1:2, ]
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    head(2)
  expect_r6_class(b3, "Table")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    head(2)
  expect_s3_class(b4, "arrow_dplyr_query")
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

  expect_r6_class(b2, "Table")
  expected <- tail(tbl[tbl$int > 5 & !is.na(tbl$int), c("int", "chr")], 2)
  expect_equal(as.data.frame(b2), expected)

  b3 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    tail(2)
  expect_r6_class(b3, "Table")
  expect_equal(as.data.frame(b3), set_names(expected, c("int", "strng")))

  b4 <- batch %>%
    select(int, strng = chr) %>%
    filter(int > 5) %>%
    group_by(int) %>%
    tail(2)
  expect_s3_class(b4, "arrow_dplyr_query")
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
  df <- tibble(a = 1, b = 1, c = 1, d = "a", e = "a", f = "a")
  expect_dplyr_equal(
    input %>% relocate(any_of(c("a", "e", "i", "o", "u"))) %>% collect(),
    df
  )
  expect_dplyr_equal(
    input %>% relocate(where(is.character)) %>% collect(),
    df
  )
  expect_dplyr_equal(
    input %>% relocate(a, b, c, .after = where(is.character)) %>% collect(),
    df
  )
  expect_dplyr_equal(
    input %>% relocate(d, e, f, .before = where(is.numeric)) %>% collect(),
    df
  )
  # works after other dplyr verbs
  expect_dplyr_equal(
    input %>%
      mutate(c = as.character(c)) %>%
      relocate(d, e, f, .after = where(is.numeric)) %>%
      collect(),
    df
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

test_that("is.finite(), is.infinite(), is.nan()", {
  df <- tibble(x =c(-4.94065645841246544e-324, 1.79769313486231570e+308, 0,
                    NA_real_, NaN, Inf, -Inf))
  expect_dplyr_equal(
    input %>%
      transmute(
        is_fin = is.finite(x),
        is_inf = is.infinite(x)
      ) %>% collect(),
    df
  )
  # is.nan() evaluates to FALSE on NA_real_ (ARROW-12850)
  expect_dplyr_equal(
    input %>%
      transmute(
        is_nan = is.nan(x)
      ) %>% collect(),
    df
  )
})

test_that("is.na() evaluates to TRUE on NaN (ARROW-12055)", {
  df <- tibble(x = c(1.1, 2.2, NA_real_, 4.4, NaN, 6.6, 7.7))
  expect_dplyr_equal(
    input %>%
      transmute(
        is_na = is.na(x)
      ) %>% collect(),
    df
  )
})

test_that("type checks with is() giving Arrow types", {
  # with class2=DataType
  expect_equal(
    Table$create(
        i32 = Array$create(1, int32()),
        dec = Array$create(pi)$cast(decimal(3, 2)),
        f64 = Array$create(1.1, float64()),
        str = Array$create("a", arrow::string())
      ) %>% transmute(
        i32_is_i32 = is(i32, int32()),
        i32_is_dec = is(i32, decimal(3, 2)),
        i32_is_i64 = is(i32, float64()),
        i32_is_str = is(i32, arrow::string()),
        dec_is_i32 = is(dec, int32()),
        dec_is_dec = is(dec, decimal(3, 2)),
        dec_is_i64 = is(dec, float64()),
        dec_is_str = is(dec, arrow::string()),
        f64_is_i32 = is(f64, int32()),
        f64_is_dec = is(f64, decimal(3, 2)),
        f64_is_i64 = is(f64, float64()),
        f64_is_str = is(f64, arrow::string()),
        str_is_i32 = is(str, int32()),
        str_is_dec = is(str, decimal(3, 2)),
        str_is_i64 = is(str, float64()),
        str_is_str = is(str, arrow::string())
      ) %>%
      collect() %>% t() %>% as.vector(),
    c(TRUE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, TRUE,
      FALSE, FALSE, FALSE, FALSE, TRUE)
  )
  # with class2=string
  expect_equal(
    Table$create(
        i32 = Array$create(1, int32()),
        f64 = Array$create(1.1, float64()),
        str = Array$create("a", arrow::string())
      ) %>% transmute(
        i32_is_i32 = is(i32, "int32"),
        i32_is_i64 = is(i32, "double"),
        i32_is_str = is(i32, "string"),
        f64_is_i32 = is(f64, "int32"),
        f64_is_i64 = is(f64, "double"),
        f64_is_str = is(f64, "string"),
        str_is_i32 = is(str, "int32"),
        str_is_i64 = is(str, "double"),
        str_is_str = is(str, "string")
      ) %>%
      collect() %>% t() %>% as.vector(),
    c(TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, TRUE)
  )
  # with class2=string alias
  expect_equal(
    Table$create(
        f16 = Array$create(NA_real_, halffloat()),
        f32 = Array$create(1.1, float()),
        f64 = Array$create(2.2, float64()),
        lgl = Array$create(TRUE, bool()),
        str = Array$create("a", arrow::string())
      ) %>% transmute(
        f16_is_f16 = is(f16, "float16"),
        f16_is_f32 = is(f16, "float32"),
        f16_is_f64 = is(f16, "float64"),
        f16_is_lgl = is(f16, "boolean"),
        f16_is_str = is(f16, "utf8"),
        f32_is_f16 = is(f32, "float16"),
        f32_is_f32 = is(f32, "float32"),
        f32_is_f64 = is(f32, "float64"),
        f32_is_lgl = is(f32, "boolean"),
        f32_is_str = is(f32, "utf8"),
        f64_is_f16 = is(f64, "float16"),
        f64_is_f32 = is(f64, "float32"),
        f64_is_f64 = is(f64, "float64"),
        f64_is_lgl = is(f64, "boolean"),
        f64_is_str = is(f64, "utf8"),
        lgl_is_f16 = is(lgl, "float16"),
        lgl_is_f32 = is(lgl, "float32"),
        lgl_is_f64 = is(lgl, "float64"),
        lgl_is_lgl = is(lgl, "boolean"),
        lgl_is_str = is(lgl, "utf8"),
        str_is_f16 = is(str, "float16"),
        str_is_f32 = is(str, "float32"),
        str_is_f64 = is(str, "float64"),
        str_is_lgl = is(str, "boolean"),
        str_is_str = is(str, "utf8")
      ) %>%
      collect() %>% t() %>% as.vector(),
    c(TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE,
      FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE,
      FALSE, FALSE, TRUE)
  )
})

test_that("type checks with is() giving R types", {
  library(bit64)
  expect_dplyr_equal(
    input %>%
      transmute(
        chr_is_chr = is(chr, "character"),
        chr_is_fct = is(chr, "factor"),
        chr_is_int = is(chr, "integer"),
        chr_is_i64 = is(chr, "integer64"),
        chr_is_lst = is(chr, "list"),
        chr_is_lgl = is(chr, "logical"),
        chr_is_num = is(chr, "numeric"),
        dbl_is_chr = is(dbl, "character"),
        dbl_is_fct = is(dbl, "factor"),
        dbl_is_int = is(dbl, "integer"),
        dbl_is_i64 = is(dbl, "integer64"),
        dbl_is_lst = is(dbl, "list"),
        dbl_is_lgl = is(dbl, "logical"),
        dbl_is_num = is(dbl, "numeric"),
        fct_is_chr = is(fct, "character"),
        fct_is_fct = is(fct, "factor"),
        fct_is_int = is(fct, "integer"),
        fct_is_i64 = is(fct, "integer64"),
        fct_is_lst = is(fct, "list"),
        fct_is_lgl = is(fct, "logical"),
        fct_is_num = is(fct, "numeric"),
        int_is_chr = is(int, "character"),
        int_is_fct = is(int, "factor"),
        int_is_int = is(int, "integer"),
        int_is_i64 = is(int, "integer64"),
        int_is_lst = is(int, "list"),
        int_is_lgl = is(int, "logical"),
        int_is_num = is(int, "numeric"),
        lgl_is_chr = is(lgl, "character"),
        lgl_is_fct = is(lgl, "factor"),
        lgl_is_int = is(lgl, "integer"),
        lgl_is_i64 = is(lgl, "integer64"),
        lgl_is_lst = is(lgl, "list"),
        lgl_is_lgl = is(lgl, "logical"),
        lgl_is_num = is(lgl, "numeric")
      ) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      transmute(
        i64_is_chr = is(i64, "character"),
        i64_is_fct = is(i64, "factor"),
        # we want Arrow to return TRUE, but bit64 returns FALSE
        #i64_is_int = is(i64, "integer"),
        i64_is_i64 = is(i64, "integer64"),
        i64_is_lst = is(i64, "list"),
        i64_is_lgl = is(i64, "logical"),
        # we want Arrow to return TRUE, but bit64 returns FALSE
        #i64_is_num = is(i64, "numeric"),
        lst_is_chr = is(lst, "character"),
        lst_is_fct = is(lst, "factor"),
        lst_is_int = is(lst, "integer"),
        lst_is_i64 = is(lst, "integer64"),
        lst_is_lst = is(lst, "list"),
        lst_is_lgl = is(lst, "logical"),
        lst_is_num = is(lst, "numeric")
      ) %>%
      collect(),
    tibble(
      i64 = as.integer64(1:3),
      lst = list(c("a", "b"), c("d", "e"), c("f", "g"))
    )
  )
})

test_that("type checks with is.*()", {
  library(bit64)
  expect_dplyr_equal(
    input %>%
      transmute(
        chr_is_chr = is.character(chr),
        chr_is_dbl = is.double(chr),
        chr_is_fct = is.factor(chr),
        chr_is_int = is.integer(chr),
        chr_is_i64 = is.integer64(chr),
        chr_is_lst = is.list(chr),
        chr_is_lgl = is.logical(chr),
        chr_is_num = is.numeric(chr),
        dbl_is_chr = is.character(dbl),
        dbl_is_dbl = is.double(dbl),
        dbl_is_fct = is.factor(dbl),
        dbl_is_int = is.integer(dbl),
        dbl_is_i64 = is.integer64(dbl),
        dbl_is_lst = is.list(dbl),
        dbl_is_lgl = is.logical(dbl),
        dbl_is_num = is.numeric(dbl),
        fct_is_chr = is.character(fct),
        fct_is_dbl = is.double(fct),
        fct_is_fct = is.factor(fct),
        fct_is_int = is.integer(fct),
        fct_is_i64 = is.integer64(fct),
        fct_is_lst = is.list(fct),
        fct_is_lgl = is.logical(fct),
        fct_is_num = is.numeric(fct),
        int_is_chr = is.character(int),
        int_is_dbl = is.double(int),
        int_is_fct = is.factor(int),
        int_is_int = is.integer(int),
        int_is_i64 = is.integer64(int),
        int_is_lst = is.list(int),
        int_is_lgl = is.logical(int),
        int_is_num = is.numeric(int),
        lgl_is_chr = is.character(lgl),
        lgl_is_dbl = is.double(lgl),
        lgl_is_fct = is.factor(lgl),
        lgl_is_int = is.integer(lgl),
        lgl_is_i64 = is.integer64(lgl),
        lgl_is_lst = is.list(lgl),
        lgl_is_lgl = is.logical(lgl),
        lgl_is_num = is.numeric(lgl)
      ) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      transmute(
        i64_is_chr = is.character(i64),
        # TODO: investigate why this is not matching when testthat runs it
        #i64_is_dbl = is.double(i64),
        i64_is_fct = is.factor(i64),
        # we want Arrow to return TRUE, but bit64 returns FALSE
        #i64_is_int = is.integer(i64),
        i64_is_i64 = is.integer64(i64),
        i64_is_lst = is.list(i64),
        i64_is_lgl = is.logical(i64),
        i64_is_num = is.numeric(i64),
        lst_is_chr = is.character(lst),
        lst_is_dbl = is.double(lst),
        lst_is_fct = is.factor(lst),
        lst_is_int = is.integer(lst),
        lst_is_i64 = is.integer64(lst),
        lst_is_lst = is.list(lst),
        lst_is_lgl = is.logical(lst),
        lst_is_num = is.numeric(lst)
      ) %>%
      collect(),
    tibble(
      i64 = as.integer64(1:3),
      lst = list(c("a", "b"), c("d", "e"), c("f", "g"))
    )
  )
})

test_that("type checks with is_*()", {
  library(rlang)
  expect_dplyr_equal(
    input %>%
      transmute(
        chr_is_chr = is_character(chr),
        chr_is_dbl = is_double(chr),
        chr_is_int = is_integer(chr),
        chr_is_lst = is_list(chr),
        chr_is_lgl = is_logical(chr),
        dbl_is_chr = is_character(dbl),
        dbl_is_dbl = is_double(dbl),
        dbl_is_int = is_integer(dbl),
        dbl_is_lst = is_list(dbl),
        dbl_is_lgl = is_logical(dbl),
        int_is_chr = is_character(int),
        int_is_dbl = is_double(int),
        int_is_int = is_integer(int),
        int_is_lst = is_list(int),
        int_is_lgl = is_logical(int),
        lgl_is_chr = is_character(lgl),
        lgl_is_dbl = is_double(lgl),
        lgl_is_int = is_integer(lgl),
        lgl_is_lst = is_list(lgl),
        lgl_is_lgl = is_logical(lgl)
      ) %>%
      collect(),
    tbl
  )
})

test_that("type checks on expressions", {
  expect_dplyr_equal(
    input %>%
      transmute(
        a = is.character(as.character(int)),
        b = is.integer(as.character(int)),
        c = is.integer(int + int),
        d = is.double(int + dbl),
        e = is.logical(dbl > pi)
      ) %>%
      collect(),
    tbl
  )

  # the code in the expectation below depends on RE2
  skip_if_not_available("re2")

  expect_dplyr_equal(
    input %>%
      transmute(
        a = is.logical(grepl("[def]", chr))
      ) %>%
      collect(),
    tbl
  )
})

test_that("type checks on R scalar literals", {
  expect_dplyr_equal(
    input %>%
      transmute(
        chr_is_chr = is.character("foo"),
        int_is_chr = is.character(42L),
        int_is_int = is.integer(42L),
        chr_is_int = is.integer("foo"),
        dbl_is_num = is.numeric(3.14159),
        int_is_num = is.numeric(42L),
        chr_is_num = is.numeric("foo"),
        dbl_is_dbl = is.double(3.14159),
        chr_is_dbl = is.double("foo"),
        lgl_is_lgl = is.logical(TRUE),
        chr_is_lgl = is.logical("foo"),
        fct_is_fct = is.factor(factor("foo", levels = c("foo", "bar", "baz"))),
        chr_is_fct = is.factor("foo"),
        lst_is_lst = is.list(list(c(a = "foo", b = "bar"))),
        chr_is_lst = is.list("foo")
      ) %>%
      collect(),
    tbl
  )
})

test_that("as.factor()/dictionary_encode()", {
  skip("ARROW-12632: ExecuteScalarExpression cannot Execute non-scalar expression {x=dictionary_encode(x, {NON-REPRESENTABLE OPTIONS})}")
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

test_that("No duplicate field names are allowed in an arrow_dplyr_query", {
  expect_error(
    Table$create(tbl, tbl) %>%
      filter(int > 0),
    regexp = 'The following field names were found more than once in the data: "int", "dbl", "dbl2", "lgl", "false", "chr", "fct", "verses", and "padded_strings"'
  )
})

test_that("abs()", {
  df <- tibble(x = c(-127, -10, -1, -0 , 0, 1, 10, 127, NA))

  expect_dplyr_equal(
    input %>%
      transmute(abs = abs(x)) %>%
      collect(),
    df
  )
})

test_that("sign()", {
  df <- tibble(x = c(-127, -10, -1, -0 , 0, 1, 10, 127, NA))

  expect_dplyr_equal(
    input %>%
      transmute(sign = sign(x)) %>%
      collect(),
    df
  )
})

test_that("ceiling(), floor(), trunc()", {
  df <- tibble(x = c(-1, -0.55, -0.5, -0.1, 0, 0.1, 0.5, 0.55, 1, NA, NaN))

  expect_dplyr_equal(
    input %>%
      mutate(
        c = ceiling(x),
        f = floor(x),
        t = trunc(x)
      ) %>%
      collect(),
    df
  )
})

test_that("log functions", {

  df <- tibble(x = c(1:10, NA, NA))

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = exp(1))) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = 2)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log(x, base = 10)) %>%
      collect(),
    df
  )

  expect_error(
    nse_funcs$log(Expression$scalar(x), base = 5),
    "`base` values other than exp(1), 2 and 10 not supported in Arrow",
    fixed = TRUE
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = logb(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log1p(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log2(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = log10(x)) %>%
      collect(),
    df
  )

})

test_that("trig functions", {

  df <- tibble(x = c(seq(from = 0, to = 1, by = 0.1), NA))

  expect_dplyr_equal(
    input %>%
      mutate(y = sin(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = cos(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = tan(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = asin(x)) %>%
      collect(),
    df
  )

  expect_dplyr_equal(
    input %>%
      mutate(y = acos(x)) %>%
      collect(),
    df
  )

})

test_that("if_else and ifelse", {
  tbl <- example_data
  tbl$another_chr <- tail(letters, 10)

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, 1, 0)
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, int, 0L)
      ) %>% collect(),
    tbl
  )

  expect_error(
    Table$create(tbl) %>%
      mutate(
        y = if_else(int > 5, 1, FALSE)
      ) %>% collect(),
    'NotImplemented: Function if_else has no kernel matching input types'
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, 1, NA_real_)
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = ifelse(int > 5, 1, 0)
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(dbl > 5, TRUE, FALSE)
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(chr %in% letters[1:3], 1L, 3L)
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, "one", "zero")
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, chr, another_chr)
      ) %>% collect(),
    tbl
  )

  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, "true", chr, missing = "MISSING")
      ) %>% collect(),
    tbl
  )

  # TODO: remove the mutate + warning after ARROW-13358 is merged and Arrow
  # supports factors in if(_)else
  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(int > 5, fct, factor("a"))
      ) %>% collect() %>%
      # This is a no-op on the Arrow side, but necessary to make the results equal
      mutate(y = as.character(y)),
    tbl,
    warning = "Dictionaries .* are currently converted to strings .* in if_else and ifelse"
  )

  # detecting NA and NaN works just fine
  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(is.na(dbl), chr, "false", missing = "MISSING")
      ) %>% collect(),
    example_data_for_sorting
  )

  # However, currently comparisons with NaNs return false and not NaNs or NAs
  skip("ARROW-13364")
  expect_dplyr_equal(
    input %>%
      mutate(
        y = if_else(dbl > 5, chr, another_chr, missing = "MISSING")
      ) %>% collect(),
    example_data_for_sorting
  )

  skip("TODO: could? should? we support the autocasting in ifelse")
  expect_dplyr_equal(
    input %>%
      mutate(y = ifelse(int > 5, 1, FALSE)) %>%
      collect(),
    tbl
  )
})

test_that("case_when()", {
  expect_dplyr_equal(
    input %>%
      transmute(cw = case_when(lgl ~ dbl, !false ~ dbl + dbl2)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(cw = case_when(int > 5 ~ 1, TRUE ~ 0)) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      transmute(cw = case_when(chr %in% letters[1:3] ~ 1L) + 41L) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      filter(case_when(
        dbl + int - 1.1 == dbl2 ~ TRUE,
        NA ~ NA,
        TRUE ~ FALSE
      ) & !is.na(dbl2)) %>%
      collect(),
    tbl
  )

  # dplyr::case_when() errors if values on right side of formulas do not have
  # exactly the same type, but the Arrow case_when kernel allows compatible types
  expect_equal(
    tbl %>%
      mutate(i64 = as.integer64(1e10)) %>%
      Table$create() %>%
      transmute(cw = case_when(
        is.na(fct) ~ int,
        is.na(chr) ~ dbl,
        TRUE ~ i64
      )) %>%
      collect(),
    tbl %>%
      transmute(
        cw = ifelse(is.na(fct), int, ifelse(is.na(chr), dbl, 1e10))
      )
  )

  # expected errors (which are caught by abandon_ship() and changed to warnings)
  # TODO: Find a way to test these directly without abandon_ship() interfering
  expect_error(
    # no cases
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when()),
      "case_when"
    )
  )
  expect_error(
    # argument not a formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(TRUE ~ FALSE, TRUE)),
      "case_when"
    )
  )
  expect_error(
    # non-logical R scalar on left side of formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(0L ~ FALSE, TRUE ~ FALSE)),
      "case_when"
    )
  )
  expect_error(
    # non-logical Arrow column reference on left side of formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(int ~ FALSE)),
      "case_when"
    )
  )
  expect_error(
    # non-logical Arrow expression on left side of formula
    expect_warning(
      tbl %>%
        Table$create() %>%
        transmute(cw = case_when(dbl + 3.14159 ~ TRUE)),
      "case_when"
    )
  )

  skip("case_when does not yet support with variable-width types (ARROW-13222)")
  expect_dplyr_equal(
    input %>%
      transmute(cw = case_when(lgl ~ "abc")) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      transmute(cw = case_when(lgl ~ verses, !false ~ paste(chr, chr))) %>%
      collect(),
    tbl
  )
  expect_dplyr_equal(
    input %>%
      mutate(
        cw = paste0(case_when(!(!(!(lgl))) ~ factor(chr), TRUE ~ fct), "!")
      ) %>%
      collect(),
    tbl
  )
})

test_that("coalesce()", {
  # character
  df <- tibble(
    w = c(NA_character_, NA_character_, NA_character_),
    x = c(NA_character_, NA_character_, "c"),
    y = c(NA_character_, "b", "c"),
    z = c("a", "b", "c")
  )
  expect_dplyr_equal(
    input %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect(),
    df
  )

  # integer
  df <- tibble(
    w = c(NA_integer_, NA_integer_, NA_integer_),
    x = c(NA_integer_, NA_integer_, 3L),
    y = c(NA_integer_, 2L, 3L),
    z = 1:3
  )
  expect_dplyr_equal(
    input %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect(),
    df
  )

  # double with NaNs
  df <- tibble(
    w = c(NA_real_, NaN, NA_real_),
    x = c(NA_real_, NaN, 3.3),
    y = c(NA_real_, 2.2, 3.3),
    z = c(1.1, 2.2, 3.3)
  )
  expect_dplyr_equal(
    input %>%
      mutate(
        cw = coalesce(w),
        cz = coalesce(z),
        cwx = coalesce(w, x),
        cwxy = coalesce(w, x, y),
        cwxyz = coalesce(w, x, y, z)
      ) %>%
      collect(),
    df
  )
  # NaNs stay NaN and are not converted to NA in the results
  # (testing this requires expect_identical())
  expect_identical(
    df %>% Table$create() %>% mutate(cwx = coalesce(w, x)) %>% collect(),
    df %>% mutate(cwx = coalesce(w, x))
  )
  expect_identical(
    df %>% Table$create() %>% transmute(cw = coalesce(w)) %>% collect(),
    df %>% transmute(cw = coalesce(w))
  )
  expect_identical(
    df %>% Table$create() %>% transmute(cn = coalesce(NaN)) %>% collect(),
    df %>% transmute(cn = coalesce(NaN))
  )
  # singles stay single
  expect_equal(
    (df %>%
      Table$create(schema = schema(
        w = float32(),
        x = float32(),
        y = float32(),
        z = float32()
      )) %>%
      transmute(c = coalesce(w, x, y, z)) %>%
      compute()
    )$schema[[1]]$type,
    float32()
  )
  # with R literal values
  expect_dplyr_equal(
    input %>%
      mutate(
        c1 = coalesce(4.4),
        c2 = coalesce(NA_real_),
        c3 = coalesce(NaN),
        c4 = coalesce(w, x, y, 5.5),
        c5 = coalesce(w, x, y, NA_real_),
        c6 = coalesce(w, x, y, NaN)
      ) %>%
      collect(),
    df
  )

  # factors
  # TODO: remove the mutate + warning after ARROW-13390 is merged and Arrow
  # supports factors in coalesce
  df <- tibble(
    x = factor("a", levels = c("a", "z")),
    y = factor("b", levels = c("a", "b", "c"))
  )
  expect_dplyr_equal(
    input %>%
      mutate(c = coalesce(x, y)) %>%
      collect() %>%
      # This is a no-op on the Arrow side, but necessary to make the results equal
      mutate(c = as.character(c)),
    df,
    warning = "Dictionaries .* are currently converted to strings .* in coalesce"
  )

  # no arguments
  expect_error(
    nse_funcs$coalesce(),
    "At least one argument must be supplied to coalesce()",
    fixed = TRUE
  )
})
