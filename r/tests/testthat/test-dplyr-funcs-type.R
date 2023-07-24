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
suppressPackageStartupMessages(library(bit64))
suppressPackageStartupMessages(library(lubridate))

skip_if_not_available("acero")

tbl <- example_data

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
      object = {
        t1 <- Table$create(x = num_int32) %>%
          transmute(x = cast(x, type)) %>%
          compute()
        t1$schema[[1]]$type
      },
      as_type(type)
    )
    expect_type_equal(
      object = {
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
      object = {
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
    object = {
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
  compare_dplyr_binding(
    .input %>%
      transmute(
        int2chr = as.character(int),
        int2dbl = as.double(int),
        int2int = as.integer(int),
        int2num = as.numeric(int),
        dbl2chr = as.character(dbl),
        dbl2dbl = as.double(dbl),
        dbl2int = as.integer(dbl),
        dbl2num = as.numeric(dbl),
        rint2chr = as.character(1L),
        rint2dbl = as.double(1),
        rint2int = as.integer(1L),
        rint2num = as.numeric(1.5),
        rdbl2chr = as.character(1.5),
        rdbl2dbl = as.double(1.5),
        rdbl2int = as.integer(1.5),
        rdbl2num = as.numeric(1.5)
      ) %>%
      collect(),
    tbl
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        chr2chr = as.character(chr),
        chr2dbl = as.double(chr),
        chr2int = as.integer(chr),
        chr2num = as.numeric(chr),
        chr2chr2 = base::as.character(chr),
        chr2dbl2 = base::as.double(chr),
        chr2int2 = base::as.integer(chr),
        chr2num2 = base::as.numeric(chr),
        rchr2chr = as.character("string"),
        rchr2dbl = as.double("1.5"),
        rchr2int = as.integer("1"),
        rchr2num = as.numeric("1.5")
      ) %>%
      collect(),
    tibble(chr = c("1", "2", "3"))
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        chr2i64 = as.integer64(chr),
        chr2i64_nmspc = bit64::as.integer64(chr),
        dbl2i64 = as.integer64(dbl),
        i642i64 = as.integer64(i64),
        rchr2i64 = as.integer64("10000000000"),
        rdbl2i64 = as.integer64(10000000000),
        ri642i64 = as.integer64(as.integer64(1e10))
      ) %>%
      collect(),
    tibble(chr = "10000000000", dbl = 10000000000, i64 = as.integer64(1e10))
  )
  compare_dplyr_binding(
    .input %>%
      transmute(
        chr2lgl = as.logical(chr),
        chr2lgl2 = base::as.logical(chr),
        dbl2lgl = as.logical(dbl),
        int2lgl = as.logical(int),
        rchr2lgl = as.logical("TRUE"),
        rdbl2lgl = as.logical(0),
        rint2lgl = as.logical(1L)
      ) %>%
      collect(),
    tibble(
      chr = c("TRUE", "FALSE", "true", "false"),
      dbl = c(1, 0, -99, 0),
      int = c(1L, 0L, -99L, 0L)
    )
  )
  compare_dplyr_binding(
    .input %>%
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
        lgl2lgl = as.logical(lgl),
        rdbl2chr = as.character(1.5),
        rdbl2dbl = as.double(1.5),
        rdbl2int = as.integer(1.5),
        rdbl2lgl = as.logical(1),
        rint2chr = as.character(1L),
        rint2dbl = as.double(1L),
        rint2int = as.integer(1L),
        rint2lgl = as.logical(1L),
        rlgl2chr = as.character(TRUE), # Arrow returns "true", "false" here ...
        rlgl2dbl = as.double(FALSE),
        rlgl2int = as.integer(NA),
        rlgl2lgl = as.logical(FALSE)
      ) %>%
      collect() %>%
      # need to use toupper() *after* collect() or else skip if utf8proc not available
      mutate(
        lgl2chr = toupper(lgl2chr),
        rlgl2chr = toupper(rlgl2chr)
      ), # ... but we need "TRUE", "FALSE"
    tibble(
      dbl = c(1, 0, NA_real_),
      int = c(1L, 0L, NA_integer_),
      lgl = c(TRUE, FALSE, NA)
    )
  )
})

test_that("is.finite(), is.infinite(), is.nan()", {
  df <- tibble(x = c(
    -4.94065645841246544e-324, 1.79769313486231570e+308, 0,
    NA_real_, NaN, Inf, -Inf
  ))
  compare_dplyr_binding(
    .input %>%
      transmute(
        is_fin = is.finite(x),
        is_inf = is.infinite(x),
        is_fin2 = base::is.finite(x),
        is_inf2 = base::is.infinite(x)
      ) %>%
      collect(),
    df
  )
  # is.nan() evaluates to FALSE on NA_real_ (ARROW-12850)
  compare_dplyr_binding(
    .input %>%
      transmute(
        is_nan = is.nan(x),
        is_nan2 = base::is.nan(x)
      ) %>%
      collect(),
    df
  )
})

test_that("is.na() evaluates to TRUE on NaN (ARROW-12055)", {
  df <- tibble(x = c(1.1, 2.2, NA_real_, 4.4, NaN, 6.6, 7.7))
  compare_dplyr_binding(
    .input %>%
      transmute(
        is_na = is.na(x),
        is_na2 = base::is.na(x)
      ) %>%
      collect(),
    df
  )
})

test_that("type checks with is() giving Arrow types", {
  # with class2=DataType
  expect_equal(
    Table$create(
      i32 = Array$create(1, int32()),
      dec = Array$create(pi)$cast(decimal(3, 2)),
      dec128 = Array$create(pi)$cast(decimal128(3, 2)),
      dec256 = Array$create(pi)$cast(decimal256(3, 2)),
      f64 = Array$create(1.1, float64()),
      str = Array$create("a", arrow::string())
    ) %>%
      transmute(
        i32_is_i32 = is(i32, int32()),
        i32_is_dec = is(i32, decimal(3, 2)),
        i32_is_dec128 = is(i32, decimal128(3, 2)),
        i32_is_dec256 = is(i32, decimal256(3, 2)),
        i32_is_f64 = is(i32, float64()),
        i32_is_str = is(i32, string()),
        dec_is_i32 = is(dec, int32()),
        dec_is_dec = is(dec, decimal(3, 2)),
        dec_is_dec128 = is(dec, decimal128(3, 2)),
        dec_is_dec256 = is(dec, decimal256(3, 2)),
        dec_is_f64 = is(dec, float64()),
        dec_is_str = is(dec, string()),
        dec128_is_i32 = is(dec128, int32()),
        dec128_is_dec128 = is(dec128, decimal128(3, 2)),
        dec128_is_dec256 = is(dec128, decimal256(3, 2)),
        dec128_is_f64 = is(dec128, float64()),
        dec128_is_str = is(dec128, string()),
        dec256_is_i32 = is(dec128, int32()),
        dec256_is_dec128 = is(dec128, decimal128(3, 2)),
        dec256_is_dec256 = is(dec128, decimal256(3, 2)),
        dec256_is_f64 = is(dec128, float64()),
        dec256_is_str = is(dec128, string()),
        f64_is_i32 = is(f64, int32()),
        f64_is_dec = is(f64, decimal(3, 2)),
        f64_is_dec128 = is(f64, decimal128(3, 2)),
        f64_is_dec256 = is(f64, decimal256(3, 2)),
        f64_is_f64 = is(f64, float64()),
        f64_is_str = is(f64, string()),
        str_is_i32 = is(str, int32()),
        str_is_dec128 = is(str, decimal128(3, 2)),
        str_is_dec256 = is(str, decimal256(3, 2)),
        str_is_i64 = is(str, float64()),
        str_is_str = is(str, string())
      ) %>%
      collect() %>%
      t() %>%
      as.vector(),
    c(
      TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, TRUE, FALSE, FALSE,
      FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE,
      FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE
    )
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
      i32_is_i32_nmspc = methods::is(i32, "int32"),
      i32_is_i64_nmspc = methods::is(i32, "double"),
      i32_is_str_nmspc = methods::is(i32, "string"),
      f64_is_i32 = is(f64, "int32"),
      f64_is_i64 = is(f64, "double"),
      f64_is_str = is(f64, "string"),
      str_is_i32 = is(str, "int32"),
      str_is_i64 = is(str, "double"),
      str_is_str = is(str, "string")
    ) %>%
      collect() %>%
      t() %>%
      as.vector(),
    c(TRUE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, TRUE)
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
      collect() %>%
      t() %>%
      as.vector(),
    c(
      TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE,
      FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE, FALSE, FALSE, FALSE,
      FALSE, FALSE, TRUE
    )
  )
})

test_that("type checks with is() giving R types", {
  library(bit64)
  compare_dplyr_binding(
    .input %>%
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
  compare_dplyr_binding(
    .input %>%
      transmute(
        i64_is_chr = is(i64, "character"),
        i64_is_fct = is(i64, "factor"),
        # we want Arrow to return TRUE, but bit64 returns FALSE
        # i64_is_int = is(i64, "integer"), # nolint
        i64_is_i64 = is(i64, "integer64"),
        i64_is_lst = is(i64, "list"),
        i64_is_lgl = is(i64, "logical"),
        # we want Arrow to return TRUE, but bit64 returns FALSE
        # i64_is_num = is(i64, "numeric"), # nolint
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
  compare_dplyr_binding(
    .input %>%
      transmute(
        chr_is_chr = is.character(chr),
        chr_is_dbl = is.double(chr),
        chr_is_fct = is.factor(chr),
        chr_is_int = is.integer(chr),
        chr_is_i64 = is.integer64(chr),
        chr_is_lst = is.list(chr),
        chr_is_lgl = is.logical(chr),
        chr_is_num = is.numeric(chr),
        chr_is_chr2 = base::is.character(chr),
        chr_is_dbl2 = base::is.double(chr),
        chr_is_fct2 = base::is.factor(chr),
        chr_is_int2 = base::is.integer(chr),
        chr_is_i642 = bit64::is.integer64(chr),
        chr_is_lst2 = base::is.list(chr),
        chr_is_lgl2 = base::is.logical(chr),
        chr_is_num2 = base::is.numeric(chr),
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
  compare_dplyr_binding(
    .input %>%
      transmute(
        i64_is_chr = is.character(i64),
        # TODO: investigate why this is not matching when testthat runs it
        # i64_is_dbl = is.double(i64), # nolint
        i64_is_fct = is.factor(i64),
        # we want Arrow to return TRUE, but bit64 returns FALSE
        # i64_is_int = is.integer(i64), # nolint
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
  library(rlang, warn.conflicts = FALSE)
  compare_dplyr_binding(
    .input %>%
      transmute(
        chr_is_chr = is_character(chr),
        chr_is_dbl = is_double(chr),
        chr_is_int = is_integer(chr),
        chr_is_lst = is_list(chr),
        chr_is_lgl = is_logical(chr),
        chr_is_chr2 = rlang::is_character(chr),
        chr_is_dbl2 = rlang::is_double(chr),
        chr_is_int2 = rlang::is_integer(chr),
        chr_is_lst2 = rlang::is_list(chr),
        chr_is_lgl2 = rlang::is_logical(chr),
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
  compare_dplyr_binding(
    .input %>%
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

  compare_dplyr_binding(
    .input %>%
      transmute(
        a = is.logical(grepl("[def]", chr))
      ) %>%
      collect(),
    tbl
  )
})

test_that("type checks on R scalar literals", {
  compare_dplyr_binding(
    .input %>%
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
  skip("ARROW-12632: ExecuteScalarExpression cannot Execute non-scalar expression")
  df1 <- tibble(x = c("C", "D", "B", NA, "D", "B", "S", "A", "B", "Z", "B"))
  df2 <- tibble(x = c(5, 5, 5, NA, 2, 3, 6, 8))

  compare_dplyr_binding(
    .input %>%
      transmute(
        x = as.factor(x),
        x2 = base::as.factor(x)
      ) %>%
      collect(),
    df1
  )

  expect_warning(
    compare_dplyr_binding(
      .input %>%
        transmute(x = as.factor(x)) %>%
        collect(),
      df2
    ),
    "Coercing dictionary values to R character factor levels"
  )

  # dictionary values with default null encoding behavior ("mask") omits
  # nulls from the dictionary values
  expect_equal(
    object = {
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
    object = {
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
    compare_dplyr_binding(
      .input %>%
        transmute(lgl2chr = as.character(lgl)) %>%
        collect(),
      tibble(lgl = c(TRUE, FALSE, NA))
    )
  )

  # Arrow fails to parse these strings as numbers (instead of returning NAs with
  # a warning like R does)
  expect_error(
    expect_warning(
      compare_dplyr_binding(
        .input %>%
          transmute(chr2num = as.numeric(chr)) %>%
          collect(),
        tibble(chr = c("l.O", "S.S", ""))
      )
    )
  )

  # Arrow fails to parse these strings as Booleans (instead of returning NAs
  # like R does)
  expect_error(
    compare_dplyr_binding(
      .input %>%
        transmute(chr2lgl = as.logical(chr)) %>%
        collect(),
      tibble(chr = c("TRU", "FAX", ""))
    )
  )
})

test_that("structs/nested data frames/tibbles can be created", {
  df <- tibble(regular_col1 = 1L, regular_col2 = "a")

  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = tibble(
          regular_col1 = regular_col1,
          regular_col2 = regular_col2
        ),
        df_col2 = tibble::tibble(
          regular_col1 = regular_col1,
          regular_col2 = regular_col2
        )
      ) %>%
      collect(),
    df
  )

  # check auto column naming
  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = tibble(regular_col1, regular_col2)
      ) %>%
      collect(),
    df
  )

  # ...and that other arguments are not supported
  expect_warning(
    record_batch(char_col = "a") %>%
      mutate(df_col = tibble(char_col, .rows = 1L)),
    ".rows not supported in Arrow"
  )

  expect_warning(
    record_batch(char_col = "a") %>%
      mutate(df_col = tibble(char_col, .name_repair = "universal")),
    ".name_repair not supported in Arrow"
  )

  # check that data.frame is mapped too
  # stringsAsFactors default is TRUE in R 3.6, which is still tested on CI
  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = data.frame(regular_col1, regular_col2, stringsAsFactors = FALSE)
      ) %>%
      collect() %>%
      mutate(df_col = as.data.frame(df_col)),
    df
  )

  # check with fix.empty.names = FALSE
  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = data.frame(regular_col1, fix.empty.names = FALSE)
      ) %>%
      collect() %>%
      mutate(df_col = as.data.frame(df_col)),
    df
  )

  # check with check.names = TRUE and FALSE
  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = data.frame(regular_col1, regular_col1, check.names = TRUE)
      ) %>%
      collect() %>%
      mutate(df_col = as.data.frame(df_col)),
    df
  )

  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = data.frame(regular_col1, regular_col1, check.names = FALSE),
        df_col2 = base::data.frame(regular_col1, regular_col1, check.names = FALSE)
      ) %>%
      collect() %>%
      mutate(
        df_col = as.data.frame(df_col),
        df_col2 = as.data.frame(df_col2)
      ),
    df
  )

  # ...and that other arguments are not supported
  expect_warning(
    record_batch(char_col = "a") %>%
      mutate(df_col = data.frame(char_col, stringsAsFactors = TRUE)),
    "stringsAsFactors = TRUE not supported in Arrow"
  )

  expect_warning(
    record_batch(char_col = "a") %>%
      mutate(df_col = data.frame(char_col, row.names = 1L)),
    "row.names not supported in Arrow"
  )

  expect_warning(
    record_batch(char_col = "a") %>%
      mutate(df_col = data.frame(char_col, check.rows = TRUE)),
    "check.rows not supported in Arrow"
  )
})

test_that("nested structs can be created from scalars and existing data frames", {
  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = tibble(b = 3)
      ) %>%
      collect(),
    tibble(a = 1:2)
  )

  # technically this is handled by Scalar$create() since there is no
  # call to data.frame or tibble() within a dplyr verb
  existing_data_frame <- tibble(b = 3)
  compare_dplyr_binding(
    .input %>%
      transmute(
        df_col = existing_data_frame
      ) %>%
      collect(),
    tibble(a = 1:2)
  )
})

test_that("format date/time", {
  # TODO(ARROW-16399): remove this workaround
  if (tolower(Sys.info()[["sysname"]]) == "windows") {
    withr::local_locale(LC_TIME = "C")
  }
  # In 3.4 the lack of tzone attribute causes spurious failures
  skip_on_r_older_than("3.5")

  times <- tibble(
    datetime = c(lubridate::ymd_hms("2018-10-07 19:04:05", tz = "Pacific/Marquesas"), NA),
    date = c(as.Date("2021-01-01"), NA)
  )
  formats <- "%a %A %w %d %b %B %m %y %Y %H %I %p %M %z %Z %j %U %W %x %X %% %G %V %u"
  formats_date <- "%a %A %w %d %b %B %m %y %Y %H %I %p %M %j %U %W %x %X %% %G %V %u"

  compare_dplyr_binding(
    .input %>%
      mutate(
        x = format(datetime, format = formats),
        x2 = base::format(datetime, format = formats)
      ) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = format(date, format = formats_date)) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = format(datetime, format = formats, tz = "Europe/Bucharest")) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(x = format(datetime, format = formats, tz = "EST", usetz = TRUE)) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        x = format(1),
        y = format(13.7, nsmall = 3)
      ) %>%
      collect(),
    times
  )

  compare_dplyr_binding(
    .input %>%
      mutate(start_date = format(as.POSIXct("2022-01-01 01:01:00"))) %>%
      collect(),
    times
  )

  withr::with_timezone(
    "Pacific/Marquesas",
    {
      compare_dplyr_binding(
        .input %>%
          mutate(
            x = format(datetime, format = formats, tz = "EST"),
            x_date = format(date, format = formats_date, tz = "EST")
          ) %>%
          collect(),
        times
      )

      compare_dplyr_binding(
        .input %>%
          mutate(
            x = format(datetime, format = formats),
            x_date = format(date, format = formats_date)
          ) %>%
          collect(),
        times
      )
    }
  )
})

test_that("format() for unsupported types returns the input as string", {
  expect_equal(
    example_data %>%
      record_batch() %>%
      mutate(x = format(int)) %>%
      collect(),
    example_data %>%
      record_batch() %>%
      mutate(x = as.character(int)) %>%
      collect()
  )
  expect_equal(
    example_data %>%
      arrow_table() %>%
      mutate(y = format(dbl)) %>%
      collect(),
    example_data %>%
      arrow_table() %>%
      mutate(y = as.character(dbl)) %>%
      collect()
  )
})
