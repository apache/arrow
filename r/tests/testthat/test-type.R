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


test_that("infer_type() gets the right type for arrow::Array", {
  a <- Array$create(1:10)
  expect_equal(infer_type(a), a$type)
})

test_that("infer_type() gets the right type for ChunkedArray", {
  a <- chunked_array(1:10, 1:10)
  expect_equal(infer_type(a), a$type)
})

test_that("infer_type() infers from R type", {
  expect_equal(infer_type(1:10), int32())
  expect_equal(infer_type(1), float64())
  expect_equal(infer_type(TRUE), boolean())
  expect_equal(infer_type(raw()), uint8())
  expect_equal(infer_type(""), utf8())
  expect_equal(
    infer_type(example_data$fct),
    dictionary(int8(), utf8(), FALSE)
  )
  expect_equal(
    infer_type(lubridate::ymd_hms("2019-02-14 13:55:05")),
    timestamp(TimeUnit$MICRO, "UTC")
  )
  expect_equal(
    infer_type(hms::hms(56, 34, 12)),
    time32(unit = TimeUnit$SECOND)
  )
  expect_equal(
    infer_type(as.difftime(123, units = "days")),
    duration(unit = TimeUnit$SECOND)
  )
  expect_equal(
    infer_type(bit64::integer64()),
    int64()
  )
})

test_that("infer_type() default method errors for unknown classes", {
  vec <- structure(list(), class = "class_not_supported")

  # check simulating a call from C++
  expect_snapshot_error(infer_type(vec, from_array_infer_type = TRUE))

  # also check the error when infer_type() is called from Array__infer_type()
  expect_snapshot_error(infer_type(vec))
})

test_that("infer_type() can infer struct types from data frames", {
  df <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])
  expect_equal(infer_type(df), struct(x = int32(), y = float64(), z = utf8()))
})

test_that("infer_type() can infer type for vctr_vctr subclasses", {
  vctr <- vctrs::new_vctr(1:5, class = "custom_vctr")
  expect_equal(
    infer_type(vctr),
    vctrs_extension_type(vctrs::vec_ptype(vctr))
  )
})

test_that("infer_type() can infer nested extension types", {
  vctr <- vctrs::new_vctr(1:5, class = "custom_vctr")
  expect_equal(
    infer_type(tibble::tibble(x = vctr)),
    struct(x = infer_type(vctr))
  )
})

test_that("infer_type() can infer vctrs::list_of() types", {
  expect_equal(infer_type(vctrs::list_of(.ptype = integer())), list_of(int32()))
})

test_that("infer_type() can infer blob type", {
  skip_if_not_installed("blob")

  expect_equal(infer_type(blob::blob()), binary())

  big_ish_raw <- raw(2 ^ 20)
  big_ish_blob <- blob::new_blob(rep(list(big_ish_raw), 2049))
  expect_equal(infer_type(big_ish_blob), large_binary())
})

test_that("DataType$Equals", {
  a <- int32()
  b <- int32()
  z <- float64()
  expect_true(a == b)
  expect_true(a$Equals(b))
  expect_false(a == z)
  expect_equal(a, b)
  expect_failure(expect_equal(a, z))
  expect_failure(expect_equal(a, z))
  expect_false(a$Equals(32L))
})

test_that("Masked data type functions still work", {
  skip("Work around masking of data type functions (ARROW-12322)")

  # Works when type function is masked
  string <- rlang::string
  expect_equal(
    Array$create("abc", type = string()),
    arrow::string()
  )
  rm(string)

  # Works when with non-Arrow function that returns an Arrow type
  # when the non-Arrow function has the same name as a base R function...
  str <- arrow::string
  expect_equal(
    Array$create("abc", type = str()),
    arrow::string()
  )
  rm(str)

  # ... and when it has the same name as an Arrow function
  type <- arrow::string
  expect_equal(
    Array$create("abc", type = type()),
    arrow::string()
  )
  rm(type)

  # Works with local variable whose value is an Arrow type
  type <- arrow::string()
  expect_equal(
    Array$create("abc", type = type),
    arrow::string()
  )
  rm(type)
})

test_that("Type strings are correctly canonicalized", {
  # data types without arguments
  expect_equal(canonical_type_str("int8"), int8()$ToString())
  expect_equal(canonical_type_str("int16"), int16()$ToString())
  expect_equal(canonical_type_str("int32"), int32()$ToString())
  expect_equal(canonical_type_str("int64"), int64()$ToString())
  expect_equal(canonical_type_str("uint8"), uint8()$ToString())
  expect_equal(canonical_type_str("uint16"), uint16()$ToString())
  expect_equal(canonical_type_str("uint32"), uint32()$ToString())
  expect_equal(canonical_type_str("uint64"), uint64()$ToString())
  expect_equal(canonical_type_str("float16"), float16()$ToString())
  expect_equal(canonical_type_str("halffloat"), halffloat()$ToString())
  expect_equal(canonical_type_str("float32"), float32()$ToString())
  expect_equal(canonical_type_str("float"), float()$ToString())
  expect_equal(canonical_type_str("float64"), float64()$ToString())
  expect_equal(canonical_type_str("double"), float64()$ToString())
  expect_equal(canonical_type_str("boolean"), boolean()$ToString())
  expect_equal(canonical_type_str("bool"), bool()$ToString())
  expect_equal(canonical_type_str("utf8"), utf8()$ToString())
  expect_equal(canonical_type_str("large_utf8"), large_utf8()$ToString())
  expect_equal(canonical_type_str("large_string"), large_utf8()$ToString())
  expect_equal(canonical_type_str("binary"), binary()$ToString())
  expect_equal(canonical_type_str("large_binary"), large_binary()$ToString())
  expect_equal(canonical_type_str("string"), arrow::string()$ToString())
  expect_equal(canonical_type_str("null"), null()$ToString())

  # data types with arguments
  expect_equal(
    canonical_type_str("fixed_size_binary"),
    sub("^([^([<]+).*$", "\\1", fixed_size_binary(42)$ToString())
  )
  expect_equal(
    canonical_type_str("date32"),
    sub("^([^([<]+).*$", "\\1", date32()$ToString())
  )
  expect_equal(
    canonical_type_str("date64"),
    sub("^([^([<]+).*$", "\\1", date64()$ToString())
  )
  expect_equal(
    canonical_type_str("time32"),
    sub("^([^([<]+).*$", "\\1", time32()$ToString())
  )
  expect_equal(
    canonical_type_str("time64"),
    sub("^([^([<]+).*$", "\\1", time64()$ToString())
  )
  expect_equal(
    canonical_type_str("timestamp"),
    sub("^([^([<]+).*$", "\\1", timestamp()$ToString())
  )
  expect_equal(
    canonical_type_str("decimal128"),
    sub("^([^([<]+).*$", "\\1", decimal(3, 2)$ToString())
  )
  expect_equal(
    canonical_type_str("decimal128"),
    sub("^([^([<]+).*$", "\\1", decimal128(3, 2)$ToString())
  )
  expect_equal(
    canonical_type_str("decimal256"),
    sub("^([^([<]+).*$", "\\1", decimal256(3, 2)$ToString())
  )
  expect_equal(
    canonical_type_str("struct"),
    sub("^([^([<]+).*$", "\\1", struct(foo = int32())$ToString())
  )
  expect_equal(
    canonical_type_str("list_of"),
    sub("^([^([<]+).*$", "\\1", list_of(int32())$ToString())
  )
  expect_equal(
    canonical_type_str("list"),
    sub("^([^([<]+).*$", "\\1", list_of(int32())$ToString())
  )
  expect_equal(
    canonical_type_str("large_list_of"),
    sub("^([^([<]+).*$", "\\1", large_list_of(int32())$ToString())
  )
  expect_equal(
    canonical_type_str("large_list"),
    sub("^([^([<]+).*$", "\\1", large_list_of(int32())$ToString())
  )
  expect_equal(
    canonical_type_str("fixed_size_list_of"),
    sub("^([^([<]+).*$", "\\1", fixed_size_list_of(int32(), 42)$ToString())
  )
  expect_equal(
    canonical_type_str("fixed_size_list"),
    sub("^([^([<]+).*$", "\\1", fixed_size_list_of(int32(), 42)$ToString())
  )
  expect_equal(
    canonical_type_str("map_of"),
    sub("^([^([<]+).*$", "\\1", map_of(utf8(), utf8())$ToString())
  )

  # unsupported data types
  expect_error(
    canonical_type_str("decimal128(3, 2)"),
    "parameters"
  )
  expect_error(
    canonical_type_str("list<item: int32>"),
    "parameters"
  )
  expect_error(
    canonical_type_str("map<key: int32, item: int32>"),
    "parameters"
  )
  expect_error(
    canonical_type_str("time32[s]"),
    "parameters"
  )

  # unrecognized data types
  expect_error(
    canonical_type_str("foo"),
    "Unrecognized"
  )
})

test_that("infer_type() gets the right type for Expression", {
  x <- Expression$scalar(32L)
  y <- Expression$scalar(10)
  add_xy <- Expression$create("add", x, y)

  expect_equal(x$type(), infer_type(x))
  expect_equal(infer_type(x), int32())
  expect_equal(y$type(), infer_type(y))
  expect_equal(infer_type(y), float64())
  expect_equal(add_xy$type(), infer_type(add_xy))
  # even though 10 is a float64, arrow will clamp it to the narrowest
  # type that can exactly represent it when building expressions
  expect_equal(infer_type(add_xy), float32())
})

test_that("infer_type() infers type for POSIXlt", {
  posix_lt <- as.POSIXlt("2021-01-01 01:23:45", tz = "UTC")
  expect_equal(
    infer_type(posix_lt),
    vctrs_extension_type(posix_lt[integer(0)])
  )
})

test_that("infer_type() infers type for vctrs", {
  vec <- vctrs::new_vctr(1:5, class = "special_integer")
  expect_equal(
    infer_type(vec),
    vctrs_extension_type(vec[integer(0)])
  )
})

test_that("type() is deprecated", {
  a <- Array$create(1:10)
  expect_deprecated(
    a_type <- type(a),
    "infer_type"
  )
  expect_equal(a_type, a$type)
})

test_that("infer_type() infers type for lists of raw() as binary()", {
  expect_equal(
    infer_type(list(raw())),
    binary()
  )

  expect_equal(
    infer_type(list(NULL, raw(), raw())),
    binary()
  )
})

test_that("infer_type() infers type for lists starting with NULL - ARROW-17639", {
  null_start_list <- list(NULL, c(2, 3), c(4, 5))

  expect_equal(
    infer_type(null_start_list),
    list_of(float64())
  )

  totally_null_list <- list(NULL, NULL, NULL)

  expect_equal(
    infer_type(totally_null_list),
    list_of(null())
  )

  empty_list <- list()
  expect_equal(
    infer_type(empty_list),
    list_of(null())
  )
})


test_that("code() works for data types without arguments",{
  # Names encode type aliases.
  # No names means the type alias matches the string
  type_strs <- c(
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "halffloat" = "float16", "halffloat", "float" = "float32", "float", "float64",
    "bool" = "boolean", "bool",
    "utf8", "large_utf8", "binary", "large_binary", "utf8" = "string",
    "null",
    "date32", "date64", "time32", "time64", "timestamp"
  )

  evaluate_type_str <- function(type_str, type_alias) {
    if(type_alias == "") {
      type_alias <- type_str
    }
    type_obj <- eval(call2(type_str, .ns=getPackageName()))

    expect_code_roundtrip(type_obj, info = type_str)
    expect_code_roundtrip(type_obj, explicit_pkg_name = TRUE, info = type_str)

    type_code <- as.character(type_obj$code())[1]  # Ignore units in time types.
    type_code_with_ns <- as.character(type_obj$code(TRUE))[1]  # Ignore units in time types.

    expect_equal(type_code, type_alias)
    expect_equal(type_code_with_ns, paste0(getPackageName(),"::", type_alias))
  }
  purrr::iwalk(type_strs, evaluate_type_str)

})

test_that("code() works for simple data types with arguments",{
  types_with_args <- list(
    # type_str, args, type_alias
    list("fixed_size_binary", list(42), "fixed_size_binary"),
    list("decimal", list(3, 2), "decimal"),
    list("decimal128", list(3, 2), "decimal"),
    list("decimal256", list(3, 2), "decimal")
  )
  evaluate_type_with_arg <- function(type_with_args) {
    type_str <- type_with_args[[1]]
    args <- type_with_args[[2]]
    type_alias <- type_with_args[[3]]

    type_obj <- eval(call2(type_str, !!!args, .ns=getPackageName()))

    if(type_str == type_alias) {
      expect_code_roundtrip(type_obj, info = type_str)
      expect_code_roundtrip(type_obj, explicit_pkg_name = TRUE, info = type_str)
    }

    type_code <- as.character(type_obj$code())
    type_code_with_ns <- as.character(type_obj$code(TRUE))

    # test info
    build_test_info <- function(test_str, explicit_pkg_name=FALSE) {
      glue::glue("`{type_str}` {test_str} (explicit_pkg_name={explicit_pkg_name})")
    }

    # type name
    expect_equal(type_code[1], type_alias, info=build_test_info("type name"))
    expect_equal(type_code_with_ns[1], paste0(getPackageName(), "::", type_alias), info=build_test_info("type name", TRUE))

    # args
    expect_equal(type_code[-1], as.character(unlist(args)), info=build_test_info("args"))
    expect_equal(type_code_with_ns[-1], as.character(unlist(args)), info=build_test_info("type name", TRUE))
  }
  purrr::walk(types_with_args, evaluate_type_with_arg)
})

test_that("code() works for nested_types",{
  # Nested Types
  nested_types <- list(
    list("struct", list(foo=int32())),
    list("list_of", list(int32())),
    list("large_list_of", list(int32())),
    list("fixed_size_list_of", list(int32(), 42))
  )
  evaluate_nested_type <- function(nested_type) {
    type_str <- nested_type[[1]]
    args <- nested_type[[2]]

    type_obj <- eval(call2(type_str, !!!args, .ns=getPackageName()))

    expect_code_roundtrip(type_obj, info = type_str)
    expect_code_roundtrip(type_obj, explicit_pkg_name = TRUE, info = type_str)

    type_code <- as.character(type_obj$code())
    type_code_with_ns <- as.character(type_obj$code(TRUE))


    # test info
    build_test_info <- function(test_str, explicit_pkg_name=FALSE) {
      glue::glue("`{type_str}` {test_str} (explicit_pkg_name={explicit_pkg_name})")
    }

    # type name
    expect_equal(type_code[1], type_str, info=build_test_info("type name"))
    expect_equal(type_code_with_ns[1], paste0(getPackageName(), "::", type_str),
                 info=build_test_info("type name", TRUE))

    # first arg (also a type)
    build_expected_str <- function(explicit_pkg_name=FALSE) {
      paste0(
        as.character(args[[1]]$code(explicit_pkg_name)),
        "()"  # The () is kept in arguments.
      )
    }
    expect_equal(type_code[2], build_expected_str(), info=build_test_info("first arg"))
    expect_equal(type_code_with_ns[2], build_expected_str(TRUE), info=build_test_info("first arg", TRUE))

    # second arg, if exists
    if(length(args) == 2) {
      second_arg <- as.character(args[[2]])
      expect_equal(type_code[3], second_arg, info=build_test_info("second arg"))
      expect_equal(type_code_with_ns[3], second_arg, info=build_test_info("second arg", TRUE))
    }
  }
  purrr::walk(nested_types, evaluate_nested_type)

})

test_that("code() works for map_of",{

  type_str <- "map_of"
  args <- list(string(), string())
  type_obj <- eval(call2(type_str, !!!args, .ns=getPackageName()))
  type_code <- as.character(type_obj$code())
  type_code_with_ns <- as.character(type_obj$code(TRUE))


  # test info
  build_test_info <- function(test_str, explicit_pkg_name=FALSE) {
    glue::glue("`{type_str}` {test_str} (explicit_pkg_name={explicit_pkg_name})")
  }

  # list_of
  expect_equal(type_code[1], "list_of", info=build_test_info("list_of"))
  expect_equal(type_code_with_ns[1], paste0(getPackageName(), "::", "list_of"),
               info=build_test_info("list_of", TRUE))

  # struct argument
  expect_struct_code_matches <- function(struct_code, explicit_pkg_name=FALSE) {
    get_code_str <- function(obj) as.character(obj$code(explicit_pkg_name))

    expect_true(grepl(get_code_str(arrow::struct()), struct_code, fixed = TRUE))
    for(arg in args) {
      expect_true(grepl(get_code_str(arg), struct_code, fixed = TRUE))
    }
  }

  expect_struct_code_matches(type_code[2])
  expect_struct_code_matches(type_code_with_ns[2])
})
