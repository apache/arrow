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


test_that("type() gets the right type for arrow::Array", {
  a <- Array$create(1:10)
  expect_equal(type(a), a$type)
})

test_that("type() gets the right type for ChunkedArray", {
  a <- chunked_array(1:10, 1:10)
  expect_equal(type(a), a$type)
})

test_that("type() infers from R type", {
  expect_equal(type(1:10), int32())
  expect_equal(type(1), float64())
  expect_equal(type(TRUE), boolean())
  expect_equal(type(raw()), uint8())
  expect_equal(type(""), utf8())
  expect_equal(
    type(example_data$fct),
    dictionary(int8(), utf8(), FALSE)
  )
  expect_equal(
    type(lubridate::ymd_hms("2019-02-14 13:55:05")),
    timestamp(TimeUnit$MICRO, "UTC")
  )
  expect_equal(
    type(hms::hms(56, 34, 12)),
    time32(unit = TimeUnit$SECOND)
  )
  expect_equal(
    type(as.difftime(123, units = "days")),
    duration(unit = TimeUnit$SECOND)
  )
  expect_equal(
    type(bit64::integer64()),
    int64()
  )
})

test_that("type() can infer struct types from data frames", {
  df <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])
  expect_equal(type(df), struct(x = int32(), y = float64(), z = utf8()))
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
    canonical_type_str("time32[s]"),
    "parameters"
  )

  # unrecognized data types
  expect_error(
    canonical_type_str("foo"),
    "Unrecognized"
  )
})
