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

context("test-type")

test_that("type() gets the right type for arrow::Array", {
  a <- Array$create(1:10)
  expect_type_equal(type(a), a$type)
})

test_that("type() gets the right type for ChunkedArray", {
  a <- chunked_array(1:10, 1:10)
  expect_type_equal(type(a), a$type)
})

test_that("type() infers from R type", {
  expect_type_equal(type(1:10), int32())
  expect_type_equal(type(1), float64())
  expect_type_equal(type(TRUE), boolean())
  expect_type_equal(type(raw()), int8())
  expect_type_equal(type(""), utf8())
  expect_type_equal(
    type(example_data$fct),
    dictionary(int8(), utf8(), FALSE)
  )
  expect_type_equal(
    type(lubridate::ymd_hms("2019-02-14 13:55:05")),
    timestamp(TimeUnit$MICRO, "UTC")
  )
  expect_type_equal(
    type(hms::hms(56, 34, 12)),
    time32(unit = TimeUnit$SECOND)
  )
  expect_type_equal(
    type(bit64::integer64()),
    int64()
  )
})

test_that("type() can infer struct types from data frames", {
  df <- tibble::tibble(x = 1:10, y = rnorm(10), z = letters[1:10])
  expect_type_equal(type(df), struct(x = int32(), y = float64(), z = utf8()))
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
  expect_failure(expect_type_equal(a, z), "int32 not equal to double")
  expect_false(a$Equals(32L))
})
