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

context("compute: vector operations")

expect_bool_function_equal <- function(array_exp, r_exp, class = "Array") {
  # Assert that the Array operation returns a boolean array
  # and that its contents are equal to expected
  expect_is(array_exp, class)
  expect_type_equal(array_exp, bool())
  expect_identical(as.vector(array_exp), r_exp)
}

expect_array_compares <- function(r_values, compared_to, Class = Array) {
  a <- Class$create(r_values)
  # Iterate over all comparison functions
  expect_bool_function_equal(a == compared_to, r_values == compared_to, class(a))
  expect_bool_function_equal(a != compared_to, r_values != compared_to, class(a))
  expect_bool_function_equal(a > compared_to, r_values > compared_to, class(a))
  expect_bool_function_equal(a >= compared_to, r_values >= compared_to, class(a))
  expect_bool_function_equal(a < compared_to, r_values < compared_to, class(a))
  expect_bool_function_equal(a <= compared_to, r_values <= compared_to, class(a))
}

expect_chunked_array_compares <- function(...) expect_array_compares(..., Class = ChunkedArray)

test_that("compare ops with Array", {
  expect_array_compares(1:5, 4L)
  expect_array_compares(1:5, 4) # implicit casting
  expect_array_compares(c(NA, 1:5), 4)
  expect_array_compares(as.numeric(c(NA, 1:5)), 4)
})

test_that("compare ops with ChunkedArray", {
  expect_chunked_array_compares(1:5, 4L)
  expect_chunked_array_compares(1:5, 4) # implicit casting
  expect_chunked_array_compares(c(NA, 1:5), 4)
  expect_chunked_array_compares(as.numeric(c(NA, 1:5)), 4)
})

test_that("logic ops with Array", {
  truth <- expand.grid(left = c(TRUE, FALSE, NA), right = c(TRUE, FALSE, NA))
  a_left <- Array$create(truth$left)
  a_right <- Array$create(truth$right)
  expect_bool_function_equal(a_left & a_right, truth$left & truth$right)
  expect_bool_function_equal(a_left | a_right, truth$left | truth$right)
  expect_bool_function_equal(a_left == a_right, truth$left == truth$right)
  expect_bool_function_equal(a_left != a_right, truth$left != truth$right)
  expect_bool_function_equal(!a_left, !truth$left)

  # More complexity
  isEqualTo <- function(x, y) x == y & !is.na(x)
  expect_bool_function_equal(
    isEqualTo(a_left, a_right),
    isEqualTo(truth$left, truth$right)
  )
})

test_that("logic ops with ChunkedArray", {
  truth <- expand.grid(left = c(TRUE, FALSE, NA), right = c(TRUE, FALSE, NA))
  a_left <- ChunkedArray$create(truth$left)
  a_right <- ChunkedArray$create(truth$right)
  expect_bool_function_equal(a_left & a_right, truth$left & truth$right, "ChunkedArray")
  expect_bool_function_equal(a_left | a_right, truth$left | truth$right, "ChunkedArray")
  expect_bool_function_equal(a_left == a_right, truth$left == truth$right, "ChunkedArray")
  expect_bool_function_equal(a_left != a_right, truth$left != truth$right, "ChunkedArray")
  expect_bool_function_equal(!a_left, !truth$left, "ChunkedArray")

  # More complexity
  isEqualTo <- function(x, y) x == y & !is.na(x)
  expect_bool_function_equal(
    isEqualTo(a_left, a_right),
    isEqualTo(truth$left, truth$right),
    "ChunkedArray"
  )
})
