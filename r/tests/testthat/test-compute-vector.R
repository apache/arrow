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

expect_bool_function_equal <- function(array_exp, r_exp) {
  # Assert that the Array operation returns a boolean array
  # and that its contents are equal to expected
  expect_r6_class(array_exp, "ArrowDatum")
  expect_type_equal(array_exp, bool())
  expect_identical(as.vector(array_exp), r_exp)
}

expect_array_compares <- function(x, compared_to) {
  r_values <- as.vector(x)
  r_compared_to <- as.vector(compared_to)
  # Iterate over all comparison functions
  expect_bool_function_equal(x == compared_to, r_values == r_compared_to)
  expect_bool_function_equal(x != compared_to, r_values != r_compared_to)
  expect_bool_function_equal(x > compared_to, r_values > r_compared_to)
  expect_bool_function_equal(x >= compared_to, r_values >= r_compared_to)
  expect_bool_function_equal(x < compared_to, r_values < r_compared_to)
  expect_bool_function_equal(x <= compared_to, r_values <= r_compared_to)
}

test_that("compare ops with Array", {
  a <- Array$create(1:5)
  expect_array_compares(a, 4L)
  expect_array_compares(a, 4) # implicit casting
  expect_array_compares(a, Scalar$create(4))
  expect_array_compares(Array$create(c(NA, 1:5)), 4)
  expect_array_compares(Array$create(as.numeric(c(NA, 1:5))), 4)
  expect_array_compares(Array$create(c(NA, 1:5)), Array$create(rev(c(NA, 1:5))))
  expect_array_compares(Array$create(c(NA, 1:5)), Array$create(rev(c(NA, 1:5)), type = double()))
})

test_that("compare ops with ChunkedArray", {
  expect_array_compares(ChunkedArray$create(1:3, 4:5), 4L)
  expect_array_compares(ChunkedArray$create(1:3, 4:5), 4) # implicit casting
  expect_array_compares(ChunkedArray$create(1:3, 4:5), Scalar$create(4))
  expect_array_compares(ChunkedArray$create(c(NA, 1:3), 4:5), 4)
  expect_array_compares(
    ChunkedArray$create(c(NA, 1:3), 4:5),
    ChunkedArray$create(4:5, c(NA, 1:3))
  )
  expect_array_compares(
    ChunkedArray$create(c(NA, 1:3), 4:5),
    Array$create(c(NA, 1:5))
  )
  expect_array_compares(
    Array$create(c(NA, 1:5)),
    ChunkedArray$create(c(NA, 1:3), 4:5)
  )
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

test_that("binary slice kernel with Array", {
  binary_array <- Array$create(
    iconv(c("a", "ab", "abc", "abcd"), toRaw = TRUE),
    type = binary()
  )

  result <- call_function(
    "binary_slice",
    binary_array,
    options = list(start = 0, stop = 1)
  )
  expect_equal(result$cast(string()), Array$create(c("a", "a", "a", "a")))

  result <- call_function(
    "binary_slice",
    binary_array,
    options = list(start = -1)
  )
  expect_equal(result$cast(string()), Array$create(c("a", "b", "c", "d")))
})

test_that("logic ops with ChunkedArray", {
  truth <- expand.grid(left = c(TRUE, FALSE, NA), right = c(TRUE, FALSE, NA))
  a_left <- ChunkedArray$create(truth$left)
  a_right <- ChunkedArray$create(truth$right)
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

test_that("call_function validation", {
  expect_error(
    call_function("filter", 4),
    'Argument 1 is of class numeric but it must be one of "Array", "ChunkedArray", "RecordBatch", "Table", or "Scalar"'
  )
  expect_error(
    call_function("filter", Array$create(1:4), 3),
    "Argument 2 is of class numeric"
  )
  expect_error(
    call_function("filter",
      Array$create(1:4),
      Array$create(c(TRUE, FALSE, TRUE)),
      options = list(keep_na = TRUE)
    ),
    "Arguments for execution of vector kernel function 'array_filter' must all be the same length"
  )
  expect_error(
    call_function("filter",
      record_batch(a = 1:3),
      Array$create(c(TRUE, FALSE, TRUE)),
      options = list(keep_na = TRUE)
    ),
    NA
  )
  expect_error(
    call_function("filter", options = list(keep_na = TRUE)),
    "accepts 2 arguments"
  )
})
