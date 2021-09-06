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


test_that("non-bound compute kernels using TrimOptions", {
  skip_if_not_available("utf8proc")
  expect_equal(
    call_function(
      "utf8_trim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("racadabr")
  )

  expect_equal(
    call_function(
      "utf8_ltrim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("racadabra")
  )

  expect_equal(
    call_function(
      "utf8_rtrim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("abracadabr")
  )

  expect_equal(
    call_function(
      "utf8_rtrim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("abracadabr")
  )

  expect_equal(
    call_function(
      "ascii_ltrim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("racadabra")
  )

  expect_equal(
    call_function(
      "ascii_rtrim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("abracadabr")
  )

  expect_equal(
    call_function(
      "ascii_rtrim",
      Scalar$create("abracadabra"),
      options = list(characters = "ab")
    ),
    Scalar$create("abracadabr")
  )
})

test_that("non-bound compute kernels using ReplaceSliceOptions", {
  skip_if_not_available("utf8proc")

  expect_equal(
    call_function(
      "binary_replace_slice",
      Array$create("I need to fix this string"),
      options = list(start = 1, stop = 1, replacement = " don't")
    ),
    Array$create("I don't need to fix this string")
  )

  expect_equal(
    call_function(
      "utf8_replace_slice",
      Array$create("I need to fix this string"),
      options = list(start = 1, stop = 1, replacement = " don't")
    ),
    Array$create("I don't need to fix this string")
  )
})

test_that("non-bound compute kernels using ModeOptions", {
  expect_equal(
    as.vector(
      call_function("mode", Array$create(c(1:10, 10, 9, NA)), options = list(n = 3))
    ),
    tibble::tibble("mode" = c(9, 10, 1), "count" = c(2L, 2L, 1L))
  )

  expect_equal(
    as.vector(
      call_function("mode", Array$create(c(1:10, 10, 9, NA)), options = list(n = 3, skip_nulls = FALSE))
    ),
    tibble::tibble("mode" = numeric(), "count" = integer())
  )
})

test_that("non-bound compute kernels using PartitionNthOptions", {
  expect_equal(
    as.vector(call_function("partition_nth_indices", Array$create(c(1:10)), options = list(pivot = 5))),
    c(1L, 0L, 4L, 3L, 2L, 5L, 6L, 7L, 8L, 9L)
  )
})
