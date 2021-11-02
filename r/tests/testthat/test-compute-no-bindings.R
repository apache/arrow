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
  result <- call_function(
    "partition_nth_indices",
    Array$create(c(11:20)),
    options = list(pivot = 3)
  )
  # Order of indices on either side of the pivot is not deterministic
  # (depends on C++ standard library implementation)
  expect_true(all(as.vector(result[1:3]) < 3))
  expect_true(all(as.vector(result[4:10]) >= 3))
})


test_that("non-bound compute kernels using MatchSubstringOptions", {
  skip_if_not_available("utf8proc")

  # Remove this test when ARROW-13924 has been completed
  expect_equal(
    call_function(
      "starts_with",
      Array$create(c("abracadabra", "abacus", "abdicate", "abrasive")),
      options = list(pattern = "abr")
    ),
    Array$create(c(TRUE, FALSE, FALSE, TRUE))
  )

  # Remove this test when ARROW-13924 has been completed
  expect_equal(
    call_function(
      "ends_with",
      Array$create(c("abracadabra", "abacus", "abdicate", "abrasive")),
      options = list(pattern = "e")
    ),
    Array$create(c(FALSE, FALSE, TRUE, TRUE))
  )

  # Remove this test when ARROW-13156 has been completed
  expect_equal(
    as.vector(
      call_function(
        "count_substring",
        Array$create(c("abracadabra", "abacus", "abdicate", "abrasive")),
        options = list(pattern = "e")
      )
    ),
    c(0, 0, 1, 1)
  )

  skip_if_not_available("re2")

  # Remove this test when ARROW-13156 has been completed
  expect_equal(
    as.vector(
      call_function(
        "count_substring_regex",
        Array$create(c("abracadabra", "abacus", "abdicate", "abrasive")),
        options = list(pattern = "e")
      )
    ),
    c(0, 0, 1, 1)
  )
})

test_that("non-bound compute kernels using ExtractRegexOptions", {
  skip_if_not_available("re2")
  expect_equal(
    call_function("extract_regex", Scalar$create("abracadabra"), options = list(pattern = "(?P<letter>[a])")),
    Scalar$create(tibble::tibble(letter = "a"))
  )
})

test_that("non-bound compute kernels using IndexOptions", {
  expect_equal(
    as.vector(
      call_function("index", Array$create(c(10, 20, 30, 40)), options = list(value = Scalar$create(40)))
    ),
    3
  )
})
