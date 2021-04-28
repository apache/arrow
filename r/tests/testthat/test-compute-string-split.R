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

test_str <-c("foobar, foo, bar", "baz, qux, quux.")

test_that("split_pattern", {
  
  test_scalar <- Scalar$create(test_str[1])
  test_array <- Array$create(test_str)
  test_charray <- chunked_array(test_str[1], test_str[2])
  
  expect_equal(
    split_pattern(test_scalar, pattern = ","),
    Scalar$create(list(c("foobar", " foo", " bar")))
  )
  
  expect_equal(
    split_pattern(test_array, pattern = ","),
    Array$create(list(c("foobar", " foo", " bar"), c("baz", " qux", " quux.")))
  )
  
  expect_equal(
    split_pattern(test_charray, pattern = ","),
    ChunkedArray$create(list(c("foobar", " foo", " bar"), c("baz", " qux", " quux.")))
  )
  
})

test_that("split_pattern with max splits and reverse", {
  
  test_scalar <- Scalar$create(test_str[1])
  test_array <- Array$create(test_str)
  test_charray <- chunked_array(test_str[1], test_str[2])
  
  expect_equal(
    split_pattern(test_scalar, pattern = ",", max_split = 1),
    Scalar$create(list(c("foobar", " foo, bar")))
  )
  
  expect_equal(
    split_pattern(test_array, pattern = ",", max_split = 1),
    Array$create(list(c("foobar", " foo, bar"), c("baz", " qux, quux.")))
  )
  
  expect_equal(
    split_pattern(test_charray, pattern = ",", max_split = 1),
    ChunkedArray$create(list(c("foobar", " foo, bar"), c("baz", " qux, quux.")))
  )
  
  expect_equal(
    split_pattern(test_scalar, pattern = ",", max_split = 1, reverse = TRUE),
    Scalar$create(list(c("foobar, foo", " bar")))
  )
  
  expect_equal(
    split_pattern(test_array, pattern = ",", max_split = 1, reverse = TRUE),
    Array$create(list(c("foobar, foo", " bar"), c("baz, qux", " quux.")))
  )
  
  expect_equal(
    split_pattern(test_charray, pattern = ",", max_split = 1, reverse = TRUE),
    ChunkedArray$create(list(c("foobar, foo", " bar"), c("baz, qux", " quux.")))
  )
  
})
