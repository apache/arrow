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

context("test-decimal128")

test_that("cast decimal128 <-> int types", {
  x <- vec_cast(1L, new_decimal128())
  expect_is(x, "arrow_decimal128")
  y <- vec_cast(x, integer())
  expect_is(y, "integer")
  expect_equal(y, 1L)

  y <- vec_cast(x, bit64::integer64())
  expect_is(y, "integer64")
  expect_equal(y, bit64::as.integer64(1))

  x <- vec_cast(bit64::as.integer64(2), new_decimal128())
  expect_is(x, "arrow_decimal128")
})

test_that("coercion up to decimal128", {
  expect_equal(vec_type2(1L, new_decimal128()), new_decimal128())
  expect_equal(vec_type2(bit64::as.integer64(2), new_decimal128()), new_decimal128())
})
