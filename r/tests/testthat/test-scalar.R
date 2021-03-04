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

context("Scalar")

expect_scalar_roundtrip <- function(x, type) {
  s <- Scalar$create(x)
  expect_is(s, "Scalar")
  expect_type_equal(s$type, type)
  expect_identical(length(s), 1L)
  if (inherits(type, "NestedType")) {
    # Should this be? Missing if all elements are missing?
    # expect_identical(is.na(s), all(is.na(x)))
  } else {
    expect_identical(is.na(s), is.na(x))
    # MakeArrayFromScalar not implemented for list types
    expect_equal(as.vector(s), x)
  }
}

test_that("Scalar object roundtrip", {
  expect_scalar_roundtrip(2, float64())
  expect_scalar_roundtrip(2L, int32())
  expect_scalar_roundtrip(c(2, 4), list_of(float64()))
  expect_scalar_roundtrip(c(NA, NA), list_of(bool()))
  expect_scalar_roundtrip(data.frame(a=2, b=4L), struct(a = double(), b = int32()))
})

test_that("Scalar print", {
  expect_output(print(Scalar$create(4)), "Scalar\n4")
})

test_that("Creating Scalars of a different type and casting them", {
  expect_type_equal(Scalar$create(4L, int8())$type, int8())
  expect_type_equal(Scalar$create(4L)$cast(float32())$type, float32())
})
