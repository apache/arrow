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


expect_scalar_roundtrip <- function(x, type) {
  s <- Scalar$create(x)
  expect_r6_class(s, "Scalar")
  expect_equal(s$type, type)
  expect_identical(length(s), 1L)
  if (inherits(type, "NestedType")) {
    # Should this be? Missing if all elements are missing?
    # expect_identical(is.na(s), all(is.na(x))) # nolint
  } else {
    expect_identical(as.vector(is.na(s)), is.na(x))
    # MakeArrayFromScalar not implemented for list types
    expect_as_vector(s, x)
  }
}

test_that("Scalar object roundtrip", {
  expect_scalar_roundtrip(2, float64())
  expect_scalar_roundtrip(2L, int32())
  expect_scalar_roundtrip(c(2, 4), list_of(float64()))
  expect_scalar_roundtrip(c(NA, NA), list_of(bool()))
  expect_scalar_roundtrip(data.frame(a = 2, b = 4L), struct(a = double(), b = int32()))
})

test_that("Scalar print", {
  expect_output(print(Scalar$create(4)), "Scalar\n4")
})

test_that("ExtensionType scalar behaviour", {
  ext_array <- vctrs_extension_array(4)
  ext_scalar <- Scalar$create(ext_array)
  expect_equal(ext_scalar$as_array(), ext_array)
  expect_identical(ext_scalar$as_vector(), 4)
  expect_identical(ext_scalar$as_vector(10), rep(4, 10))
  expect_output(print(ext_scalar), "Scalar\n4")
})

test_that("Creating Scalars of a different type and casting them", {
  expect_equal(Scalar$create(4L, int8())$type, int8())
  expect_equal(Scalar$create(4L)$cast(float32())$type, float32())
})

test_that("Scalar to Array", {
  a <- Scalar$create(42)
  expect_equal(a$as_array(), Array$create(42))
  expect_equal(Array$create(a), Array$create(42))
})

test_that("Scalar$Equals", {
  a <- Scalar$create(42)
  aa <- Array$create(42)
  b <- Scalar$create(42)
  d <- Scalar$create(43)
  expect_equal(a, b)
  expect_true(a$Equals(b))
  expect_false(a$Equals(d))
  expect_false(a$Equals(aa))
})

test_that("Scalar$ApproxEquals", {
  a <- Scalar$create(1.0000000000001)
  aa <- Array$create(1.0000000000001)
  b <- Scalar$create(1.0)
  d <- 2.400000000000001
  expect_false(a$Equals(b))
  expect_true(a$ApproxEquals(b))
  expect_false(a$ApproxEquals(d))
  expect_false(a$ApproxEquals(aa))
})

test_that("Handling string data with embedded nuls", {
  raws <- as.raw(c(0x6d, 0x61, 0x00, 0x6e))
  expect_error(
    rawToChar(raws),
    "embedded nul in string: 'ma\\0n'", # See?
    fixed = TRUE
  )
  scalar_with_nul <- Scalar$create(raws, binary())$cast(utf8())

  # The behavior of the warnings/errors is slightly different with and without
  # altrep. Without it (i.e. 3.5.0 and below, the error would trigger immediately
  # on `as.vector()` where as with it, the error only happens on materialization)
  skip_on_r_older_than("3.6")
  v <- expect_error(as.vector(scalar_with_nul), NA)
  expect_error(
    v[1],
    paste0(
      "embedded nul in string: 'ma\\0n'; to strip nuls when converting from Arrow to R, ",
      "set options(arrow.skip_nul = TRUE)"
    ),
    fixed = TRUE
  )

  withr::with_options(list(arrow.skip_nul = TRUE), {
    expect_warning(
      expect_identical(
        as.vector(scalar_with_nul)[],
        "man"
      ),
      "Stripping '\\0' (nul) from character vector",
      fixed = TRUE
    )
  })
})
