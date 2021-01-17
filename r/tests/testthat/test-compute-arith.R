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

test_that("Addition", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_type_equal(a, int32())
  expect_type_equal(a + 4, int32())
  expect_equal(a + 4, Array$create(c(5:8, NA_integer_)))
  expect_identical(as.vector(a + 4), c(5:8, NA_integer_))
  expect_equal(a + 4L, Array$create(c(5:8, NA_integer_)))
  expect_vector(a + 4L, c(5:8, NA_integer_))
  expect_equal(a + NA_integer_, Array$create(rep(NA_integer_, 5)))

  # overflow errors — this is slightly different from R's `NA` coercion when
  # overflowing, but better than the alternative of silently restarting
  casted <- a$cast(int8())
  expect_error(casted + 127)
  expect_error(casted + 200)

  skip("autocasting should happen in compute kernels; R workaround fails on this ARROW-8919")
  expect_type_equal(a + 4.1, float64())
  expect_equal(a + 4.1, Array$create(c(5.1, 6.1, 7.1, 8.1, NA_real_)))
})

test_that("Subtraction", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a - 3, Array$create(c(-2:1, NA_integer_)))
})

test_that("Multiplication", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a * 2, Array$create(c(1:4 * 2L, NA_integer_)))
})

test_that("Division", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a / 2, Array$create(c(1:4 / 2, NA_real_)))
  expect_equal(a %/% 2, Array$create(c(0L, 1L, 1L, 2L, NA_integer_)))
  expect_equal(a / 2 / 2, Array$create(c(1:4 / 2 / 2, NA_real_)))
  expect_equal(a %/% 2 %/% 2, Array$create(c(0L, 0L, 0L, 1L, NA_integer_)))

  b <- a$cast(float64())
  expect_equal(b / 2, Array$create(c(1:4 / 2, NA_real_)))
  expect_equal(b %/% 2, Array$create(c(0L, 1L, 1L, 2L, NA_integer_)))

  # the behavior of %/% matches R's (i.e. the integer of the quotient, not
  # simply dividing two integers)
  expect_equal(b / 2.2, Array$create(c(1:4 / 2.2, NA_real_)))
  # c(1:4) %/% 2.2 != c(1:4) %/% as.integer(2.2)
  # c(1:4) %/% 2.2             == c(0L, 0L, 1L, 1L)
  # c(1:4) %/% as.integer(2.2) == c(0L, 1L, 1L, 2L)
  expect_equal(b %/% 2.2, Array$create(c(0L, 0L, 1L, 1L, NA_integer_)))

  expect_equal(a %% 2, Array$create(c(1L, 0L, 1L, 0L, NA_integer_)))

  expect_equal(b %% 2, Array$create(c(1:4 %% 2, NA_real_)))
})

test_that("Dates casting", {
  a <- Array$create(c(Sys.Date() + 1:4, NA_integer_))

  skip("autocasting should happen in compute kernels; R workaround fails on this ARROW-8919")
  expect_equal(a + 2, Array$create(c((Sys.Date() + 1:4 ) + 2), NA_integer_))
})
