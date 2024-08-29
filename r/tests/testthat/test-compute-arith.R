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
  expect_type_equal(a + 4L, int32())
  expect_type_equal(a + 4, float64())
  expect_equal(a + 4L, Array$create(c(5:8, NA_integer_)))
  expect_identical(as.vector(a + 4L), c(5:8, NA_integer_))
  expect_equal(a + 4L, Array$create(c(5:8, NA_integer_)))
  expect_as_vector(a + 4L, c(5:8, NA_integer_))
  expect_equal(a + NA_integer_, Array$create(rep(NA_integer_, 5)))

  a8 <- a$cast(int8())
  expect_type_equal(a8 + Scalar$create(1, int8()), int8())

  # int8 will be promoted to int32 when added to int32
  expect_type_equal(a8 + 127L, int32())
  expect_equal(a8 + 127L, Array$create(c(128:131, NA_integer_)))

  b <- Array$create(c(4:1, NA_integer_))
  expect_type_equal(a8 + b, int32())
  expect_equal(a8 + b, Array$create(c(5L, 5L, 5L, 5L, NA_integer_)))

  expect_type_equal(a + 4.1, float64())
  expect_equal(a + 4.1, Array$create(c(5.1, 6.1, 7.1, 8.1, NA_real_)))
})

test_that("Subtraction", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a - 3L, Array$create(c(-2:1, NA_integer_)))

  expect_equal(
    Array$create(c(5.1, 6.1, 7.1, 8.1, NA_real_)) - a,
    Array$create(c(4.1, 4.1, 4.1, 4.1, NA_real_))
  )
})

test_that("Multiplication", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a * 2L, Array$create(c(1:4 * 2L, NA_integer_)))

  expect_equal(
    (a * 0.5) * 3L,
    Array$create(c(1.5, 3, 4.5, 6, NA_real_))
  )
})

test_that("Division", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a / 2, Array$create(c(1:4 / 2, NA_real_)))
  expect_equal(a %/% 0, Array$create(c(Inf, Inf, Inf, Inf, NA_real_)))
  expect_equal(a %/% 2, Array$create(c(0, 1, 1, 2, NA_real_)))
  expect_equal(a %/% 2L, Array$create(c(0L, 1L, 1L, 2L, NA_integer_)))
  expect_equal(a %/% 0L, Array$create(rep(NA_integer_, 5)))
  expect_equal(a / 2 / 2, Array$create(c(1:4 / 2 / 2, NA_real_)))
  expect_equal(a %/% 2L %/% 2L, Array$create(c(0L, 0L, 0L, 1L, NA_integer_)))
  expect_equal(a / 0, Array$create(c(Inf, Inf, Inf, Inf, NA_real_)))
  # TODO add tests for integer division %/% by 0
  # see https://issues.apache.org/jira/browse/ARROW-14297

  b <- a$cast(float64())
  expect_equal(b / 2, Array$create(c(1:4 / 2, NA_real_)))
  expect_equal(b %/% 0, Array$create(c(Inf, Inf, Inf, Inf, NA_real_)))
  expect_equal(b %/% 0L, Array$create(c(Inf, Inf, Inf, Inf, NA_real_)))
  expect_equal(b %/% 2, Array$create(c(0, 1, 1, 2, NA_real_)))
  expect_equal(b %/% 2L, Array$create(c(0, 1, 1, 2, NA_real_)))
  expect_equal(b / 0, Array$create(c(Inf, Inf, Inf, Inf, NA_real_)))
  # TODO add tests for integer division %/% by 0
  # see https://issues.apache.org/jira/browse/ARROW-14297

  # the behavior of %/% matches R's (i.e. the integer of the quotient, not
  # simply dividing two integers)
  expect_equal(b / 2.2, Array$create(c(1:4 / 2.2, NA_real_)))
  # nolint start
  # c(1:4) %/% 2.2 != c(1:4) %/% as.integer(2.2)
  # c(1:4) %/% 2.2             == c(0L, 0L, 1L, 1L)
  # c(1:4) %/% as.integer(2.2) == c(0L, 1L, 1L, 2L)
  # nolint end
  expect_equal(b %/% 2.2, Array$create(c(0, 0, 1, 1, NA_integer_)))

  expect_equal(a %% 2, Array$create(c(1L, 0L, 1L, 0L, NA_integer_)))

  expect_equal(b %% 2, Array$create(c(1:4 %% 2, NA_real_)))
})

test_that("Power", {
  a <- Array$create(c(1:4, NA_integer_))
  b <- a$cast(float64())
  c <- a$cast(int64())
  d <- a$cast(uint64())

  expect_equal(a^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(a^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(a^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(a^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))

  expect_equal(b^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(b^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(b^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(b^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))

  expect_equal(c^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(c^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(c^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(c^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))

  expect_equal(d^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(d^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(d^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(d^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))
})

test_that("Dates casting", {
  a <- Array$create(c(Sys.Date() + 1:4, NA_integer_))

  skip("ARROW-17043 (date/datetime arithmetic with integers)")
  # Error: NotImplemented: Function 'add_checked' has no kernel matching input types (timestamp[s], int32)
  expect_equal(a + 2L, Array$create(c((Sys.Date() + 1:4) + 2), NA_integer_))
})

test_that("Unary Ops group generics work on Array objects", {
  expect_equal(+Array$create(1L), Array$create(1L))
  expect_equal(-Array$create(1L), Array$create(-1L))
  expect_equal(!Array$create(c(TRUE, FALSE, NA)), Array$create(c(FALSE, TRUE, NA)))
})

test_that("Math group generics work on Array objects", {
  expect_equal(abs(Array$create(c(-1L, 1L))), Array$create(c(1L, 1L)))
  expect_equal(
    sign(Array$create(c(-5L, 2L))),
    Array$create(c(-1L, 1L))$cast(int8())
  )
  expect_equal(floor(Array$create(c(1.3, 2.1))), Array$create(c(1, 2)))
  expect_equal(ceiling(Array$create(c(1.3, 2.1))), Array$create(c(2, 3)))
  expect_equal(trunc(Array$create(c(1.3, 2.1))), Array$create(c(1, 2)))
  expect_equal(cos(Array$create(c(0.6, 2.1))), Array$create(cos(c(0.6, 2.1))))
  expect_equal(sin(Array$create(c(0.6, 2.1))), Array$create(sin(c(0.6, 2.1))))
  expect_equal(tan(Array$create(c(0.6, 2.1))), Array$create(tan(c(0.6, 2.1))))
  expect_equal(acos(Array$create(c(0.6, 0.9))), Array$create(acos(c(0.6, 0.9))))
  expect_equal(asin(Array$create(c(0.6, 0.9))), Array$create(asin(c(0.6, 0.9))))
  expect_equal(atan(Array$create(c(0.6, 0.9))), Array$create(atan(c(0.6, 0.9))))

  expect_equal(log(Array$create(c(0.6, 2.1))), Array$create(log(c(0.6, 2.1))))
  expect_equal(
    log(Array$create(c(0.6, 2.1)), base = 2),
    Array$create(log(c(0.6, 2.1), base = 2))
  )
  expect_equal(log10(Array$create(c(0.6, 2.1))), Array$create(log10(c(0.6, 2.1))))

  expect_equal(round(Array$create(c(0.6, 2.1))), Array$create(c(1, 2)))
  expect_equal(
    round(Array$create(c(0.61, 2.15)), digits = 1),
    Array$create(c(0.6, 2.2))
  )

  expect_equal(sqrt(Array$create(c(2L, 1L))), Array$create(sqrt(c(2L, 1L))))
  # this operation only roundtrips to 10 decimal places
  expect_equal(
    round(exp(Array$create(c(2L, 1L))), digits = 10),
    Array$create(round(exp(c(2L, 1L)), 10))
  )
  expect_as_vector(
    cumsum(Array$create(c(2.3, -1.0, 7.9, NA_real_, 1.0))),
    c(2.3, 1.3, 9.2, NA_real_, NA_real_)
  )
  expect_equal(cumsum(Array$create(-10L)), Array$create(-10L))
  expect_equal(cumsum(Array$create(NA_integer_)), Array$create(NA_integer_))
  expect_as_vector(
    cumsum(ChunkedArray$create(c(2L, 7L, 8L), c(-1L, 2L, 17L, NA_integer_, 3L), 18L)),
    c(2L, 9L, 17L, 16L, 18L, 35L, NA_integer_, NA_integer_, NA_integer_)
  )

  expect_error(
    cumprod(Array$create(c(4L, 1L))),
    "Unsupported operation on `Array`"
  )
})
