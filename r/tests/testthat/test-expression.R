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

context("Expressions")

test_that("Can create an expression", {
  expect_is(Array$create(1:5) + 4, "array_expression")
})

test_that("Recursive expression generation", {
  a <- Array$create(1:5)
  expect_is(a == 4 | a == 3, "array_expression")
})

test_that("as.vector(array_expression)", {
  a <- Array$create(1:5)
  expect_equal(as.vector(a + 4), 5:9)
  expect_equal(as.vector(a == 4 | a == 3), c(FALSE, FALSE, TRUE, TRUE, FALSE))
})

test_that("array_expression print method", {
  a <- Array$create(1:5)
  expect_output(
    print(a == 4 | a == 3),
    capture.output(print(c(FALSE, FALSE, TRUE, TRUE, FALSE))),
    fixed = TRUE
  )
})

test_that("C++ expressions", {
  f <- Expression$field_ref("f")
  g <- Expression$field_ref("g")
  date <- Expression$scalar(as.Date("2020-01-15"))
  ts <- Expression$scalar(as.POSIXct("2020-01-17 11:11:11"))
  i64 <- Expression$scalar(bit64::as.integer64(42))
  time <- Expression$scalar(hms::hms(56, 34, 12))

  expect_is(f == g, "Expression")
  expect_is(f == 4, "Expression")
  expect_is(f == "", "Expression")
  expect_is(f == NULL, "Expression")
  expect_is(f == date, "Expression")
  expect_is(f == i64, "Expression")
  expect_is(f == time, "Expression")
  # can't seem to make this work right now because of R Ops.method dispatch
  # expect_is(f == as.Date("2020-01-15"), "Expression")
  expect_is(f == ts, "Expression")
  expect_is(f <= 2L, "Expression")
  expect_is(f != FALSE, "Expression")
  expect_is(f > 4, "Expression")
  expect_is(f < 4 & f > 2, "Expression")
  expect_is(f < 4 | f > 2, "Expression")
  expect_is(!(f < 4), "Expression")
  expect_output(
    print(f > 4),
    'Expression\n(f > 4:double)',
    fixed = TRUE
  )
  # Interprets that as a list type
  expect_is(f == c(1L, 2L), "Expression")
})
