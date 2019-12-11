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
  f <- FieldExpression$create("f")
  g <- FieldExpression$create("g")
  expect_is(f == g, "ComparisonExpression")
  expect_is(f == 4, "ComparisonExpression")
  expect_is(f <= 2L, "ComparisonExpression")
  expect_is(f != FALSE, "ComparisonExpression")
  expect_is(f > 4, "ComparisonExpression")
  expect_is(f < 4 & f > 2, "AndExpression")
  expect_is(f < 4 | f > 2, "OrExpression")
  expect_is(!(f < 4), "NotExpression")
  expect_output(
    print(f > 4),
    'ComparisonExpression\n(f > 4:double)',
    fixed = TRUE
  )
})
