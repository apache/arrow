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
  expect_is(Array$create(1:5) + 4, "Expression")
})

test_that("Recursive expression generation", {
  a <- Array$create(1:5)
  expect_is(a == 4 | a == 3, "Expression")
})

test_that("as.vector(expression)", {
  a <- Array$create(1:5)
  expect_equal(as.vector(a + 4), 5:9)
  expect_equal(as.vector(a == 4 | a == 3), c(FALSE, FALSE, TRUE, TRUE, FALSE))
})

test_that("Expression print method", {
  a <- Array$create(1:5)
  expect_output(
    print(a == 4 | a == 3),
    capture.output(print(c(FALSE, FALSE, TRUE, TRUE, FALSE))),
    fixed = TRUE
  )
})
