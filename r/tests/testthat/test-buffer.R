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

context("arrow::Buffer")

test_that("arrow::Buffer can be created from raw vector", {
  vec <- raw(123)
  buf <- buffer(vec)
  expect_is(buf, "arrow::Buffer")
  expect_equal(buf$size, 123)
})

test_that("arrow::Buffer can be created from integer vector", {
  vec <- integer(17)
  buf <- buffer(vec)
  expect_is(buf, "arrow::Buffer")
  expect_equal(buf$size, 17 * 4)
})

test_that("arrow::Buffer can be created from numeric vector", {
  vec <- numeric(17)
  buf <- buffer(vec)
  expect_is(buf, "arrow::Buffer")
  expect_equal(buf$size, 17 * 8)
})

test_that("arrow::Buffer can be created from complex vector", {
  vec <- complex(3)
  buf <- buffer(vec)
  expect_is(buf, "arrow::Buffer")
  expect_equal(buf$size, 3 * 16)
})
