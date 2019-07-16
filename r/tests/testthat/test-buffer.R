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

test_that("can convert arrow::Buffer to raw", {
  buf <- buffer(rnorm(10))
  expect_equal(buf$data(), as.raw(buf))
})

test_that("can read remaining bytes of a RandomAccessFile", {
  tbl <- tibble::tibble(
    int = 1:10, dbl = as.numeric(1:10),
    lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
    chr = letters[1:10]
  )
  tab <- arrow::table(!!!tbl)

  tf <- tempfile()
  all_bytes <- write_arrow(tab, tf)

  file <- ReadableFile(tf)
  x <- file$Read(20)$data()
  y <- file$Read()$data()

  file <- ReadableFile(tf)
  z <- file$Read()$data()

  file <- ReadableFile(tf)
  a <- file$ReadAt(20)$data()

  expect_equal(file$GetSize(), length(x) + length(y))
  expect_equal(z, c(x, y))
  expect_equal(a, y)
})
