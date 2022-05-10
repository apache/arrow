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

test_that("Can't $new() an object with anything other than a pointer", {
  expect_error(
    Array$new(1:5),
    "Array$new() requires a pointer as input: did you mean $create() instead?",
    fixed = TRUE
  )
})

test_that("assert_is", {
  x <- 42
  expect_true(assert_is(x, "numeric"))
  expect_true(assert_is(x, c("numeric", "character")))
  expect_error(assert_is(x, "factor"), 'x must be a "factor"')
  expect_error(
    assert_is(x, c("factor", "list")),
    'x must be a "factor" or "list"'
  )
  expect_error(
    assert_is(x, c("factor", "character", "list")),
    'x must be a "factor", "character", or "list"'
  )
})

test_that("arrow gracefully fails to load objects from other sessions (ARROW-10071)", {
  a <- Array$create(1:10)
  tf <- tempfile()
  on.exit(unlink(tf))
  saveRDS(a, tf)

  b <- readRDS(tf)
  expect_error(b$length(), "Invalid <Array>")
})

test_that("check for an ArrowObject in functions use std::shared_ptr", {
  expect_error(Array__length(1), "Invalid R object")
})

test_that("MemoryPool calls gc() to free memory when allocation fails (ARROW-10080)", {
  # There is a valgrind error on this test because there cannot be memory allocated
  # which is exactly what this test is checking, but we quiet this
  skip_on_linux_devel()

  env <- new.env()
  suppressMessages(trace(gc, print = FALSE, tracer = function() {
    env$gc_was_called <- TRUE
  }))
  on.exit(suppressMessages(untrace(gc)))
  # We expect this should fail because we don't have this much memory,
  # but it should gc() and retry (and fail again)
  expect_error(BufferOutputStream$create(2**60))
  expect_true(env$gc_was_called)
})
