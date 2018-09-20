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

context("ChunkedArray")

test_that("ChunkedArray", {
  x <- chunked_array(1:10, 1:10, 1:5)
  expect_equal(x$type(), int32())
  expect_equal(x$num_chunks(), 3L)
  expect_equal(x$length(), 25L)
  expect_equal(x$as_vector(), c(1:10, 1:10, 1:5))

  x <- chunked_array(c(1,2,3), c(4,5,6))
  expect_equal(x$type(), float64())
  expect_equal(x$num_chunks(), 2L)
  expect_equal(x$length(), 6L)
  expect_equal(x$as_vector(), as.numeric(1:6))
})
