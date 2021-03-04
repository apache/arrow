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

test_that("default_memory_pool and its attributes", {
  pool <- default_memory_pool()
  # Not integer bc can be >2gb, so we cast to double
  expect_is(pool$bytes_allocated, "numeric")
  expect_is(pool$max_memory, "numeric")
  expect_true(pool$backend_name %in% c("system", "jemalloc", "mimalloc"))

  expect_true(all(supported_memory_backends() %in% c("system", "jemalloc", "mimalloc")))
})

test_that("arrow_info()", {
  expect_is(arrow_info(), "arrow_info")
  expect_output(print(arrow_info()), "Arrow package version")
  options(arrow.foo=FALSE)
  expect_output(print(arrow_info()), "arrow.foo")
})
