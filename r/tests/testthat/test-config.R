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

test_that("set_io_thread_count() sets the number of io threads", {
  current_io_thread_count <- io_thread_count()
  on.exit(set_io_thread_count(current_io_thread_count))

  previous_io_thread_count <- set_io_thread_count(1)
  expect_identical(previous_io_thread_count, current_io_thread_count)
  expect_identical(io_thread_count(), 1L)

  expect_identical(set_io_thread_count(current_io_thread_count), 1L)
})

test_that("set_cpu_count() sets the number of CPU threads", {
  current_cpu_count <- cpu_count()
  on.exit(set_cpu_count(current_cpu_count))

  previous_cpu_count <- set_cpu_count(1)
  expect_identical(previous_cpu_count, current_cpu_count)
  expect_identical(cpu_count(), 1L)

  expect_identical(set_cpu_count(current_cpu_count), 1L)
})
