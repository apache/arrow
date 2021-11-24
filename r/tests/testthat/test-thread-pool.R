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


test_that("can set/get cpu thread pool capacity", {
  old <- cpu_count()
  set_cpu_count(19)
  expect_equal(cpu_count(), 19L)
  set_cpu_count(old)
  expect_equal(cpu_count(), old)
})

test_that("can set/get I/O thread pool capacity", {
  old <- io_thread_count()
  set_io_thread_count(19)
  expect_equal(io_thread_count(), 19L)
  set_io_thread_count(old)
  expect_equal(io_thread_count(), old)
})
