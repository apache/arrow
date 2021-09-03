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

test_that("non-bound compute kernels using TrimOptions", {
  expect_equal(
    call_function("utf8_trim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("racadabr")
  )

  expect_equal(
    call_function("utf8_ltrim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("racadabra")
  )

  expect_equal(
    call_function("utf8_rtrim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("abracadabr")
  )

  expect_equal(
    call_function("utf8_rtrim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("abracadabr")
  )

  expect_equal(
    call_function("ascii_ltrim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("racadabra")
  )

  expect_equal(
    call_function("ascii_rtrim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("abracadabr")
  )

  expect_equal(
    call_function("ascii_rtrim", Scalar$create("abracadabra"), options = list(characters = "ab")),
    Scalar$create("abracadabr")
  )
})
