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

context("altrep")

skip_if(getRversion() <= "3.5.0")

test_that("altrep vectors from int32 and dbl arrays with no nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(1:1000)
  v_dbl <- Array$create(as.numeric(1:1000))

  expect_true(is_altrep_int_nonull(as.vector(v_int)))
  expect_true(is_altrep_int_nonull(as.vector(v_int$Slice(1))))
  expect_true(is_altrep_dbl_nonull(as.vector(v_dbl)))
  expect_true(is_altrep_dbl_nonull(as.vector(v_dbl$Slice(1))))

  withr::local_options(list(arrow.use_altrep = NULL))
  expect_true(is_altrep_int_nonull(as.vector(v_int)))
  expect_true(is_altrep_int_nonull(as.vector(v_int$Slice(1))))
  expect_true(is_altrep_dbl_nonull(as.vector(v_dbl)))
  expect_true(is_altrep_dbl_nonull(as.vector(v_dbl$Slice(1))))

  withr::local_options(list(arrow.use_altrep = FALSE))
  expect_false(is_altrep_int_nonull(as.vector(v_int)))
  expect_false(is_altrep_int_nonull(as.vector(v_int$Slice(1))))
  expect_false(is_altrep_dbl_nonull(as.vector(v_dbl)))
  expect_false(is_altrep_dbl_nonull(as.vector(v_dbl$Slice(1))))
})

test_that("altrep vectors from int32 and dbl arrays with nulls", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(c(1L, NA, 3L))
  v_dbl <- Array$create(c(1, NA, 3))

  # cannot be altrep because one NA
  expect_false(is_altrep_int_nonull(as.vector(v_int)))
  expect_false(is_altrep_int_nonull(as.vector(v_int$Slice(1))))
  expect_false(is_altrep_dbl_nonull(as.vector(v_dbl)))
  expect_false(is_altrep_dbl_nonull(as.vector(v_dbl$Slice(1))))

  # but then, no NA beyond, so can be altrep again
  expect_true(is_altrep_int_nonull(as.vector(v_int$Slice(2))))
  expect_true(is_altrep_dbl_nonull(as.vector(v_dbl$Slice(2))))
})

test_that("empty vectors are not altrep", {
  withr::local_options(list(arrow.use_altrep = TRUE))
  v_int <- Array$create(integer())
  v_dbl <- Array$create(numeric())

  expect_false(is_altrep_int_nonull(as.vector(v_int)))
  expect_false(is_altrep_dbl_nonull(as.vector(v_dbl)))
})
