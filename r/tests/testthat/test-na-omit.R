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

data_no_na <- c(2:10)
data_na <- c(data_no_na, NA_real_)

test_that("na.fail on Scalar", {
  scalar_na <- Scalar$create(NA)
  scalar_one <- Scalar$create(1)
  expect_as_vector(na.fail(scalar_one), 1)
  expect_error(na.fail(scalar_na), "missing values in object")
})

test_that("na.omit on Array and ChunkedArray", {
  compare_expression(na.omit(.input), data_no_na)
  compare_expression(na.omit(.input), data_na, ignore_attr = TRUE)
})

test_that("na.exclude on Array and ChunkedArray", {
  compare_expression(na.exclude(.input), data_no_na)
  compare_expression(na.exclude(.input), data_na, ignore_attr = TRUE)
})

test_that("na.fail on Array and ChunkedArray", {
  compare_expression(na.fail(.input), data_no_na, ignore_attr = TRUE)
  compare_expression_error(na.fail(.input), data_na)
})

test_that("na.omit on Table", {
  tbl <- Table$create(example_data)
  expect_equal_data_frame(
    na.omit(tbl),
    na.omit(example_data),
    # We don't include an attribute with the rows omitted
    ignore_attr = "na.action"
  )
})

test_that("na.exclude on Table", {
  tbl <- Table$create(example_data)
  expect_equal_data_frame(
    na.exclude(tbl),
    na.exclude(example_data),
    ignore_attr = "na.action"
  )
})

test_that("na.fail on Table", {
  tbl <- Table$create(example_data)
  expect_error(na.fail(tbl), "missing values in object")
})

test_that("na.omit on RecordBatch", {
  batch <- record_batch(example_data)
  expect_equal_data_frame(
    na.omit(batch),
    na.omit(example_data),
    ignore_attr = "na.action"
  )
})

test_that("na.exclude on RecordBatch", {
  batch <- record_batch(example_data)
  expect_equal_data_frame(
    na.exclude(batch),
    na.omit(example_data),
    ignore_attr = "na.action"
  )
})

test_that("na.fail on RecordBatch", {
  batch <- record_batch(example_data)
  expect_error(na.fail(batch), "missing values in object")
})
