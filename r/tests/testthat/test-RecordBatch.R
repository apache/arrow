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

context("arrow::RecordBatch")

test_that("RecordBatch", {
  tbl <- tibble::tibble(int = 1:10, dbl = as.numeric(1:10))
  batch <- record_batch(tbl)

  expect_true(batch == batch)
  expect_equal(
    batch$schema(),
    schema(int = int32(), dbl = float64())
  )
  expect_equal(batch$num_columns(), 2L)
  expect_equal(batch$num_rows(), 10L)
  expect_equal(batch$column_name(0), "int")
  expect_equal(batch$column_name(1), "dbl")
  expect_equal(names(batch), c("int", "dbl"))

  col_int <- batch$column(0)
  expect_true(inherits(col_int, 'arrow::Array'))
  expect_equal(col_int$as_vector(), tbl$int)
  expect_equal(col_int$type(), int32())

  col_dbl <- batch$column(1)
  expect_true(inherits(col_dbl, 'arrow::Array'))
  expect_equal(col_dbl$as_vector(), tbl$dbl)
  expect_equal(col_dbl$type(), float64())

  batch2 <- batch$RemoveColumn(0)
  expect_equal(
    batch2$schema(),
    schema(dbl = float64())
  )
  expect_equal(batch2$column(0), batch$column(1))

  batch3 <- batch$Slice(5)
  expect_equal(batch3$num_rows(), 5)
  expect_equal(batch3$column(0)$as_vector(), 6:10)

  batch4 <- batch$Slice(5, 2)
  expect_equal(batch4$num_rows(), 2)
  expect_equal(batch4$column(0)$as_vector(), 6:7)
})
