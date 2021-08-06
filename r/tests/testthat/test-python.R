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

context("To/from Python")

test_that("install_pyarrow", {
  skip_on_cran()
  skip_if_not_dev_mode()
  # Python problems on Apple M1 still
  skip_if(grepl("arm-apple|aarch64.*darwin", R.Version()$platform))
  skip_if_not_installed("reticulate")
  venv <- try(reticulate::virtualenv_create("arrow-test"))
  # Bail out if virtualenv isn't available
  skip_if(inherits(venv, "try-error"))
  expect_error(install_pyarrow("arrow-test", nightly = TRUE), NA)
  # Set this up for the following tests
  reticulate::use_virtualenv("arrow-test")
})

skip_if_no_pyarrow()

test_that("Array from Python", {
  pa <- reticulate::import("pyarrow")
  py <- pa$array(c(1, 2, 3))
  expect_equal(py, Array$create(c(1, 2, 3)))
})

test_that("Array to Python", {
  pa <- reticulate::import("pyarrow", convert = FALSE)
  r <- Array$create(c(1, 2, 3))
  py <- pa$concat_arrays(list(r))
  expect_s3_class(py, "pyarrow.lib.Array")
  expect_equal(reticulate::py_to_r(py), r)
})

test_that("RecordBatch to/from Python", {
  pa <- reticulate::import("pyarrow", convert = FALSE)
  batch <- record_batch(col1 = c(1, 2, 3), col2 = letters[1:3])
  py <- reticulate::r_to_py(batch)
  expect_s3_class(py, "pyarrow.lib.RecordBatch")
  expect_equal(reticulate::py_to_r(py), batch)
})

test_that("Table and ChunkedArray from Python", {
  pa <- reticulate::import("pyarrow", convert = FALSE)
  batch <- record_batch(col1 = c(1, 2, 3), col2 = letters[1:3])
  tab <- Table$create(batch, batch)
  pybatch <- reticulate::r_to_py(batch)
  pytab <- pa$Table$from_batches(list(pybatch, pybatch))
  expect_s3_class(pytab, "pyarrow.lib.Table")
  expect_s3_class(pytab[0], "pyarrow.lib.ChunkedArray")
  expect_equal(reticulate::py_to_r(pytab[0]), tab$col1)
  expect_equal(reticulate::py_to_r(pytab), tab)
})

test_that("Table and ChunkedArray to Python", {
  batch <- record_batch(col1 = c(1, 2, 3), col2 = letters[1:3])
  tab <- Table$create(batch, batch)

  pychunked <- reticulate::r_to_py(tab$col1)
  expect_s3_class(pychunked, "pyarrow.lib.ChunkedArray")
  expect_equal(reticulate::py_to_r(pychunked), tab$col1)

  pytab <- reticulate::r_to_py(tab)
  expect_s3_class(pytab, "pyarrow.lib.Table")
  expect_equal(reticulate::py_to_r(pytab), tab)
})

test_that("RecordBatch with metadata roundtrip", {
  batch <- RecordBatch$create(example_with_times)
  pybatch <- reticulate::r_to_py(batch)
  expect_s3_class(pybatch, "pyarrow.lib.RecordBatch")
  expect_equal(reticulate::py_to_r(pybatch), batch)
  expect_identical(as.data.frame(reticulate::py_to_r(pybatch)), example_with_times)
})

test_that("Table with metadata roundtrip", {
  tab <- Table$create(example_with_times)
  pytab <- reticulate::r_to_py(tab)
  expect_s3_class(pytab, "pyarrow.lib.Table")
  expect_equal(reticulate::py_to_r(pytab), tab)
  expect_identical(as.data.frame(reticulate::py_to_r(pytab)), example_with_times)
})

test_that("DataType roundtrip", {
  r <- timestamp("ms", timezone = "Pacific/Marquesas")
  py <- reticulate::r_to_py(r)
  expect_s3_class(py, "pyarrow.lib.DataType")
  expect_equal(reticulate::py_to_r(py), r)
})

test_that("Field roundtrip", {
  r <- field("x", time32("s"))
  py <- reticulate::r_to_py(r)
  expect_s3_class(py, "pyarrow.lib.Field")
  expect_equal(reticulate::py_to_r(py), r)
})

test_that("RecordBatchReader to python", {
  library(dplyr)

  tab <- Table$create(example_data)
  scan <- tab %>%
    select(int, lgl) %>%
    filter(int > 6) %>%
    Scanner$create()
  reader <- scan$ToRecordBatchReader()
  pyreader <- reticulate::r_to_py(reader)
  expect_s3_class(pyreader, "pyarrow.lib.RecordBatchReader")
  pytab <- pyreader$read_all()
  expect_s3_class(pytab, "pyarrow.lib.Table")
  back_to_r <- reticulate::py_to_r(pytab)
  expect_r6_class(back_to_r, "Table")
  expect_identical(
    as.data.frame(back_to_r),
    example_data %>%
      select(int, lgl) %>%
      filter(int > 6)
  )
})

test_that("RecordBatchReader from python", {
  tab <- Table$create(example_data)
  scan <- Scanner$create(tab)
  reader <- scan$ToRecordBatchReader()
  pyreader <- reticulate::r_to_py(reader)
  back_to_r <- reticulate::py_to_r(pyreader)
  rt_table <- back_to_r$read_table()
  expect_r6_class(rt_table, "Table")
  expect_identical(as.data.frame(rt_table), example_data)
})
