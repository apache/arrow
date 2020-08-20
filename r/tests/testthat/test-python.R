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
  skip_if_not_installed("reticulate")
  venv <- try(reticulate::virtualenv_create("arrow-test"))
  # Bail out if virtualenv isn't available
  skip_if(inherits(venv, "try-error"))
  expect_error(install_pyarrow("arrow-test", nightly = TRUE), NA)
})

test_that("Array from Python", {
  skip_if_no_pyarrow()
  pa <- reticulate::import("pyarrow")
  py <- pa$array(c(1, 2, 3))
  expect_equal(py, Array$create(c(1, 2, 3)))
})

test_that("Array to Python", {
  skip_if_no_pyarrow()
  pa <- reticulate::import("pyarrow", convert=FALSE)
  r <- Array$create(c(1, 2, 3))
  py <- pa$concat_arrays(list(r))
  expect_is(py, "pyarrow.lib.Array")
  expect_equal(reticulate::py_to_r(py), r)
})

test_that("RecordBatch to/from Python", {
  skip_if_no_pyarrow()
  pa <- reticulate::import("pyarrow", convert=FALSE)
  batch <- record_batch(col1=c(1, 2, 3), col2=letters[1:3])
  py <- reticulate::r_to_py(batch)
  expect_is(py, "pyarrow.lib.RecordBatch")
  expect_equal(reticulate::py_to_r(py), batch)
})

test_that("Table and ChunkedArray from Python", {
  skip_if_no_pyarrow()
  pa <- reticulate::import("pyarrow", convert=FALSE)
  batch <- record_batch(col1=c(1, 2, 3), col2=letters[1:3])
  tab <- Table$create(batch, batch)
  pybatch <- reticulate::r_to_py(batch)
  pytab <- pa$Table$from_batches(list(pybatch, pybatch))
  expect_is(pytab, "pyarrow.lib.Table")
  expect_is(pytab[0], "pyarrow.lib.ChunkedArray")
  expect_equal(reticulate::py_to_r(pytab[0]), tab$col1)
  expect_equal(reticulate::py_to_r(pytab), tab)
})

test_that("Table and ChunkedArray to Python", {
  skip_if_no_pyarrow()
  pa <- reticulate::import("pyarrow", convert=FALSE)
  batch <- record_batch(col1=c(1, 2, 3), col2=letters[1:3])
  tab <- Table$create(batch, batch)

  pychunked <- reticulate::r_to_py(tab$col1)
  expect_is(pychunked, "pyarrow.lib.ChunkedArray")
  expect_equal(reticulate::py_to_r(pychunked), tab$col1)

  pytab <- reticulate::r_to_py(tab)
  expect_is(pytab, "pyarrow.lib.Table")
  expect_equal(reticulate::py_to_r(pytab), tab)
})
