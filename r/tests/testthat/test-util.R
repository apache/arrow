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

test_that("as_writable_table() works for data.frame, RecordBatch, and Table", {
  table <- arrow_table(col1 = 1, col2 = "two")
  expect_identical(as_writable_table(table), table)

  batch <- record_batch(col1 = 1, col2 = "two")
  expect_equal(as_writable_table(batch), table)

  tbl <- tibble::tibble(col1 = 1, col2 = "two")
  # because of metadata
  table_from_tbl <- as_writable_table(tbl)
  table_from_tbl$metadata <- NULL
  expect_equal(table_from_tbl, table)
})

test_that("as_writable_table() errors for invalid input", {
  # check errors from a wrapper function (i.e., simulate write_*() functions)
  wrapper_fun <- function(x) as_writable_table(x)

  # make sure we get the custom error message
  expect_snapshot_error(wrapper_fun("not a table"))

  # make sure other errors make it through
  expect_snapshot_error(wrapper_fun(data.frame(x = I(list(1, "a")))))
})

test_that("all_funs() identifies namespace-qualified and unqualified functions", {
  expect_equal(
    all_funs(rlang::quo(pkg::fun())),
    "pkg::fun"
  )
  expect_equal(
    all_funs(rlang::quo(pkg::fun(other_pkg::obj))),
    "pkg::fun"
  )
  expect_equal(
    all_funs(rlang::quo(other_fun(pkg::fun()))),
    c("other_fun", "pkg::fun")
  )
  expect_equal(
    all_funs(rlang::quo(other_pkg::other_fun(pkg::fun()))),
    c("other_pkg::other_fun", "pkg::fun")
  )
  expect_equal(
    all_funs(rlang::quo(other_pkg::other_fun(pkg::fun(sum(base::log()))))),
    c("other_pkg::other_fun", "pkg::fun", "sum", "base::log")
  )
  expect_equal(
    all_funs(rlang::quo(other_fun(fun(sum(log()))))),
    c("other_fun", "fun", "sum", "log")
  )
  expect_equal(
    all_funs(rlang::quo(other_fun(fun(sum(base::log()))))),
    c("other_fun", "fun", "sum", "base::log")
  )
})

test_that("parse_compact_col_spec() converts string specs to schema", {
  # Test valid inputs
  schema <- parse_compact_col_spec("idc", c("int_col", "double_col", "string_col"))
  expect_s3_class(schema, "Schema")
  expect_equal(schema$fields[[1]]$type$ToString(), "int32")
  expect_equal(schema$fields[[2]]$type$ToString(), "double")
  expect_equal(schema$fields[[3]]$type$ToString(), "string")
  
  # Test all supported types
  schema <- parse_compact_col_spec("cidlDTtf_-?", 
                                  c("c", "i", "d", "l", "D", "T", "t", "f", "_", "-", "?"))
  expect_equal(schema$fields[[1]]$type$ToString(), "string")
  expect_equal(schema$fields[[2]]$type$ToString(), "int32")
  expect_equal(schema$fields[[3]]$type$ToString(), "double")
  expect_equal(schema$fields[[4]]$type$ToString(), "bool")
  expect_equal(schema$fields[[5]]$type$ToString(), "date32[day]")
  expect_equal(schema$fields[[6]]$type$ToString(), "timestamp[ns]")
  expect_equal(schema$fields[[7]]$type$ToString(), "time32[ms]")
  expect_equal(schema$fields[[8]]$type$ToString(), "dictionary<values=string, indices=int32>")
  
  # Count all fields (null types are included in actual implementation)
  expect_equal(length(schema$fields), 10)
  
  # Test error conditions
  expect_error(parse_compact_col_spec(c("i", "d"), c("a", "b")))
  expect_error(parse_compact_col_spec("idc", c("a", "b")))
  expect_error(parse_compact_col_spec("y", "a"))
})

