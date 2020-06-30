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

context("Schema metadata and R attributes")

test_that("Schema metadata", {
  s <- schema(b = double())
  expect_equivalent(s$metadata, list())
  expect_false(s$HasMetadata)
  s$metadata <- list(test = TRUE)
  expect_identical(s$metadata, list(test = "TRUE"))
  expect_true(s$HasMetadata)
  s$metadata$foo <- 42
  expect_identical(s$metadata, list(test = "TRUE", foo = "42"))
  expect_true(s$HasMetadata)
  s$metadata$foo <- NULL
  expect_identical(s$metadata, list(test = "TRUE"))
  expect_true(s$HasMetadata)
  s$metadata <- NULL
  expect_equivalent(s$metadata, list())
  expect_false(s$HasMetadata)
  expect_error(
    s$metadata <- 4,
    "Key-value metadata must be a named list or character vector"
  )
})

test_that("Table metadata", {
  tab <- Table$create(x = 1:2, y = c("a", "b"))
  expect_equivalent(tab$metadata, list())
  tab$metadata <- list(test = TRUE)
  expect_identical(tab$metadata, list(test = "TRUE"))
  tab$metadata$foo <- 42
  expect_identical(tab$metadata, list(test = "TRUE", foo = "42"))
  tab$metadata$foo <- NULL
  expect_identical(tab$metadata, list(test = "TRUE"))
  tab$metadata <- NULL
  expect_equivalent(tab$metadata, list())
})

test_that("Table R metadata", {
  tab <- Table$create(example_with_metadata)
  expect_output(print(tab$metadata), "arrow_r_metadata")
  expect_identical(as.data.frame(tab), example_with_metadata)
})

test_that("R metadata is not stored for types that map to Arrow types (factor, Date, etc.)", {
  tab <- Table$create(example_data[1:6])
  expect_null(tab$metadata$r)
  expect_null(Table$create(example_with_times[1:3])$metadata$r)
})

test_that("Garbage R metadata doesn't break things", {
  tab <- Table$create(example_data[1:6])
  tab$metadata$r <- "garbage"
  expect_warning(
    expect_identical(as.data.frame(tab), example_data[1:6]),
    "Invalid metadata$r",
    fixed = TRUE
  )
  tab$metadata$r <- .serialize_arrow_r_metadata("garbage")
  expect_warning(
    expect_identical(as.data.frame(tab), example_data[1:6]),
    "Invalid metadata$r",
    fixed = TRUE
  )
})

test_that("RecordBatch metadata", {
  rb <- RecordBatch$create(x = 1:2, y = c("a", "b"))
  expect_equivalent(rb$metadata, list())
  rb$metadata <- list(test = TRUE)
  expect_identical(rb$metadata, list(test = "TRUE"))
  rb$metadata$foo <- 42
  expect_identical(rb$metadata, list(test = "TRUE", foo = "42"))
  rb$metadata$foo <- NULL
  expect_identical(rb$metadata, list(test = "TRUE"))
  rb$metadata <- NULL
  expect_equivalent(rb$metadata, list())
})

test_that("RecordBatch R metadata", {
  expect_identical(as.data.frame(record_batch(example_with_metadata)), example_with_metadata)
})

test_that("R metadata roundtrip via parquet", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write_parquet(example_with_metadata, tf)
  expect_identical(read_parquet(tf), example_with_metadata)
})

test_that("R metadata roundtrip via feather", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write_feather(example_with_metadata, tf)
  expect_identical(read_feather(tf), example_with_metadata)
})

test_that("haven types roundtrip via feather", {
  tf <- tempfile()
  on.exit(unlink(tf))

  write_feather(haven_data, tf)
  expect_identical(read_feather(tf), haven_data)
})

test_that("Date/time type roundtrip", {
  rb <- record_batch(example_with_times)
  expect_is(rb$schema$posixlt$type, "StructType")
  expect_identical(as.data.frame(rb), example_with_times)
})
