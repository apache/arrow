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

test_that("read_ipc_stream() and write_ipc_stream() accept connection objects", {
  tf <- tempfile()
  on.exit(unlink(tf))

  test_tbl <- tibble::tibble(
    x = 1:1e4,
    y = vapply(x, rlang::hash, character(1), USE.NAMES = FALSE),
    z = vapply(y, rlang::hash, character(1), USE.NAMES = FALSE)
  )

  write_ipc_stream(test_tbl, file(tf))
  expect_identical(read_ipc_stream(tf), test_tbl)
  expect_identical(read_ipc_stream(file(tf)), read_ipc_stream(tf))
})

test_that("write_ipc_stream can write Table", {
  table <- arrow_table(col1 = 1, col2 = "two")
  tf <- tempfile()
  on.exit(unlink(tf))

  expect_identical(write_ipc_stream(table, tf), table)
  expect_equal(read_ipc_stream(tf, as_data_frame = FALSE), table)
})

test_that("write_ipc_stream errors for invalid input type", {
  bad_input <- Array$create(1:5)
  expect_snapshot_error(write_ipc_stream(bad_input, feather_file))
})
