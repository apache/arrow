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

context("Parquet file reading/writing")

pq_file <- system.file("v0.7.1.parquet", package="arrow")

test_that("reading a known Parquet file to tibble", {
  df <- read_parquet(pq_file)
  expect_true(tibble::is_tibble(df))
  expect_identical(dim(df), c(10L, 11L))
  # TODO: assert more about the contents
})

test_that("simple int column roundtrip", {
  df <- tibble::tibble(x = 1:5)
  pq_tmp_file <- tempfile() # You can specify the .parquet here but that's probably not necessary
  on.exit(unlink(pq_tmp_file))

  write_parquet(df, pq_tmp_file)
  df_read <- read_parquet(pq_tmp_file)
  expect_identical(df, df_read)
})
