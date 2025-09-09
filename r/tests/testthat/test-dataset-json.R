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

skip_if_not_available("dataset")

library(dplyr, warn.conflicts = FALSE)

test_that("JSON dataset", {
  # set up JSON directory for testing
  json_dir <- make_temp_dir()

  on.exit(unlink(json_dir, recursive = TRUE))
  dir.create(file.path(json_dir, 5))
  dir.create(file.path(json_dir, 6))

  con_file1 <- file(file.path(json_dir, 5, "file1.json"), open = "wb")
  jsonlite::stream_out(df1, con = con_file1, verbose = FALSE)
  close(con_file1)

  con_file2 <- file(file.path(json_dir, 6, "file2.json"), open = "wb")
  jsonlite::stream_out(df2, con = con_file2, verbose = FALSE)
  close(con_file2)

  ds <- open_dataset(json_dir, format = "json", partitioning = "part")

  expect_r6_class(ds$format, "JsonFileFormat")
  expect_r6_class(ds$filesystem, "LocalFileSystem")
  expect_named(ds, c(names(df1), "part"))
  expect_identical(dim(ds), c(20L, 7L))

  expect_equal(
    ds %>%
      select(string = chr, integer = int, part) %>%
      filter(integer > 6 & part == 5) %>%
      collect() %>%
      summarize(mean = mean(as.numeric(integer))), # as.numeric bc they're being parsed as int64
    df1 %>%
      select(string = chr, integer = int) %>%
      filter(integer > 6) %>%
      summarize(mean = mean(integer))
  )
  # Collecting virtual partition column works
  expect_equal(
    collect(ds) %>% arrange(part) %>% pull(part),
    c(rep(5, 10), rep(6, 10))
  )
})

test_that("JSON Fragment scan options", {
  options <- FragmentScanOptions$create("json")
  expect_equal(options$type, "json")

  expect_error(FragmentScanOptions$create("json", invalid_selection = TRUE), regexp = "invalid_selection")
})
