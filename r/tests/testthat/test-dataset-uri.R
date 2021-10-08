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

skip_on_os("windows")
skip_if_not_available("parquet")
skip_if_not_available("dataset")


library(dplyr, warn.conflicts = FALSE)

dataset_dir <- make_temp_dir()

test_that("Setup (putting data in the dir)", {
  dir.create(file.path(dataset_dir, 1))
  dir.create(file.path(dataset_dir, 2))
  write_parquet(df1, file.path(dataset_dir, 1, "file1.parquet"))
  write_parquet(df2, file.path(dataset_dir, 2, "file2.parquet"))
  expect_length(dir(dataset_dir, recursive = TRUE), 2)
})

files <- c(
  file.path(dataset_dir, 1, "file1.parquet", fsep = "/"),
  file.path(dataset_dir, 2, "file2.parquet", fsep = "/")
)


test_that("dataset from single local file path", {
  ds <- open_dataset(files[1])
  expect_r6_class(ds, "Dataset")
  expect_equal(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7) %>%
      collect() %>%
      arrange(dbl),
    df1[8:10, c("chr", "dbl")]
  )
})

test_that("dataset from vector of file paths", {
  ds <- open_dataset(files)
  expect_r6_class(ds, "Dataset")
  expect_equal(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )
})

test_that("dataset from directory URI", {
  uri <- paste0("file://", dataset_dir)
  ds <- open_dataset(uri, partitioning = schema(part = uint8()))
  expect_r6_class(ds, "Dataset")
  expect_equal(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )
})

test_that("dataset from single file URI", {
  uri <- paste0("file://", files[1])
  ds <- open_dataset(uri)
  expect_r6_class(ds, "Dataset")
  expect_equal(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7) %>%
      collect() %>%
      arrange(dbl),
    df1[8:10, c("chr", "dbl")]
  )
})

test_that("dataset from vector of file URIs", {
  uris <- paste0("file://", files)
  ds <- open_dataset(uris)
  expect_r6_class(ds, "Dataset")
  expect_equal(
    ds %>%
      select(chr, dbl) %>%
      filter(dbl > 7 & dbl < 53L) %>%
      collect() %>%
      arrange(dbl),
    rbind(
      df1[8:10, c("chr", "dbl")],
      df2[1:2, c("chr", "dbl")]
    )
  )
})

test_that("open_dataset errors on mixed paths and URIs", {
  expect_error(
    open_dataset(c(files[1], paste0("file://", files[2]))),
    "Vectors of mixed paths and URIs are not supported"
  )
})
