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

# To write a new version of a test file for a current version:
# write_parquet(example_with_metadata, test_path("golden-files/data-arrow_2.0.0.parquet"))

# To write a new version of a test file for an old version, use docker(-compose)
# to setup a linux distribution and use RStudio's public package manager binary
# repo to install the old version. The following commands should be run at the
# root of the arrow repo directory and might need slight adjusments.
# R_ORG=rstudio R_IMAGE=r-base R_TAG=4.0-focal docker-compose build --no-cache r
# R_ORG=rstudio R_IMAGE=r-base R_TAG=4.0-focal docker-compose run r /bin/bash
# R
# options(repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest")
# remotes::install_version("arrow", version = "1.0.1")
# # get example data into the global env
# write_parquet(example_with_metadata, "arrow/r/tests/testthat/golden-files/data-arrow_1.0.1.parquet")
# quit()/exit

test_that("reading a known Parquet file to dataframe with 2.0.0", {
  skip_if_not_available("snappy")
  pq_file <- test_path("golden-files/data-arrow_2.0.0.parquet")

  df <- read_parquet(pq_file)
  expect_equal(df, example_with_metadata)
  expect_identical(dim(df), c(1L, 4L))

  expect_equal(
    attributes(df),
    list(
      names = letters[1:4],
      row.names = 1L,
      top_level = list(field_one = 12, field_two = "more stuff"),
      class = c("tbl_df", "tbl", "data.frame"))
  )
  expect_equal(attributes(df$a), list(class = "special_string"))
  expect_null(attributes(df$b))
  expect_equal(
    attributes(df$c),
    list(
      row.names = 1L,
      names = c("c1", "c2", "c3"),
      class = c("tbl_df", "tbl", "data.frame")
    )
  )
  expect_null(attributes(df$d))
})

test_that("reading a known Parquet file to dataframe with 1.0.1", {
  skip_if_not_available("snappy")
  pq_file <- test_path("golden-files/data-arrow_1.0.1.parquet")

  df <- read_parquet(pq_file)
  # 1.0.1 didn't save top-level metadata, so we need to remove it.
  example_with_metadata_sans_toplevel <- example_with_metadata
  attributes(example_with_metadata_sans_toplevel)$top_level <- NULL
  expect_equal(df, example_with_metadata_sans_toplevel)
  expect_identical(dim(df), c(1L, 4L))

  expect_equal(
    attributes(df),
    list(
      names = letters[1:4],
      row.names = 1L,
      class = c("tbl_df", "tbl", "data.frame"))
  )
  expect_equal(attributes(df$a), list(class = "special_string"))
  expect_null(attributes(df$b))
  expect_equal(
    attributes(df$c),
    list(
      row.names = 1L,
      names = c("c1", "c2", "c3"),
      class = c("tbl_df", "tbl", "data.frame")
    )
  )
  expect_null(attributes(df$d))
})

for (comp in c("lz4", "uncompressed", "zstd")) {
  # write_feather(example_with_metadata, test_path("golden-files/data-arrow_2.0.0_lz4.feather"), compression = "lz4")
  # write_feather(example_with_metadata, test_path("golden-files/data-arrow_2.0.0_uncompressed.feather"), compression = "uncompressed")
  # write_feather(example_with_metadata, test_path("golden-files/data-arrow_2.0.0_zstd.feather"), compression = "zstd")
  test_that("reading a known Feather file to dataframe with 2.0.0", {
    skip_if_not_available(comp)
    feather_file <- test_path(paste0("golden-files/data-arrow_2.0.0_", comp,".feather"))

    df <- read_feather(feather_file)
    expect_equal(df, example_with_metadata)
    expect_identical(dim(df), c(1L, 4L))

    expect_equal(
      attributes(df),
      list(
        names = letters[1:4],
        row.names = 1L,
        top_level = list(field_one = 12, field_two = "more stuff"),
        class = c("tbl_df", "tbl", "data.frame"))
    )
    expect_equal(attributes(df$a), list(class = "special_string"))
    expect_null(attributes(df$b))
    expect_equal(
      attributes(df$c),
      list(
        row.names = 1L,
        names = c("c1", "c2", "c3"),
        class = c("tbl_df", "tbl", "data.frame")
      )
    )
    expect_null(attributes(df$d))
  })

  test_that("reading a known Feather file to dataframe with 1.0.1", {
    skip_if_not_available(comp)
    feather_file <- test_path(paste0("golden-files/data-arrow_1.0.1_", comp,".feather"))

    df <- read_feather(feather_file)
    # 1.0.1 didn't save top-level metadata, so we need to remove it.
    example_with_metadata_sans_toplevel <- example_with_metadata
    attributes(example_with_metadata_sans_toplevel)$top_level <- NULL
    expect_equal(df, example_with_metadata_sans_toplevel)
    expect_identical(dim(df), c(1L, 4L))

    expect_equal(
      attributes(df),
      list(
        names = letters[1:4],
        row.names = 1L,
        class = c("tbl_df", "tbl", "data.frame"))
    )
    expect_equal(attributes(df$a), list(class = "special_string"))
    expect_null(attributes(df$b))
    expect_equal(
      attributes(df$c),
      list(
        row.names = 1L,
        names = c("c1", "c2", "c3"),
        class = c("tbl_df", "tbl", "data.frame")
      )
    )
    expect_null(attributes(df$d))
  })

  test_that("reading a known Feather file to dataframe with 0.17.0", {
    skip_if_not_available(comp)
    feather_file <- test_path(paste0("golden-files/data-arrow_0.17.0_", comp,".feather"))

    if (comp %in% c("lz4", "zstd")) {
      # there is a case mis-match with versions 0.17.0 and before for the codec names
      expect_error(df <- read_feather(feather_file), "Unrecognized compression type:")
    } else {
      expect_error(df <- read_feather(feather_file), NA)
    }
  })
}

# TODO: streams(?)
