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

# nolint start
# To write a new version of a test file for a current version:
# write_parquet(example_with_metadata, test_path("golden-files/data-arrow_2.0.0.parquet"))

# To write a new version of a test file for an old version, use docker(-compose)
# to setup a linux distribution and use RStudio's public package manager binary
# repo to install the old version. The following commands should be run at the
# root of the arrow repo directory and might need slight adjustments.
# R_ORG=rstudio R_IMAGE=r-base R_TAG=4.0-focal docker-compose build --no-cache r
# R_ORG=rstudio R_IMAGE=r-base R_TAG=4.0-focal docker-compose run r /bin/bash
# R
# options(repos = "https://packagemanager.rstudio.com/all/__linux__/focal/latest")
# remotes::install_version("arrow", version = "1.0.1")
# # get example data into the global env
# write_parquet(example_with_metadata, "arrow/r/tests/testthat/golden-files/data-arrow_1.0.1.parquet")
# quit()/exit
# nolint end

skip_if(getRversion() < "3.5.0", "The serialization format changed in 3.5")

expect_identical_with_metadata <- function(object, expected, ..., top_level = TRUE) {
  attrs_to_keep <- c("names", "class", "row.names")
  if (!top_level) {
    # remove not-tbl and not-data.frame attributes
    for (attribute in names(attributes(expected))) {
      if (attribute %in% attrs_to_keep) next
      attributes(expected)[[attribute]] <- NULL
    }
  }
  expect_identical(object, expected, ...)
}

test_that("reading a known Parquet file to dataframe with 3.0.0", {
  skip_if_not_available("parquet")
  skip_if_not_available("snappy")
  pq_file <- test_path("golden-files/data-arrow-extra-meta_3.0.0.parquet")

  df <- read_parquet(pq_file)
  # this is equivalent to `expect_identical()`
  expect_identical_with_metadata(df, example_with_extra_metadata)
})

test_that("reading a known Parquet file to dataframe with 2.0.0", {
  skip_if_not_available("parquet")
  skip_if_not_available("snappy")
  pq_file <- test_path("golden-files/data-arrow_2.0.0.parquet")

  df <- read_parquet(pq_file)
  # this is equivalent to `expect_identical()`
  expect_identical_with_metadata(df, example_with_metadata)
})

test_that("reading a known Parquet file to dataframe with 1.0.1", {
  skip_if_not_available("parquet")
  skip_if_not_available("snappy")
  pq_file <- test_path("golden-files/data-arrow_1.0.1.parquet")

  df <- read_parquet(pq_file)
  # 1.0.1 didn't save top-level metadata, so we need to remove it.
  expect_identical_with_metadata(df, example_with_metadata, top_level = FALSE)
})

for (comp in c("lz4", "uncompressed", "zstd")) {
  # nolint start
  # write_feather(example_with_metadata, test_path("golden-files/data-arrow_2.0.0_lz4.feather"), compression = "lz4")
  # write_feather(example_with_metadata, test_path("golden-files/data-arrow_2.0.0_uncompressed.feather"), compression = "uncompressed")
  # write_feather(example_with_metadata, test_path("golden-files/data-arrow_2.0.0_zstd.feather"), compression = "zstd")
  # nolint end
  test_that("reading a known Feather file to dataframe with 2.0.0", {
    skip_if_not_available("parquet")
    skip_if_not_available(comp)
    feather_file <- test_path(paste0("golden-files/data-arrow_2.0.0_", comp, ".feather"))

    df <- read_feather(feather_file)
    expect_identical_with_metadata(df, example_with_metadata)
  })

  test_that("reading a known Feather file to dataframe with 1.0.1", {
    skip_if_not_available("parquet")
    skip_if_not_available(comp)
    feather_file <- test_path(paste0("golden-files/data-arrow_1.0.1_", comp, ".feather"))

    df <- read_feather(feather_file)
    # 1.0.1 didn't save top-level metadata, so we need to remove it.
    expect_identical_with_metadata(df, example_with_metadata, top_level = FALSE)
  })

  test_that("reading a known Feather file to dataframe with 0.17.0", {
    skip_if_not_available("parquet")
    skip_if_not_available(comp)
    feather_file <- test_path(paste0("golden-files/data-arrow_0.17.0_", comp, ".feather"))

    df <- read_feather(feather_file)
    # the metadata from 0.17.0 doesn't have the top level, the special class is
    # not maintained and the embedded tibble's attributes are read in a wrong
    # order. Since this is prior to 1.0.0 punting on checking the attributes
    # though classes are always checked, so that must be removed before checking.
    example_with_metadata_sans_special_class <- example_with_metadata
    example_with_metadata_sans_special_class$a <- unclass(example_with_metadata_sans_special_class$a)
    expect_equal(df, example_with_metadata_sans_special_class, ignore_attr = TRUE)
  })
}

test_that("sfc columns written by arrow <= 7.0.0 can be re-read", {
  # nolint start
  # df <- data.frame(x = I(list(structure(1, foo = "bar"), structure(2, baz = "qux"))))
  # class(df$x) <- c("sfc_MULTIPOLYGON", "sfc", "list")
  # withr::with_options(
  #   list("arrow.preserve_row_level_metadata" = TRUE), {
  #     arrow::write_feather(
  #       df,
  #       "tests/testthat/golden-files/data-arrow-sf_7.0.0.feather",
  #       compression = "uncompressed"
  #     )
  #   })
  # nolint end

  df <- read_feather(
    test_path("golden-files/data-arrow-sf_7.0.0.feather")
  )

  # make sure the class was restored
  expect_s3_class(df$x, c("sfc_MULTIPOLYGON", "sfc", "list"))

  # make sure the row-level metadata was restored
  expect_identical(attr(df$x[[1]], "foo"), "bar")
  expect_identical(attr(df$x[[2]], "baz"), "qux")
})

# TODO: streams(?)
