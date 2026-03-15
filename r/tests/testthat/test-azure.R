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

skip_if_not_available("azure")

# test_filesystem requires dplyr
library(dplyr)

# TODO: Add local azurite install to setup script
# skip_if_not(nzchar(Sys.which("azurite")), message = "azurite is not installed.")

# TODO: Start azurite from the test code instead of relying on it to be already running externally.

# Use default azurite credentials,
# see https://learn.microsoft.com/en-us/azure/storage/common/storage-connect-azurite?tabs=blob-storage
azurite_account_name <- "devstoreaccount1"
# Note that this is a well-known default credential for local development on Azurite.
azurite_account_key <- "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
azurite_blob_host <- "host.docker.internal"
azurite_blob_port <- "10000"
azurite_blob_storage_authority <- sprintf("%s:%s",azurite_blob_host, azurite_blob_port)
azurite_blob_storage_scheme <- "http"

# Helper functions for Azure URIs and paths
azure_uri <- function(...) {
  endpoint <- sprintf("%s%s%s", azurite_blob_host, "%3A", azurite_blob_port)
  template <- "abfs://%s:%s@%s?endpoint=%s"
  # URL encode the account key because it contains reserved characters
  encoded_key <- curl::curl_escape(azurite_account_key)
  sprintf(template, azurite_account_name, encoded_key, azure_path(...), endpoint)
}

azure_path <- function(...) {
  # 'dir' is the container name (following the convention in the s3 tests).
  paste(dir, ..., sep = "/")
}

fs <- AzureFileSystem$create(
  account_name=azurite_account_name,
  account_key=azurite_account_key,
  blob_storage_authority=azurite_blob_storage_authority,
  blob_storage_scheme=azurite_blob_storage_scheme
)

# (1) CreateDir and DeleteDir work correctly
dir <- "test"
fs$CreateDir(dir)
# Clean up when we're all done
withr::defer(fs$DeleteDir(dir))

# (2) Run default filesystem tests on azure filesystem

# TODO: As far as I can tell, there is no way to pass an Azurite URI to write_feather,
# so some of the test_filesystem tests can't be run with AzureFilesystem. Some tests
# below cover some of the skipped cases in test_filesystem.
test_filesystem("azure", fs, azure_path, azure_uri)

# (3) Test write/read parquet

example_data <- tibble::tibble(
  int = c(1:3, NA_integer_, 5:10),
  dbl = c(1:8, NA, 10) + 0.1,
  dbl2 = rep(5, 10),
  lgl = sample(c(TRUE, FALSE, NA), 10, replace = TRUE),
  false = logical(10),
  chr = letters[c(1:5, NA, 7:10)],
  fct = factor(letters[c(1:4, NA, NA, 7:10)])
)

test_that("read/write Parquet on azure", {
  skip_if_not_available("parquet")
  write_parquet(example_data, fs$path(azure_path("test.parquet")))
  expect_identical(read_parquet(fs$path(azure_path("test.parquet"))), example_data)
})

# (4) open_dataset with a vector of azure file paths

# TODO: I couldn't pass a vector of paths similar to the original test in
# test_filesystem, but you can pass a folder containing many files.
write_feather(example_data, fs$path(azure_path("openmulti/dataset1.feather")))
write_feather(example_data, fs$path(azure_path("openmulti/dataset2.feather")))

open_multi_fs = arrow:::az_bucket(
  bucket=azure_path("openmulti"),
  account_name=azurite_account_name,
  account_key=azurite_account_key,
  blob_storage_authority=azurite_blob_storage_authority,
  blob_storage_scheme=azurite_blob_storage_scheme
)

test_that("open_dataset with AzureFileSystem folder", {
  expect_identical(
    open_dataset(
      open_multi_fs,
      format = "feather"
    ) |>
      arrange(int) |>
      collect(),
    rbind(example_data, example_data) |> arrange(int)
  )
})



