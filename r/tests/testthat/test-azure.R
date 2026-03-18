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

# This test script depends on ./ci/scripts/install_azurite.sh
skip_if_not(nzchar(Sys.which("azurite")), message = "azurite is not installed.")

# Use default azurite credentials,
# see https://learn.microsoft.com/en-us/azure/storage/common/storage-connect-azurite?tabs=blob-storage
azurite_account_name <- "devstoreaccount1"
# Note that this is a well-known default credential for local development on Azurite.
azurite_account_key <- "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
azurite_blob_host <- "127.0.0.1"
azurite_blob_port <- "10000"
azurite_blob_storage_authority <- sprintf("%s:%s", azurite_blob_host, azurite_blob_port)
azurite_blob_storage_scheme <- "http"

pid_azurite <- sys::exec_background(
  "azurite",
  c("azurite", "--inMemoryPersistence", "--blobHost", azurite_blob_host),
  std_out = FALSE
)
# Kill azurite background process once tests have finished running.
withr::defer(tools::pskill(pid_azurite))

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
  account_name = azurite_account_name,
  account_key = azurite_account_key,
  blob_storage_authority = azurite_blob_storage_authority,
  blob_storage_scheme = azurite_blob_storage_scheme
)

# (1) CreateDir and DeleteDir work correctly
dir <- "test"
fs$CreateDir(dir)
# Clean up when we're all done
withr::defer(fs$DeleteDir(dir))

# (2) Run default filesystem tests on azure filesystem

# TODO: As far as I can tell, there is no way to pass an Azurite URI to write_feather
# (or any other read/write helper), so some of the test_filesystem tests can't be run
# with AzureFilesystem. Some tests below cover some of the skipped cases in
# test_filesystem.
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

open_multi_fs <- arrow:::az_container(
  container_path = azure_path("openmulti"),
  account_name = azurite_account_name,
  account_key = azurite_account_key,
  blob_storage_authority = azurite_blob_storage_authority,
  blob_storage_scheme = azurite_blob_storage_scheme
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

# (5) Check that multiple valid combinations of options can be used to
# instantiate AzureFileSystem.

fs1 <- AzureFileSystem$create(account_name = "fake-account-name")
expect_s3_class(fs1, "AzureFileSystem")

fs2 <- AzureFileSystem$create(account_name = "fake-account-name", account_key = "fakeaccountkey")
expect_s3_class(fs2, "AzureFileSystem")


fs3 <- AzureFileSystem$create(
  account_name = "fake-account", account_key = "fakeaccount",
  blob_storage_authority = "fake-blob-authority",
  dfs_storage_authority = "fake-dfs-authority",
  blob_storage_scheme = "https",
  dfs_storage_scheme = "https"
)
expect_s3_class(fs3, "AzureFileSystem")

fs4 <- AzureFileSystem$create(
  account_name = "fake-account-name",
  sas_token = "fakesastoken"
)
expect_s3_class(fs4, "AzureFileSystem")

fs5 <- AzureFileSystem$create(
  account_name = "fake-account-name",
  tenant_id = "fake-tenant-id",
  client_id = "fake-client-id",
  client_secret = "fake-client-secret"
)
expect_s3_class(fs5, "AzureFileSystem")

fs6 <- AzureFileSystem$create(
  account_name = "fake-account-name",
  client_id = "fake-client-id"
)
expect_s3_class(fs6, "AzureFileSystem")

# (6) Check that invalid argument combinations are caught upfront
# with appropriate error message.

error_msg_1 <- "`client_id` must be given with `tenant_id` and `client_secret`"
error_msg_2 <- "Provide only `client_id` to authenticate with Managed Identity Credential, or provide `client_id`, `tenant_id`, and`client_secret` to authenticate with Client Secret Credential"

test_that("client_id must be specified with account_name and tenant_id", {
  expect_error(
    AzureFileSystem$create(
      account_name = "fake-account-name",
      tenant_id = "fake-tenant-id"
    ),
    error_msg_1,
    fixed = TRUE
  )
})

test_that("client_id must be specified with account_name and client_secret", {
  expect_error(
    AzureFileSystem$create(
      account_name = "fake-account-name",
      client_secret = "fake-client-secret"
    ),
    error_msg_1,
    fixed = TRUE
  )
})

test_that("client_secret must not be provided with client_id", {
  expect_error(
    AzureFileSystem$create(
      account_name = "fake-account-name",
      client_id = "fake-client-id",
      client_secret = "fake-client-secret"
    ),
    error_msg_2,
    fixed = TRUE
  )
})

test_that("client_id must be specified with account_name, tenant_id, and client_secret", {
  expect_error(
    AzureFileSystem$create(
      account_name = "fake-account-name",
      tenant_id = "fake-tenant-id",
      client_secret = "fake-client-secret"
    ),
    error_msg_1,
    fixed = TRUE
  )
})


test_that("client_id must be provided alone or with tenant_id and client_secret", {
  expect_error(
    AzureFileSystem$create(
      account_name = "fake-account-name",
      tenant_id = "fake-tenant-id",
      client_id = "fake-client-id"
    ),
    error_msg_2,
    fixed = TRUE
  )
})

test_that("cannot specify both account_key and sas_token", {
  expect_error(
    AzureFileSystem$create(account_name='fake-account-name', account_key='fakeaccount',
                sas_token='fakesastoken'),
    "Cannot specify both `account_key` and `sas_token`",
    fixed = TRUE
  )
})

test_that("at a minimum account_name must be passed", {
  expect_error(
    AzureFileSystem$create(),
    "Missing `account_name`",
    fixed = TRUE
  )
})
