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
# TODO: Add local azurite install to setup script
# skip_if_not(nzchar(Sys.which("azurite")), message = "azurite is not installed.")

# TODO: Start azurite from the test code instead of relying on it to be already running externally.

# Use default azurite credentials,
# see https://learn.microsoft.com/en-us/azure/storage/common/storage-connect-azurite?tabs=blob-storage
azurite_account_name <- "devstoreaccount1"
# Note that this is a well-known default credential for local development on Azurite.
azurite_account_key <- "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
azurite_blob_host <- "127.0.0.1"
azurite_blob_port <- "10000"
azurite_blob_storage_authority <- sprintf("%s:%s",azurite_blob_host, azurite_blob_port)
azurite_blob_storage_scheme <- "http"

# Helper functions for Azure URIs and paths
azure_uri <- function(...) {
  template <- "az://%s:%s@%s?scheme=http&blob_endpoint=localhost%s%s"
  # URL encode the account key because it contains reserved characters
  encoded_key <- curl::curl_escape(azurite_account_key)
  sprintf(template, azurite_account_name, encoded_key, azure_path(...), "%3A", azurite_blob_port)
}

azure_path <- azure_path <- function(...) {
  # 'now' is the container name (following the convention in the s3 tests).
  paste(now, ..., sep = "/")
}

fs <- AzureFileSystem$create(
  account_name="devstoreaccount1",
  account_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
  blob_storage_authority="127.0.0.1:10000",
  blob_storage_scheme="http"
)

now <- as.character(as.numeric(Sys.time()))
fs$CreateDir(now)
# Clean up when we're all done
withr::defer(fs$DeleteDir(now))

# (1) Run default filesystem tests on azure filesystem
test_filesystem("azure", fs, azure_path, azure_uri)
