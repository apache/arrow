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

skip_if_not_available("s3")
skip_if_not(nzchar(Sys.which("minio")), message = "minio is not installed.")

library(dplyr)

minio_dir <- Sys.getenv("MINIO_DATA_DIR", tempfile())
minio_key <- "minioadmin"
minio_secret <- "minioadmin"
minio_port <- Sys.getenv("MINIO_PORT", "9000")

# Start minio server
dir.create(minio_dir, showWarnings = FALSE)
pid_minio <- sys::exec_background("minio", c("server", minio_dir, "--address", sprintf(":%s", minio_port)),
  std_out = FALSE
)
withr::defer(tools::pskill(pid_minio))

# Helper function for minio URIs
minio_uri <- function(...) {
  template <- "s3://%s:%s@%s?scheme=http&endpoint_override=localhost%s%s"
  sprintf(template, minio_key, minio_secret, minio_path(...), "%3A", minio_port)
}
minio_path <- function(...) paste(now, ..., sep = "/")

# Create a "bucket" on minio for this test run, which we'll delete when done.
fs <- S3FileSystem$create(
  access_key = minio_key,
  secret_key = minio_secret,
  scheme = "http",
  endpoint_override = paste0("localhost:", minio_port),
  allow_bucket_creation = TRUE,
  allow_bucket_deletion = TRUE
)
limited_fs <- S3FileSystem$create(
  access_key = minio_key,
  secret_key = minio_secret,
  scheme = "http",
  endpoint_override = paste0("localhost:", minio_port),
  allow_bucket_creation = FALSE,
  allow_bucket_deletion = FALSE
)
now <- as.character(as.numeric(Sys.time()))
fs$CreateDir(now)
# Clean up when we're all done
withr::defer(fs$DeleteDir(now))

test_filesystem("s3", fs, minio_path, minio_uri)

test_that("CreateDir fails on bucket if allow_bucket_creation=False", {
  now_tmp <- paste0(now, "-test-fail-delete")
  fs$CreateDir(now_tmp)

  expect_error(limited_fs$CreateDir("should-fail"))
  expect_error(limited_fs$DeleteDir(now_tmp))
})

test_that("S3FileSystem input validation", {
  expect_error(
    S3FileSystem$create(access_key = "foo"),
    "Key authentication requires both access_key and secret_key"
  )
  expect_error(
    S3FileSystem$create(secret_key = "foo"),
    "Key authentication requires both access_key and secret_key"
  )
  expect_error(
    S3FileSystem$create(session_token = "foo"),
    paste0(
      "In order to initialize a session with temporary credentials, ",
      "both secret_key and access_key must be provided ",
      "in addition to session_token."
    )
  )
  expect_error(
    S3FileSystem$create(access_key = "foo", secret_key = "asdf", anonymous = TRUE),
    'Cannot specify "access_key" and "secret_key" when anonymous = TRUE'
  )
  expect_error(
    S3FileSystem$create(access_key = "foo", secret_key = "asdf", role_arn = "qwer"),
    "Cannot provide both key authentication and role_arn"
  )
  expect_error(
    S3FileSystem$create(access_key = "foo", secret_key = "asdf", external_id = "qwer"),
    'Cannot specify "external_id" without providing a role_arn string'
  )
  expect_error(
    S3FileSystem$create(external_id = "foo"),
    'Cannot specify "external_id" without providing a role_arn string'
  )
})

test_that("Confirm s3_bucket works with endpoint_override", {
  bucket <- s3_bucket(
    now,
    access_key = minio_key,
    secret_key = minio_secret,
    scheme = "http",
    endpoint_override = paste0("localhost:", minio_port)
  )

  expect_r6_class(bucket, "SubTreeFileSystem")

  os <- bucket$OpenOutputStream("bucket-test.csv")
  write_csv_arrow(example_data, os)
  os$close()
  expect_true("bucket-test.csv" %in% bucket$ls())
  bucket$DeleteFile("bucket-test.csv")
})

# Cleanup
withr::deferred_run()
