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

context("S3 tests using local minio")

run_these <- tryCatch({
  if (arrow_with_s3() && nzchar(Sys.which("minio"))) {
    # Get minio config, with expected defaults
    minio_key <- Sys.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret <- Sys.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_port <- Sys.getenv("MINIO_PORT", "9000")

    # Helper function for minio URIs
    minio_uri <- function(...) {
      template <- "s3://%s:%s@%s?scheme=http&endpoint_override=localhost%s%s"
      segments <- paste(..., sep = "/")
      sprintf(template, minio_key, minio_secret, segments, "%3A", minio_port)
    }

    # Create a "bucket" on minio for this test run, which we'll delete when done.
    fs <- S3FileSystem$create(
      access_key = minio_key,
      secret_key = minio_secret,
      scheme = "http",
      endpoint_override = paste0("localhost:", minio_port)
    )
    now <- as.character(as.numeric(Sys.time()))
    # If minio isn't running, this will hang for a few seconds and fail with a
    # curl timeout, causing `run_these` to be set to FALSE and skipping the tests
    fs$CreateDir(now)
    on.exit(fs$DeleteDir(now))
    TRUE
  } else {
    FALSE
  }
}, error = function(e) FALSE)


if (run_these) {
  test_that("read/write Feather on minio", {
    write_feather(example_data, minio_uri(now, "test.feather"))
    expect_identical(read_feather(minio_uri(now, "test.feather")), example_data)
  })

  test_that("read/write Parquet on minio", {
    write_parquet(example_data, minio_uri(now, "test.parquet"))
    expect_identical(read_parquet(minio_uri(now, "test.parquet")), example_data)
  })
}
