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

run_these <- tryCatch(
  expr = {
    if (arrow_with_s3() &&
      identical(tolower(Sys.getenv("ARROW_R_DEV")), "true") &&
      !identical(Sys.getenv("AWS_ACCESS_KEY_ID"), "") &&
      !identical(Sys.getenv("AWS_SECRET_ACCESS_KEY"), "")) {
      # See if we have access to the test bucket
      bucket <- s3_bucket("ursa-labs-r-test")
      bucket$GetFileInfo("")
      TRUE
    } else {
      FALSE
    }
  },
  error = function(e) FALSE
)

bucket_uri <- function(..., bucket = "s3://ursa-labs-r-test/%s?region=us-west-2") {
  segments <- paste(..., sep = "/")
  sprintf(bucket, segments)
}

if (run_these) {
  now <- as.numeric(Sys.time())
  on.exit(bucket$DeleteDir(now))

  test_that("read/write Feather on S3", {
    write_feather(example_data, bucket_uri(now, "test.feather"))
    expect_identical(read_feather(bucket_uri(now, "test.feather")), example_data)
  })

  test_that("read/write Parquet on S3", {
    skip_if_not_available("parquet")
    write_parquet(example_data, bucket_uri(now, "test.parquet"))
    expect_identical(read_parquet(bucket_uri(now, "test.parquet")), example_data)
  })

  test_that("RandomAccessFile$ReadMetadata() works for S3FileSystem", {
    file <- bucket$OpenInputFile(paste0(now, "/", "test.parquet"))
    metadata <- file$ReadMetadata()
    expect_true(is.list(metadata))
    expect_true("ETag" %in% names(metadata))
  })
}
