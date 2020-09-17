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

if (arrow_with_s3() && process_is_running("minio server")) {
  # Get minio config, with expected defaults
  minio_key <- Sys.getenv("MINIO_ACCESS_KEY", "minioadmin")
  minio_secret <- Sys.getenv("MINIO_SECRET_KEY", "minioadmin")
  minio_port <- Sys.getenv("MINIO_PORT", "9000")

  # Helper function for minio URIs
  minio_uri <- function(...) {
    template <- "s3://%s:%s@%s?scheme=http&endpoint_override=localhost%s%s"
    sprintf(template, minio_key, minio_secret, minio_path(...), "%3A", minio_port)
  }
  minio_path <- function(...) paste(now, ..., sep = "/")

  test_that("minio setup", {
    # Create a "bucket" on minio for this test run, which we'll delete when done.
    fs <- S3FileSystem$create(
      access_key = minio_key,
      secret_key = minio_secret,
      scheme = "http",
      endpoint_override = paste0("localhost:", minio_port)
    )
    expect_is(fs, "S3FileSystem")
    now <- as.character(as.numeric(Sys.time()))
    # If minio isn't running, this will hang for a few seconds and fail with a
    # curl timeout, causing `run_these` to be set to FALSE and skipping the tests
    fs$CreateDir(now)
    # Clean up when we're all done
    on.exit(fs$DeleteDir(now))
  })

  test_that("read/write Feather on minio", {
    write_feather(example_data, minio_uri("test.feather"))
    expect_identical(read_feather(minio_uri("test.feather")), example_data)
  })

  test_that("read/write Feather by filesystem, not URI", {
    write_feather(example_data, minio_path("test2.feather"), filesystem = fs)
    expect_identical(
      read_feather(minio_path("test2.feather"), filesystem = fs),
      example_data
    )
  })

  test_that("read/write stream", {
    write_ipc_stream(example_data, minio_path("test3.ipc"), filesystem = fs)
    expect_identical(
      read_ipc_stream(minio_path("test3.ipc"), filesystem = fs),
      example_data
    )
  })

  test_that("read/write Parquet on minio", {
    write_parquet(example_data, minio_uri("test.parquet"))
    expect_identical(read_parquet(minio_uri("test.parquet")), example_data)
  })

  # Dataset test setup, cf. test-dataset.R
  library(dplyr)
  first_date <- lubridate::ymd_hms("2015-04-29 03:12:39")
  df1 <- tibble(
    int = 1:10,
    dbl = as.numeric(1:10),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[1:10],
    fct = factor(LETTERS[1:10]),
    ts = first_date + lubridate::days(1:10)
  )

  second_date <- lubridate::ymd_hms("2017-03-09 07:01:02")
  df2 <- tibble(
    int = 101:110,
    dbl = as.numeric(51:60),
    lgl = rep(c(TRUE, FALSE, NA, TRUE, FALSE), 2),
    chr = letters[10:1],
    fct = factor(LETTERS[10:1]),
    ts = second_date + lubridate::days(10:1)
  )

  # This is also to set up the dataset tests
  test_that("write_parquet with filesystem arg", {
    fs$CreateDir(minio_path("hive_dir", "group=1", "other=xxx"))
    fs$CreateDir(minio_path("hive_dir", "group=2", "other=yyy"))
    expect_length(fs$GetFileInfo(FileSelector$create(minio_path("hive_dir"))), 2)
    write_parquet(df1, minio_path("hive_dir", "group=1", "other=xxx", "file1.parquet"), filesystem = fs)
    write_parquet(df2, minio_path("hive_dir", "group=2", "other=yyy", "file2.parquet"), filesystem = fs)
    expect_identical(
      read_parquet(minio_path("hive_dir", "group=1", "other=xxx", "file1.parquet"), filesystem = fs),
      df1
    )
  })

  test_that("open_dataset with fs", {
    ds <- open_dataset(minio_path("hive_dir"), filesystem = fs)
    expect_identical(
      ds %>% select(dbl, lgl) %>% collect(),
      rbind(df1[, c("dbl", "lgl")], df2[, c("dbl", "lgl")])
    )
  })

  test_that("write_dataset with fs", {
    ds <- open_dataset(minio_path("hive_dir"), filesystem = fs)
    write_dataset(ds, minio_path("new_dataset_dir"), filesystem = fs)
    expect_length(fs$GetFileInfo(FileSelector$create(minio_path("new_dataset_dir"))), 2)
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
} else {
  # Kinda hacky, let's put a skipped test here, just so we note that the tests
  # didn't run
  test_that("S3FileSystem tests with Minio", {
    skip("Minio is not running")
  })
}
