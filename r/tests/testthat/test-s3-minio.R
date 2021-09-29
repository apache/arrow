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
    expect_r6_class(fs, "S3FileSystem")
    now <- as.character(as.numeric(Sys.time()))
    # If minio isn't running, this will hang for a few seconds and fail with a
    # curl timeout, causing `run_these` to be set to FALSE and skipping the tests
    fs$CreateDir(now)
  })
  # Clean up when we're all done
  on.exit(fs$DeleteDir(now))

  test_that("read/write Feather on minio", {
    write_feather(example_data, minio_uri("test.feather"))
    expect_identical(read_feather(minio_uri("test.feather")), example_data)
  })

  test_that("read/write Feather by filesystem, not URI", {
    write_feather(example_data, fs$path(minio_path("test2.feather")))
    expect_identical(
      read_feather(fs$path(minio_path("test2.feather"))),
      example_data
    )
  })

  test_that("read/write stream", {
    write_ipc_stream(example_data, fs$path(minio_path("test3.ipc")))
    expect_identical(
      read_ipc_stream(fs$path(minio_path("test3.ipc"))),
      example_data
    )
  })

  test_that("read/write Parquet on minio", {
    skip_if_not_available("parquet")
    write_parquet(example_data, fs$path(minio_uri("test.parquet")))
    expect_identical(read_parquet(minio_uri("test.parquet")), example_data)
  })

  if (arrow_with_dataset()) {
    library(dplyr)

    make_temp_dir <- function() {
      path <- tempfile()
      dir.create(path)
      normalizePath(path, winslash = "/")
    }

    test_that("open_dataset with an S3 file (not directory) URI", {
      skip_if_not_available("parquet")
      expect_identical(
        open_dataset(minio_uri("test.parquet")) %>% collect() %>% arrange(int),
        example_data %>% arrange(int)
      )
    })

    test_that("open_dataset with vector of S3 file URIs", {
      expect_identical(
        open_dataset(
          c(minio_uri("test.feather"), minio_uri("test2.feather")),
          format = "feather"
        ) %>%
          arrange(int) %>%
          collect(),
        rbind(example_data, example_data) %>% arrange(int)
      )
    })

    test_that("open_dataset errors on URIs for different file systems", {
      td <- make_temp_dir()
      expect_error(
        open_dataset(
          c(
            minio_uri("test.feather"),
            paste0("file://", file.path(td, "fake.feather"))
          ),
          format = "feather"
        ),
        "Vectors of URIs for different file systems are not supported"
      )
    })

    # Dataset test setup, cf. test-dataset.R
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
      skip_if_not_available("parquet")
      fs$CreateDir(minio_path("hive_dir", "group=1", "other=xxx"))
      fs$CreateDir(minio_path("hive_dir", "group=2", "other=yyy"))
      expect_length(fs$ls(minio_path("hive_dir")), 2)
      write_parquet(df1, fs$path(minio_path("hive_dir", "group=1", "other=xxx", "file1.parquet")))
      write_parquet(df2, fs$path(minio_path("hive_dir", "group=2", "other=yyy", "file2.parquet")))
      expect_identical(
        read_parquet(fs$path(minio_path("hive_dir", "group=1", "other=xxx", "file1.parquet"))),
        df1
      )
    })

    test_that("open_dataset with fs", {
      ds <- open_dataset(fs$path(minio_path("hive_dir")))
      expect_identical(
        ds %>% select(int, dbl, lgl) %>% collect() %>% arrange(int),
        rbind(df1[, c("int", "dbl", "lgl")], df2[, c("int", "dbl", "lgl")]) %>% arrange(int)
      )
    })

    test_that("write_dataset with fs", {
      ds <- open_dataset(fs$path(minio_path("hive_dir")))
      write_dataset(ds, fs$path(minio_path("new_dataset_dir")))
      expect_length(fs$ls(minio_path("new_dataset_dir")), 1)
    })

    test_that("Let's test copy_files too", {
      td <- make_temp_dir()
      copy_files(minio_uri("hive_dir"), td)
      expect_length(dir(td), 2)
      ds <- open_dataset(td)
      expect_identical(
        ds %>% select(int, dbl, lgl) %>% collect() %>% arrange(int),
        rbind(df1[, c("int", "dbl", "lgl")], df2[, c("int", "dbl", "lgl")]) %>% arrange(int)
      )

      # Let's copy the other way and use a SubTreeFileSystem rather than URI
      copy_files(td, fs$path(minio_path("hive_dir2")))
      ds2 <- open_dataset(fs$path(minio_path("hive_dir2")))
      expect_identical(
        ds2 %>% select(int, dbl, lgl) %>% collect() %>% arrange(int),
        rbind(df1[, c("int", "dbl", "lgl")], df2[, c("int", "dbl", "lgl")]) %>% arrange(int)
      )
    })
  }

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
