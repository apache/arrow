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

#' Run standard suite of integration tests for a filesystem
#'
#' @param name Name of filesystem to be printed in test name
#' @param fs A `FileSystem` instance to test with
#' @param path_formatter A function that takes a sequence of path segments and
#' returns a absolute path.
#' @param uri_formatter A function that takes a sequence of path segments and
#' returns a URI containing the filesystem scheme (e.g. 's3://', 'gs://'), the
#' absolute path, and any necessary connection options as URL query parameters.
test_filesystem <- function(name, fs, path_formatter, uri_formatter) {
  # NOTE: it's important that we label these tests with name of filesystem so
  # that we can differentiate the different calls to these test in the output.
  test_that(sprintf("read/write Feather on %s using URIs", name), {
    write_feather(example_data, uri_formatter("test.feather"))
    expect_identical(read_feather(uri_formatter("test.feather")), example_data)
  })

  test_that(sprintf("read/write Feather on %s using Filesystem", name), {
    write_feather(example_data, fs$path(path_formatter("test2.feather")))
    expect_identical(
      read_feather(fs$path(path_formatter("test2.feather"))),
      example_data
    )
  })

  if (!("package:dplyr" %in% search())) {
    abort("library(dplyr) required for test_filesystem()")
  }

  test_that(sprintf("read/write compressed csv on %s using FileSystem", name), {
    skip_if_not_available("gzip")
    dat <- tibble(x = seq(1, 10, by = 0.2))
    write_csv_arrow(dat, fs$path(path_formatter("test.csv.gz")))
    expect_identical(
      read_csv_arrow(fs$path(path_formatter("test.csv.gz"))),
      dat
    )
  })

  test_that(sprintf("read/write csv on %s using FileSystem", name), {
    skip_if_not_available("gzip")
    dat <- tibble(x = seq(1, 10, by = 0.2))
    write_csv_arrow(dat, fs$path(path_formatter("test.csv")))
    expect_identical(
      read_csv_arrow(fs$path(path_formatter("test.csv"))),
      dat
    )
  })

  test_that(sprintf("read/write IPC stream on %s", name), {
    write_ipc_stream(example_data, fs$path(path_formatter("test3.ipc")))
    expect_identical(
      read_ipc_stream(fs$path(path_formatter("test3.ipc"))),
      example_data
    )
  })

  test_that(sprintf("read/write Parquet on %s", name), {
    skip_if_not_available("parquet")
    write_parquet(example_data, fs$path(path_formatter("test.parquet")))
    expect_identical(read_parquet(uri_formatter("test.parquet")), example_data)
  })

  if (arrow_with_dataset()) {
    make_temp_dir <- function() {
      path <- tempfile()
      dir.create(path)
      normalizePath(path, winslash = "/")
    }

    test_that(sprintf("open_dataset with an %s file (not directory) URI", name), {
      skip_if_not_available("parquet")
      expect_identical(
        open_dataset(uri_formatter("test.parquet")) %>% collect() %>% arrange(int),
        example_data %>% arrange(int)
      )
    })

    test_that(sprintf("open_dataset with vector of %s file URIs", name), {
      expect_identical(
        open_dataset(
          c(uri_formatter("test.feather"), uri_formatter("test2.feather")),
          format = "feather"
        ) %>%
          arrange(int) %>%
          collect(),
        rbind(example_data, example_data) %>% arrange(int)
      )
    })

    test_that(sprintf("open_dataset errors if passed URIs mixing %s and local fs", name), {
      td <- make_temp_dir()
      expect_error(
        open_dataset(
          c(
            uri_formatter("test.feather"),
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
    test_that(sprintf("write_parquet with %s filesystem arg", name), {
      skip_if_not_available("parquet")
      fs$CreateDir(path_formatter("hive_dir", "group=1", "other=xxx"))
      fs$CreateDir(path_formatter("hive_dir", "group=2", "other=yyy"))
      expect_length(fs$ls(path_formatter("hive_dir")), 2)
      write_parquet(df1, fs$path(path_formatter("hive_dir", "group=1", "other=xxx", "file1.parquet")))
      write_parquet(df2, fs$path(path_formatter("hive_dir", "group=2", "other=yyy", "file2.parquet")))
      expect_identical(
        read_parquet(fs$path(path_formatter("hive_dir", "group=1", "other=xxx", "file1.parquet"))),
        df1
      )
    })

    test_that(sprintf("open_dataset with %s", name), {
      ds <- open_dataset(fs$path(path_formatter("hive_dir")))
      expect_identical(
        ds %>% select(int, dbl, lgl) %>% collect() %>% arrange(int),
        rbind(df1[, c("int", "dbl", "lgl")], df2[, c("int", "dbl", "lgl")]) %>% arrange(int)
      )
    })

    test_that(sprintf("write_dataset with %s", name), {
      ds <- open_dataset(fs$path(path_formatter("hive_dir")))
      write_dataset(ds, fs$path(path_formatter("new_dataset_dir")))
      expect_length(fs$ls(path_formatter("new_dataset_dir")), 1)
    })

    test_that(sprintf("copy files with %s", name), {
      td <- make_temp_dir()
      copy_files(uri_formatter("hive_dir"), td)
      expect_length(dir(td), 2)
      ds <- open_dataset(td)
      expect_identical(
        ds %>% select(int, dbl, lgl) %>% collect() %>% arrange(int),
        rbind(df1[, c("int", "dbl", "lgl")], df2[, c("int", "dbl", "lgl")]) %>% arrange(int)
      )

      # Let's copy the other way and use a SubTreeFileSystem rather than URI
      copy_files(td, fs$path(path_formatter("hive_dir2")))
      ds2 <- open_dataset(fs$path(path_formatter("hive_dir2")))
      expect_identical(
        ds2 %>% select(int, dbl, lgl) %>% collect() %>% arrange(int),
        rbind(df1[, c("int", "dbl", "lgl")], df2[, c("int", "dbl", "lgl")]) %>% arrange(int)
      )
    })
  } # if(arrow_with_dataset())
}
