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


# Usage: run testthat::test_dir(".") inside of this directory

# Flag so that we just load the functions and don't evaluate them like we do
# when called from configure.R
TESTING <- TRUE
# The functions use `on_macos` from the env they were sourced in, so we need tool
# explicitly set it in that environment.
nixlibs_env <- environment()
source("nixlibs.R", local = nixlibs_env)

test_that("identify_binary() based on LIBARROW_BINARY", {
  expect_null(identify_binary("FALSE"))
  expect_identical(identify_binary("linux-openssl-1.0"), "linux-openssl-1.0")
  expect_null(identify_binary("", info = list(id = "debian")))
})

test_that("select_binary() based on system", {
  expect_output(
    expect_null(select_binary("linux", arch = "aarch64")), # Not built today
    "Building on linux aarch64"
  )
})

test_that("compile_test_program()", {
  expect_null(attr(compile_test_program("int a;"), "status"))
  fail <- compile_test_program("#include <wrong/NOTAHEADER.h>")
  expect_true(attr(fail, "status") > 0)
  expect_true(header_not_found("wrong/NOTAHEADER", fail))
})

test_that("determine_binary_from_stderr", {
  expect_output(
    expect_identical(
      determine_binary_from_stderr(compile_test_program("int a;")),
      "openssl-1.1"
    ),
    "Found libcurl and OpenSSL >= 1.1"
  )

  nixlibs_env$on_macos <- FALSE
  expect_output(
    expect_identical(
      determine_binary_from_stderr(compile_test_program("#error Using OpenSSL version 1.0")),
      "openssl-1.0"
    ),
    "Found libcurl and OpenSSL < 1.1"
  )
  nixlibs_env$on_macos <- TRUE
  expect_output(
    expect_null(
      determine_binary_from_stderr(compile_test_program("#error Using OpenSSL version 1.0"))
    ),
    "OpenSSL 1.0 is not supported on macOS"
  )
  expect_output(
    expect_identical(
      determine_binary_from_stderr(compile_test_program("#error Using OpenSSL version 3")),
      "openssl-3.0"
    ),
    "Found libcurl and OpenSSL >= 3.0.0"
  )
  expect_output(
    expect_null(
      determine_binary_from_stderr(compile_test_program("#error OpenSSL version too old"))
    ),
    "OpenSSL found but version >= 1.0.2 is required for some features"
  )
})

test_that("select_binary() with test program", {
  nixlibs_env$on_macos <- FALSE
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", "int a;"),
      "linux-openssl-1.1"
    ),
    "Found libcurl and OpenSSL >= 1.1"
  )
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", "#error Using OpenSSL version 1.0"),
      "linux-openssl-1.0"
    ),
    "Found libcurl and OpenSSL < 1.1"
  )
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", "#error Using OpenSSL version 3"),
      "linux-openssl-3.0"
    ),
    "Found libcurl and OpenSSL >= 3.0.0"
  )
  nixlibs_env$on_macos <- TRUE
  expect_output(
    expect_identical(
      select_binary("darwin", "x86_64", "int a;"),
      "darwin-x86_64-openssl-1.1"
    ),
    "Found libcurl and OpenSSL >= 1.1"
  )
  expect_output(
    expect_identical(
      select_binary("darwin", "x86_64", "#error Using OpenSSL version 3"),
      "darwin-x86_64-openssl-3.0"
    ),
    "Found libcurl and OpenSSL >= 3.0.0"
  )
  expect_output(
    expect_identical(
      select_binary("darwin", "arm64", "int a;"),
      "darwin-arm64-openssl-1.1"
    ),
    "Found libcurl and OpenSSL >= 1.1"
  )
  expect_output(
    expect_identical(
      select_binary("darwin", "arm64", "#error Using OpenSSL version 3"),
      "darwin-arm64-openssl-3.0"
    ),
    "Found libcurl and OpenSSL >= 3.0.0"
  )
  expect_output(
    expect_null(
      select_binary("darwin", "x86_64", "#error Using OpenSSL version 1.0")
    ),
    "OpenSSL 1.0 is not supported on macOS"
  )
})

test_that("check_allowlist", {
  tf <- tempfile()
  cat("tu$\n^cent\n^dar\n", file = tf)
  expect_true(check_allowlist("ubuntu", tf))
  expect_true(check_allowlist("centos", tf))
  expect_true(check_allowlist("darwin", tf))
  expect_false(check_allowlist("redhat", tf)) # remote allowlist doesn't have this
  expect_true(check_allowlist("redhat", tempfile())) # remote allowlist doesn't exist, so we fall back to the default list, which contains redhat
  expect_false(check_allowlist("debian", tempfile()))
})

test_that("find_latest_nightly()", {
  tf <- tempfile()
  tf_uri <- paste0("file://", tf)
  on.exit(unlink(tf))

  writeLines(
    c(
      "Version: 13.0.0.100000333",
      "Version: 13.0.0.100000334",
      "Version: 13.0.0.100000335",
      "Version: 14.0.0.100000001"
    ),
    tf
  )

  expect_output(
    expect_identical(
      find_latest_nightly(package_version("13.0.1.9000"), list_uri = tf_uri),
      package_version("13.0.0.100000335")
    ),
    "Latest available nightly"
  )

  expect_output(
    expect_identical(
      find_latest_nightly(package_version("14.0.0.9000"), list_uri = tf_uri),
      package_version("14.0.0.100000001")
    ),
    "Latest available nightly"
  )

  expect_output(
    expect_identical(
      find_latest_nightly(package_version("15.0.0.9000"), list_uri = tf_uri),
      package_version("15.0.0.9000")
    ),
    "No nightly binaries were found for version"
  )

  # Check empty input
  writeLines(character(), tf)
  expect_output(
    expect_identical(
      find_latest_nightly(package_version("15.0.0.9000"), list_uri = tf_uri),
      package_version("15.0.0.9000")
    ),
    "No nightly binaries were found for version"
  )

  # Check input that will throw an error
  expect_output(
    expect_identical(
      suppressWarnings(
        find_latest_nightly(
          package_version("15.0.0.9000"),
          list_uri = "this is not a URI",
          hush = TRUE
        )
      ),
      package_version("15.0.0.9000")
    ),
    "Failed to find latest nightly"
  )
})
