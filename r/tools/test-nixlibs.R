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
# We capture.output for a cleaner testthat output.
nixlibs_env <- environment()
capture.output(source("nixlibs.R", local = nixlibs_env))

test_that("identify_binary() based on LIBARROW_BINARY", {
  expect_null(identify_binary("FALSE"))
  expect_identical(
    identify_binary("linux-x86_64"),
    "linux-x86_64"
  )
  expect_null(identify_binary("", info = list(id = "debian")))

  expect_output(
    expect_identical(
      identify_binary("linux-x86_64-openssl-3.0"),
      "linux-x86_64"
    ),
    "OpenSSL suffix deprecated in LIBARROW_BINARY, using 'linux-x86_64'",
    fixed = TRUE
  )

  expect_error(
    identify_binary("linux-x86_64-openssl-1.0"),
    "OpenSSL 1.x binaries are no longer provided. Use LIBARROW_BINARY='linux-x86_64'",
    fixed = TRUE
  )
})

test_that("select_binary() based on system", {
  expect_output(
    expect_null(select_binary("freebsd", arch = "x86_64")),
    "Building on freebsd x86_64"
  )
})

test_that("compile_test_program()", {
  expect_null(attr(compile_test_program("int a;"), "status"))
  fail <- compile_test_program("#include <wrong/NOTAHEADER.h>")
  expect_true(attr(fail, "status") > 0)
  expect_true(header_not_found("wrong/NOTAHEADER", fail))
})

test_that("has_binary_sysreqs", {
  expect_output(
    expect_true(
      has_binary_sysreqs(compile_test_program("int a;"))
    ),
    "Found libcurl and OpenSSL >= 3.0",
  )

  nixlibs_env$on_macos <- FALSE
  expect_output(
    expect_false(
      has_binary_sysreqs(compile_test_program(
        "#error OpenSSL version must be 3.0 or greater"
      ))
    ),
    "OpenSSL found but version >= 3.0 is required"
  )
})

test_that("select_binary() with test program", {
  nixlibs_env$on_macos <- FALSE
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", "int a;"),
      "linux-x86_64"
    ),
    "Found libcurl and OpenSSL >= 3.0"
  )
  expect_output(
    expect_null(
      select_binary(
        "linux",
        "x86_64",
        "#error OpenSSL version must be 3.0 or greater"
      )
    ),
    "OpenSSL found but version >= 3.0 is required"
  )
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", character(0)), # Successful compile = OpenSSL >= 3.0
      "linux-x86_64"
    ),
    "Found libcurl and OpenSSL >= 3.0"
  )
  nixlibs_env$on_macos <- TRUE
  expect_output(
    expect_identical(
      select_binary("darwin", "x86_64", "int a;"),
      "darwin-x86_64"
    ),
    "Found libcurl and OpenSSL >= 3.0"
  )
  expect_output(
    expect_identical(
      select_binary("darwin", "x86_64", character(0)), # Successful compile = OpenSSL >= 3.0
      "darwin-x86_64"
    ),
    "Found libcurl and OpenSSL >= 3.0"
  )
  expect_output(
    expect_identical(
      select_binary("darwin", "arm64", "int a;"),
      "darwin-arm64"
    ),
    "Found libcurl and OpenSSL >= 3.0"
  )
  expect_output(
    expect_identical(
      select_binary("darwin", "arm64", character(0)), # Successful compile = OpenSSL >= 3.0
      "darwin-arm64"
    ),
    "Found libcurl and OpenSSL >= 3.0"
  )
  expect_output(
    expect_null(
      select_binary(
        "darwin",
        "x86_64",
        "#error OpenSSL version must be 3.0 or greater"
      )
    ),
    "OpenSSL found but version >= 3.0 is required"
  )
})

test_that("check_allowlist", {
  # because we read from a file when we can't get the allow list from github,
  # we need to make sure we are in the same directory as we would be when building
  # (which is one level higher, so we can find `tools/nixlibs.R`)
  # TODO: it's possible that we don't want to run this whole file in that directory
  # like we do currently.
  withr::local_dir("..")

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
