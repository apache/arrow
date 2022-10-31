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

source("nixlibs.R", local = TRUE)

test_that("identify_binary() based on LIBARROW_BINARY", {
  expect_null(identify_binary("FALSE"))
  expect_identical(identify_binary("ubuntu-18.04"), "ubuntu-18.04")
  expect_null(identify_binary("", info = list(id = "debian")))
})

test_that("select_binary() based on system", {
  expect_output(
    expect_null(select_binary("darwin", "x86_64")), # Not built today
    "Building on darwin x86_64"
  )
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
      "ubuntu-18.04"
    ),
    "Found libcurl and openssl >= 1.0.2"
  )
  expect_output(
    expect_identical(
      determine_binary_from_stderr(compile_test_program("#error Using OpenSSL version 3")),
      "ubuntu-22.04"
    ),
    "Found libcurl and openssl >= 3.0.0"
  )
  expect_output(
    expect_null(
      determine_binary_from_stderr(compile_test_program("#error OpenSSL version too old"))
    ),
    "openssl found but version >= 1.0.2 is required for some features"
  )
})

test_that("select_binary() with test program", {
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", "int a;"),
      "ubuntu-18.04"
    ),
    "Found libcurl and openssl >= 1.0.2"
  )
  expect_output(
    expect_identical(
      select_binary("linux", "x86_64", "#error Using OpenSSL version 3"),
      "ubuntu-22.04"
    ),
    "Found libcurl and openssl >= 3.0.0"
  )
})

test_that("check_allowlist", {
  tf <- tempfile()
  cat("tu$\n^cent\n", file = tf)
  expect_true(check_allowlist("ubuntu", tf))
  expect_true(check_allowlist("centos", tf))
  expect_false(check_allowlist("redhat", tf)) # remote allowlist doesn't have this
  expect_true(check_allowlist("redhat", tempfile())) # remote allowlist doesn't exist, so we fall back to the default list, which contains redhat
  expect_false(check_allowlist("debian", tempfile()))
})
