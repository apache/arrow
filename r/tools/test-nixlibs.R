# Run this with testthat::test_dir(".") inside of this directory

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
  expect_null(select_binary(arch = "aarch64")) # Not built today
  gcc48 <- c(
    "g++-4.8 (Ubuntu 4.8.4-2ubuntu1~14.04.3) 4.8.4",
    "Copyright (C) 2013 Free Software Foundation, Inc.",
    "This is free software; see the source for copying conditions.  There is NO",
    "warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE."
  )
  expect_identical(
    select_binary(arch = "x86_64", compiler_version = gcc48),
    "centos-7"
  )

  # print(find_openssl_version())
  # print(find_cmake())
  print(cmake_find_package("CURL", NULL, list(CMAKE = find_cmake())))
  print(cmake_find_package("OpenSSL", NULL, list(CMAKE = find_cmake())))

  # TODO: trace instead to inject
  # real_has_libcurl <- has_libcurl
  # has_libcurl <- function() FALSE
  # expect_null(select_binary("x86_64", "clang"))

  # real_find_openssl_version <- find_openssl_version
  # find_openssl_version <- function() packageVersion("1.0.2")
  # has_libcurl <- function() TRUE
  # expect_identical(select_binary("x86_64", "clang"), "ubuntu-18.04")

  # find_openssl_version <- function() packageVersion("3.0.0")
  # expect_identical(select_binary("x86_64", "clang"), "ubuntu-22.04")
})
