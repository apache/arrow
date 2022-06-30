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
  expect_null(select_binary("darwin"))
  expect_null(select_binary("linux", arch = "aarch64")) # Not built today
  gcc48 <- c(
    "g++-4.8 (Ubuntu 4.8.4-2ubuntu1~14.04.3) 4.8.4",
    "Copyright (C) 2013 Free Software Foundation, Inc.",
    "This is free software; see the source for copying conditions.  There is NO",
    "warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE."
  )
  expect_identical(
    select_binary("linux", "x86_64", compiler_version = gcc48),
    "centos-7"
  )
})

test_that("compile_test_program()", {
  expect_null(attr(compile_test_program("int a;"), "status"))
  expect_true(any(grepl("<wrong/NOTAHEADER.h>", compile_test_program("#include <wrong/NOTAHEADER.h>"))))
})

test_that("determine_binary_from_stderr", {
  expect_identical(determine_binary_from_stderr(compile_test_program("int a;")), "ubuntu-18.04")
  expect_identical(determine_binary_from_stderr(compile_test_program("#error Using OpenSSL version 3")), "ubuntu-22.04")
})

test_that("select_binary() with test program", {
  expect_identical(
    select_binary("linux", "x86_64", "clang", "int a;"),
    "ubuntu-18.04"
  )
  expect_identical(
    select_binary("linux", "x86_64", "clang", "#error Using OpenSSL version 3"),
    "ubuntu-22.04"
  )
})
