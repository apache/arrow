test_that("plain R metadata is preserved including names and empty elements", {
  metadata <- list(
    attributes = list(label = "test", count = 1L, value = 2.5, flag = TRUE, z = 1i),
    columns = setNames(list(NULL, list(attributes = list(levels = letters)), list()), c("x", "x", ""))
  )
  expect_identical(safe_r_metadata(metadata), metadata)
  expect_identical(safe_r_metadata(metadata, on_save = TRUE), metadata)
})

test_that("metadata filtering retains warning order and unsafe behavior", {
  metadata <- list(first = new.env(), second = list(as.raw(1), function() 1), third = quote(x))
  expect_warning(filtered <- safe_r_metadata(metadata), 'environment.*symbol.*raw.*closure')
  expect_identical(filtered, list(second = list()))
  expect_identical(safe_r_metadata(metadata, on_save = TRUE), filtered)
  withr::local_options(arrow.unsafe_metadata = TRUE)
  expect_warning(unsafe <- safe_r_metadata(metadata), 'environment.*symbol.*raw.*closure')
  expect_identical(unsafe, metadata)
})

test_that("metadata filtering retains nested error context", {
  metadata <- list(col = matrix(list(1, new.env()), nrow = 1))
  expect_error(safe_r_metadata(metadata), class = "purrr_error_indexed")
  expect_error(safe_r_metadata(metadata), "col")
})

test_that("metadata fast path falls back for deep trees and custom attributes", {
  metadata <- list(version = package_version("1.2.3"), a = structure(1, label = "a"))
  expect_identical(safe_r_metadata(metadata), metadata)
  deep <- list(value = 1)
  for (i in seq_len(100)) deep <- list(deep)
  expect_identical(safe_r_metadata(deep), deep)
})

test_that("promise metadata is rejected without evaluation", {
  # A serialized list containing an unevaluated promise to stop().
  payload <- paste0("A\n3\n263427\n197888\n5\nUTF-8\n19\n1\n1029\n253\n252\n6\n1\n262153\n4\nstop\n2\n16\n1\n262153\n21\nPROMISE_WAS_EVALUATED\n254\n")
  expect_warning(value <- unserialize_r_metadata(payload), 'promise')
  expect_identical(value, list())
  # Remove the enclosing list from the serialized stream.
  top <- sub("UTF-8\n19\n1\n", "UTF-8\n", payload, fixed = TRUE)
  expect_error(unserialize_r_metadata(top), "promise")
})

test_that("metadata preflight does not dispatch list class methods", {
  names.evil <- function(x) stop("DISPATCHED_NAMES")
  old <- get0("names.evil", envir = globalenv(), inherits = FALSE)
  assign("names.evil", names.evil, envir = globalenv())
  withr::defer({
    if (is.null(old)) rm("names.evil", envir = globalenv()) else assign("names.evil", old, envir = globalenv())
  })
  metadata <- structure(list(1), class = "evil")
  expect_identical(safe_r_metadata(metadata), metadata)
})

test_that("plain metadata still evaluates the on_save argument", {
  expect_error(safe_r_metadata(list(1), on_save = NA), "missing value")
  expect_error(safe_r_metadata(list(1), on_save = logical()), "length zero")
  touched <- FALSE
  expect_identical(safe_r_metadata(list(1), on_save = { touched <- TRUE; FALSE }), list(1))
  expect_true(touched)
})
