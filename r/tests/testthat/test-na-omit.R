data_no_na <- c(2:10)
data_na <- c(NA_real_, data_no_na)
scalar_na <- Scalar$create(NA)
scalar_one <- Scalar$create(1)
tbl <- Table$create(example_data)

test_that("na.omit on Array and ChunkedArray", {
  expect_vector_equal(na.omit(input), data_no_na)
  expect_vector_equivalent(na.omit(input), data_na)
})

test_that("na.exclude on Array and ChunkedArray", {
  expect_vector_equal(na.exclude(input), data_no_na)
  expect_vector_equivalent(na.exclude(input), data_na)
})

test_that("na.fail on Array and ChunkedArray", {
  expect_vector_equivalent(na.fail(input), data_no_na)
  expect_vector_error(na.fail(input), data_na)
})

test_that("na.pass on Array and ChunkedArray", {
  expect_vector_equivalent(na.pass(input), data_no_na)
  expect_vector_equal(na.pass(input), data_na)
})

test_that("na.fail on Scalar", {
  expect_error(na.fail(scalar_na), regexp = "missing values in object")
  expect_vector(na.fail(scalar_one), na.fail(1))
})

test_that("na.pass on Scalar", {
  expect_vector(na.pass(scalar_na), na.pass(NA))
  expect_vector(na.pass(scalar_one), na.pass(1))
})

test_that("na.omit on Table", {
  expect_data_frame(na.omit(tbl), na.omit(example_data))
})

test_that("na.exclude on Table", {
  expect_data_frame(na.exclude(tbl), na.exclude(example_data))
})

test_that("na.fail on Table", {
  expect_data_frame(na.fail(tbl), na.fail(example_data))
})

test_that("na.pass on Table", {
  expect_data_frame(na.pass(tbl), na.pass(example_data))
})
