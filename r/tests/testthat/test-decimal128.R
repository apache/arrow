context("test-decimal128")

test_that("cast decimal128 <-> int types", {
  x <- vec_cast(1L, new_decimal128())
  expect_is(x, "arrow_decimal128")
  y <- vec_cast(x, integer())
  expect_is(y, "integer")
  expect_equal(y, 1L)

  y <- vec_cast(x, new_int64())
  expect_is(y, "integer64")
  expect_equal(y, bit64::as.integer64(1))

  x <- vec_cast(bit64::as.integer64(2), new_decimal128())
  expect_is(x, "arrow_decimal128")
})

test_that("coercion up to decimal128", {
  expect_equal(vec_type2(1L, new_decimal128()), new_decimal128())
  expect_equal(vec_type2(bit64::as.integer64(2), new_decimal128()), new_decimal128())
})
