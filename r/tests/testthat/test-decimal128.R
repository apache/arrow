context("test-decimal128")

test_that("cast to decimal128", {
  x <- vec_cast(1L, new_decimal128())
  expect_is(x, "arrow_decimal128")

  x <- vec_cast(bit64::as.integer64(2), new_decimal128())
  expect_is(x, "arrow_decimal128")
})

test_that("coercion up to decimal128", {
  expect_equal(vec_type2(1L, new_decimal128()), new_decimal128())
  expect_equal(vec_type2(bit64::as.integer64(2), new_decimal128()), new_decimal128())
})
