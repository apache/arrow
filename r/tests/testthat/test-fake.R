test_that("test that should pass", {
  expect_true(arrow_with_s3())
})

test_that("this should fail", {
  expect_true(FALSE)
})
