context("General checks")

if (identical(Sys.getenv("TEST_R_WITH_ARROW"), "TRUE")) {
  testthat::test_that("Arrow C++ is available", {
    expect_true(arrow_available())
  })
}
