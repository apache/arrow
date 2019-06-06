
test_that <- function(what, code) {
  testthat::test_that(what, {
    skip_if(!arrow_available(), "arrow C++ library not available")
    code
  })
}
