
test_that <- function(what, ...) {
  testthat::test_that(what, {
    skip_if(!arrow_available(), "arrow C++ library not available")
    ...
  })
}
