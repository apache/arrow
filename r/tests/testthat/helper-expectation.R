expect_vector <- function(x, y, ...) {
  expect_equal(as.vector(x), y, ...)
}

expect_data_frame <- function(x, y, ...) {
  expect_equal(as.data.frame(x), y, ...)
}
