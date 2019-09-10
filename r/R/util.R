oxford_paste <- function(x, conjunction = "and") {
  if (is.character(x)) {
    x <- paste0('"', x, '"')
  }
  if (length(x) < 2) {
    return(x)
  }
  x[length(x)] <- paste(conjunction, x[length(x)])
  if (length(x) > 2) {
    return(paste(x, collapse = ", "))
  } else {
    return(paste(x, collapse = " "))
  }
}

assert_is <- function(object, class) {
  msg <- paste(substitute(object), "must be a", oxford_paste(class, "or"))
  assert_that(inherits(object, class), msg = msg)
}
