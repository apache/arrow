#' @export
arrow_array <- function(input) {
  structure(list(ptr = array(input)), class = "arrow_array")
}

#' @export
print.arrow_array <- function(x, ...) {
  cat(array_string(x$ptr))
}

#' @export
to_r <- function(x) {
  as_r_int(x$ptr)
}
