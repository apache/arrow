#' @importFrom purrr map2
#' @importFrom assertthat assert_that
#' @export
struct <- function(..., .list = list(...)){
  assert_that( !is.null(nms <- names(.list)) )
  fields <- map2(.list, nms, ~field(.y, .x))
  struct_( fields )
}
