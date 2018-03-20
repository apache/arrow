#' @importFrom purrr map2
#' @importFrom assertthat assert_that
.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(.list, nms, ~field(.y, .x))
}

#' @export
struct <- function(..., .list = list(...)){
  struct_( .fields(.list) )
}

#' @export
schema <- function(..., .list = list(...)){
  schema_( .fields(.list) )
}
