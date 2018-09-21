#' @include R6.R

`arrow::ListType` <- R6Class("arrow::ListType",
  inherit = `arrow::NestedType`
)

#' @rdname DataType
#' @export
list_of <- function(type) `arrow::ListType`$new(list__(type))
