#' @include R6.R

`arrow::StructType` <- R6Class("arrow::StructType",
  inherit = `arrow::NestedType`
)

#' @rdname DataType
#' @export
struct <- function(...){
  `arrow::StructType`$new(struct_(.fields(list(...))))
}
