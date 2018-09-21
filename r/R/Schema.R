#' @include R6.R

`arrow::Schema` <- R6Class("arrow::Schema",
  inherit = `arrow::Object`,
  public = list(
    ToString = function() Schema__ToString(self)
  )
)

#' @rdname DataType
#' @export
schema <- function(...){
  `arrow::Schema`$new(schema_(.fields(list(...))))
}
