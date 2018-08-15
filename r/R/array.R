#' @include R6.R

`arrow::ArrayData` <- R6Class("arrow::ArrayData",
  inherit = `arrow::Object`,

  public = list(
    initialize = function(type, length, null_count = -1, offset = 0) {
      private$xp <- ArrayData_initialize(type$pointer(), length, null_count, offset)
    }
  ),
  active = list(
    type = function() ArrayData_get_type(private$xp),
    length = function() ArrayData_get_length(private$xp),
    null_count = function() ArrayData_get_null_count(private$xp),
    offset = function() ArrayData_get_offset(private$xp)
  )
)

#' @export
array_data <- function(...){
  `arrow::ArrayData`$new(...)
}

`arrow::Array` <- R6Class("arrow::Array",
  inherit = `arrow::Object`,
  public = list(
    initialize = function(data) {
      private$xp <- Array_initialize(data$pointer())
    },
    IsNUll = function(i) Array_IsNull(private$xp, i),
    IsValid = function(i) Array_IsValid(private$xp, i),
    length = function() Array_length(private$xp),
    offset = function() Array_offset(private$xp),
    null_count = function() Array_null_count(private$xp),
    type = function() Array_type(private$xp),
    type_id = function() Array_type_id(private$xp)
  )
)

#' @export
MakeArray <- function(data){
  assert_that(inherits(data, "arrow::ArrayData"))
  `arrow::Array`$new(data)
}
