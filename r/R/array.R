#' @include R6.R

`arrow::ArrayData` <- R6Class("arrow::ArrayData",
  inherit = `arrow::Object`,

  public = list(
    initialize = function(type, length, null_count = -1, offset = 0) {
      self$set_pointer(ArrayData_initialize(type, length, null_count, offset))
    }
  ),
  active = list(
    type = function() ArrayData_get_type(self),
    length = function() ArrayData_get_length(self),
    null_count = function() ArrayData_get_null_count(self),
    offset = function() ArrayData_get_offset(self)
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
      self$set_pointer(Array_initialize(data))
    },
    IsNull = function(i) Array_IsNull(self, i),
    IsValid = function(i) Array_IsValid(self, i),
    length = function() Array_length(self),
    offset = function() Array_offset(self),
    null_count = function() Array_null_count(self),
    type = function() Array_type(self),
    type_id = function() Array_type_id(self),
    Equals = function(other) Array_Equals(self, other),
    ApproxEquals = function(othet) Array_ApproxEquals(self, other),
    data = function() Array_data(self)
  )
)

#' @export
`length.arrow::Array` <- function(x) x$length()

#' @export
`==.arrow::Array` <- function(x, y) x$Equals(y)

#' @export
`!=.arrow::Array` <- function(x, y) !x$Equals(y)

#' @export
MakeArray <- function(data){
  assert_that(inherits(data, "arrow::ArrayData"))
  `arrow::Array`$new(data)
}
