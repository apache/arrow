#' @include R6.R

`arrow::Array` <- R6Class("arrow::Array",
  inherit = `arrow::Object`,
  public = list(
    IsNUll = function(i) Array_IsNull(private$xp, i),
    IsValid = function(i) Array_IsValid(private$xp, i),
    length = function() Array_length(private$xp),
    offset = function() Array_offset(private$xp),
    null_count = function() Array_null_count(private$xp),
    type = function() Array_type(private$xp),
    type_id = function() Array_type_id(private$xp)
  )
)

#
# array_of <- function(type){
#
# }
