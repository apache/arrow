#' @include R6.R

Status <- R6Class("arrow::Status",
  inherit = `arrow::Object`,
  public = list(
    initialize = function(xp){
      self$set_pointer(xp)
    },
    ToString = function() Status_ToString(self),
    CodeAsString = function() Status_CodeAsString(self),
    code = function() Status_code(self),
    message = function() Status_message(self)
  )
)
