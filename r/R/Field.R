#' @include R6.R

`arrow::Field` <- R6Class("arrow::Field",
  inherit = `arrow::Object`,
  public = list(
    ToString = function() {
      Field__ToString(self)
    },
    name = function() {
      Field__name(self)
    },
    nullable = function() {
      Field__nullable(self)
    },
    Equals = function(other) {
      inherits(other, "arrow::Field") && Field__Equals(self, other)
    }
  )
)

#' @export
`==.arrow::Field` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

field <- function(name, type) {
  `arrow::Field`$new(Field__initialize(name, type))
}

.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(nms, .list, field)
}
