#' @export
`$.arrow-enum` <- function(x, y){
  structure( unclass(x)[[y]], class = class(x) )
}

#' @export
`print.arrow-enum` <- function(x, ...){
  NextMethod()
}
