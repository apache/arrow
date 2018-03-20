#' @export
timestamp <- function(unit, timezone){
  if( missing(timezone)){
    timestamp1(unit)
  } else {
    timestamp2(unit, timezone)
  }
}

#' @export
`print.arrow::DataType` <- function(x, ...){
  cat( "<arrow::DataType>\n")
  invisible(x)
}
