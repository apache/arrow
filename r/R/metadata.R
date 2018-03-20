#' @export
timestamp <- function(unit, timezone){
  if( missing(timezone)){
    timestamp1(unit)
  } else {
    timestamp2(unit, timezone)
  }
}

#' @importFrom glue glue
#' @export
`print.arrow::DataType` <- function(x, ...){
  cat( glue( "DataType({s})", s = DataType_ToString(x)))
  invisible(x)
}

#' @export
`print.arrow::StructType` <- function(x, ...){
  cat( glue( "StructType({s})", s = DataType_ToString(x)))
  invisible(x)
}
