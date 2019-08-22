to_arrow <- function(x) {
  UseMethod("to_arrow")
}

`to_arrow.arrow::RecordBatch` <- function(x) x
`to_arrow.arrow::Table` <- function(x) x

# splice the data frame as arguments of table()
# see ?rlang::list2()
`to_arrow.data.frame` <- function(x) table(!!!x)

#' Create an arrow::Table from a data frame
#'
#' @param ... arrays, chunked arrays, or R vectors
#' @param schema a schema. The default (`NULL`) infers the schema from the `...`
#'
#' @return an arrow::Table
#'
#' @export
table <- function(..., schema = NULL){
  dots <- list2(...)
  # making sure there are always names
  if (is.null(names(dots))) {
    names(dots) <- rep_len("", length(dots))
  }
  stopifnot(length(dots) > 0)
  shared_ptr(`arrow::Table`, Table__from_dots(dots, schema))
}

#' create an [arrow::Array][arrow__Array] from an R vector
#'
#' @param x R object
#' @param type Explicit [type][arrow__DataType], or NULL (the default) to infer from the data
#'
#' @export
array <- function(x, type = NULL){
  `arrow::Array`$dispatch(Array__from_vector(x, type))
}
