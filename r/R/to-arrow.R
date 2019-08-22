to_arrow <- function(x, table, ...) {
  UseMethod("to_arrow")
}

`to_arrow.arrow::RecordBatch` <- function(x, table, ...) x
`to_arrow.arrow::Table` <- function(x, table, ...) x
`to_arrow.arrow::Array` <- function(x, table, ...) x

#' Create an arrow::Table from a data frame
#' ... may include `schema` when making a table or `type` when making an array
to_arrow.data.frame <- function(x, table = is.null(list(...)$type), ...) {
  if (table) {
    # Default: make an arrow Table
    shared_ptr(`arrow::Table`, Table__from_dots(x, schema = list(...)$schema))
  } else {
    # Make this a struct array
    to_arrow.default(x, type = list(...)$type)
  }
}

to_arrow.default <- function(x, table, ...) {
  `arrow::Array`$dispatch(Array__from_vector(x, s_type = list(...)$type))
}

#' Create an arrow::Table from diverse inputs
#'
#' @param ... arrays, chunked arrays, or R vectors
#' @param schema a schema. The default (`NULL`) infers the schema from the `...`
#'
#' @return an arrow::Table
#'
#' @export
table_from_dots <- function(..., schema = NULL){
  dots <- list2(...)
  # making sure there are always names
  if (is.null(names(dots))) {
    names(dots) <- rep_len("", length(dots))
  }
  stopifnot(length(dots) > 0)
  shared_ptr(`arrow::Table`, Table__from_dots(dots, schema))
}
