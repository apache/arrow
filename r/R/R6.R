NAMESPACE <- environment()

#' @importFrom R6 R6Class
#' @importFrom glue glue
#' @importFrom purrr map map_int
#' @importFrom rlang dots_n
datatype_arrow_class <- function(name){

  initialize_methods <- map(grep( glue("^{name}_initialize"), ls(env = NAMESPACE), value = TRUE), get, env = NAMESPACE, mode = "function", inherits = FALSE)
  nargs <- map_int(initialize_methods, ~length(formals(.)))
  initialize <- function(...){
    fun <- initialize_methods[[ which(nargs == dots_n()) ]]
    fun(...)
  }

  R6::R6Class(
    glue("arrow::{name}"),
    public = list(
      initialize = initialize
    ),
    private = list(
      xp = NULL
    )
  )
}

delayedAssign("Int8" , datatype_arrow_class("Int8"))
delayedAssign("Int16", datatype_arrow_class("Int16"))
delayedAssign("Int32", datatype_arrow_class("Int32"))
delayedAssign("Int64", datatype_arrow_class("Int64"))

delayedAssign("UInt8" , datatype_arrow_class("UInt8"))
delayedAssign("UInt16", datatype_arrow_class("UInt16"))
delayedAssign("UInt32", datatype_arrow_class("UInt32"))
delayedAssign("UInt64", datatype_arrow_class("UInt64"))

delayedAssign("Float16", datatype_arrow_class("Float16"))
delayedAssign("Float32", datatype_arrow_class("Float32"))
delayedAssign("Float64", datatype_arrow_class("Float64"))

delayedAssign("Boolean", datatype_arrow_class("Boolean"))
delayedAssign("Utf8", datatype_arrow_class("Utf8"))

delayedAssign("Date32", datatype_arrow_class("Date32"))
delayedAssign("Date64", datatype_arrow_class("Date64"))

delayedAssign("Null", datatype_arrow_class("Null"))

#' @export
int8 <- function() Int8$new()

#' @export
int16 <- function() Int16$new()

#' @export
int32 <- function() Int32$new()

#' @export
int64 <- function() Int64$new()

#' @export
uint8 <- function() UInt8$new()

#' @export
uint16 <- function() UInt16$new()

#' @export
uint32 <- function() UInt32$new()

#' @export
uint64 <- function() UInt64$new()

#' @export
float16 <- function() Float16$new()

#' @export
float32 <- function() Float32$new()

#' @export
float64 <- function() Float64$new()

#' @export
boolean <- function() Boolean$new()

#' @export
utf8 <- function() Utf8$new()

#' @export
date32 <- function() Date32$new()

#' @export
date64 <- function() Date64$new()

#' @export
null <- function() Null$new()
