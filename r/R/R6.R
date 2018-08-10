NAMESPACE <- environment()

`arrow::Object` <- R6Class("arrow::Object",
  public = list(
    pointer = function() private$xp,
    list = function() list_(self)
  ),
  private = list(xp = NULL)
)

`arrow::DataType` <- R6Class("arrow::DataType",
  inherit = `arrow::Object`,
  public = list(
    print = function(...) {
      cat( glue( "DataType({s})", s = DataType_ToString(private$xp)))
    },
    name = function() {
      DataType_name(private$xp)
    },
    Equals = function(rhs) {
      inherits(rhs, "arrow::DataType") && DataType_Equals(private$xp, rhs$pointer())
    }
  )
)

#' @export
`==.arrow::DataType` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

#' @export
`!=.arrow::DataType` <- function(lhs, rhs){
  ! lhs == rhs
}

#----- metadata

`arrow::FixedWidthType` <- R6Class("arrow::FixedWidthType",
  inherit = `arrow::DataType`,
  public = list(
    bit_width = function() FixedWidthType_bit_width(private$xp)
  )
)

#' @importFrom R6 R6Class
#' @importFrom glue glue
#' @importFrom purrr map map_int
#' @importFrom rlang dots_n
datatype_arrow_class <- function(name){

  initialize_methods <- map(grep( glue("^{name}_initialize"), ls(env = NAMESPACE), value = TRUE), get, env = NAMESPACE, mode = "function", inherits = FALSE)
  nargs <- map_int(initialize_methods, ~length(formals(.)))

  R6::R6Class(
    glue("arrow::{name}"),
    inherit = `arrow::FixedWidthType`,
    public = list(
      initialize = function(...){
        fun <- initialize_methods[[ which(nargs == dots_n(...)) ]]
        private$xp <- fun(...)
      }
    )
  )
}

delayedAssign("arrow::Int8" , datatype_arrow_class("Int8"))
delayedAssign("arrow::Int16", datatype_arrow_class("Int16"))
delayedAssign("arrow::Int32", datatype_arrow_class("Int32"))
delayedAssign("arrow::Int64", datatype_arrow_class("Int64"))

delayedAssign("arrow::UInt8" , datatype_arrow_class("UInt8"))
delayedAssign("arrow::UInt16", datatype_arrow_class("UInt16"))
delayedAssign("arrow::UInt32", datatype_arrow_class("UInt32"))
delayedAssign("arrow::UInt64", datatype_arrow_class("UInt64"))

delayedAssign("arrow::Float16", datatype_arrow_class("Float16"))
delayedAssign("arrow::Float32", datatype_arrow_class("Float32"))
delayedAssign("arrow::Float64", datatype_arrow_class("Float64"))

delayedAssign("arrow::Boolean", datatype_arrow_class("Boolean"))
delayedAssign("arrow::Utf8", datatype_arrow_class("Utf8"))

delayedAssign("arrow::Date32", datatype_arrow_class("Date32"))
delayedAssign("arrow::Date64", datatype_arrow_class("Date64"))

`arrow::Null` <- R6Class("arrow::Null",
  inherit = `arrow::DataType`,
  public = list(
    initialize = function() {
      private$xp <- Null_initialize()
    }
  )
)

delayedAssign("arrow::Timestamp", datatype_arrow_class("Timestamp"))

#' @export
int8 <- function() `arrow::Int8`$new()

#' @export
int16 <- function() `arrow::Int16`$new()

#' @export
int32 <- function() `arrow::Int32`$new()

#' @export
int64 <- function() `arrow::Int64`$new()

#' @export
uint8 <- function() `arrow::UInt8`$new()

#' @export
uint16 <- function() `arrow::UInt16`$new()

#' @export
uint32 <- function() `arrow::UInt32`$new()

#' @export
uint64 <- function() `arrow::UInt64`$new()

#' @export
float16 <- function() `arrow::Float16`$new()

#' @export
float32 <- function() `arrow::Float32`$new()

#' @export
float64 <- function() `arrow::Float64`$new()

#' @export
boolean <- function() `arrow::Boolean`$new()

#' @export
utf8 <- function() `arrow::Utf8`$new()

#' @export
date32 <- function() `arrow::Date32`$new()

#' @export
date64 <- function() `arrow::Date64`$new()

#' @export
null <- function() `arrow::Null`$new()

#' @export
timestamp <- function(...) `arrow::Timestamp`$new(...)

#------- struct and schema

#' @importFrom purrr map2
#' @importFrom assertthat assert_that
.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(.list, nms, ~field(.y, .x$pointer()))
}

`arrow::StructType` <- R6Class("arrow::StructType",
  inherit = `arrow::Object`,
  public = list(
    print = function(...) {
      cat( glue( "StructType({s})", s = DataType_ToString(private$xp)))
    },
    initialize = function(...){
      private$xp = struct_( .fields(list(...)) )
    }
  )
)

`arrow::Schema` <- R6Class("arrow::Schema",
  inherit = `arrow::Object`,
  public = list(
    print = function(...) {
      cat( glue( "{s}", s = Schema_ToString(private$xp)))
    },
    initialize = function(...){
      private$xp = schema_( .fields(list(...)) )
    }
  )
)

#' @export
struct <- function(...) `arrow::StructType`$new(...)

#' @export
schema <- function(...) `arrow::Schema`$new(...)

#--------- list

`arrow::List` <- R6Class("arrow::List",
  inherit = `arrow::Object`,
  public = list(
    print = function(...) {
      cat( glue( "ListType({s})", s = ListType_ToString(private$xp)))
    },
    initialize = function(x){
      private$xp = list__(x$pointer())
    }
  )
)

#' @export
list_ <- function(x) `arrow::List`$new(x)
