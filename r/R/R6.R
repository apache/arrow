#' @importFrom R6 R6Class
#' @importFrom glue glue
#' @importFrom purrr map map_int map2
#' @importFrom rlang dots_n
#' @importFrom assertthat assert_that

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
    ToString = function() {
      DataType_ToString(private$xp)
    },
    print = function(...) {
      cat( glue( "DataType({s})", s = DataType_ToString(private$xp) ))
    },
    name = function() {
      DataType_name(private$xp)
    },
    Equals = function(other) {
      inherits(other, "arrow::DataType") && DataType_Equals(private$xp, other$pointer())
    },
    num_children = function() {
      DataType_num_children(private$xp)
    },
    children = function() {
      map(DataType_children_pointer(private$xp), field)
    }
  )
)

#----- metadata

`arrow::FixedWidthType` <- R6Class("arrow::FixedWidthType",
  inherit = `arrow::DataType`,
  public = list(
    bit_width = function() FixedWidthType_bit_width(private$xp)
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

`arrow::DecimalType` <- R6Class("arrow:::DecimalType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    precision = function() DecimalType_precision(private$xp),
    scale = function() DecimalType_scale(private$xp)
  )
)

datatype_arrow_class <- function(name, super = `arrow::FixedWidthType`){
  init <- get(glue("{name}_initialize"), envir = NAMESPACE)

  R6::R6Class(
    glue("arrow::{name}"),
    inherit = super ,
    public = list(
      initialize = function(...) private$xp <- init(...)
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

`arrow::DateType` <- R6Class("arrow::DateType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    unit = function() DateType_unit(private$xp)
  )
)
delayedAssign("arrow::Date32", datatype_arrow_class("Date32", super = `arrow::DateType`))
delayedAssign("arrow::Date64", datatype_arrow_class("Date64", super = `arrow::DateType`))

`arrow::TimeType` <- R6Class("arrow::TimeType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    unit = function() TimeType_unit(private$xp)
  )
)
delayedAssign("arrow::Time32", datatype_arrow_class("Time32", super = `arrow::TimeType`))
delayedAssign("arrow::Time64", datatype_arrow_class("Time64", super = `arrow::TimeType`))

`arrow::Null` <- R6Class("arrow::Null",
  inherit = `arrow::DataType`,
  public = list(
    initialize = function() {
      private$xp <- Null_initialize()
    }
  )
)

`arrow::Timestamp` <- R6::R6Class(
  glue("arrow::Timestamp"),
  inherit = `arrow::FixedWidthType` ,
  public = list(
    initialize = function(unit, timezone){
      private$xp <- if (missing(timezone)) {
        Timestamp_initialize1(unit)
      } else {
        Timestamp_initialize2(unit, timezone)
      }
    },

    timezone = function()  TimestampType_timezone(private$xp),
    unit = function() TimestampType_unit(private$xp)
  )
)


delayedAssign("arrow::Decimal128Type" , datatype_arrow_class("Decimal128Type", super = `arrow::DecimalType` ))

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
time32 <- function(unit) {
  `arrow::Time32`$new(unit)
}

#' @export
time64 <- function(unit) `arrow::Time64`$new(unit)

#' @export
null <- function() `arrow::Null`$new()

#' @export
timestamp <- function(...) `arrow::Timestamp`$new(...)

#' @export
decimal <- function(precision, scale) `arrow::Decimal128Type`$new(precision, scale)

#------- field

`arrow::Field` <- R6Class("arrow::Field",
  inherit = `arrow::Object`,
  public = list(
    initialize = function(xp){
      private$xp <- xp
    },
    ToString = function() {
      Field_ToString(private$xp)
    },
    name = function() {
      Field_name(private$xp)
    },
    nullable = function() {
      Field_nullable(private$xp)
    },
    print = function(...) {
      cat( glue( "Field<{s}>", s = Field_ToString(private$xp)))
    },
    Equals = function(other) {
      inherits(other, "arrow::Field") && Field_Equals(private$xp, other$pointer())
    }
  )
)

#' @export
`==.arrow::Field` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

#' @export
`!=.arrow::Field` <- function(lhs, rhs){
  ! lhs == rhs
}

field <- function(xp) `arrow::Field`$new(xp)

#------- struct and schema

.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(.list, nms, ~ field_pointer(.y, .x$pointer()))
}

`arrow::StructType` <- R6Class("arrow::StructType",
  inherit = `arrow::DataType`,
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
