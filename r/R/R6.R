#' @include RcppExports.R
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

"arrow::Int8"    <- R6Class("arrow::Int8",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Int8_initialize()
  )
)

"arrow::Int16"    <- R6Class("arrow::Int16",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Int16_initialize()
  )
)

"arrow::Int32"    <- R6Class("arrow::Int32",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Int32_initialize()
  )
)

"arrow::Int64"    <- R6Class("arrow::Int64",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Int64_initialize()
  )
)


"arrow::UInt8"    <- R6Class("arrow::UInt8",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- UInt8_initialize()
  )
)

"arrow::UInt16"    <- R6Class("arrow::UInt16",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- UInt16_initialize()
  )
)

"arrow::UInt32"    <- R6Class("arrow::UInt32",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- UInt32_initialize()
  )
)

"arrow::UInt64"    <- R6Class("arrow::UInt64",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- UInt64_initialize()
  )
)

"arrow::Float16"    <- R6Class("arrow::Float16",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Float16_initialize()
  )
)
"arrow::Float32"    <- R6Class("arrow::Float32",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Float32_initialize()
  )
)
"arrow::Float64"    <- R6Class("arrow::Float64",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Float64_initialize()
  )
)

"arrow::Boolean"    <- R6Class("arrow::Boolean",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Boolean_initialize()
  )
)

"arrow::Utf8"    <- R6Class("arrow::Utf8",
  inherit = `arrow::FixedWidthType`,
  public = list(
    initialize = function() private$xp <- Utf8_initialize()
  )
)

`arrow::DateType` <- R6Class("arrow::DateType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    unit = function() DateType_unit(private$xp)
  )
)

"arrow::Date32"    <- R6Class("arrow::Date32",
  inherit = `arrow::DateType`,
  public = list(
    initialize = function() private$xp <- Date32_initialize()
  )
)
"arrow::Date64"    <- R6Class("arrow::Date64",
  inherit = `arrow::DateType`,
  public = list(
    initialize = function() private$xp <- Date64_initialize()
  )
)

`arrow::TimeType` <- R6Class("arrow::TimeType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    unit = function() TimeType_unit(private$xp)
  )
)
"arrow::Time32"    <- R6Class("arrow::Time32",
  inherit = `arrow::TimeType`,
  public = list(
    initialize = function() private$xp <- Time32_initialize()
  )
)
"arrow::Time64"    <- R6Class("arrow::Time64",
  inherit = `arrow::TimeType`,
  public = list(
    initialize = function() private$xp <- Time64_initialize()
  )
)

`arrow::Null` <- R6Class("arrow::Null",
  inherit = `arrow::DataType`,
  public = list(
    initialize = function() {
      private$xp <- Null_initialize()
    }
  )
)

`arrow::Timestamp` <- R6Class(
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

`arrow::DecimalType` <- R6Class("arrow:::DecimalType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    precision = function() DecimalType_precision(private$xp),
    scale = function() DecimalType_scale(private$xp)
  )
)

"arrow::Decimal128Type"    <- R6Class("arrow::Decimal128Type",
  inherit = `arrow::DecimalType`,
  public = list(
    initialize = function(precision, scale) private$xp <- Decimal128Type_initialize(precision, scale)
  )
)

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
