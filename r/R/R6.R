# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#' @include enums.R
#' @importFrom R6 R6Class
#' @importFrom glue glue
#' @importFrom purrr map map_int map2
#' @importFrom rlang dots_n
#' @importFrom assertthat assert_that

`arrow::Object` <- R6Class("arrow::Object",
  public = list(
    initialize = function(xp) self$set_pointer(xp),

    pointer = function() self$`.:xp:.`,
    `.:xp:.` = NULL,
    set_pointer = function(xp){
      self$`.:xp:.` <- xp
    }
  )
)

`arrow::DataType` <- R6Class("arrow::DataType",
  inherit = `arrow::Object`,
  public = list(

    ToString = function() {
      DataType_ToString(self)
    },
    print = function(...) {
      cat( glue( "DataType({s})", s = DataType_ToString(self) ))
    },
    name = function() {
      DataType_name(self)
    },
    Equals = function(other) {
      inherits(other, "arrow::DataType") && DataType_Equals(self, other)
    },
    num_children = function() {
      DataType_num_children(self)
    },
    children = function() {
      map(DataType_children_pointer(self), ~`arrow::Field`$new(xp=.x))
    },
    id = function(){
      DataType_id(self)
    },
    ..dispatch = function(){
      switch(names(Type)[self$id()+1],
        "NA" = null(),
        BOOL = boolean(),
        UINT8 = uint8(),
        INT8 = int8(),
        UINT16 = uint16(),
        INT16 = int16(),
        UINT32 = uint32(),
        INT32 = int32(),
        UINT64 = uint64(),
        INT64 = int64(),
        HALF_FLOAT = float16(),
        FLOAT = float32(),
        DOUBLE = float64(),
        STRING = utf8(),
        BINARY = stop("Type BINARY not implemented yet"),
        DATE32 = date32(),
        DATE64 = date64(),
        TIMESTAMP = `arrow::Timestamp`$new(xp = self$pointer()),
        INTERVAL = stop("Type INTERVAL not implemented yet"),
        DECIMAL = `arrow::Decimal128Type`$new(xp = self$pointer()),
        LIST = `arrow::ListType`$new(xp = self$pointer()),
        STRUCT = `arrow::StructType`$new(xp = self$pointer()),
        UNION = stop("Type UNION not implemented yet"),
        DICTIONARY = stop("Type DICTIONARY not implemented yet"),
        MAP = stop("Type MAP not implemented yet")
      )
    }
  )
)

`arrow::DataType`$dispatch <- function(xp){
  `arrow::DataType`$new(xp)$..dispatch()
}

#----- metadata

`arrow::FixedWidthType` <- R6Class("arrow::FixedWidthType",
  inherit = `arrow::DataType`,
  public = list(
    bit_width = function() FixedWidthType_bit_width(self)
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
)

"arrow::Int16"    <- R6Class("arrow::Int16",
  inherit = `arrow::FixedWidthType`
)

"arrow::Int32"    <- R6Class("arrow::Int32",
  inherit = `arrow::FixedWidthType`
)

"arrow::Int64"    <- R6Class("arrow::Int64",
  inherit = `arrow::FixedWidthType`
)


"arrow::UInt8"    <- R6Class("arrow::UInt8",
  inherit = `arrow::FixedWidthType`
)

"arrow::UInt16"    <- R6Class("arrow::UInt16",
  inherit = `arrow::FixedWidthType`
)

"arrow::UInt32"    <- R6Class("arrow::UInt32",
  inherit = `arrow::FixedWidthType`
)

"arrow::UInt64"    <- R6Class("arrow::UInt64",
  inherit = `arrow::FixedWidthType`
)

"arrow::Float16"    <- R6Class("arrow::Float16",
  inherit = `arrow::FixedWidthType`
)
"arrow::Float32"    <- R6Class("arrow::Float32",
  inherit = `arrow::FixedWidthType`
)
"arrow::Float64"    <- R6Class("arrow::Float64",
  inherit = `arrow::FixedWidthType`
)

"arrow::Boolean"    <- R6Class("arrow::Boolean",
  inherit = `arrow::FixedWidthType`
)

"arrow::Utf8"    <- R6Class("arrow::Utf8",
  inherit = `arrow::DataType`
)

`arrow::DateType` <- R6Class("arrow::DateType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    unit = function() DateType_unit(self)
  )
)

"arrow::Date32"    <- R6Class("arrow::Date32",
  inherit = `arrow::DateType`
)
"arrow::Date64"    <- R6Class("arrow::Date64",
  inherit = `arrow::DateType`
)

"arrow::TimeType" <- R6Class("arrow::TimeType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    unit = function() TimeType_unit(self)
  )
)
"arrow::Time32"    <- R6Class("arrow::Time32",
  inherit = `arrow::TimeType`
)
"arrow::Time64"    <- R6Class("arrow::Time64",
  inherit = `arrow::TimeType`
)

"arrow::Null" <- R6Class("arrow::Null",
  inherit = `arrow::DataType`
)

`arrow::Timestamp` <- R6Class(
  "arrow::Timestamp",
  inherit = `arrow::FixedWidthType` ,
  public = list(
    timezone = function()  TimestampType_timezone(self),
    unit = function() TimestampType_unit(self)
  )
)

`arrow::DecimalType` <- R6Class("arrow:::DecimalType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    precision = function() DecimalType_precision(self),
    scale = function() DecimalType_scale(self)
  )
)

"arrow::Decimal128Type"    <- R6Class("arrow::Decimal128Type",
  inherit = `arrow::DecimalType`
)

#' Apache Arrow data types
#'
#' Apache Arrow data types
#'
#' @param unit time unit
#' @param precision precision
#' @param scale scale
#' @param type type
#' @param ... ...
#'
#' @rdname DataType
#' @export
int8 <- function() `arrow::Int8`$new(Int8_initialize())

#' @rdname DataType
#' @export
int16 <- function() `arrow::Int16`$new(Int16_initialize())

#' @rdname DataType
#' @export
int32 <- function() `arrow::Int32`$new(Int32_initialize())

#' @rdname DataType
#' @export
int64 <- function() `arrow::Int64`$new(Int64_initialize())

#' @rdname DataType
#' @export
uint8 <- function() `arrow::UInt8`$new(UInt8_initialize())

#' @rdname DataType
#' @export
uint16 <- function() `arrow::UInt16`$new(UInt16_initialize())

#' @rdname DataType
#' @export
uint32 <- function() `arrow::UInt32`$new(UInt32_initialize())

#' @rdname DataType
#' @export
uint64 <- function() `arrow::UInt64`$new(UInt64_initialize())

#' @rdname DataType
#' @export
float16 <- function() `arrow::Float16`$new(Float16_initialize())

#' @rdname DataType
#' @export
float32 <- function() `arrow::Float32`$new(Float32_initialize())

#' @rdname DataType
#' @export
float64 <- function() `arrow::Float64`$new(Float64_initialize())

#' @rdname DataType
#' @export
boolean <- function() `arrow::Boolean`$new(Boolean_initialize())

#' @rdname DataType
#' @export
utf8 <- function() `arrow::Utf8`$new(Utf8_initialize())

#' @rdname DataType
#' @export
date32 <- function() `arrow::Date32`$new(Date32_initialize())

#' @rdname DataType
#' @export
date64 <- function() `arrow::Date64`$new(Date64_initialize())

#' @rdname DataType

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
  `arrow::Time32`$new(Time32_initialize(unit))
}

#' @rdname DataType
#' @export
time64 <- function(unit) `arrow::Time64`$new(Time64_initialize(unit))

#' @rdname DataType
#' @export
null <- function() `arrow::Null`$new(Null_initialize())

#' @rdname DataType
#' @export
timestamp <- function(unit, timezone) {
  if (missing(timezone)) {
    `arrow::Timestamp`$new(Timestamp_initialize1(unit))
  } else {
    `arrow::Timestamp`$new(Timestamp_initialize2(unit, timezone))
  }
}

#' @rdname DataType

#' @export
time64 <- function(unit) `arrow::Time64`$new(unit)

#' @export
null <- function() `arrow::Null`$new()

#' @export
timestamp <- function(...) `arrow::Timestamp`$new(...)

#' @export
decimal <- function(precision, scale) `arrow::Decimal128Type`$new(Decimal128Type_initialize(precision, scale))

#------- field

`arrow::Field` <- R6Class("arrow::Field",
  inherit = `arrow::Object`,
  public = list(
    ToString = function() {
      Field_ToString(self)
    },
    name = function() {
      Field_name(self)
    },
    nullable = function() {
      Field_nullable(self)
    },
    print = function(...) {
      cat( glue( "Field<{s}>", s = Field_ToString(self)))
    },
    Equals = function(other) {
      inherits(other, "arrow::Field") && Field_Equals(self, other)
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

field <- function(name, type) {
  `arrow::Field`$new(Field_initialize(name, type))
}

#------- struct and schema

.fields <- function(.list){
  assert_that( !is.null(nms <- names(.list)) )
  map2(nms, .list, field)
}

`arrow::NestedType` <- R6Class("arrow::NestedType", inherit = `arrow::DataType`)

`arrow::StructType` <- R6Class("arrow::StructType",
  inherit = `arrow::NestedType`,
  public = list(
    print = function(...) {
      cat( glue( "StructType({s})", s = DataType_ToString(self)))
    }
  )
)

`arrow::Schema` <- R6Class("arrow::Schema",
  inherit = `arrow::Object`,
  public = list(
    print = function(...) {
      cat( glue( "{s}", s = Schema_ToString(self)))
    }
  )
)

#' @rdname DataType
#' @export
struct <- function(...){
  `arrow::StructType`$new(struct_(.fields(list(...))))
}

#' @rdname DataType
#' @export
schema <- function(...){
  `arrow::Schema`$new(schema_(.fields(list(...))))
}

#--------- list

`arrow::ListType` <- R6Class("arrow::ListType",
  inherit = `arrow::NestedType`,
  public = list(
    print = function(...) {
      cat( glue( "ListType({s})", s = ListType_ToString(self)))
    }
  )
)

#' @rdname DataType
#' @export
list_of <- function(type) `arrow::ListType`$new(list__(type))
