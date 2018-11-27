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
    },
    print = function(...){
      cat(crayon::silver(glue::glue("{cl}", cl = class(self)[[1]])), "\n")
      if(!is.null(self$ToString)){
        cat(self$ToString(), "\n")
      }
      invisible(self)
    }
  )
)

shared_ptr <- function(class, xp) {
  if(!shared_ptr_is_null(xp)) class$new(xp)
}

unique_ptr <- function(class, xp) {
  if(!unique_ptr_is_null(xp)) class$new(xp)
}

#' @export
`!=.arrow::Object` <- function(lhs, rhs){
  !(lhs == rhs)
}

#' @title class arrow::DataType
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__DataType
#' @name arrow__DataType
`arrow::DataType` <- R6Class("arrow::DataType",
  inherit = `arrow::Object`,
  public = list(
    ToString = function() {
      DataType__ToString(self)
    },
    Equals = function(other) {
      assert_that(inherits(other, "arrow::DataType"))
      DataType__Equals(self, other)
    },
    num_children = function() {
      DataType__num_children(self)
    },
    children = function() {
      map(DataType__children_pointer(self), shared_ptr, class= `arrow::Field`)
    },

    ..dispatch = function(){
      switch(names(Type)[self$id + 1],
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
        TIMESTAMP = shared_ptr(`arrow::Timestamp`,self$pointer()),
        TIME32 = shared_ptr(`arrow::Time32`,self$pointer()),
        TIME64 = shared_ptr(`arrow::Time64`,self$pointer()),
        INTERVAL = stop("Type INTERVAL not implemented yet"),
        DECIMAL = shared_ptr(`arrow::Decimal128Type`, self$pointer()),
        LIST = shared_ptr(`arrow::ListType`, self$pointer()),
        STRUCT = shared_ptr(`arrow::StructType`, self$pointer()),
        UNION = stop("Type UNION not implemented yet"),
        DICTIONARY = shared_ptr(`arrow::DictionaryType`, self$pointer()),
        MAP = stop("Type MAP not implemented yet")
      )
    }
  ),

  active = list(
    id = function(){
      DataType__id(self)
    },
    name = function() {
      DataType__name(self)
    }
  )
)

`arrow::DataType`$dispatch <- function(xp){
  shared_ptr(`arrow::DataType`, xp)$..dispatch()
}

#----- metadata

#' @title class arrow::FixedWidthType
#'
#' @usage NULL
#' @format NULL
#' @docType class
#'
#' @section Methods:
#'
#' TODO
#'
#' @rdname arrow__FixedWidthType
#' @name arrow__FixedWidthType
`arrow::FixedWidthType` <- R6Class("arrow::FixedWidthType",
  inherit = `arrow::DataType`,
  active = list(
    bit_width = function() FixedWidthType__bit_width(self)
  )
)

#' @export
`==.arrow::DataType` <- function(lhs, rhs){
  lhs$Equals(rhs)
}

"arrow::Int8"    <- R6Class("arrow::Int8",
  inherit = `arrow::FixedWidthType`
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
    unit = function() DateType__unit(self)
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
    unit = function() TimeType__unit(self)
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
    timezone = function()  TimestampType__timezone(self),
    unit = function() TimestampType__unit(self)
  )
)

`arrow::DecimalType` <- R6Class("arrow:::DecimalType",
  inherit = `arrow::FixedWidthType`,
  public = list(
    precision = function() DecimalType__precision(self),
    scale = function() DecimalType__scale(self)
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
#' @param timezone time zone
#' @param precision precision
#' @param scale scale
#' @param type type
#' @param ... ...
#'
#' @rdname DataType
#' @export
int8 <- function() shared_ptr(`arrow::Int8`, Int8__initialize())

#' @rdname DataType
#' @export
int16 <- function() shared_ptr(`arrow::Int16`, Int16__initialize())

#' @rdname DataType
#' @export
int32 <- function() shared_ptr(`arrow::Int32`, Int32__initialize())

#' @rdname DataType
#' @export
int64 <- function() shared_ptr(`arrow::Int64`, Int64__initialize())

#' @rdname DataType
#' @export
uint8 <- function() shared_ptr(`arrow::UInt8`, UInt8__initialize())

#' @rdname DataType
#' @export
uint16 <- function() shared_ptr(`arrow::UInt16`, UInt16__initialize())

#' @rdname DataType
#' @export
uint32 <- function() shared_ptr(`arrow::UInt32`, UInt32__initialize())

#' @rdname DataType
#' @export
uint64 <- function() shared_ptr(`arrow::UInt64`, UInt64__initialize())

#' @rdname DataType
#' @export
float16 <- function() shared_ptr(`arrow::Float16`,  Float16__initialize())

#' @rdname DataType
#' @export
float32 <- function() shared_ptr(`arrow::Float32`, Float32__initialize())

#' @rdname DataType
#' @export
float64 <- function() shared_ptr(`arrow::Float64`, Float64__initialize())

#' @rdname DataType
#' @export
boolean <- function() shared_ptr(`arrow::Boolean`, Boolean__initialize())

#' @rdname DataType
#' @export
utf8 <- function() shared_ptr(`arrow::Utf8`, Utf8__initialize())

#' @rdname DataType
#' @export
date32 <- function() shared_ptr(`arrow::Date32`, Date32__initialize())

#' @rdname DataType
#' @export
date64 <- function() shared_ptr(`arrow::Date64`, Date64__initialize())

#' @rdname DataType
#' @export
time32 <- function(unit) shared_ptr(`arrow::Time32`, Time32__initialize(unit))

#' @rdname DataType
#' @export
time64 <- function(unit) shared_ptr(`arrow::Time64`, Time64__initialize(unit))

#' @rdname DataType
#' @export
null <- function() shared_ptr(`arrow::Null`, Null__initialize())

#' @rdname DataType
#' @export
timestamp <- function(unit, timezone) {
  if (missing(timezone)) {
    shared_ptr(`arrow::Timestamp`, Timestamp__initialize1(unit))
  } else {
    shared_ptr(`arrow::Timestamp`, Timestamp__initialize2(unit, timezone))
  }
}

#' @rdname DataType
#' @export
decimal <- function(precision, scale) shared_ptr(`arrow::Decimal128Type`, Decimal128Type__initialize(precision, scale))

`arrow::NestedType` <- R6Class("arrow::NestedType", inherit = `arrow::DataType`)
