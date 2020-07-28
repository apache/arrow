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

#' @include arrow-package.R
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
#' @rdname DataType
#' @name DataType
DataType <- R6Class("DataType",
  inherit = ArrowObject,
  public = list(
    ToString = function() {
      DataType__ToString(self)
    },
    Equals = function(other, ...) {
      inherits(other, "DataType") && DataType__Equals(self, other)
    },
    num_children = function() {
      DataType__num_children(self)
    },
    children = function() {
      map(DataType__children_pointer(self), shared_ptr, class = Field)
    },

    ..dispatch = function() {
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
        BINARY = binary(),
        FIXED_SIZE_BINARY = shared_ptr(FixedSizeBinary, self$pointer()),
        DATE32 = date32(),
        DATE64 = date64(),
        TIMESTAMP = shared_ptr(Timestamp, self$pointer()),
        TIME32 = shared_ptr(Time32, self$pointer()),
        TIME64 = shared_ptr(Time64, self$pointer()),
        INTERVAL = stop("Type INTERVAL not implemented yet"),
        DECIMAL = shared_ptr(Decimal128Type, self$pointer()),
        LIST = shared_ptr(ListType, self$pointer()),
        STRUCT = shared_ptr(StructType, self$pointer()),
        SPARSE_UNION = stop("Type SPARSE_UNION not implemented yet"),
        DENSE_UNION = stop("Type DENSE_UNION not implemented yet"),
        DICTIONARY = shared_ptr(DictionaryType, self$pointer()),
        MAP = stop("Type MAP not implemented yet"),
        EXTENSION = stop("Type EXTENSION not implemented yet"),
        FIXED_SIZE_LIST = shared_ptr(FixedSizeListType, self$pointer()),
        DURATION = stop("Type DURATION not implemented yet"),
        LARGE_STRING = large_utf8(),
        LARGE_BINARY = large_binary(),
        LARGE_LIST = shared_ptr(LargeListType, self$pointer())
      )
    }
  ),

  active = list(
    id = function() DataType__id(self),
    name = function() DataType__name(self)
  )
)

DataType$create <- function(xp) shared_ptr(DataType, xp)$..dispatch()

INTEGER_TYPES <- as.character(outer(c("uint", "int"), c(8, 16, 32, 64), paste0))
FLOAT_TYPES <- c("float16", "float32", "float64", "halffloat", "float", "double")

#' infer the arrow Array type from an R vector
#'
#' @param x an R vector
#'
#' @return an arrow logical type
#' @export
type <- function(x) UseMethod("type")

#' @export
type.default <- function(x) DataType$create(Array__infer_type(x))

#' @export
type.Array <- function(x) x$type

#' @export
type.ChunkedArray <- function(x) x$type


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
#' @rdname FixedWidthType
#' @name FixedWidthType
FixedWidthType <- R6Class("FixedWidthType",
  inherit = DataType,
  active = list(
    bit_width = function() FixedWidthType__bit_width(self)
  )
)

Int8 <- R6Class("Int8", inherit = FixedWidthType)
Int16 <- R6Class("Int16", inherit = FixedWidthType)
Int32 <- R6Class("Int32", inherit = FixedWidthType)
Int64 <- R6Class("Int64", inherit = FixedWidthType)
UInt8 <- R6Class("UInt8", inherit = FixedWidthType)
UInt16 <- R6Class("UInt16", inherit = FixedWidthType)
UInt32 <- R6Class("UInt32", inherit = FixedWidthType)
UInt64 <- R6Class("UInt64", inherit = FixedWidthType)
Float16 <- R6Class("Float16", inherit = FixedWidthType)
Float32 <- R6Class("Float32", inherit = FixedWidthType)
Float64 <- R6Class("Float64", inherit = FixedWidthType)
Boolean <- R6Class("Boolean", inherit = FixedWidthType)
Utf8 <- R6Class("Utf8", inherit = DataType)
LargeUtf8 <- R6Class("LargeUtf8", inherit = DataType)
Binary <- R6Class("Binary", inherit = DataType)
FixedSizeBinary <- R6Class("FixedSizeBinary", inherit = FixedWidthType)
LargeBinary <- R6Class("LargeBinary", inherit = DataType)

DateType <- R6Class("DateType",
  inherit = FixedWidthType,
  public = list(
    unit = function() DateType__unit(self)
  )
)
Date32 <- R6Class("Date32", inherit = DateType)
Date64 <- R6Class("Date64", inherit = DateType)

TimeType <- R6Class("TimeType",
  inherit = FixedWidthType,
  public = list(
    unit = function() TimeType__unit(self)
  )
)
Time32 <- R6Class("Time32", inherit = TimeType)
Time64 <- R6Class("Time64", inherit = TimeType)

Null <- R6Class("Null", inherit = DataType)

Timestamp <- R6Class("Timestamp",
  inherit = FixedWidthType,
  public = list(
    timezone = function()  TimestampType__timezone(self),
    unit = function() TimestampType__unit(self)
  )
)

DecimalType <- R6Class("DecimalType",
  inherit = FixedWidthType,
  public = list(
    precision = function() DecimalType__precision(self),
    scale = function() DecimalType__scale(self)
  )
)
Decimal128Type <- R6Class("Decimal128Type", inherit = DecimalType)

NestedType <- R6Class("NestedType", inherit = DataType)

#' Apache Arrow data types
#'
#' These functions create type objects corresponding to Arrow types. Use them
#' when defining a [schema()] or as inputs to other types, like `struct`. Most
#' of these functions don't take arguments, but a few do.
#'
#' A few functions have aliases:
#'
#' * `utf8()` and `string()`
#' * `float16()` and `halffloat()`
#' * `float32()` and `float()`
#' * `bool()` and `boolean()`
#' * Called from `schema()` or `struct()`, `double()` also is supported as a
#' way of creating a `float64()`
#'
#' `date32()` creates a datetime type with a "day" unit, like the R `Date`
#' class. `date64()` has a "ms" unit.
#'
#' @param unit For time/timestamp types, the time unit. `time32()` can take
#' either "s" or "ms", while `time64()` can be "us" or "ns". `timestamp()` can
#' take any of those four values.
#' @param timezone For `timestamp()`, an optional time zone string.
#' @param byte_width byte width for `FixedSizeBinary` type.
#' @param list_size list size for `FixedSizeList` type.
#' @param precision For `decimal()`, precision
#' @param scale For `decimal()`, scale
#' @param type For `list_of()`, a data type to make a list-of-type
#' @param ... For `struct()`, a named list of types to define the struct columns
#'
#' @name data-type
#' @return An Arrow type object inheriting from DataType.
#' @export
#' @seealso [dictionary()] for creating a dictionary (factor-like) type.
#' @examples
#' \donttest{
#' bool()
#' struct(a = int32(), b = double())
#' timestamp("ms", timezone = "CEST")
#' time64("ns")
#' }
int8 <- function() shared_ptr(Int8, Int8__initialize())

#' @rdname data-type
#' @export
int16 <- function() shared_ptr(Int16, Int16__initialize())

#' @rdname data-type
#' @export
int32 <- function() shared_ptr(Int32, Int32__initialize())

#' @rdname data-type
#' @export
int64 <- function() shared_ptr(Int64, Int64__initialize())

#' @rdname data-type
#' @export
uint8 <- function() shared_ptr(UInt8, UInt8__initialize())

#' @rdname data-type
#' @export
uint16 <- function() shared_ptr(UInt16, UInt16__initialize())

#' @rdname data-type
#' @export
uint32 <- function() shared_ptr(UInt32, UInt32__initialize())

#' @rdname data-type
#' @export
uint64 <- function() shared_ptr(UInt64, UInt64__initialize())

#' @rdname data-type
#' @export
float16 <- function() shared_ptr(Float16,  Float16__initialize())

#' @rdname data-type
#' @export
halffloat <- float16

#' @rdname data-type
#' @export
float32 <- function() shared_ptr(Float32, Float32__initialize())

#' @rdname data-type
#' @export
float <- float32

#' @rdname data-type
#' @export
float64 <- function() shared_ptr(Float64, Float64__initialize())

#' @rdname data-type
#' @export
boolean <- function() shared_ptr(Boolean, Boolean__initialize())

#' @rdname data-type
#' @export
bool <- boolean

#' @rdname data-type
#' @export
utf8 <- function() shared_ptr(Utf8, Utf8__initialize())

#' @rdname data-type
#' @export
large_utf8 <- function() shared_ptr(LargeUtf8, LargeUtf8__initialize())

#' @rdname data-type
#' @export
binary <- function() {
  shared_ptr(Binary, Binary__initialize())
}

#' @rdname data-type
#' @export
large_binary <- function() {
  shared_ptr(LargeBinary, LargeBinary__initialize())
}

#' @rdname data-type
#' @export
fixed_size_binary <- function(byte_width) {
  shared_ptr(FixedSizeBinary, FixedSizeBinary__initialize(byte_width))
}

#' @rdname data-type
#' @export
string <- utf8

#' @rdname data-type
#' @export
date32 <- function() shared_ptr(Date32, Date32__initialize())

#' @rdname data-type
#' @export
date64 <- function() shared_ptr(Date64, Date64__initialize())

#' @rdname data-type
#' @export
time32 <- function(unit = c("ms", "s")) {
  if (is.character(unit)) {
    unit <- match.arg(unit)
  }
  unit <- make_valid_time_unit(unit, valid_time32_units)
  shared_ptr(Time32, Time32__initialize(unit))
}

valid_time32_units <- c(
  "ms" = TimeUnit$MILLI,
  "s" = TimeUnit$SECOND
)

valid_time64_units <- c(
  "ns" = TimeUnit$NANO,
  "us" = TimeUnit$MICRO
)

make_valid_time_unit <- function(unit, valid_units) {
  if (is.character(unit)) {
    unit <- valid_units[match.arg(unit, choices = names(valid_units))]
  }
  if (is.numeric(unit)) {
    # Allow non-integer input for convenience
    unit <- as.integer(unit)
  } else {
    stop('"unit" should be one of ', oxford_paste(names(valid_units), "or"), call. = FALSE)
  }
  if (!(unit %in% valid_units)) {
    stop('"unit" should be one of ', oxford_paste(valid_units, "or"), call. = FALSE)
  }
  unit
}

#' @rdname data-type
#' @export
time64 <- function(unit = c("ns", "us")) {
  if (is.character(unit)) {
    unit <- match.arg(unit)
  }
  unit <- make_valid_time_unit(unit, valid_time64_units)
  shared_ptr(Time64, Time64__initialize(unit))
}

#' @rdname data-type
#' @export
null <- function() shared_ptr(Null, Null__initialize())

#' @rdname data-type
#' @export
timestamp <- function(unit = c("s", "ms", "us", "ns"), timezone = "") {
  if (is.character(unit)) {
    unit <- match.arg(unit)
  }
  unit <- make_valid_time_unit(unit, c(valid_time64_units, valid_time32_units))
  assert_that(is.string(timezone))
  shared_ptr(Timestamp, Timestamp__initialize(unit, timezone))
}

#' @rdname data-type
#' @export
decimal <- function(precision, scale) {
  if (is.numeric(precision)) {
    precision <- as.integer(precision)
  } else {
    stop('"precision" must be an integer', call. = FALSE)
  }
  if (is.numeric(scale)) {
    scale <- as.integer(scale)
  } else {
    stop('"scale" must be an integer', call. = FALSE)
  }
  shared_ptr(Decimal128Type, Decimal128Type__initialize(precision, scale))
}

as_type <- function(type, name = "type") {
  if (identical(type, double())) {
    # Magic so that we don't have to mask this base function
    type <- float64()
  }
  if (!inherits(type, "DataType")) {
    stop(name, " must be a DataType, not ", class(type), call. = FALSE)
  }
  type
}


# vctrs support -----------------------------------------------------------
str_dup <- function(x, times) {
  paste0(rep(x, times = times), collapse = "")
}

indent <- function(x, n) {
  pad <- str_dup(" ", n)
  sapply(x, gsub, pattern = "(\n+)", replacement = paste0("\\1", pad))
}

#' @importFrom vctrs vec_ptype_full vec_ptype_abbr
#' @export
vec_ptype_full.arrow_fixed_size_binary <- function(x, ...) {
  paste0("fixed_size_binary<", attr(x, "byte_width"), ">")
}

#' @export
vec_ptype_full.arrow_list <- function(x, ...) {
  param <- vec_ptype_full(attr(x, "ptype"))
  if (grepl("\n", param)) {
    param <- paste0(indent(paste0("\n", param), 2), "\n")
  }
  paste0("list<", param, ">")
}

#' @export
vec_ptype_full.arrow_large_list <- function(x, ...) {
  param <- vec_ptype_full(attr(x, "ptype"))
  if (grepl("\n", param)) {
    param <- paste0(indent(paste0("\n", param), 2), "\n")
  }
  paste0("large_list<", param, ">")
}

#' @export
vec_ptype_full.arrow_fixed_size_list <- function(x, ...) {
  param <- vec_ptype_full(attr(x, "ptype"))
  if (grepl("\n", param)) {
    param <- paste0(indent(paste0("\n", param), 2), "\n")
  }
  paste0("fixed_size_list<", param, ", ", attr(x, "list_size"), ">")
}

#' @export
vec_ptype_abbr.arrow_fixed_size_binary <- function(x, ...) {
  vec_ptype_full(x, ...)
}
#' @export
vec_ptype_abbr.arrow_list <- function(x, ...) {
  vec_ptype_full(x, ...)
}
#' @export
vec_ptype_abbr.arrow_large_list <- function(x, ...) {
  vec_ptype_full(x, ...)
}
#' @export
vec_ptype_abbr.arrow_fixed_size_list <- function(x, ...) {
  vec_ptype_full(x, ...)
}
