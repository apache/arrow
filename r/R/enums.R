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

#' @export
`$.arrow-enum` <- function(x, y){
  structure(unclass(x)[[y]], class = class(x))
}

#' @export
`print.arrow-enum` <- function(x, ...){
  NextMethod()
}

#' @importFrom rlang seq2 quo_name set_names
#' @importFrom purrr map_chr
enum <- function(class, ...){
  names <- purrr::map_chr(rlang::quos(...), rlang::quo_name)
  names[is.na(names)] <- "NA"

  structure(
    rlang::set_names(rlang::seq2(0L, length(names)-1), names),
    class = c(class, "arrow-enum")
  )
}

#' @rdname DataType
#' @export
TimeUnit <- enum("arrow::TimeUnit::type", SECOND, MILLI, MICRO, NANO)

#' @rdname DataType
#' @export
DateUnit <- enum("arrow::DateUnit", DAY, MILLI)

#' @rdname DataType
#' @export
Type <- enum("arrow::Type::type",
  NA, BOOL, UINT8, INT8, UINT16, INT16, UINT32, INT32, UINT64, INT64,
  HALF_FLOAT, FLOAT, DOUBLE, STRING, BINARY, DATE32, DATE64, TIMESTAMP,
  INTERVAL, DECIMAL, LIST, STRUCT, UNION, DICTIONARY, MAP
)

#' @rdname DataType
#' @export
StatusCode <- enum("arrow::StatusCode",
  OK, OutOfMemory, KeyError, TypeError, Invalid, IOError,
  CapacityError, UnknownError, NotImplemented, SerializationError,
  PythonError, PlasmaObjectExists, PlasmaObjectNonexistent, PlasmaStoreFull,
  PlasmaObjectAlreadySealed
)
