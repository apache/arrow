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

#' @include R6.R

`arrow::ArrayData` <- R6Class("arrow::ArrayData",
  inherit = `arrow::Object`,
  active = list(
    type = function() `arrow::DataType`$dispatch(ArrayData_get_type(self)),
    length = function() ArrayData_get_length(self),
    null_count = function() ArrayData_get_null_count(self),
    offset = function() ArrayData_get_offset(self)
  )
)

`arrow::Array` <- R6Class("arrow::Array",
  inherit = `arrow::Object`,
  public = list(
    IsNull = function(i) Array_IsNull(self, i),
    IsValid = function(i) Array_IsValid(self, i),
    length = function() Array_length(self),
    offset = function() Array_offset(self),
    null_count = function() Array_null_count(self),
    type = function() `arrow::DataType`$dispatch(Array_type(self)),
    type_id = function() Array_type_id(self),
    Equals = function(other) Array_Equals(self, other),
    ApproxEquals = function(othet) Array_ApproxEquals(self, other),
    data = function() `arrow::ArrayData`$new(Array_data(self)),
    as_vector = function() Array__as_vector(self),
    ToString = function() Array_ToString(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        `arrow::Array`$new(Array__Slice1(self, offset))
      } else {
        `arrow::Array`$new(Array__Slice2(self, offset, length))
      }
    }
  )
)

#' @export
`length.arrow::Array` <- function(x) x$length()

#' @export
`==.arrow::Array` <- function(x, y) x$Equals(y)

#' @export
`!=.arrow::Array` <- function(x, y) !x$Equals(y)

#' create an arrow::Array from an R vector
#'
#' @param \dots Vectors to coerce
#'
#' @export
array <- function(...){
  `arrow::Array`$new(Array__from_vector(vctrs::vec_c(...)))
}

`arrow::RecordBatch` <- R6Class("arrow::RecordBatch", inherit = `arrow::Object`,
  public = list(
    num_columns = function() RecordBatch__num_columns(self),
    num_rows = function() RecordBatch__num_rows(self),
    schema = function() `arrow::Schema`$new(RecordBatch__schema(self)),
    to_file = function(path) invisible(RecordBatch__to_file(self, fs::path_abs(path))),
    column = function(i) `arrow::Array`$new(RecordBatch__column(self, i))
  )
)

#' @export
`as_tibble.arrow::RecordBatch` <- function(x, ...){
  RecordBatch__to_dataframe(x)
}

#' Create an arrow::RecordBatch from a data frame
#'
#' @param .data a data frame
#'
#' @export
record_batch <- function(.data){
  `arrow::RecordBatch`$new(dataframe_to_RecordBatch(.data))
}

read_record_batch <- function(path){
  `arrow::RecordBatch`$new(read_record_batch_(fs::path_abs(path)))
}

`arrow::Column` <- R6Class("arrow::Column", inherit = `arrow::Object`,
  public = list(
    length = function() Column__length(self),
    null_count = function() Column__null_count(self),
    type = function() `arrow::DataType`$dispatch(Column__type(self)),
    data = function() `arrow::ChunkedArray`$new(Column__data(self))
  )
)

`arrow::ChunkedArray` <- R6Class("arrow::ChunkedArray", inherit = `arrow::Object`,
  public = list(
    length = function() ChunkedArray__length(self),
    null_count = function() ChunkedArray__null_count(self),
    num_chunks = function() ChunkedArray__num_chunks(self),
    chunk = function(i) `arrow::Array`$new(ChunkedArray__chunk(self, i)),
    chunks = function() purrr::map(ChunkedArray__chunks(self), `arrow::Array`$new),
    type = function() `arrow::DataType`$dispatch(ChunkedArray__type(self)),
    as_vector = function() ChunkedArray__as_vector(self),
    Slice = function(offset, length = NULL){
      if (is.null(length)) {
        `arrow::ChunkedArray`$new(ChunkArray__Slice1(self, offset))
      } else {
        `arrow::ChunkedArray`$new(ChunkArray__Slice2(self, offset, length))
      }
    }
  )
)

`arrow::Table` <- R6Class("arrow::Table", inherit = `arrow::Object`,
  public = list(
    num_columns = function() Table_num_columns(self),
    num_rows = function() Table_num_rows(self),
    schema = function() `arrow::Schema`$new(Table_schema(self)),
    to_file = function(path) invisible(Table_to_file(self, fs::path_abs(path))),
    column = function(i) `arrow::Column`$new(Table__column(self, i))
  )
)

#' Create an arrow::Table from a data frame
#'
#' @param .data a data frame
#'
#' @export
table <- function(.data){
  `arrow::Table`$new(dataframe_to_Table(.data))
}

#' Write a tibble in a binary arrow file
#'
#' @param data a [tibble::tibble]
#' @param path file path
#'
#' @export
write_arrow <- function(data, path){
  table(data)$to_file(path)
}

read_table <- function(path){
  `arrow::Table`$new(read_table_(fs::path_abs(path)))
}

#' @export
`as_tibble.arrow::Table` <- function(x, ...){
  Table_to_dataframe(x)
}

#' Read an tibble from an arrow::Table on disk
#'
#' @param path binary arrow file
#'
#' @return a [tibble::tibble]
#'
#' @export
read_arrow <- function(path){
  as_tibble(read_table(path))
}

#' create an arrow::Array from an R vector
#'
#' @param \dots Vectors to coerce
#'
#' @export
chunked_array <- function(...){
  `arrow::ChunkedArray`$new(ChunkedArray__Make(list(...)))
}
