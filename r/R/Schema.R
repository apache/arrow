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

`arrow::Schema` <- R6Class("arrow::Schema",
  inherit = `arrow::Object`,
  public = list(
    ToString = function() Schema__ToString(self),
    num_fields = function() Schema__num_fields(self),
    field = function(i) shared_ptr(`arrow::Field`, Schema__field(self, i))
  ),
  active = list(
    names = function() Schema__names(self)
  )
)

#' Schema functions
#'
#' @param ... named list of data types
#'
#' @return a Schema
#'
#' @export
schema <- function(...){
  shared_ptr(`arrow::Schema`, schema_(.fields(list(...))))
}

#' read a Schema from a stream
#'
#' @param stream a stream
#' @param ... currently ignored
#'
#' @export
read_schema <- function(stream, ...) {
  UseMethod("read_schema")
}

#' @export
read_schema.default <- function(stream, ...) {
  stop("unsupported")
}

#' @export
`read_schema.arrow::io::InputStream` <- function(stream, ...) {
  shared_ptr(`arrow::Schema`, ipc___ReadSchema_InputStream(stream))
}

#' @export
`read_schema.arrow::Buffer` <- function(stream, ...) {
  read_schema(buffer_reader(stream), ...)
}

#' @export
`read_schema.raw` <- function(stream, ...) {
  read_schema(buffer(stream), ...)
}
