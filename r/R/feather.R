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

`arrow::ipc::feather::TableWriter` <- R6Class("arrow::ipc::feather::TableWriter", inherit = `arrow::Object`,
  public = list(
    SetDescription = function(description) ipc___feather___TableWriter__SetDescription(self, description),
    SetNumRows = function(num_rows) ipc___feather___TableWriter__SetNumRows(self, num_rows),
    Append = function(name, values) ipc___feather___TableWriter__Append(self, name, values),
    Finalize = function() ipc___feather___TableWriter__Finalize(self)
  )
)

#' Create TableWriter that writes into a stream
#'
#' @param stream an OutputStream
#'
#' @export
table_writer <- function(stream) {
  UseMethod("table_writer")
}

#' @export
`table_writer.arrow::io::OutputStream` <- function(stream){
  unique_ptr(`arrow::ipc::feather::TableWriter`, ipc___feather___TableWriter__Open(stream))
}

#' @export
write_feather <- function(data, stream) {
  UseMethod("write_feather", stream)
}

#' @export
write_feather.default <- function(data, stream) {
  stop("unsupported")
}

#' @export
`write_feather.character` <- function(data, stream) {
  write_feather(data, fs::path_abs(stream))
}

#' @importFrom purrr walk2
#' @export
`write_feather.fs_path` <- function(data, stream) {
  nms <- names(data)

  file_stream <- close_on_exit(file_output_stream(stream))
  writer <- table_writer(file_stream)

  walk2(names(data), data, ~writer$Append(.x, array(.y)))
  writer$Finalize()
}
