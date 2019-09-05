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
#' @include arrow-package.R
#' @include io.R

Codec <- R6Class("Codec", inherit = Object)

#' codec
#'
#' @param type type of codec
#'
#' @export
compression_codec <- function(type = "GZIP") {
  type <- CompressionType[[match.arg(type, names(CompressionType))]]
  unique_ptr(Codec, util___Codec__Create(type))
}


CompressedOutputStream <- R6Class("CompressedOutputStream", inherit = OutputStream)

CompressedOutputStream$create <- function(stream, codec = compression_codec()){
  if (.Platform$OS.type == "windows") {
    stop("'CompressedOutputStream' is unsupported in Windows.")
  }
  assert_that(inherits(codec, "Codec"))
  if (is.character(stream)) {
    stream <- FileOutputStream$create(stream)
  }
  assert_that(inherits(stream, "OutputStream"))
  shared_ptr(CompressedOutputStream, io___CompressedOutputStream__Make(codec, stream))
}

#' Compressed output stream
#'
#' @details This function is not supported in Windows.
#'
#' @param stream Underlying raw output stream
#' @param codec a codec
#' @export
compressed_output_stream <- CompressedOutputStream$create


CompressedInputStream <- R6Class("CompressedInputStream", inherit = InputStream)

CompressedInputStream$create <- function(stream, codec = compression_codec()){
  # TODO (npr): why would CompressedInputStream work on Windows if CompressedOutputStream doesn't? (and is it still the case that it does not?)
  assert_that(inherits(codec, "Codec"))
  if (is.character(stream)) {
    stream <- ReadableFile$create(stream)
  }
  assert_that(inherits(stream, "InputStream"))
  shared_ptr(CompressedInputStream, io___CompressedInputStream__Make(codec, stream))
}

#' Compressed input stream
#'
#' @param stream Underlying raw input stream
#' @param codec a codec
#' @export
compressed_input_stream <- CompressedInputStream$create
