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

#' @title Compressed stream classes
#' @rdname compression
#' @name compression
#' @aliases CompressedInputStream CompressedOutputStream
#' @docType class
#' @usage NULL
#' @format NULL
#' @description `CompressedInputStream` and `CompressedOutputStream`
#' allow you to apply a [compression_codec()] to an
#' input or output stream.
#'
#' @section Factory:
#'
#' The `CompressedInputStream$create()` and `CompressedOutputStream$create()`
#' factory methods instantiate the object and take the following arguments:
#'
#' - `stream` An [InputStream] or [OutputStream], respectively
#' - `codec` A `Codec`
#'
#' @section Methods:
#'
#' Methods are inherited from [InputStream] and [OutputStream], respectively
#' @export
#' @include arrow-package.R
CompressedOutputStream <- R6Class("CompressedOutputStream", inherit = OutputStream)
CompressedOutputStream$create <- function(stream, codec = compression_codec()){
  if (.Platform$OS.type == "windows") {
    stop("'CompressedOutputStream' is unsupported in Windows.")
  }
  assert_is(codec, "Codec")
  if (is.character(stream)) {
    stream <- FileOutputStream$create(stream)
  }
  assert_is(stream, "OutputStream")
  shared_ptr(CompressedOutputStream, io___CompressedOutputStream__Make(codec, stream))
}

#' @rdname compression
#' @usage NULL
#' @format NULL
#' @export
CompressedInputStream <- R6Class("CompressedInputStream", inherit = InputStream)
CompressedInputStream$create <- function(stream, codec = compression_codec()){
  # TODO (npr): why would CompressedInputStream work on Windows if CompressedOutputStream doesn't? (and is it still the case that it does not?)
  assert_is(codec, "Codec")
  if (is.character(stream)) {
    stream <- ReadableFile$create(stream)
  }
  assert_is(stream, "InputStream")
  shared_ptr(CompressedInputStream, io___CompressedInputStream__Make(codec, stream))
}
