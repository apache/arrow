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
#' @include R6.R
#' @include io.R

`arrow::util::Codec` <- R6Class("arrow::util::Codec", inherit = `arrow::Object`)

`arrow::io::CompressedOutputStream` <- R6Class("arrow::io::CompressedOutputStream", inherit = `arrow::io::OutputStream`)
`arrow::io::CompressedInputStream` <- R6Class("arrow::io::CompressedInputStream", inherit = `arrow::io::InputStream`)

#' codec
#'
#' @param type type of codec
#'
#' @export
compression_codec <- function(type = "GZIP") {
  type <- CompressionType[[match.arg(type, names(CompressionType))]]
  unique_ptr(`arrow::util::Codec`, util___Codec__Create(type))
}


#' Compressed output stream
#'
#' @details This function is not supported in Windows.
#'
#' @param stream Underlying raw output stream
#' @param codec a codec
#' @export
CompressedOutputStream <- function(stream, codec = compression_codec("GZIP")){
  if (.Platform$OS.type == "windows") stop("'CompressedOutputStream' is unsupported in Windows.")

  UseMethod("CompressedOutputStream")
}

#' @export
CompressedOutputStream.character <- function(stream, codec = compression_codec("GZIP")){
  CompressedOutputStream(FileOutputStream(stream), codec = codec)
}

#' @export
`CompressedOutputStream.arrow::io::OutputStream` <- function(stream, codec = compression_codec("GZIP")) {
  assert_that(inherits(codec, "arrow::util::Codec"))
  shared_ptr(`arrow::io::CompressedOutputStream`, io___CompressedOutputStream__Make(codec, stream))
}

#' Compressed input stream
#'
#' @param stream Underlying raw input stream
#' @param codec a codec
#' @export
CompressedInputStream <- function(stream, codec = codec("GZIP")){
  UseMethod("CompressedInputStream")
}

#' @export
CompressedInputStream.character <- function(stream, codec = compression_codec("GZIP")){
  CompressedInputStream(ReadableFile(stream), codec = codec)
}

#' @export
`CompressedInputStream.arrow::io::InputStream` <- function(stream, codec = compression_codec("GZIP")) {
  assert_that(inherits(codec, "arrow::util::Codec"))
  shared_ptr(`arrow::io::CompressedInputStream`, io___CompressedInputStream__Make(codec, stream))
}
