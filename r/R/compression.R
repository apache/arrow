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
#' @include arrow-object.R
#' @include io.R

#' @title Compression Codec class
#' @usage NULL
#' @format NULL
#' @docType class
#' @description Codecs allow you to create [compressed input and output
#' streams][compression].
#' @section Factory:
#' The `Codec$create()` factory method takes the following arguments:
#' * `type`: string name of the compression method. Possible values are
#'    "uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4", "lzo", or
#'    "bz2". `type` may be upper- or lower-cased. Not all methods may be
#'    available; support depends on build-time flags for the C++ library.
#'    See [codec_is_available()]. Most builds support at least "snappy" and
#'    "gzip". All support "uncompressed".
#' * `compression_level`: compression level, the default value (`NA`) uses the
#'    default compression level for the selected compression `type`.
#' @rdname Codec
#' @name Codec
#' @export
Codec <- R6Class("Codec",
  inherit = ArrowObject,
  active = list(
    name = function() util___Codec__name(self),
    level = function() abort("Codec$level() not yet implemented")
  )
)
Codec$create <- function(type = "gzip", compression_level = NA) {
  if (is.string(type)) {
    type <- util___Codec__Create(
      compression_from_name(type), compression_level
    )
  }
  assert_is(type, "Codec")
  type
}

#' Check whether a compression codec is available
#'
#' Support for compression libraries depends on the build-time settings of
#' the Arrow C++ library. This function lets you know which are available for
#' use.
#' @param type A string, one of "uncompressed", "snappy", "gzip", "brotli",
#' "zstd", "lz4", "lzo", or "bz2", case insensitive.
#' @return Logical: is `type` available?
#' @export
#' @examples
#' codec_is_available("gzip")
codec_is_available <- function(type) {
  util___Codec__IsAvailable(compression_from_name(type))
}

compression_from_name <- function(name) {
  map_int(name, ~ CompressionType[[match.arg(toupper(.x), names(CompressionType))]])
}

#' @title Compressed stream classes
#' @rdname compression
#' @name compression
#' @aliases CompressedInputStream CompressedOutputStream
#' @docType class
#' @usage NULL
#' @format NULL
#' @description `CompressedInputStream` and `CompressedOutputStream`
#' allow you to apply a compression [Codec] to an
#' input or output stream.
#'
#' @section Factory:
#'
#' The `CompressedInputStream$create()` and `CompressedOutputStream$create()`
#' factory methods instantiate the object and take the following arguments:
#'
#' - `stream` An [InputStream] or [OutputStream], respectively
#' - `codec` A `Codec`, either a [Codec][Codec] instance or a string
#' - `compression_level` compression level for when the `codec` argument is given as a string
#'
#' @section Methods:
#'
#' Methods are inherited from [InputStream] and [OutputStream], respectively
#' @export
#' @include arrow-object.R
CompressedOutputStream <- R6Class("CompressedOutputStream", inherit = OutputStream)
CompressedOutputStream$create <- function(stream, codec = "gzip", compression_level = NA) {
  codec <- Codec$create(codec, compression_level = compression_level)
  if (is.string(stream)) {
    stream <- FileOutputStream$create(stream)
  }
  assert_is(stream, "OutputStream")
  io___CompressedOutputStream__Make(codec, stream)
}

#' @rdname compression
#' @usage NULL
#' @format NULL
#' @export
CompressedInputStream <- R6Class("CompressedInputStream", inherit = InputStream)
CompressedInputStream$create <- function(stream, codec = "gzip", compression_level = NA) {
  codec <- Codec$create(codec, compression_level = compression_level)
  if (is.string(stream)) {
    stream <- ReadableFile$create(stream)
  }
  assert_is(stream, "InputStream")
  io___CompressedInputStream__Make(codec, stream)
}
