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
`print.arrow-enum` <- function(x, ...) {
  NextMethod()
}

enum <- function(class, ..., .list = list(...)) {
  structure(
    .list,
    class = c(class, "arrow-enum")
  )
}

#' Arrow enums
#' @name enums
#' @export
#' @keywords internal
TimeUnit <- enum("TimeUnit::type",
  SECOND = 0L, MILLI = 1L, MICRO = 2L, NANO = 3L
)

#' @rdname enums
#' @export
DateUnit <- enum("DateUnit", DAY = 0L, MILLI = 1L)

#' @rdname enums
#' @export
Type <- enum("Type::type",
  "NA" = 0L,
  BOOL = 1L,
  UINT8 = 2L,
  INT8 = 3L,
  UINT16 = 4L,
  INT16 = 5L,
  UINT32 = 6L,
  INT32 = 7L,
  UINT64 = 8L,
  INT64 = 9L,
  HALF_FLOAT = 10L,
  FLOAT = 11L,
  DOUBLE = 12L,
  STRING = 13L,
  BINARY = 14L,
  FIXED_SIZE_BINARY = 15L,
  DATE32 = 16L,
  DATE64 = 17L,
  TIMESTAMP = 18L,
  TIME32 = 19L,
  TIME64 = 20L,
  INTERVAL_MONTHS = 21L,
  INTERVAL_DAY_TIME = 22L,
  DECIMAL128 = 23L,
  DECIMAL256 = 24L,
  LIST = 25L,
  STRUCT = 26L,
  SPARSE_UNION = 27L,
  DENSE_UNION = 28L,
  DICTIONARY = 29L,
  MAP = 30L,
  EXTENSION = 31L,
  FIXED_SIZE_LIST = 32L,
  DURATION = 33L,
  LARGE_STRING = 34L,
  LARGE_BINARY = 35L,
  LARGE_LIST = 36L
)

TYPES_WITH_NAN <- Type[c("HALF_FLOAT", "FLOAT", "DOUBLE")]
TYPES_NUMERIC <- Type[
  c(
    "INT8", "UINT8", "INT16", "UINT16", "INT32", "UINT32",
    "INT64", "UINT64", "HALF_FLOAT", "FLOAT", "DOUBLE",
    "DECIMAL128", "DECIMAL256"
    )
  ]

#' @rdname enums
#' @export
StatusCode <- enum("StatusCode",
  OK = 0L, OutOfMemory = 1L, KeyError = 2L, TypeError = 3L,
  Invalid = 4L, IOError = 5L, CapacityError = 6L, IndexError = 7L,
  UnknownError = 9L, NotImplemented = 10L, SerializationError = 11L,
  PythonError = 12L, RError = 13L
)

#' @rdname enums
#' @export
FileMode <- enum("FileMode",
  READ = 0L, WRITE = 1L, READWRITE = 2L
)

#' @rdname enums
#' @export
MessageType <- enum("MessageType",
  NONE = 0L, SCHEMA = 1L, DICTIONARY_BATCH = 2L, RECORD_BATCH = 3L, TENSOR = 4L
)

#' @rdname enums
#' @export
CompressionType <- enum("Compression::type",
  UNCOMPRESSED = 0L, SNAPPY = 1L, GZIP = 2L, BROTLI = 3L, ZSTD = 4L, LZ4 = 5L,
  LZ4_FRAME = 6L, LZO = 7L, BZ2 = 8L
)

#' @export
#' @rdname enums
FileType <- enum("FileType",
  NotFound = 0L, Unknown = 1L, File = 2L, Directory = 3L
)

#' @export
#' @rdname enums
ParquetVersionType <- enum("ParquetVersionType",
  PARQUET_1_0 = 0L, PARQUET_2_0 = 1L, PARQUET_2_4 = 2L, PARQUET_2_6 = 3L
)

#' @export
#' @rdname enums
MetadataVersion <- enum("MetadataVersion",
  V1 = 0L, V2 = 1L, V3 = 2L, V4 = 3L, V5 = 4L
)

#' @export
#' @rdname enums
QuantileInterpolation <- enum("QuantileInterpolation",
  LINEAR = 0L, LOWER = 1L, HIGHER = 2L, NEAREST = 3L, MIDPOINT = 4L
)

#' @export
#' @rdname enums
NullEncodingBehavior <- enum("NullEncodingBehavior",
  ENCODE = 0L, MASK = 1L
)

#' @export
#' @rdname enums
NullHandlingBehavior <- enum("NullHandlingBehavior",
  EMIT_NULL = 0L, SKIP = 1L, REPLACE = 2L
)

#' @export
#' @rdname enums
RoundMode <- enum("RoundMode",
  DOWN = 0L,
  UP = 1L,
  TOWARDS_ZERO = 2L,
  TOWARDS_INFINITY = 3L,
  HALF_DOWN = 4L,
  HALF_UP = 5L,
  HALF_TOWARDS_ZERO = 6L,
  HALF_TOWARDS_INFINITY = 7L,
  HALF_TO_EVEN = 8L,
  HALF_TO_ODD = 9L
)

#' @export
#' @rdname enums
JoinType <- enum("JoinType",
  LEFT_SEMI = 0L,
  RIGHT_SEMI = 1L,
  LEFT_ANTI = 2L,
  RIGHT_ANTI = 3L,
  INNER = 4L,
  LEFT_OUTER = 5L,
  RIGHT_OUTER = 6L,
  FULL_OUTER = 7L
)
