/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <glib-object.h>

G_BEGIN_DECLS

/**
 * GArrowType:
 * @GARROW_TYPE_NA: A degenerate NULL type represented as 0 bytes/bits.
 * @GARROW_TYPE_BOOLEAN: A boolean value represented as 1-bit.
 * @GARROW_TYPE_UINT8: Little-endian 8-bit unsigned integer.
 * @GARROW_TYPE_INT8: Little-endian 8-bit signed integer.
 * @GARROW_TYPE_UINT16: Little-endian 16-bit unsigned integer.
 * @GARROW_TYPE_INT16: Little-endian 16-bit signed integer.
 * @GARROW_TYPE_UINT32: Little-endian 32-bit unsigned integer.
 * @GARROW_TYPE_INT32: Little-endian 32-bit signed integer.
 * @GARROW_TYPE_UINT64: Little-endian 64-bit unsigned integer.
 * @GARROW_TYPE_INT64: Little-endian 64-bit signed integer.
 * @GARROW_TYPE_HALF_FLOAT: 2-byte floating point value.
 * @GARROW_TYPE_FLOAT: 4-byte floating point value.
 * @GARROW_TYPE_DOUBLE: 8-byte floating point value.
 * @GARROW_TYPE_STRING: UTF-8 variable-length string.
 * @GARROW_TYPE_BINARY: Variable-length bytes (no guarantee of UTF-8-ness).
 * @GARROW_TYPE_FIXED_SIZE_BINARY: Fixed-size binary. Each value occupies
 *   the same number of bytes.
 * @GARROW_TYPE_DATE32: int32 days since the UNIX epoch.
 * @GARROW_TYPE_DATE64: int64 milliseconds since the UNIX epoch.
 * @GARROW_TYPE_TIMESTAMP: Exact timestamp encoded with int64 since UNIX epoch.
 *   Default unit millisecond.
 * @GARROW_TYPE_TIME32: Exact time encoded with int32, supporting seconds or milliseconds
 * @GARROW_TYPE_TIME64: Exact time encoded with int64, supporting micro- or nanoseconds
 * @GARROW_TYPE_MONTH_INTERVAL: YEAR_MONTH interval in SQL style.
 * @GARROW_TYPE_DAY_TIME_INTERVAL: DAY_TIME interval in SQL style.
 * @GARROW_TYPE_DECIMAL128: Precision- and scale-based decimal
 *   type with 128-bit. Storage type depends on the parameters.
 * @GARROW_TYPE_DECIMAL256: Precision- and scale-based decimal
 *   type with 256-bit. Storage type depends on the parameters.
 * @GARROW_TYPE_LIST: A list of some logical data type.
 * @GARROW_TYPE_STRUCT: Struct of logical types.
 * @GARROW_TYPE_SPARSE_UNION: Sparse unions of logical types.
 * @GARROW_TYPE_DENSE_UNION: Dense unions of logical types.
 * @GARROW_TYPE_DICTIONARY: Dictionary aka Category type.
 * @GARROW_TYPE_MAP: A repeated struct logical type.
 * @GARROW_TYPE_EXTENSION: Custom data type, implemented by user.
 * @GARROW_TYPE_FIXED_SIZE_LIST: Fixed size list of some logical type.
 * @GARROW_TYPE_DURATION: Measure of elapsed time in either seconds,
 *   milliseconds, microseconds or nanoseconds.
 * @GARROW_TYPE_LARGE_STRING: 64bit offsets UTF-8 variable-length string.
 * @GARROW_TYPE_LARGE_BINARY: 64bit offsets Variable-length bytes (no guarantee of UTF-8-ness).
 * @GARROW_TYPE_LARGE_LIST: A list of some logical data type with 64-bit offsets.
 * @GARROW_TYPE_MONTH_DAY_NANO_INTERVAL: MONTH_DAY_NANO interval in SQL style.
 * @GARROW_TYPE_RUN_END_ENCODED: Run-end encoded data.
 *
 * They are corresponding to `arrow::Type::type` values.
 */
typedef enum {
  GARROW_TYPE_NA,
  GARROW_TYPE_BOOLEAN,
  GARROW_TYPE_UINT8,
  GARROW_TYPE_INT8,
  GARROW_TYPE_UINT16,
  GARROW_TYPE_INT16,
  GARROW_TYPE_UINT32,
  GARROW_TYPE_INT32,
  GARROW_TYPE_UINT64,
  GARROW_TYPE_INT64,
  GARROW_TYPE_HALF_FLOAT,
  GARROW_TYPE_FLOAT,
  GARROW_TYPE_DOUBLE,
  GARROW_TYPE_STRING,
  GARROW_TYPE_BINARY,
  GARROW_TYPE_FIXED_SIZE_BINARY,
  GARROW_TYPE_DATE32,
  GARROW_TYPE_DATE64,
  GARROW_TYPE_TIMESTAMP,
  GARROW_TYPE_TIME32,
  GARROW_TYPE_TIME64,
  GARROW_TYPE_MONTH_INTERVAL,
  GARROW_TYPE_DAY_TIME_INTERVAL,
  GARROW_TYPE_DECIMAL128,
  GARROW_TYPE_DECIMAL256,
  GARROW_TYPE_LIST,
  GARROW_TYPE_STRUCT,
  GARROW_TYPE_SPARSE_UNION,
  GARROW_TYPE_DENSE_UNION,
  GARROW_TYPE_DICTIONARY,
  GARROW_TYPE_MAP,
  GARROW_TYPE_EXTENSION,
  GARROW_TYPE_FIXED_SIZE_LIST,
  GARROW_TYPE_DURATION,
  GARROW_TYPE_LARGE_STRING,
  GARROW_TYPE_LARGE_BINARY,
  GARROW_TYPE_LARGE_LIST,
  GARROW_TYPE_MONTH_DAY_NANO_INTERVAL,
  GARROW_TYPE_RUN_END_ENCODED,
} GArrowType;

/**
 * GArrowTimeUnit:
 * @GARROW_TIME_UNIT_SECOND: Second.
 * @GARROW_TIME_UNIT_MILLI: Millisecond.
 * @GARROW_TIME_UNIT_MICRO: Microsecond.
 * @GARROW_TIME_UNIT_NANO: Nanosecond.
 *
 * They are corresponding to `arrow::TimeUnit::type` values.
 */
typedef enum {
  GARROW_TIME_UNIT_SECOND,
  GARROW_TIME_UNIT_MILLI,
  GARROW_TIME_UNIT_MICRO,
  GARROW_TIME_UNIT_NANO
} GArrowTimeUnit;

/**
 * GArrowIntervalType:
 * @GARROW_INTERVAL_TYPE_MONTH: A number of months.
 * @GARROW_INTERVAL_TYPE_DAY_TIME: A number of days and
 *   milliseconds (fraction of day).
 * @GARROW_INTERVAL_TYPE_MONTH_DAY_NANO: A number of months, days and
 *   nanoseconds between two dates.
 *
 * They are corresponding to `arrow::IntervalType::type` values.
 *
 * Since 7.0.0
 */
typedef enum {
  GARROW_INTERVAL_TYPE_MONTH,
  GARROW_INTERVAL_TYPE_DAY_TIME,
  GARROW_INTERVAL_TYPE_MONTH_DAY_NANO,
} GArrowIntervalType;

G_END_DECLS
