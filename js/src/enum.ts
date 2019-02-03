// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import * as Schema_ from './fb/Schema';
import * as Message_ from './fb/Message';

export import ArrowType = Schema_.org.apache.arrow.flatbuf.Type;
export import DateUnit = Schema_.org.apache.arrow.flatbuf.DateUnit;
export import TimeUnit = Schema_.org.apache.arrow.flatbuf.TimeUnit;
export import Precision = Schema_.org.apache.arrow.flatbuf.Precision;
export import UnionMode = Schema_.org.apache.arrow.flatbuf.UnionMode;
export import VectorType = Schema_.org.apache.arrow.flatbuf.VectorType;
export import IntervalUnit = Schema_.org.apache.arrow.flatbuf.IntervalUnit;
export import MessageHeader = Message_.org.apache.arrow.flatbuf.MessageHeader;
export import MetadataVersion = Schema_.org.apache.arrow.flatbuf.MetadataVersion;

/**
 * *
 * Main data type enumeration:
 * *
 * Data types in this library are all *logical*. They can be expressed as
 * either a primitive physical type (bytes or bits of some fixed size), a
 * nested type consisting of other data types, or another data type (e.g. a
 * timestamp encoded as an int64)
 */
export enum Type {
    NONE            =  0,  // The default placeholder type
    Null            =  1,  // A NULL type having no physical storage
    Int             =  2,  // Signed or unsigned 8, 16, 32, or 64-bit little-endian integer
    Float           =  3,  // 2, 4, or 8-byte floating point value
    Binary          =  4,  // Variable-length bytes (no guarantee of UTF8-ness)
    Utf8            =  5,  // UTF8 variable-length string as List<Char>
    Bool            =  6,  // Boolean as 1 bit, LSB bit-packed ordering
    Decimal         =  7,  // Precision-and-scale-based decimal type. Storage type depends on the parameters.
    Date            =  8,  // int32_t days or int64_t milliseconds since the UNIX epoch
    Time            =  9,  // Time as signed 32 or 64-bit integer, representing either seconds, milliseconds, microseconds, or nanoseconds since midnight since midnight
    Timestamp       = 10,  // Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond)
    Interval        = 11,  // YEAR_MONTH or DAY_TIME interval in SQL style
    List            = 12,  // A list of some logical data type
    Struct          = 13,  // Struct of logical types
    Union           = 14,  // Union of logical types
    FixedSizeBinary = 15,  // Fixed-size binary. Each value occupies the same number of bytes
    FixedSizeList   = 16,  // Fixed-size list. Each value occupies the same number of bytes
    Map             = 17,  // Map of named logical types

    // These enum values are here so that TypeScript can narrow the type signatures further
    // beyond the base Arrow types. The base Arrow types include metadata like bitWidths that
    // impact the type signatures of the values we return. For example, the Int8Vector reads
    // 1-byte numbers from an Int8Array, an Int32Vector reads a 4-byte number from an Int32Array,
    // and an Int64Vector reads a pair of 4-byte lo, hi int32s, and returns them as a zero-copy
    // slice from an underlying Int32Array. Library consumers benefit by doing this type narrowing,
    // since we can ensure the types across all public methods are propagated and never bail to `any`.
    // These values are _never_ actually used at runtime, and they will _never_ be written into the
    // flatbuffers metadata of serialized Arrow IPC payloads.
    Dictionary            = -1, // Dictionary aka Category type
    Int8                  = -2,
    Int16                 = -3,
    Int32                 = -4,
    Int64                 = -5,
    Uint8                 = -6,
    Uint16                = -7,
    Uint32                = -8,
    Uint64                = -9,
    Float16               = -10,
    Float32               = -11,
    Float64               = -12,
    DateDay               = -13,
    DateMillisecond       = -14,
    TimestampSecond       = -15,
    TimestampMillisecond  = -16,
    TimestampMicrosecond  = -17,
    TimestampNanosecond   = -18,
    TimeSecond            = -19,
    TimeMillisecond       = -20,
    TimeMicrosecond       = -21,
    TimeNanosecond        = -22,
    DenseUnion            = -23,
    SparseUnion           = -24,
    IntervalDayTime       = -25,
    IntervalYearMonth     = -26,
}
