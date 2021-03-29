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

//! Contains Rust mappings for Thrift definition.
//! Refer to `parquet.thrift` file to see raw definitions.

use std::{convert, fmt, result, str};

use parquet_format as parquet;

use crate::errors::ParquetError;

// Re-export parquet_format types used in this module
pub use parquet_format::{
    BsonType, DateType, DecimalType, EnumType, IntType, JsonType, ListType, MapType,
    NullType, StringType, TimeType, TimeUnit, TimestampType, UUIDType,
};

// ----------------------------------------------------------------------
// Types from the Thrift definition

// ----------------------------------------------------------------------
// Mirrors `parquet::Type`

/// Types supported by Parquet.
/// These physical types are intended to be used in combination with the encodings to
/// control the on disk storage format.
/// For example INT16 is not included as a type since a good encoding of INT32
/// would handle this.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
    BOOLEAN,
    INT32,
    INT64,
    INT96,
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::ConvertedType`

/// Common types (converted types) used by frameworks when using Parquet.
/// This helps map between types in those frameworks to the base types in Parquet.
/// This is only metadata and not needed to read or write the data.
///
/// This struct was renamed from `LogicalType` in version 4.0.0.
/// If targeting Parquet format 2.4.0 or above, please use [LogicalType] instead.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConvertedType {
    NONE,
    /// A BYTE_ARRAY actually contains UTF8 encoded chars.
    UTF8,

    /// A map is converted as an optional field containing a repeated key/value pair.
    MAP,

    /// A key/value pair is converted into a group of two fields.
    MAP_KEY_VALUE,

    /// A list is converted into an optional field containing a repeated field for its
    /// values.
    LIST,

    /// An enum is converted into a binary field
    ENUM,

    /// A decimal value.
    /// This may be used to annotate binary or fixed primitive types. The
    /// underlying byte array stores the unscaled value encoded as two's
    /// complement using big-endian byte order (the most significant byte is the
    /// zeroth element).
    ///
    /// This must be accompanied by a (maximum) precision and a scale in the
    /// SchemaElement. The precision specifies the number of digits in the decimal
    /// and the scale stores the location of the decimal point. For example 1.23
    /// would have precision 3 (3 total digits) and scale 2 (the decimal point is
    /// 2 digits over).
    DECIMAL,

    /// A date stored as days since Unix epoch, encoded as the INT32 physical type.
    DATE,

    /// The total number of milliseconds since midnight. The value is stored as an INT32
    /// physical type.
    TIME_MILLIS,

    /// The total number of microseconds since midnight. The value is stored as an INT64
    /// physical type.
    TIME_MICROS,

    /// Date and time recorded as milliseconds since the Unix epoch.
    /// Recorded as a physical type of INT64.
    TIMESTAMP_MILLIS,

    /// Date and time recorded as microseconds since the Unix epoch.
    /// The value is stored as an INT64 physical type.
    TIMESTAMP_MICROS,

    /// An unsigned 8 bit integer value stored as INT32 physical type.
    UINT_8,

    /// An unsigned 16 bit integer value stored as INT32 physical type.
    UINT_16,

    /// An unsigned 32 bit integer value stored as INT32 physical type.
    UINT_32,

    /// An unsigned 64 bit integer value stored as INT64 physical type.
    UINT_64,

    /// A signed 8 bit integer value stored as INT32 physical type.
    INT_8,

    /// A signed 16 bit integer value stored as INT32 physical type.
    INT_16,

    /// A signed 32 bit integer value stored as INT32 physical type.
    INT_32,

    /// A signed 64 bit integer value stored as INT64 physical type.
    INT_64,

    /// A JSON document embedded within a single UTF8 column.
    JSON,

    /// A BSON document embedded within a single BINARY column.
    BSON,

    /// An interval of time.
    ///
    /// This type annotates data stored as a FIXED_LEN_BYTE_ARRAY of length 12.
    /// This data is composed of three separate little endian unsigned integers.
    /// Each stores a component of a duration of time. The first integer identifies
    /// the number of months associated with the duration, the second identifies
    /// the number of days associated with the duration and the third identifies
    /// the number of milliseconds associated with the provided duration.
    /// This duration of time is independent of any particular timezone or date.
    INTERVAL,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::LogicalType`

/// Logical types used by version 2.4.0+ of the Parquet format.
///
/// This is an *entirely new* struct as of version
/// 4.0.0. The struct previously named `LogicalType` was renamed to
/// [`ConvertedType`]. Please see the README.md for more details.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalType {
    STRING(StringType),
    MAP(MapType),
    LIST(ListType),
    ENUM(EnumType),
    DECIMAL(DecimalType),
    DATE(DateType),
    TIME(TimeType),
    TIMESTAMP(TimestampType),
    INTEGER(IntType),
    UNKNOWN(NullType),
    JSON(JsonType),
    BSON(BsonType),
    UUID(UUIDType),
}

// ----------------------------------------------------------------------
// Mirrors `parquet::FieldRepetitionType`

/// Representation of field types in schema.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Repetition {
    /// Field is required (can not be null) and each record has exactly 1 value.
    REQUIRED,
    /// Field is optional (can be null) and each record has 0 or 1 values.
    OPTIONAL,
    /// Field is repeated and can contain 0 or more values.
    REPEATED,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::Encoding`

/// Encodings supported by Parquet.
/// Not all encodings are valid for all types. These enums are also used to specify the
/// encoding of definition and repetition levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// Default byte encoding.
    /// - BOOLEAN - 1 bit per value, 0 is false; 1 is true.
    /// - INT32 - 4 bytes per value, stored as little-endian.
    /// - INT64 - 8 bytes per value, stored as little-endian.
    /// - FLOAT - 4 bytes per value, stored as little-endian.
    /// - DOUBLE - 8 bytes per value, stored as little-endian.
    /// - BYTE_ARRAY - 4 byte length stored as little endian, followed by bytes.
    /// - FIXED_LEN_BYTE_ARRAY - just the bytes are stored.
    PLAIN,

    /// **Deprecated** dictionary encoding.
    ///
    /// The values in the dictionary are encoded using PLAIN encoding.
    /// Since it is deprecated, RLE_DICTIONARY encoding is used for a data page, and
    /// PLAIN encoding is used for dictionary page.
    PLAIN_DICTIONARY,

    /// Group packed run length encoding.
    ///
    /// Usable for definition/repetition levels encoding and boolean values.
    RLE,

    /// Bit packed encoding.
    ///
    /// This can only be used if the data has a known max width.
    /// Usable for definition/repetition levels encoding.
    BIT_PACKED,

    /// Delta encoding for integers, either INT32 or INT64.
    ///
    /// Works best on sorted data.
    DELTA_BINARY_PACKED,

    /// Encoding for byte arrays to separate the length values and the data.
    ///
    /// The lengths are encoded using DELTA_BINARY_PACKED encoding.
    DELTA_LENGTH_BYTE_ARRAY,

    /// Incremental encoding for byte arrays.
    ///
    /// Prefix lengths are encoded using DELTA_BINARY_PACKED encoding.
    /// Suffixes are stored using DELTA_LENGTH_BYTE_ARRAY encoding.
    DELTA_BYTE_ARRAY,

    /// Dictionary encoding.
    ///
    /// The ids are encoded using the RLE encoding.
    RLE_DICTIONARY,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::CompressionCodec`

/// Supported compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compression {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::PageType`

/// Available data pages for Parquet file format.
/// Note that some of the page types may not be supported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    DATA_PAGE,
    INDEX_PAGE,
    DICTIONARY_PAGE,
    DATA_PAGE_V2,
}

// ----------------------------------------------------------------------
// Mirrors `parquet::ColumnOrder`

/// Sort order for page and column statistics.
///
/// Types are associated with sort orders and column stats are aggregated using a sort
/// order, and a sort order should be considered when comparing values with statistics
/// min/max.
///
/// See reference in
/// <https://github.com/apache/parquet-cpp/blob/master/src/parquet/types.h>
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SortOrder {
    /// Signed (either value or legacy byte-wise) comparison.
    SIGNED,
    /// Unsigned (depending on physical type either value or byte-wise) comparison.
    UNSIGNED,
    /// Comparison is undefined.
    UNDEFINED,
}

/// Column order that specifies what method was used to aggregate min/max values for
/// statistics.
///
/// If column order is undefined, then it is the legacy behaviour and all values should
/// be compared as signed values/bytes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnOrder {
    /// Column uses the order defined by its logical or physical type
    /// (if there is no logical type), parquet-format 2.4.0+.
    TYPE_DEFINED_ORDER(SortOrder),
    /// Undefined column order, means legacy behaviour before parquet-format 2.4.0.
    /// Sort order is always SIGNED.
    UNDEFINED,
}

impl ColumnOrder {
    /// Returns sort order for a physical/logical type.
    pub fn get_sort_order(
        logical_type: Option<LogicalType>,
        converted_type: ConvertedType,
        physical_type: Type,
    ) -> SortOrder {
        // TODO: Should this take converted and logical type, for compatibility?
        match logical_type {
            Some(logical) => match logical {
                LogicalType::STRING(_)
                | LogicalType::ENUM(_)
                | LogicalType::JSON(_)
                | LogicalType::BSON(_) => SortOrder::UNSIGNED,
                LogicalType::INTEGER(t) => match t.is_signed {
                    true => SortOrder::SIGNED,
                    false => SortOrder::UNSIGNED,
                },
                LogicalType::MAP(_) | LogicalType::LIST(_) => SortOrder::UNDEFINED,
                LogicalType::DECIMAL(_) => SortOrder::SIGNED,
                LogicalType::DATE(_) => SortOrder::SIGNED,
                LogicalType::TIME(_) => SortOrder::SIGNED,
                LogicalType::TIMESTAMP(_) => SortOrder::SIGNED,
                LogicalType::UNKNOWN(_) => SortOrder::UNDEFINED,
                LogicalType::UUID(_) => SortOrder::UNSIGNED,
            },
            // Fall back to converted type
            None => Self::get_converted_sort_order(converted_type, physical_type),
        }
    }

    fn get_converted_sort_order(
        converted_type: ConvertedType,
        physical_type: Type,
    ) -> SortOrder {
        match converted_type {
            // Unsigned byte-wise comparison.
            ConvertedType::UTF8
            | ConvertedType::JSON
            | ConvertedType::BSON
            | ConvertedType::ENUM => SortOrder::UNSIGNED,

            ConvertedType::INT_8
            | ConvertedType::INT_16
            | ConvertedType::INT_32
            | ConvertedType::INT_64 => SortOrder::SIGNED,

            ConvertedType::UINT_8
            | ConvertedType::UINT_16
            | ConvertedType::UINT_32
            | ConvertedType::UINT_64 => SortOrder::UNSIGNED,

            // Signed comparison of the represented value.
            ConvertedType::DECIMAL => SortOrder::SIGNED,

            ConvertedType::DATE => SortOrder::SIGNED,

            ConvertedType::TIME_MILLIS
            | ConvertedType::TIME_MICROS
            | ConvertedType::TIMESTAMP_MILLIS
            | ConvertedType::TIMESTAMP_MICROS => SortOrder::SIGNED,

            ConvertedType::INTERVAL => SortOrder::UNDEFINED,

            ConvertedType::LIST | ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => {
                SortOrder::UNDEFINED
            }

            // Fall back to physical type.
            ConvertedType::NONE => Self::get_default_sort_order(physical_type),
        }
    }

    /// Returns default sort order based on physical type.
    fn get_default_sort_order(physical_type: Type) -> SortOrder {
        match physical_type {
            // Order: false, true
            Type::BOOLEAN => SortOrder::UNSIGNED,
            Type::INT32 | Type::INT64 => SortOrder::SIGNED,
            Type::INT96 => SortOrder::UNDEFINED,
            // Notes to remember when comparing float/double values:
            // If the min is a NaN, it should be ignored.
            // If the max is a NaN, it should be ignored.
            // If the min is +0, the row group may contain -0 values as well.
            // If the max is -0, the row group may contain +0 values as well.
            // When looking for NaN values, min and max should be ignored.
            Type::FLOAT | Type::DOUBLE => SortOrder::SIGNED,
            // Unsigned byte-wise comparison
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => SortOrder::UNSIGNED,
        }
    }

    /// Returns sort order associated with this column order.
    pub fn sort_order(&self) -> SortOrder {
        match *self {
            ColumnOrder::TYPE_DEFINED_ORDER(order) => order,
            ColumnOrder::UNDEFINED => SortOrder::SIGNED,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for ConvertedType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for Repetition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for PageType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Display for ColumnOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// ----------------------------------------------------------------------
// parquet::Type <=> Type conversion

impl convert::From<parquet::Type> for Type {
    fn from(value: parquet::Type) -> Self {
        match value {
            parquet::Type::Boolean => Type::BOOLEAN,
            parquet::Type::Int32 => Type::INT32,
            parquet::Type::Int64 => Type::INT64,
            parquet::Type::Int96 => Type::INT96,
            parquet::Type::Float => Type::FLOAT,
            parquet::Type::Double => Type::DOUBLE,
            parquet::Type::ByteArray => Type::BYTE_ARRAY,
            parquet::Type::FixedLenByteArray => Type::FIXED_LEN_BYTE_ARRAY,
        }
    }
}

impl convert::From<Type> for parquet::Type {
    fn from(value: Type) -> Self {
        match value {
            Type::BOOLEAN => parquet::Type::Boolean,
            Type::INT32 => parquet::Type::Int32,
            Type::INT64 => parquet::Type::Int64,
            Type::INT96 => parquet::Type::Int96,
            Type::FLOAT => parquet::Type::Float,
            Type::DOUBLE => parquet::Type::Double,
            Type::BYTE_ARRAY => parquet::Type::ByteArray,
            Type::FIXED_LEN_BYTE_ARRAY => parquet::Type::FixedLenByteArray,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::ConvertedType <=> ConvertedType conversion

impl convert::From<Option<parquet::ConvertedType>> for ConvertedType {
    fn from(option: Option<parquet::ConvertedType>) -> Self {
        match option {
            None => ConvertedType::NONE,
            Some(value) => match value {
                parquet::ConvertedType::Utf8 => ConvertedType::UTF8,
                parquet::ConvertedType::Map => ConvertedType::MAP,
                parquet::ConvertedType::MapKeyValue => ConvertedType::MAP_KEY_VALUE,
                parquet::ConvertedType::List => ConvertedType::LIST,
                parquet::ConvertedType::Enum => ConvertedType::ENUM,
                parquet::ConvertedType::Decimal => ConvertedType::DECIMAL,
                parquet::ConvertedType::Date => ConvertedType::DATE,
                parquet::ConvertedType::TimeMillis => ConvertedType::TIME_MILLIS,
                parquet::ConvertedType::TimeMicros => ConvertedType::TIME_MICROS,
                parquet::ConvertedType::TimestampMillis => {
                    ConvertedType::TIMESTAMP_MILLIS
                }
                parquet::ConvertedType::TimestampMicros => {
                    ConvertedType::TIMESTAMP_MICROS
                }
                parquet::ConvertedType::Uint8 => ConvertedType::UINT_8,
                parquet::ConvertedType::Uint16 => ConvertedType::UINT_16,
                parquet::ConvertedType::Uint32 => ConvertedType::UINT_32,
                parquet::ConvertedType::Uint64 => ConvertedType::UINT_64,
                parquet::ConvertedType::Int8 => ConvertedType::INT_8,
                parquet::ConvertedType::Int16 => ConvertedType::INT_16,
                parquet::ConvertedType::Int32 => ConvertedType::INT_32,
                parquet::ConvertedType::Int64 => ConvertedType::INT_64,
                parquet::ConvertedType::Json => ConvertedType::JSON,
                parquet::ConvertedType::Bson => ConvertedType::BSON,
                parquet::ConvertedType::Interval => ConvertedType::INTERVAL,
            },
        }
    }
}

impl convert::From<ConvertedType> for Option<parquet::ConvertedType> {
    fn from(value: ConvertedType) -> Self {
        match value {
            ConvertedType::NONE => None,
            ConvertedType::UTF8 => Some(parquet::ConvertedType::Utf8),
            ConvertedType::MAP => Some(parquet::ConvertedType::Map),
            ConvertedType::MAP_KEY_VALUE => Some(parquet::ConvertedType::MapKeyValue),
            ConvertedType::LIST => Some(parquet::ConvertedType::List),
            ConvertedType::ENUM => Some(parquet::ConvertedType::Enum),
            ConvertedType::DECIMAL => Some(parquet::ConvertedType::Decimal),
            ConvertedType::DATE => Some(parquet::ConvertedType::Date),
            ConvertedType::TIME_MILLIS => Some(parquet::ConvertedType::TimeMillis),
            ConvertedType::TIME_MICROS => Some(parquet::ConvertedType::TimeMicros),
            ConvertedType::TIMESTAMP_MILLIS => {
                Some(parquet::ConvertedType::TimestampMillis)
            }
            ConvertedType::TIMESTAMP_MICROS => {
                Some(parquet::ConvertedType::TimestampMicros)
            }
            ConvertedType::UINT_8 => Some(parquet::ConvertedType::Uint8),
            ConvertedType::UINT_16 => Some(parquet::ConvertedType::Uint16),
            ConvertedType::UINT_32 => Some(parquet::ConvertedType::Uint32),
            ConvertedType::UINT_64 => Some(parquet::ConvertedType::Uint64),
            ConvertedType::INT_8 => Some(parquet::ConvertedType::Int8),
            ConvertedType::INT_16 => Some(parquet::ConvertedType::Int16),
            ConvertedType::INT_32 => Some(parquet::ConvertedType::Int32),
            ConvertedType::INT_64 => Some(parquet::ConvertedType::Int64),
            ConvertedType::JSON => Some(parquet::ConvertedType::Json),
            ConvertedType::BSON => Some(parquet::ConvertedType::Bson),
            ConvertedType::INTERVAL => Some(parquet::ConvertedType::Interval),
        }
    }
}

// ----------------------------------------------------------------------
// parquet::LogicalType <=> LogicalType conversion

impl convert::From<parquet::LogicalType> for LogicalType {
    fn from(value: parquet::LogicalType) -> Self {
        match value {
            parquet::LogicalType::STRING(t) => LogicalType::STRING(t),
            parquet::LogicalType::MAP(t) => LogicalType::MAP(t),
            parquet::LogicalType::LIST(t) => LogicalType::LIST(t),
            parquet::LogicalType::ENUM(t) => LogicalType::ENUM(t),
            parquet::LogicalType::DECIMAL(t) => LogicalType::DECIMAL(t),
            parquet::LogicalType::DATE(t) => LogicalType::DATE(t),
            parquet::LogicalType::TIME(t) => LogicalType::TIME(t),
            parquet::LogicalType::TIMESTAMP(t) => LogicalType::TIMESTAMP(t),
            parquet::LogicalType::INTEGER(t) => LogicalType::INTEGER(t),
            parquet::LogicalType::UNKNOWN(t) => LogicalType::UNKNOWN(t),
            parquet::LogicalType::JSON(t) => LogicalType::JSON(t),
            parquet::LogicalType::BSON(t) => LogicalType::BSON(t),
            parquet::LogicalType::UUID(t) => LogicalType::UUID(t),
        }
    }
}

impl convert::From<LogicalType> for parquet::LogicalType {
    fn from(value: LogicalType) -> Self {
        match value {
            LogicalType::STRING(t) => parquet::LogicalType::STRING(t),
            LogicalType::MAP(t) => parquet::LogicalType::MAP(t),
            LogicalType::LIST(t) => parquet::LogicalType::LIST(t),
            LogicalType::ENUM(t) => parquet::LogicalType::ENUM(t),
            LogicalType::DECIMAL(t) => parquet::LogicalType::DECIMAL(t),
            LogicalType::DATE(t) => parquet::LogicalType::DATE(t),
            LogicalType::TIME(t) => parquet::LogicalType::TIME(t),
            LogicalType::TIMESTAMP(t) => parquet::LogicalType::TIMESTAMP(t),
            LogicalType::INTEGER(t) => parquet::LogicalType::INTEGER(t),
            LogicalType::UNKNOWN(t) => parquet::LogicalType::UNKNOWN(t),
            LogicalType::JSON(t) => parquet::LogicalType::JSON(t),
            LogicalType::BSON(t) => parquet::LogicalType::BSON(t),
            LogicalType::UUID(t) => parquet::LogicalType::UUID(t),
        }
    }
}

// ----------------------------------------------------------------------
// LogicalType <=> ConvertedType conversion

// Note: To prevent type loss when converting from ConvertedType to LogicalType,
// the conversion from ConvertedType -> LogicalType is not implemented.
// Such type loss includes:
// - Not knowing the decimal scale and precision of ConvertedType
// - Time and timestamp nanosecond precision, that is not supported in ConvertedType.

impl From<Option<LogicalType>> for ConvertedType {
    fn from(value: Option<LogicalType>) -> Self {
        match value {
            Some(value) => match value {
                LogicalType::STRING(_) => ConvertedType::UTF8,
                LogicalType::MAP(_) => ConvertedType::MAP,
                LogicalType::LIST(_) => ConvertedType::LIST,
                LogicalType::ENUM(_) => ConvertedType::ENUM,
                LogicalType::DECIMAL(_) => ConvertedType::DECIMAL,
                LogicalType::DATE(_) => ConvertedType::DATE,
                LogicalType::TIME(t) => match t.unit {
                    TimeUnit::MILLIS(_) => ConvertedType::TIME_MILLIS,
                    TimeUnit::MICROS(_) => ConvertedType::TIME_MICROS,
                    TimeUnit::NANOS(_) => ConvertedType::NONE,
                },
                LogicalType::TIMESTAMP(t) => match t.unit {
                    TimeUnit::MILLIS(_) => ConvertedType::TIMESTAMP_MILLIS,
                    TimeUnit::MICROS(_) => ConvertedType::TIMESTAMP_MICROS,
                    TimeUnit::NANOS(_) => ConvertedType::NONE,
                },
                LogicalType::INTEGER(t) => match (t.bit_width, t.is_signed) {
                    (8, true) => ConvertedType::INT_8,
                    (16, true) => ConvertedType::INT_16,
                    (32, true) => ConvertedType::INT_32,
                    (64, true) => ConvertedType::INT_64,
                    (8, false) => ConvertedType::UINT_8,
                    (16, false) => ConvertedType::UINT_16,
                    (32, false) => ConvertedType::UINT_32,
                    (64, false) => ConvertedType::UINT_64,
                    t => panic!("Integer type {:?} is not supported", t),
                },
                LogicalType::UNKNOWN(_) => ConvertedType::NONE,
                LogicalType::JSON(_) => ConvertedType::JSON,
                LogicalType::BSON(_) => ConvertedType::BSON,
                LogicalType::UUID(_) => ConvertedType::NONE,
            },
            None => ConvertedType::NONE,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::FieldRepetitionType <=> Repetition conversion

impl convert::From<parquet::FieldRepetitionType> for Repetition {
    fn from(value: parquet::FieldRepetitionType) -> Self {
        match value {
            parquet::FieldRepetitionType::Required => Repetition::REQUIRED,
            parquet::FieldRepetitionType::Optional => Repetition::OPTIONAL,
            parquet::FieldRepetitionType::Repeated => Repetition::REPEATED,
        }
    }
}

impl convert::From<Repetition> for parquet::FieldRepetitionType {
    fn from(value: Repetition) -> Self {
        match value {
            Repetition::REQUIRED => parquet::FieldRepetitionType::Required,
            Repetition::OPTIONAL => parquet::FieldRepetitionType::Optional,
            Repetition::REPEATED => parquet::FieldRepetitionType::Repeated,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::Encoding <=> Encoding conversion

impl convert::From<parquet::Encoding> for Encoding {
    fn from(value: parquet::Encoding) -> Self {
        match value {
            parquet::Encoding::Plain => Encoding::PLAIN,
            parquet::Encoding::PlainDictionary => Encoding::PLAIN_DICTIONARY,
            parquet::Encoding::Rle => Encoding::RLE,
            parquet::Encoding::BitPacked => Encoding::BIT_PACKED,
            parquet::Encoding::DeltaBinaryPacked => Encoding::DELTA_BINARY_PACKED,
            parquet::Encoding::DeltaLengthByteArray => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            parquet::Encoding::DeltaByteArray => Encoding::DELTA_BYTE_ARRAY,
            parquet::Encoding::RleDictionary => Encoding::RLE_DICTIONARY,
        }
    }
}

impl convert::From<Encoding> for parquet::Encoding {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::PLAIN => parquet::Encoding::Plain,
            Encoding::PLAIN_DICTIONARY => parquet::Encoding::PlainDictionary,
            Encoding::RLE => parquet::Encoding::Rle,
            Encoding::BIT_PACKED => parquet::Encoding::BitPacked,
            Encoding::DELTA_BINARY_PACKED => parquet::Encoding::DeltaBinaryPacked,
            Encoding::DELTA_LENGTH_BYTE_ARRAY => parquet::Encoding::DeltaLengthByteArray,
            Encoding::DELTA_BYTE_ARRAY => parquet::Encoding::DeltaByteArray,
            Encoding::RLE_DICTIONARY => parquet::Encoding::RleDictionary,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::CompressionCodec <=> Compression conversion

impl convert::From<parquet::CompressionCodec> for Compression {
    fn from(value: parquet::CompressionCodec) -> Self {
        match value {
            parquet::CompressionCodec::Uncompressed => Compression::UNCOMPRESSED,
            parquet::CompressionCodec::Snappy => Compression::SNAPPY,
            parquet::CompressionCodec::Gzip => Compression::GZIP,
            parquet::CompressionCodec::Lzo => Compression::LZO,
            parquet::CompressionCodec::Brotli => Compression::BROTLI,
            parquet::CompressionCodec::Lz4 => Compression::LZ4,
            parquet::CompressionCodec::Zstd => Compression::ZSTD,
        }
    }
}

impl convert::From<Compression> for parquet::CompressionCodec {
    fn from(value: Compression) -> Self {
        match value {
            Compression::UNCOMPRESSED => parquet::CompressionCodec::Uncompressed,
            Compression::SNAPPY => parquet::CompressionCodec::Snappy,
            Compression::GZIP => parquet::CompressionCodec::Gzip,
            Compression::LZO => parquet::CompressionCodec::Lzo,
            Compression::BROTLI => parquet::CompressionCodec::Brotli,
            Compression::LZ4 => parquet::CompressionCodec::Lz4,
            Compression::ZSTD => parquet::CompressionCodec::Zstd,
        }
    }
}

// ----------------------------------------------------------------------
// parquet::PageType <=> PageType conversion

impl convert::From<parquet::PageType> for PageType {
    fn from(value: parquet::PageType) -> Self {
        match value {
            parquet::PageType::DataPage => PageType::DATA_PAGE,
            parquet::PageType::IndexPage => PageType::INDEX_PAGE,
            parquet::PageType::DictionaryPage => PageType::DICTIONARY_PAGE,
            parquet::PageType::DataPageV2 => PageType::DATA_PAGE_V2,
        }
    }
}

impl convert::From<PageType> for parquet::PageType {
    fn from(value: PageType) -> Self {
        match value {
            PageType::DATA_PAGE => parquet::PageType::DataPage,
            PageType::INDEX_PAGE => parquet::PageType::IndexPage,
            PageType::DICTIONARY_PAGE => parquet::PageType::DictionaryPage,
            PageType::DATA_PAGE_V2 => parquet::PageType::DataPageV2,
        }
    }
}

// ----------------------------------------------------------------------
// String conversions for schema parsing.

impl str::FromStr for Repetition {
    type Err = ParquetError;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "REQUIRED" => Ok(Repetition::REQUIRED),
            "OPTIONAL" => Ok(Repetition::OPTIONAL),
            "REPEATED" => Ok(Repetition::REPEATED),
            other => Err(general_err!("Invalid repetition {}", other)),
        }
    }
}

impl str::FromStr for Type {
    type Err = ParquetError;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "BOOLEAN" => Ok(Type::BOOLEAN),
            "INT32" => Ok(Type::INT32),
            "INT64" => Ok(Type::INT64),
            "INT96" => Ok(Type::INT96),
            "FLOAT" => Ok(Type::FLOAT),
            "DOUBLE" => Ok(Type::DOUBLE),
            "BYTE_ARRAY" | "BINARY" => Ok(Type::BYTE_ARRAY),
            "FIXED_LEN_BYTE_ARRAY" => Ok(Type::FIXED_LEN_BYTE_ARRAY),
            other => Err(general_err!("Invalid type {}", other)),
        }
    }
}

impl str::FromStr for ConvertedType {
    type Err = ParquetError;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "NONE" => Ok(ConvertedType::NONE),
            "UTF8" => Ok(ConvertedType::UTF8),
            "MAP" => Ok(ConvertedType::MAP),
            "MAP_KEY_VALUE" => Ok(ConvertedType::MAP_KEY_VALUE),
            "LIST" => Ok(ConvertedType::LIST),
            "ENUM" => Ok(ConvertedType::ENUM),
            "DECIMAL" => Ok(ConvertedType::DECIMAL),
            "DATE" => Ok(ConvertedType::DATE),
            "TIME_MILLIS" => Ok(ConvertedType::TIME_MILLIS),
            "TIME_MICROS" => Ok(ConvertedType::TIME_MICROS),
            "TIMESTAMP_MILLIS" => Ok(ConvertedType::TIMESTAMP_MILLIS),
            "TIMESTAMP_MICROS" => Ok(ConvertedType::TIMESTAMP_MICROS),
            "UINT_8" => Ok(ConvertedType::UINT_8),
            "UINT_16" => Ok(ConvertedType::UINT_16),
            "UINT_32" => Ok(ConvertedType::UINT_32),
            "UINT_64" => Ok(ConvertedType::UINT_64),
            "INT_8" => Ok(ConvertedType::INT_8),
            "INT_16" => Ok(ConvertedType::INT_16),
            "INT_32" => Ok(ConvertedType::INT_32),
            "INT_64" => Ok(ConvertedType::INT_64),
            "JSON" => Ok(ConvertedType::JSON),
            "BSON" => Ok(ConvertedType::BSON),
            "INTERVAL" => Ok(ConvertedType::INTERVAL),
            other => Err(general_err!("Invalid converted type {}", other)),
        }
    }
}

impl str::FromStr for LogicalType {
    type Err = ParquetError;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            // The type is a placeholder that gets updated elsewhere
            "INTEGER" => Ok(LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: false,
            })),
            "MAP" => Ok(LogicalType::MAP(MapType {})),
            "LIST" => Ok(LogicalType::LIST(ListType {})),
            "ENUM" => Ok(LogicalType::ENUM(EnumType {})),
            "DECIMAL" => Ok(LogicalType::DECIMAL(DecimalType {
                precision: -1,
                scale: -1,
            })),
            "DATE" => Ok(LogicalType::DATE(DateType {})),
            "TIME" => Ok(LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(parquet::MilliSeconds {}),
            })),
            "TIMESTAMP" => Ok(LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(parquet::MilliSeconds {}),
            })),
            "STRING" => Ok(LogicalType::STRING(StringType {})),
            "JSON" => Ok(LogicalType::JSON(JsonType {})),
            "BSON" => Ok(LogicalType::BSON(BsonType {})),
            "UUID" => Ok(LogicalType::UUID(UUIDType {})),
            "UNKNOWN" => Ok(LogicalType::UNKNOWN(NullType {})),
            "INTERVAL" => Err(general_err!("Interval logical type not yet supported")),
            other => Err(general_err!("Invalid logical type {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_type() {
        assert_eq!(Type::BOOLEAN.to_string(), "BOOLEAN");
        assert_eq!(Type::INT32.to_string(), "INT32");
        assert_eq!(Type::INT64.to_string(), "INT64");
        assert_eq!(Type::INT96.to_string(), "INT96");
        assert_eq!(Type::FLOAT.to_string(), "FLOAT");
        assert_eq!(Type::DOUBLE.to_string(), "DOUBLE");
        assert_eq!(Type::BYTE_ARRAY.to_string(), "BYTE_ARRAY");
        assert_eq!(
            Type::FIXED_LEN_BYTE_ARRAY.to_string(),
            "FIXED_LEN_BYTE_ARRAY"
        );
    }

    #[test]
    fn test_from_type() {
        assert_eq!(Type::from(parquet::Type::Boolean), Type::BOOLEAN);
        assert_eq!(Type::from(parquet::Type::Int32), Type::INT32);
        assert_eq!(Type::from(parquet::Type::Int64), Type::INT64);
        assert_eq!(Type::from(parquet::Type::Int96), Type::INT96);
        assert_eq!(Type::from(parquet::Type::Float), Type::FLOAT);
        assert_eq!(Type::from(parquet::Type::Double), Type::DOUBLE);
        assert_eq!(Type::from(parquet::Type::ByteArray), Type::BYTE_ARRAY);
        assert_eq!(
            Type::from(parquet::Type::FixedLenByteArray),
            Type::FIXED_LEN_BYTE_ARRAY
        );
    }

    #[test]
    fn test_into_type() {
        assert_eq!(parquet::Type::Boolean, Type::BOOLEAN.into());
        assert_eq!(parquet::Type::Int32, Type::INT32.into());
        assert_eq!(parquet::Type::Int64, Type::INT64.into());
        assert_eq!(parquet::Type::Int96, Type::INT96.into());
        assert_eq!(parquet::Type::Float, Type::FLOAT.into());
        assert_eq!(parquet::Type::Double, Type::DOUBLE.into());
        assert_eq!(parquet::Type::ByteArray, Type::BYTE_ARRAY.into());
        assert_eq!(
            parquet::Type::FixedLenByteArray,
            Type::FIXED_LEN_BYTE_ARRAY.into()
        );
    }

    #[test]
    fn test_from_string_into_type() {
        assert_eq!(
            Type::BOOLEAN.to_string().parse::<Type>().unwrap(),
            Type::BOOLEAN
        );
        assert_eq!(
            Type::INT32.to_string().parse::<Type>().unwrap(),
            Type::INT32
        );
        assert_eq!(
            Type::INT64.to_string().parse::<Type>().unwrap(),
            Type::INT64
        );
        assert_eq!(
            Type::INT96.to_string().parse::<Type>().unwrap(),
            Type::INT96
        );
        assert_eq!(
            Type::FLOAT.to_string().parse::<Type>().unwrap(),
            Type::FLOAT
        );
        assert_eq!(
            Type::DOUBLE.to_string().parse::<Type>().unwrap(),
            Type::DOUBLE
        );
        assert_eq!(
            Type::BYTE_ARRAY.to_string().parse::<Type>().unwrap(),
            Type::BYTE_ARRAY
        );
        assert_eq!("BINARY".parse::<Type>().unwrap(), Type::BYTE_ARRAY);
        assert_eq!(
            Type::FIXED_LEN_BYTE_ARRAY
                .to_string()
                .parse::<Type>()
                .unwrap(),
            Type::FIXED_LEN_BYTE_ARRAY
        );
    }

    #[test]
    fn test_display_converted_type() {
        assert_eq!(ConvertedType::NONE.to_string(), "NONE");
        assert_eq!(ConvertedType::UTF8.to_string(), "UTF8");
        assert_eq!(ConvertedType::MAP.to_string(), "MAP");
        assert_eq!(ConvertedType::MAP_KEY_VALUE.to_string(), "MAP_KEY_VALUE");
        assert_eq!(ConvertedType::LIST.to_string(), "LIST");
        assert_eq!(ConvertedType::ENUM.to_string(), "ENUM");
        assert_eq!(ConvertedType::DECIMAL.to_string(), "DECIMAL");
        assert_eq!(ConvertedType::DATE.to_string(), "DATE");
        assert_eq!(ConvertedType::TIME_MILLIS.to_string(), "TIME_MILLIS");
        assert_eq!(ConvertedType::DATE.to_string(), "DATE");
        assert_eq!(ConvertedType::TIME_MICROS.to_string(), "TIME_MICROS");
        assert_eq!(
            ConvertedType::TIMESTAMP_MILLIS.to_string(),
            "TIMESTAMP_MILLIS"
        );
        assert_eq!(
            ConvertedType::TIMESTAMP_MICROS.to_string(),
            "TIMESTAMP_MICROS"
        );
        assert_eq!(ConvertedType::UINT_8.to_string(), "UINT_8");
        assert_eq!(ConvertedType::UINT_16.to_string(), "UINT_16");
        assert_eq!(ConvertedType::UINT_32.to_string(), "UINT_32");
        assert_eq!(ConvertedType::UINT_64.to_string(), "UINT_64");
        assert_eq!(ConvertedType::INT_8.to_string(), "INT_8");
        assert_eq!(ConvertedType::INT_16.to_string(), "INT_16");
        assert_eq!(ConvertedType::INT_32.to_string(), "INT_32");
        assert_eq!(ConvertedType::INT_64.to_string(), "INT_64");
        assert_eq!(ConvertedType::JSON.to_string(), "JSON");
        assert_eq!(ConvertedType::BSON.to_string(), "BSON");
        assert_eq!(ConvertedType::INTERVAL.to_string(), "INTERVAL");
    }

    #[test]
    fn test_from_converted_type() {
        let parquet_conv_none: Option<parquet::ConvertedType> = None;
        assert_eq!(ConvertedType::from(parquet_conv_none), ConvertedType::NONE);
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Utf8)),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Map)),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::MapKeyValue)),
            ConvertedType::MAP_KEY_VALUE
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::List)),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Enum)),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Decimal)),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Date)),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::TimeMillis)),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::TimeMicros)),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::TimestampMillis)),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::TimestampMicros)),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Uint8)),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Uint16)),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Uint32)),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Uint64)),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Int8)),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Int16)),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Int32)),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Int64)),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Json)),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Bson)),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::from(Some(parquet::ConvertedType::Interval)),
            ConvertedType::INTERVAL
        );
    }

    #[test]
    fn test_into_converted_type() {
        let converted_type: Option<parquet::ConvertedType> = None;
        assert_eq!(converted_type, ConvertedType::NONE.into());
        assert_eq!(
            Some(parquet::ConvertedType::Utf8),
            ConvertedType::UTF8.into()
        );
        assert_eq!(Some(parquet::ConvertedType::Map), ConvertedType::MAP.into());
        assert_eq!(
            Some(parquet::ConvertedType::MapKeyValue),
            ConvertedType::MAP_KEY_VALUE.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::List),
            ConvertedType::LIST.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Enum),
            ConvertedType::ENUM.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Decimal),
            ConvertedType::DECIMAL.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Date),
            ConvertedType::DATE.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimeMillis),
            ConvertedType::TIME_MILLIS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimeMicros),
            ConvertedType::TIME_MICROS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimestampMillis),
            ConvertedType::TIMESTAMP_MILLIS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::TimestampMicros),
            ConvertedType::TIMESTAMP_MICROS.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint8),
            ConvertedType::UINT_8.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint16),
            ConvertedType::UINT_16.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint32),
            ConvertedType::UINT_32.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Uint64),
            ConvertedType::UINT_64.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Int8),
            ConvertedType::INT_8.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Int16),
            ConvertedType::INT_16.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Int32),
            ConvertedType::INT_32.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Int64),
            ConvertedType::INT_64.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Json),
            ConvertedType::JSON.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Bson),
            ConvertedType::BSON.into()
        );
        assert_eq!(
            Some(parquet::ConvertedType::Interval),
            ConvertedType::INTERVAL.into()
        );
    }

    #[test]
    fn test_from_string_into_converted_type() {
        assert_eq!(
            ConvertedType::NONE
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::UTF8
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::MAP
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::MAP_KEY_VALUE
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::MAP_KEY_VALUE
        );
        assert_eq!(
            ConvertedType::LIST
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::ENUM
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::DECIMAL
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::DATE
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::TIME_MILLIS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::TIME_MICROS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::TIMESTAMP_MILLIS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::TIMESTAMP_MICROS
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::UINT_8
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::UINT_16
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::UINT_32
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::UINT_64
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::INT_8
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::INT_16
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::INT_32
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::INT_64
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::JSON
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::BSON
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::INTERVAL
                .to_string()
                .parse::<ConvertedType>()
                .unwrap(),
            ConvertedType::INTERVAL
        );
    }

    #[test]
    fn test_logical_to_converted_type() {
        let logical_none: Option<LogicalType> = None;
        assert_eq!(ConvertedType::from(logical_none), ConvertedType::NONE);
        assert_eq!(
            ConvertedType::from(Some(LogicalType::DECIMAL(DecimalType {
                precision: 20,
                scale: 5
            }))),
            ConvertedType::DECIMAL
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::BSON(Default::default()))),
            ConvertedType::BSON
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::JSON(Default::default()))),
            ConvertedType::JSON
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::STRING(Default::default()))),
            ConvertedType::UTF8
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::DATE(Default::default()))),
            ConvertedType::DATE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::TIME(TimeType {
                unit: TimeUnit::MILLIS(Default::default()),
                is_adjusted_to_u_t_c: true,
            }))),
            ConvertedType::TIME_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::TIME(TimeType {
                unit: TimeUnit::MICROS(Default::default()),
                is_adjusted_to_u_t_c: true,
            }))),
            ConvertedType::TIME_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::TIME(TimeType {
                unit: TimeUnit::NANOS(Default::default()),
                is_adjusted_to_u_t_c: false,
            }))),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::TIMESTAMP(TimestampType {
                unit: TimeUnit::MILLIS(Default::default()),
                is_adjusted_to_u_t_c: true,
            }))),
            ConvertedType::TIMESTAMP_MILLIS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::TIMESTAMP(TimestampType {
                unit: TimeUnit::MICROS(Default::default()),
                is_adjusted_to_u_t_c: false,
            }))),
            ConvertedType::TIMESTAMP_MICROS
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::TIMESTAMP(TimestampType {
                unit: TimeUnit::NANOS(Default::default()),
                is_adjusted_to_u_t_c: false,
            }))),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: false
            }))),
            ConvertedType::UINT_8
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: true
            }))),
            ConvertedType::INT_8
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 16,
                is_signed: false
            }))),
            ConvertedType::UINT_16
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 16,
                is_signed: true
            }))),
            ConvertedType::INT_16
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 32,
                is_signed: false
            }))),
            ConvertedType::UINT_32
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 32,
                is_signed: true
            }))),
            ConvertedType::INT_32
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 64,
                is_signed: false
            }))),
            ConvertedType::UINT_64
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::INTEGER(IntType {
                bit_width: 64,
                is_signed: true
            }))),
            ConvertedType::INT_64
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::LIST(Default::default()))),
            ConvertedType::LIST
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::MAP(Default::default()))),
            ConvertedType::MAP
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::UUID(Default::default()))),
            ConvertedType::NONE
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::ENUM(Default::default()))),
            ConvertedType::ENUM
        );
        assert_eq!(
            ConvertedType::from(Some(LogicalType::UNKNOWN(Default::default()))),
            ConvertedType::NONE
        );
    }

    #[test]
    fn test_display_repetition() {
        assert_eq!(Repetition::REQUIRED.to_string(), "REQUIRED");
        assert_eq!(Repetition::OPTIONAL.to_string(), "OPTIONAL");
        assert_eq!(Repetition::REPEATED.to_string(), "REPEATED");
    }

    #[test]
    fn test_from_repetition() {
        assert_eq!(
            Repetition::from(parquet::FieldRepetitionType::Required),
            Repetition::REQUIRED
        );
        assert_eq!(
            Repetition::from(parquet::FieldRepetitionType::Optional),
            Repetition::OPTIONAL
        );
        assert_eq!(
            Repetition::from(parquet::FieldRepetitionType::Repeated),
            Repetition::REPEATED
        );
    }

    #[test]
    fn test_into_repetition() {
        assert_eq!(
            parquet::FieldRepetitionType::Required,
            Repetition::REQUIRED.into()
        );
        assert_eq!(
            parquet::FieldRepetitionType::Optional,
            Repetition::OPTIONAL.into()
        );
        assert_eq!(
            parquet::FieldRepetitionType::Repeated,
            Repetition::REPEATED.into()
        );
    }

    #[test]
    fn test_from_string_into_repetition() {
        assert_eq!(
            Repetition::REQUIRED
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::REQUIRED
        );
        assert_eq!(
            Repetition::OPTIONAL
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::OPTIONAL
        );
        assert_eq!(
            Repetition::REPEATED
                .to_string()
                .parse::<Repetition>()
                .unwrap(),
            Repetition::REPEATED
        );
    }

    #[test]
    fn test_display_encoding() {
        assert_eq!(Encoding::PLAIN.to_string(), "PLAIN");
        assert_eq!(Encoding::PLAIN_DICTIONARY.to_string(), "PLAIN_DICTIONARY");
        assert_eq!(Encoding::RLE.to_string(), "RLE");
        assert_eq!(Encoding::BIT_PACKED.to_string(), "BIT_PACKED");
        assert_eq!(
            Encoding::DELTA_BINARY_PACKED.to_string(),
            "DELTA_BINARY_PACKED"
        );
        assert_eq!(
            Encoding::DELTA_LENGTH_BYTE_ARRAY.to_string(),
            "DELTA_LENGTH_BYTE_ARRAY"
        );
        assert_eq!(Encoding::DELTA_BYTE_ARRAY.to_string(), "DELTA_BYTE_ARRAY");
        assert_eq!(Encoding::RLE_DICTIONARY.to_string(), "RLE_DICTIONARY");
    }

    #[test]
    fn test_from_encoding() {
        assert_eq!(Encoding::from(parquet::Encoding::Plain), Encoding::PLAIN);
        assert_eq!(
            Encoding::from(parquet::Encoding::PlainDictionary),
            Encoding::PLAIN_DICTIONARY
        );
        assert_eq!(Encoding::from(parquet::Encoding::Rle), Encoding::RLE);
        assert_eq!(
            Encoding::from(parquet::Encoding::BitPacked),
            Encoding::BIT_PACKED
        );
        assert_eq!(
            Encoding::from(parquet::Encoding::DeltaBinaryPacked),
            Encoding::DELTA_BINARY_PACKED
        );
        assert_eq!(
            Encoding::from(parquet::Encoding::DeltaLengthByteArray),
            Encoding::DELTA_LENGTH_BYTE_ARRAY
        );
        assert_eq!(
            Encoding::from(parquet::Encoding::DeltaByteArray),
            Encoding::DELTA_BYTE_ARRAY
        );
    }

    #[test]
    fn test_into_encoding() {
        assert_eq!(parquet::Encoding::Plain, Encoding::PLAIN.into());
        assert_eq!(
            parquet::Encoding::PlainDictionary,
            Encoding::PLAIN_DICTIONARY.into()
        );
        assert_eq!(parquet::Encoding::Rle, Encoding::RLE.into());
        assert_eq!(parquet::Encoding::BitPacked, Encoding::BIT_PACKED.into());
        assert_eq!(
            parquet::Encoding::DeltaBinaryPacked,
            Encoding::DELTA_BINARY_PACKED.into()
        );
        assert_eq!(
            parquet::Encoding::DeltaLengthByteArray,
            Encoding::DELTA_LENGTH_BYTE_ARRAY.into()
        );
        assert_eq!(
            parquet::Encoding::DeltaByteArray,
            Encoding::DELTA_BYTE_ARRAY.into()
        );
    }

    #[test]
    fn test_display_compression() {
        assert_eq!(Compression::UNCOMPRESSED.to_string(), "UNCOMPRESSED");
        assert_eq!(Compression::SNAPPY.to_string(), "SNAPPY");
        assert_eq!(Compression::GZIP.to_string(), "GZIP");
        assert_eq!(Compression::LZO.to_string(), "LZO");
        assert_eq!(Compression::BROTLI.to_string(), "BROTLI");
        assert_eq!(Compression::LZ4.to_string(), "LZ4");
        assert_eq!(Compression::ZSTD.to_string(), "ZSTD");
    }

    #[test]
    fn test_from_compression() {
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Uncompressed),
            Compression::UNCOMPRESSED
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Snappy),
            Compression::SNAPPY
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Gzip),
            Compression::GZIP
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Lzo),
            Compression::LZO
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Brotli),
            Compression::BROTLI
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Lz4),
            Compression::LZ4
        );
        assert_eq!(
            Compression::from(parquet::CompressionCodec::Zstd),
            Compression::ZSTD
        );
    }

    #[test]
    fn test_into_compression() {
        assert_eq!(
            parquet::CompressionCodec::Uncompressed,
            Compression::UNCOMPRESSED.into()
        );
        assert_eq!(
            parquet::CompressionCodec::Snappy,
            Compression::SNAPPY.into()
        );
        assert_eq!(parquet::CompressionCodec::Gzip, Compression::GZIP.into());
        assert_eq!(parquet::CompressionCodec::Lzo, Compression::LZO.into());
        assert_eq!(
            parquet::CompressionCodec::Brotli,
            Compression::BROTLI.into()
        );
        assert_eq!(parquet::CompressionCodec::Lz4, Compression::LZ4.into());
        assert_eq!(parquet::CompressionCodec::Zstd, Compression::ZSTD.into());
    }

    #[test]
    fn test_display_page_type() {
        assert_eq!(PageType::DATA_PAGE.to_string(), "DATA_PAGE");
        assert_eq!(PageType::INDEX_PAGE.to_string(), "INDEX_PAGE");
        assert_eq!(PageType::DICTIONARY_PAGE.to_string(), "DICTIONARY_PAGE");
        assert_eq!(PageType::DATA_PAGE_V2.to_string(), "DATA_PAGE_V2");
    }

    #[test]
    fn test_from_page_type() {
        assert_eq!(
            PageType::from(parquet::PageType::DataPage),
            PageType::DATA_PAGE
        );
        assert_eq!(
            PageType::from(parquet::PageType::IndexPage),
            PageType::INDEX_PAGE
        );
        assert_eq!(
            PageType::from(parquet::PageType::DictionaryPage),
            PageType::DICTIONARY_PAGE
        );
        assert_eq!(
            PageType::from(parquet::PageType::DataPageV2),
            PageType::DATA_PAGE_V2
        );
    }

    #[test]
    fn test_into_page_type() {
        assert_eq!(parquet::PageType::DataPage, PageType::DATA_PAGE.into());
        assert_eq!(parquet::PageType::IndexPage, PageType::INDEX_PAGE.into());
        assert_eq!(
            parquet::PageType::DictionaryPage,
            PageType::DICTIONARY_PAGE.into()
        );
        assert_eq!(parquet::PageType::DataPageV2, PageType::DATA_PAGE_V2.into());
    }

    #[test]
    fn test_display_sort_order() {
        assert_eq!(SortOrder::SIGNED.to_string(), "SIGNED");
        assert_eq!(SortOrder::UNSIGNED.to_string(), "UNSIGNED");
        assert_eq!(SortOrder::UNDEFINED.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_display_column_order() {
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED).to_string(),
            "TYPE_DEFINED_ORDER(SIGNED)"
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED).to_string(),
            "TYPE_DEFINED_ORDER(UNSIGNED)"
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED).to_string(),
            "TYPE_DEFINED_ORDER(UNDEFINED)"
        );
        assert_eq!(ColumnOrder::UNDEFINED.to_string(), "UNDEFINED");
    }

    #[test]
    fn test_column_order_get_logical_type_sort_order() {
        // Helper to check the order in a list of values.
        // Only logical type is checked.
        fn check_sort_order(types: Vec<LogicalType>, expected_order: SortOrder) {
            for tpe in types {
                assert_eq!(
                    ColumnOrder::get_sort_order(
                        Some(tpe),
                        ConvertedType::NONE,
                        Type::BYTE_ARRAY
                    ),
                    expected_order
                );
            }
        }

        // Unsigned comparison (physical type does not matter)
        let unsigned = vec![
            LogicalType::STRING(Default::default()),
            LogicalType::JSON(Default::default()),
            LogicalType::BSON(Default::default()),
            LogicalType::ENUM(Default::default()),
            LogicalType::UUID(Default::default()),
            LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: false,
            }),
            LogicalType::INTEGER(IntType {
                bit_width: 16,
                is_signed: false,
            }),
            LogicalType::INTEGER(IntType {
                bit_width: 32,
                is_signed: false,
            }),
            LogicalType::INTEGER(IntType {
                bit_width: 64,
                is_signed: false,
            }),
        ];
        check_sort_order(unsigned, SortOrder::UNSIGNED);

        // Signed comparison (physical type does not matter)
        let signed = vec![
            LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: true,
            }),
            LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: true,
            }),
            LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: true,
            }),
            LogicalType::INTEGER(IntType {
                bit_width: 8,
                is_signed: true,
            }),
            LogicalType::DECIMAL(DecimalType {
                scale: 20,
                precision: 4,
            }),
            LogicalType::DATE(Default::default()),
            LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(Default::default()),
            }),
            LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS(Default::default()),
            }),
            LogicalType::TIME(TimeType {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS(Default::default()),
            }),
            LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MILLIS(Default::default()),
            }),
            LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c: false,
                unit: TimeUnit::MICROS(Default::default()),
            }),
            LogicalType::TIMESTAMP(TimestampType {
                is_adjusted_to_u_t_c: true,
                unit: TimeUnit::NANOS(Default::default()),
            }),
        ];
        check_sort_order(signed, SortOrder::SIGNED);

        // Undefined comparison
        let undefined = vec![
            LogicalType::LIST(Default::default()),
            LogicalType::MAP(Default::default()),
        ];
        check_sort_order(undefined, SortOrder::UNDEFINED);
    }

    #[test]
    fn test_column_order_get_coverted_type_sort_order() {
        // Helper to check the order in a list of values.
        // Only converted type is checked.
        fn check_sort_order(types: Vec<ConvertedType>, expected_order: SortOrder) {
            for tpe in types {
                assert_eq!(
                    ColumnOrder::get_sort_order(None, tpe, Type::BYTE_ARRAY),
                    expected_order
                );
            }
        }

        // Unsigned comparison (physical type does not matter)
        let unsigned = vec![
            ConvertedType::UTF8,
            ConvertedType::JSON,
            ConvertedType::BSON,
            ConvertedType::ENUM,
            ConvertedType::UINT_8,
            ConvertedType::UINT_16,
            ConvertedType::UINT_32,
            ConvertedType::UINT_64,
        ];
        check_sort_order(unsigned, SortOrder::UNSIGNED);

        // Signed comparison (physical type does not matter)
        let signed = vec![
            ConvertedType::INT_8,
            ConvertedType::INT_16,
            ConvertedType::INT_32,
            ConvertedType::INT_64,
            ConvertedType::DECIMAL,
            ConvertedType::DATE,
            ConvertedType::TIME_MILLIS,
            ConvertedType::TIME_MICROS,
            ConvertedType::TIMESTAMP_MILLIS,
            ConvertedType::TIMESTAMP_MICROS,
        ];
        check_sort_order(signed, SortOrder::SIGNED);

        // Undefined comparison
        let undefined = vec![
            ConvertedType::LIST,
            ConvertedType::MAP,
            ConvertedType::MAP_KEY_VALUE,
            ConvertedType::INTERVAL,
        ];
        check_sort_order(undefined, SortOrder::UNDEFINED);

        // Check None logical type
        // This should return a sort order for byte array type.
        check_sort_order(vec![ConvertedType::NONE], SortOrder::UNSIGNED);
    }

    #[test]
    fn test_column_order_get_default_sort_order() {
        // Comparison based on physical type
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::BOOLEAN),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::INT32),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::INT64),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::INT96),
            SortOrder::UNDEFINED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::FLOAT),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::DOUBLE),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::get_default_sort_order(Type::FIXED_LEN_BYTE_ARRAY),
            SortOrder::UNSIGNED
        );
    }

    #[test]
    fn test_column_order_sort_order() {
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::SIGNED).sort_order(),
            SortOrder::SIGNED
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED).sort_order(),
            SortOrder::UNSIGNED
        );
        assert_eq!(
            ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNDEFINED).sort_order(),
            SortOrder::UNDEFINED
        );
        assert_eq!(ColumnOrder::UNDEFINED.sort_order(), SortOrder::SIGNED);
    }
}
