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

#include <cmath>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/util/checked_cast.h"
#include "arrow/util/compression.h"
#include "arrow/util/logging.h"

#include "parquet/exception.h"
#include "parquet/parquet_types.h"
#include "parquet/types.h"

using ::arrow::internal::checked_cast;
using arrow::util::Codec;

namespace parquet {

std::unique_ptr<Codec> GetCodecFromArrow(Compression::type codec) {
  std::unique_ptr<Codec> result;
  switch (codec) {
    case Compression::UNCOMPRESSED:
      break;
    case Compression::SNAPPY:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::SNAPPY, &result));
      break;
    case Compression::GZIP:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::GZIP, &result));
      break;
    case Compression::LZO:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::LZO, &result));
      break;
    case Compression::BROTLI:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::BROTLI, &result));
      break;
    case Compression::LZ4:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::LZ4, &result));
      break;
    case Compression::ZSTD:
      PARQUET_THROW_NOT_OK(Codec::Create(::arrow::Compression::ZSTD, &result));
      break;
    default:
      break;
  }
  return result;
}

std::string FormatStatValue(Type::type parquet_type, const std::string& val) {
  std::stringstream result;
  switch (parquet_type) {
    case Type::BOOLEAN:
      result << reinterpret_cast<const bool*>(val.c_str())[0];
      break;
    case Type::INT32:
      result << reinterpret_cast<const int32_t*>(val.c_str())[0];
      break;
    case Type::INT64:
      result << reinterpret_cast<const int64_t*>(val.c_str())[0];
      break;
    case Type::DOUBLE:
      result << reinterpret_cast<const double*>(val.c_str())[0];
      break;
    case Type::FLOAT:
      result << reinterpret_cast<const float*>(val.c_str())[0];
      break;
    case Type::INT96: {
      auto const i32_val = reinterpret_cast<const int32_t*>(val.c_str());
      result << i32_val[0] << " " << i32_val[1] << " " << i32_val[2];
      break;
    }
    case Type::BYTE_ARRAY: {
      return val;
    }
    case Type::FIXED_LEN_BYTE_ARRAY: {
      return val;
    }
    case Type::UNDEFINED:
    default:
      break;
  }
  return result.str();
}

std::string FormatStatValue(Type::type parquet_type, const char* val) {
  std::stringstream result;
  switch (parquet_type) {
    case Type::BOOLEAN:
      result << reinterpret_cast<const bool*>(val)[0];
      break;
    case Type::INT32:
      result << reinterpret_cast<const int32_t*>(val)[0];
      break;
    case Type::INT64:
      result << reinterpret_cast<const int64_t*>(val)[0];
      break;
    case Type::DOUBLE:
      result << reinterpret_cast<const double*>(val)[0];
      break;
    case Type::FLOAT:
      result << reinterpret_cast<const float*>(val)[0];
      break;
    case Type::INT96: {
      auto const i32_val = reinterpret_cast<const int32_t*>(val);
      result << i32_val[0] << " " << i32_val[1] << " " << i32_val[2];
      break;
    }
    case Type::BYTE_ARRAY: {
      result << val;
      break;
    }
    case Type::FIXED_LEN_BYTE_ARRAY: {
      result << val;
      break;
    }
    case Type::UNDEFINED:
    default:
      break;
  }
  return result.str();
}

std::string EncodingToString(Encoding::type t) {
  switch (t) {
    case Encoding::PLAIN:
      return "PLAIN";
    case Encoding::PLAIN_DICTIONARY:
      return "PLAIN_DICTIONARY";
    case Encoding::RLE:
      return "RLE";
    case Encoding::BIT_PACKED:
      return "BIT_PACKED";
    case Encoding::DELTA_BINARY_PACKED:
      return "DELTA_BINARY_PACKED";
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
      return "DELTA_LENGTH_BYTE_ARRAY";
    case Encoding::DELTA_BYTE_ARRAY:
      return "DELTA_BYTE_ARRAY";
    case Encoding::RLE_DICTIONARY:
      return "RLE_DICTIONARY";
    default:
      return "UNKNOWN";
  }
}

std::string CompressionToString(Compression::type t) {
  switch (t) {
    case Compression::UNCOMPRESSED:
      return "UNCOMPRESSED";
    case Compression::SNAPPY:
      return "SNAPPY";
    case Compression::GZIP:
      return "GZIP";
    case Compression::LZO:
      return "LZO";
    case Compression::BROTLI:
      return "BROTLI";
    case Compression::LZ4:
      return "LZ4";
    case Compression::ZSTD:
      return "ZSTD";
    default:
      return "UNKNOWN";
  }
}

std::string TypeToString(Type::type t) {
  switch (t) {
    case Type::BOOLEAN:
      return "BOOLEAN";
    case Type::INT32:
      return "INT32";
    case Type::INT64:
      return "INT64";
    case Type::INT96:
      return "INT96";
    case Type::FLOAT:
      return "FLOAT";
    case Type::DOUBLE:
      return "DOUBLE";
    case Type::BYTE_ARRAY:
      return "BYTE_ARRAY";
    case Type::FIXED_LEN_BYTE_ARRAY:
      return "FIXED_LEN_BYTE_ARRAY";
    case Type::UNDEFINED:
    default:
      return "UNKNOWN";
  }
}

std::string ConvertedTypeToString(ConvertedType::type t) {
  switch (t) {
    case ConvertedType::NONE:
      return "NONE";
    case ConvertedType::UTF8:
      return "UTF8";
    case ConvertedType::MAP:
      return "MAP";
    case ConvertedType::MAP_KEY_VALUE:
      return "MAP_KEY_VALUE";
    case ConvertedType::LIST:
      return "LIST";
    case ConvertedType::ENUM:
      return "ENUM";
    case ConvertedType::DECIMAL:
      return "DECIMAL";
    case ConvertedType::DATE:
      return "DATE";
    case ConvertedType::TIME_MILLIS:
      return "TIME_MILLIS";
    case ConvertedType::TIME_MICROS:
      return "TIME_MICROS";
    case ConvertedType::TIMESTAMP_MILLIS:
      return "TIMESTAMP_MILLIS";
    case ConvertedType::TIMESTAMP_MICROS:
      return "TIMESTAMP_MICROS";
    case ConvertedType::UINT_8:
      return "UINT_8";
    case ConvertedType::UINT_16:
      return "UINT_16";
    case ConvertedType::UINT_32:
      return "UINT_32";
    case ConvertedType::UINT_64:
      return "UINT_64";
    case ConvertedType::INT_8:
      return "INT_8";
    case ConvertedType::INT_16:
      return "INT_16";
    case ConvertedType::INT_32:
      return "INT_32";
    case ConvertedType::INT_64:
      return "INT_64";
    case ConvertedType::JSON:
      return "JSON";
    case ConvertedType::BSON:
      return "BSON";
    case ConvertedType::INTERVAL:
      return "INTERVAL";
    case ConvertedType::UNDEFINED:
    default:
      return "UNKNOWN";
  }
}

int GetTypeByteSize(Type::type parquet_type) {
  switch (parquet_type) {
    case Type::BOOLEAN:
      return type_traits<BooleanType::type_num>::value_byte_size;
    case Type::INT32:
      return type_traits<Int32Type::type_num>::value_byte_size;
    case Type::INT64:
      return type_traits<Int64Type::type_num>::value_byte_size;
    case Type::INT96:
      return type_traits<Int96Type::type_num>::value_byte_size;
    case Type::DOUBLE:
      return type_traits<DoubleType::type_num>::value_byte_size;
    case Type::FLOAT:
      return type_traits<FloatType::type_num>::value_byte_size;
    case Type::BYTE_ARRAY:
      return type_traits<ByteArrayType::type_num>::value_byte_size;
    case Type::FIXED_LEN_BYTE_ARRAY:
      return type_traits<FLBAType::type_num>::value_byte_size;
    case Type::UNDEFINED:
    default:
      return 0;
  }
  return 0;
}

// Return the Sort Order of the Parquet Physical Types
SortOrder::type DefaultSortOrder(Type::type primitive) {
  switch (primitive) {
    case Type::BOOLEAN:
    case Type::INT32:
    case Type::INT64:
    case Type::FLOAT:
    case Type::DOUBLE:
      return SortOrder::SIGNED;
    case Type::BYTE_ARRAY:
    case Type::FIXED_LEN_BYTE_ARRAY:
      return SortOrder::UNSIGNED;
    case Type::INT96:
    case Type::UNDEFINED:
      return SortOrder::UNKNOWN;
  }
  return SortOrder::UNKNOWN;
}

// Return the SortOrder of the Parquet Types using Logical or Physical Types
SortOrder::type GetSortOrder(ConvertedType::type converted, Type::type primitive) {
  if (converted == ConvertedType::NONE) return DefaultSortOrder(primitive);
  switch (converted) {
    case ConvertedType::INT_8:
    case ConvertedType::INT_16:
    case ConvertedType::INT_32:
    case ConvertedType::INT_64:
    case ConvertedType::DATE:
    case ConvertedType::TIME_MICROS:
    case ConvertedType::TIME_MILLIS:
    case ConvertedType::TIMESTAMP_MICROS:
    case ConvertedType::TIMESTAMP_MILLIS:
      return SortOrder::SIGNED;
    case ConvertedType::UINT_8:
    case ConvertedType::UINT_16:
    case ConvertedType::UINT_32:
    case ConvertedType::UINT_64:
    case ConvertedType::ENUM:
    case ConvertedType::UTF8:
    case ConvertedType::BSON:
    case ConvertedType::JSON:
      return SortOrder::UNSIGNED;
    case ConvertedType::DECIMAL:
    case ConvertedType::LIST:
    case ConvertedType::MAP:
    case ConvertedType::MAP_KEY_VALUE:
    case ConvertedType::INTERVAL:
    case ConvertedType::NONE:  // required instead of default
    case ConvertedType::NA:    // required instead of default
    case ConvertedType::UNDEFINED:
      return SortOrder::UNKNOWN;
  }
  return SortOrder::UNKNOWN;
}

SortOrder::type GetSortOrder(const std::shared_ptr<const LogicalType>& logical_type,
                             Type::type primitive) {
  SortOrder::type o = SortOrder::UNKNOWN;
  if (logical_type && logical_type->is_valid()) {
    o = (logical_type->is_none() ? DefaultSortOrder(primitive)
                                 : logical_type->sort_order());
  }
  return o;
}

ColumnOrder ColumnOrder::undefined_ = ColumnOrder(ColumnOrder::UNDEFINED);
ColumnOrder ColumnOrder::type_defined_ = ColumnOrder(ColumnOrder::TYPE_DEFINED_ORDER);

// Static methods for LogicalType class

std::shared_ptr<const LogicalType> LogicalType::FromConvertedType(
    const ConvertedType::type converted_type,
    const schema::DecimalMetadata converted_decimal_metadata) {
  switch (converted_type) {
    case ConvertedType::UTF8:
      return StringLogicalType::Make();
    case ConvertedType::MAP_KEY_VALUE:
    case ConvertedType::MAP:
      return MapLogicalType::Make();
    case ConvertedType::LIST:
      return ListLogicalType::Make();
    case ConvertedType::ENUM:
      return EnumLogicalType::Make();
    case ConvertedType::DECIMAL:
      return DecimalLogicalType::Make(converted_decimal_metadata.precision,
                                      converted_decimal_metadata.scale);
    case ConvertedType::DATE:
      return DateLogicalType::Make();
    case ConvertedType::TIME_MILLIS:
      return TimeLogicalType::Make(true, LogicalType::TimeUnit::MILLIS);
    case ConvertedType::TIME_MICROS:
      return TimeLogicalType::Make(true, LogicalType::TimeUnit::MICROS);
    case ConvertedType::TIMESTAMP_MILLIS:
      return TimestampLogicalType::Make(true, LogicalType::TimeUnit::MILLIS,
                                        /*is_from_converted_type=*/true,
                                        /*force_set_converted_type=*/false);
    case ConvertedType::TIMESTAMP_MICROS:
      return TimestampLogicalType::Make(true, LogicalType::TimeUnit::MICROS,
                                        /*is_from_converted_type=*/true,
                                        /*force_set_converted_type=*/false);
    case ConvertedType::INTERVAL:
      return IntervalLogicalType::Make();
    case ConvertedType::INT_8:
      return IntLogicalType::Make(8, true);
    case ConvertedType::INT_16:
      return IntLogicalType::Make(16, true);
    case ConvertedType::INT_32:
      return IntLogicalType::Make(32, true);
    case ConvertedType::INT_64:
      return IntLogicalType::Make(64, true);
    case ConvertedType::UINT_8:
      return IntLogicalType::Make(8, false);
    case ConvertedType::UINT_16:
      return IntLogicalType::Make(16, false);
    case ConvertedType::UINT_32:
      return IntLogicalType::Make(32, false);
    case ConvertedType::UINT_64:
      return IntLogicalType::Make(64, false);
    case ConvertedType::JSON:
      return JSONLogicalType::Make();
    case ConvertedType::BSON:
      return BSONLogicalType::Make();
    case ConvertedType::NONE:
      return NoLogicalType::Make();
    case ConvertedType::NA:
    case ConvertedType::UNDEFINED:
      return UnknownLogicalType::Make();
  }
  return UnknownLogicalType::Make();
}

std::shared_ptr<const LogicalType> LogicalType::FromThrift(
    const format::LogicalType& type) {
  if (type.__isset.STRING) {
    return StringLogicalType::Make();
  } else if (type.__isset.MAP) {
    return MapLogicalType::Make();
  } else if (type.__isset.LIST) {
    return ListLogicalType::Make();
  } else if (type.__isset.ENUM) {
    return EnumLogicalType::Make();
  } else if (type.__isset.DECIMAL) {
    return DecimalLogicalType::Make(type.DECIMAL.precision, type.DECIMAL.scale);
  } else if (type.__isset.DATE) {
    return DateLogicalType::Make();
  } else if (type.__isset.TIME) {
    LogicalType::TimeUnit::unit unit;
    if (type.TIME.unit.__isset.MILLIS) {
      unit = LogicalType::TimeUnit::MILLIS;
    } else if (type.TIME.unit.__isset.MICROS) {
      unit = LogicalType::TimeUnit::MICROS;
    } else if (type.TIME.unit.__isset.NANOS) {
      unit = LogicalType::TimeUnit::NANOS;
    } else {
      unit = LogicalType::TimeUnit::UNKNOWN;
    }
    return TimeLogicalType::Make(type.TIME.isAdjustedToUTC, unit);
  } else if (type.__isset.TIMESTAMP) {
    LogicalType::TimeUnit::unit unit;
    if (type.TIMESTAMP.unit.__isset.MILLIS) {
      unit = LogicalType::TimeUnit::MILLIS;
    } else if (type.TIMESTAMP.unit.__isset.MICROS) {
      unit = LogicalType::TimeUnit::MICROS;
    } else if (type.TIMESTAMP.unit.__isset.NANOS) {
      unit = LogicalType::TimeUnit::NANOS;
    } else {
      unit = LogicalType::TimeUnit::UNKNOWN;
    }
    return TimestampLogicalType::Make(type.TIMESTAMP.isAdjustedToUTC, unit);
    // TODO(tpboudreau): activate the commented code after parquet.thrift
    // recognizes IntervalType as a LogicalType
    //} else if (type.__isset.INTERVAL) {
    //  return IntervalLogicalType::Make();
  } else if (type.__isset.INTEGER) {
    return IntLogicalType::Make(static_cast<int>(type.INTEGER.bitWidth),
                                type.INTEGER.isSigned);
  } else if (type.__isset.UNKNOWN) {
    return NullLogicalType::Make();
  } else if (type.__isset.JSON) {
    return JSONLogicalType::Make();
  } else if (type.__isset.BSON) {
    return BSONLogicalType::Make();
  } else if (type.__isset.UUID) {
    return UUIDLogicalType::Make();
  } else {
    throw ParquetException("Metadata contains Thrift LogicalType that is not recognized");
  }
}

std::shared_ptr<const LogicalType> LogicalType::String() {
  return StringLogicalType::Make();
}

std::shared_ptr<const LogicalType> LogicalType::Map() { return MapLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::List() { return ListLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::Enum() { return EnumLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::Decimal(int32_t precision,
                                                        int32_t scale) {
  return DecimalLogicalType::Make(precision, scale);
}

std::shared_ptr<const LogicalType> LogicalType::Date() { return DateLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::Time(
    bool is_adjusted_to_utc, LogicalType::TimeUnit::unit time_unit) {
  DCHECK(time_unit != LogicalType::TimeUnit::UNKNOWN);
  return TimeLogicalType::Make(is_adjusted_to_utc, time_unit);
}

std::shared_ptr<const LogicalType> LogicalType::Timestamp(
    bool is_adjusted_to_utc, LogicalType::TimeUnit::unit time_unit,
    bool is_from_converted_type, bool force_set_converted_type) {
  DCHECK(time_unit != LogicalType::TimeUnit::UNKNOWN);
  return TimestampLogicalType::Make(is_adjusted_to_utc, time_unit, is_from_converted_type,
                                    force_set_converted_type);
}

std::shared_ptr<const LogicalType> LogicalType::Interval() {
  return IntervalLogicalType::Make();
}

std::shared_ptr<const LogicalType> LogicalType::Int(int bit_width, bool is_signed) {
  DCHECK(bit_width == 64 || bit_width == 32 || bit_width == 16 || bit_width == 8);
  return IntLogicalType::Make(bit_width, is_signed);
}

std::shared_ptr<const LogicalType> LogicalType::Null() { return NullLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::JSON() { return JSONLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::BSON() { return BSONLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::UUID() { return UUIDLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::None() { return NoLogicalType::Make(); }

std::shared_ptr<const LogicalType> LogicalType::Unknown() {
  return UnknownLogicalType::Make();
}

/*
 * The logical type implementation classes are built in four layers: (1) the base
 * layer, which establishes the interface and provides generally reusable implementations
 * for the ToJSON() and Equals() methods; (2) an intermediate derived layer for the
 * "compatibility" methods, which provides implementations for is_compatible() and
 * ToConvertedType(); (3) another intermediate layer for the "applicability" methods
 * that provides several implementations for the is_applicable() method; and (4) the
 * final derived classes, one for each logical type, which supply implementations
 * for those methods that remain virtual (usually just ToString() and ToThrift()) or
 * otherwise need to be overridden.
 */

// LogicalTypeImpl base class

class LogicalType::Impl {
 public:
  virtual bool is_applicable(parquet::Type::type primitive_type,
                             int32_t primitive_length = -1) const = 0;

  virtual bool is_compatible(ConvertedType::type converted_type,
                             schema::DecimalMetadata converted_decimal_metadata = {
                                 false, -1, -1}) const = 0;

  virtual ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const = 0;

  virtual std::string ToString() const = 0;

  virtual bool is_serialized() const {
    return !(type_ == LogicalType::Type::NONE || type_ == LogicalType::Type::UNKNOWN);
  }

  virtual std::string ToJSON() const {
    std::stringstream json;
    json << R"({"Type": ")" << ToString() << R"("})";
    return json.str();
  }

  virtual format::LogicalType ToThrift() const {
    // logical types inheriting this method should never be serialized
    std::stringstream ss;
    ss << "Logical type " << ToString() << " should not be serialized";
    throw ParquetException(ss.str());
  }

  virtual bool Equals(const LogicalType& other) const { return other.type() == type_; }

  LogicalType::Type::type type() const { return type_; }

  SortOrder::type sort_order() const { return order_; }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  virtual ~Impl() noexcept {}

  class Compatible;
  class SimpleCompatible;
  class Incompatible;

  class Applicable;
  class SimpleApplicable;
  class TypeLengthApplicable;
  class UniversalApplicable;
  class Inapplicable;

  class String;
  class Map;
  class List;
  class Enum;
  class Decimal;
  class Date;
  class Time;
  class Timestamp;
  class Interval;
  class Int;
  class Null;
  class JSON;
  class BSON;
  class UUID;
  class No;
  class Unknown;

 protected:
  Impl(LogicalType::Type::type t, SortOrder::type o) : type_(t), order_(o) {}
  Impl() = default;

 private:
  LogicalType::Type::type type_ = LogicalType::Type::UNKNOWN;
  SortOrder::type order_ = SortOrder::UNKNOWN;
};

// Special methods for public LogicalType class

LogicalType::LogicalType() = default;
LogicalType::~LogicalType() noexcept = default;

// Delegating methods for public LogicalType class

bool LogicalType::is_applicable(parquet::Type::type primitive_type,
                                int32_t primitive_length) const {
  return impl_->is_applicable(primitive_type, primitive_length);
}

bool LogicalType::is_compatible(
    ConvertedType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  return impl_->is_compatible(converted_type, converted_decimal_metadata);
}

ConvertedType::type LogicalType::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  return impl_->ToConvertedType(out_decimal_metadata);
}

std::string LogicalType::ToString() const { return impl_->ToString(); }

std::string LogicalType::ToJSON() const { return impl_->ToJSON(); }

format::LogicalType LogicalType::ToThrift() const { return impl_->ToThrift(); }

bool LogicalType::Equals(const LogicalType& other) const { return impl_->Equals(other); }

LogicalType::Type::type LogicalType::type() const { return impl_->type(); }

SortOrder::type LogicalType::sort_order() const { return impl_->sort_order(); }

// Type checks for public LogicalType class

bool LogicalType::is_string() const { return impl_->type() == LogicalType::Type::STRING; }
bool LogicalType::is_map() const { return impl_->type() == LogicalType::Type::MAP; }
bool LogicalType::is_list() const { return impl_->type() == LogicalType::Type::LIST; }
bool LogicalType::is_enum() const { return impl_->type() == LogicalType::Type::ENUM; }
bool LogicalType::is_decimal() const {
  return impl_->type() == LogicalType::Type::DECIMAL;
}
bool LogicalType::is_date() const { return impl_->type() == LogicalType::Type::DATE; }
bool LogicalType::is_time() const { return impl_->type() == LogicalType::Type::TIME; }
bool LogicalType::is_timestamp() const {
  return impl_->type() == LogicalType::Type::TIMESTAMP;
}
bool LogicalType::is_interval() const {
  return impl_->type() == LogicalType::Type::INTERVAL;
}
bool LogicalType::is_int() const { return impl_->type() == LogicalType::Type::INT; }
bool LogicalType::is_null() const { return impl_->type() == LogicalType::Type::NIL; }
bool LogicalType::is_JSON() const { return impl_->type() == LogicalType::Type::JSON; }
bool LogicalType::is_BSON() const { return impl_->type() == LogicalType::Type::BSON; }
bool LogicalType::is_UUID() const { return impl_->type() == LogicalType::Type::UUID; }
bool LogicalType::is_none() const { return impl_->type() == LogicalType::Type::NONE; }
bool LogicalType::is_valid() const { return impl_->type() != LogicalType::Type::UNKNOWN; }
bool LogicalType::is_invalid() const { return !is_valid(); }
bool LogicalType::is_nested() const {
  return (impl_->type() == LogicalType::Type::LIST) ||
         (impl_->type() == LogicalType::Type::MAP);
}
bool LogicalType::is_nonnested() const { return !is_nested(); }
bool LogicalType::is_serialized() const { return impl_->is_serialized(); }

// LogicalTypeImpl intermediate "compatibility" classes

class LogicalType::Impl::Compatible : public virtual LogicalType::Impl {
 protected:
  Compatible() = default;
};

#define set_decimal_metadata(m___, i___, p___, s___) \
  {                                                  \
    if (m___) {                                      \
      (m___)->isset = (i___);                        \
      (m___)->scale = (s___);                        \
      (m___)->precision = (p___);                    \
    }                                                \
  }

#define reset_decimal_metadata(m___) \
  { set_decimal_metadata(m___, false, -1, -1); }

// For logical types that always translate to the same converted type
class LogicalType::Impl::SimpleCompatible : public virtual LogicalType::Impl::Compatible {
 public:
  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override {
    return (converted_type == converted_type_) && !converted_decimal_metadata.isset;
  }

  ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override {
    reset_decimal_metadata(out_decimal_metadata);
    return converted_type_;
  }

 protected:
  explicit SimpleCompatible(ConvertedType::type c) : converted_type_(c) {}

 private:
  ConvertedType::type converted_type_ = ConvertedType::NA;
};

// For logical types that have no corresponding converted type
class LogicalType::Impl::Incompatible : public virtual LogicalType::Impl {
 public:
  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override {
    return (converted_type == ConvertedType::NONE ||
            converted_type == ConvertedType::NA) &&
           !converted_decimal_metadata.isset;
  }

  ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override {
    reset_decimal_metadata(out_decimal_metadata);
    return ConvertedType::NONE;
  }

 protected:
  Incompatible() = default;
};

// LogicalTypeImpl intermediate "applicability" classes

class LogicalType::Impl::Applicable : public virtual LogicalType::Impl {
 protected:
  Applicable() = default;
};

// For logical types that can apply only to a single
// physical type
class LogicalType::Impl::SimpleApplicable : public virtual LogicalType::Impl::Applicable {
 public:
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override {
    return primitive_type == type_;
  }

 protected:
  explicit SimpleApplicable(parquet::Type::type t) : type_(t) {}

 private:
  parquet::Type::type type_;
};

// For logical types that can apply only to a particular
// physical type and physical length combination
class LogicalType::Impl::TypeLengthApplicable
    : public virtual LogicalType::Impl::Applicable {
 public:
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override {
    return primitive_type == type_ && primitive_length == length_;
  }

 protected:
  TypeLengthApplicable(parquet::Type::type t, int32_t l) : type_(t), length_(l) {}

 private:
  parquet::Type::type type_;
  int32_t length_;
};

// For logical types that can apply to any physical type
class LogicalType::Impl::UniversalApplicable
    : public virtual LogicalType::Impl::Applicable {
 public:
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override {
    return true;
  }

 protected:
  UniversalApplicable() = default;
};

// For logical types that can never apply to any primitive
// physical type
class LogicalType::Impl::Inapplicable : public virtual LogicalType::Impl {
 public:
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override {
    return false;
  }

 protected:
  Inapplicable() = default;
};

// LogicalType implementation final classes

#define OVERRIDE_TOSTRING(n___) \
  std::string ToString() const override { return #n___; }

#define OVERRIDE_TOTHRIFT(t___, s___)             \
  format::LogicalType ToThrift() const override { \
    format::LogicalType type;                     \
    format::t___ subtype;                         \
    type.__set_##s___(subtype);                   \
    return type;                                  \
  }

class LogicalType::Impl::String final : public LogicalType::Impl::SimpleCompatible,
                                        public LogicalType::Impl::SimpleApplicable {
 public:
  friend class StringLogicalType;

  OVERRIDE_TOSTRING(String)
  OVERRIDE_TOTHRIFT(StringType, STRING)

 private:
  String()
      : LogicalType::Impl(LogicalType::Type::STRING, SortOrder::UNSIGNED),
        LogicalType::Impl::SimpleCompatible(ConvertedType::UTF8),
        LogicalType::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

// Each public logical type class's Make() creation method instantiates a corresponding
// LogicalType::Impl::* object and installs that implementation in the logical type
// it returns.

#define GENERATE_MAKE(a___)                                      \
  std::shared_ptr<const LogicalType> a___##LogicalType::Make() { \
    auto* logical_type = new a___##LogicalType();                \
    logical_type->impl_.reset(new LogicalType::Impl::a___());    \
    return std::shared_ptr<const LogicalType>(logical_type);     \
  }

GENERATE_MAKE(String)

class LogicalType::Impl::Map final : public LogicalType::Impl::SimpleCompatible,
                                     public LogicalType::Impl::Inapplicable {
 public:
  friend class MapLogicalType;

  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override {
    return (converted_type == ConvertedType::MAP ||
            converted_type == ConvertedType::MAP_KEY_VALUE) &&
           !converted_decimal_metadata.isset;
  }

  OVERRIDE_TOSTRING(Map)
  OVERRIDE_TOTHRIFT(MapType, MAP)

 private:
  Map()
      : LogicalType::Impl(LogicalType::Type::MAP, SortOrder::UNKNOWN),
        LogicalType::Impl::SimpleCompatible(ConvertedType::MAP) {}
};

GENERATE_MAKE(Map)

class LogicalType::Impl::List final : public LogicalType::Impl::SimpleCompatible,
                                      public LogicalType::Impl::Inapplicable {
 public:
  friend class ListLogicalType;

  OVERRIDE_TOSTRING(List)
  OVERRIDE_TOTHRIFT(ListType, LIST)

 private:
  List()
      : LogicalType::Impl(LogicalType::Type::LIST, SortOrder::UNKNOWN),
        LogicalType::Impl::SimpleCompatible(ConvertedType::LIST) {}
};

GENERATE_MAKE(List)

class LogicalType::Impl::Enum final : public LogicalType::Impl::SimpleCompatible,
                                      public LogicalType::Impl::SimpleApplicable {
 public:
  friend class EnumLogicalType;

  OVERRIDE_TOSTRING(Enum)
  OVERRIDE_TOTHRIFT(EnumType, ENUM)

 private:
  Enum()
      : LogicalType::Impl(LogicalType::Type::ENUM, SortOrder::UNSIGNED),
        LogicalType::Impl::SimpleCompatible(ConvertedType::ENUM),
        LogicalType::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

GENERATE_MAKE(Enum)

// The parameterized logical types (currently Decimal, Time, Timestamp, and Int)
// generally can't reuse the simple method implementations available in the base and
// intermediate classes and must (re)implement them all

class LogicalType::Impl::Decimal final : public LogicalType::Impl::Compatible,
                                         public LogicalType::Impl::Applicable {
 public:
  friend class DecimalLogicalType;

  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override;
  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalType& other) const override;

  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }

 private:
  Decimal(int32_t p, int32_t s)
      : LogicalType::Impl(LogicalType::Type::DECIMAL, SortOrder::SIGNED),
        precision_(p),
        scale_(s) {}
  int32_t precision_ = -1;
  int32_t scale_ = -1;
};

bool LogicalType::Impl::Decimal::is_applicable(parquet::Type::type primitive_type,
                                               int32_t primitive_length) const {
  bool ok = false;
  switch (primitive_type) {
    case parquet::Type::INT32: {
      ok = (1 <= precision_) && (precision_ <= 9);
    } break;
    case parquet::Type::INT64: {
      ok = (1 <= precision_) && (precision_ <= 18);
      if (precision_ < 10) {
        // FIXME(tpb): warn that INT32 could be used
      }
    } break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      ok = precision_ <= static_cast<int32_t>(std::floor(
                             std::log10(std::pow(2.0, (8.0 * primitive_length) - 1.0))));
    } break;
    case parquet::Type::BYTE_ARRAY: {
      ok = true;
    } break;
    default: { } break; }
  return ok;
}

bool LogicalType::Impl::Decimal::is_compatible(
    ConvertedType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  return converted_type == ConvertedType::DECIMAL &&
         (converted_decimal_metadata.isset &&
          converted_decimal_metadata.scale == scale_ &&
          converted_decimal_metadata.precision == precision_);
}

ConvertedType::type LogicalType::Impl::Decimal::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  set_decimal_metadata(out_decimal_metadata, true, precision_, scale_);
  return ConvertedType::DECIMAL;
}

std::string LogicalType::Impl::Decimal::ToString() const {
  std::stringstream type;
  type << "Decimal(precision=" << precision_ << ", scale=" << scale_ << ")";
  return type.str();
}

std::string LogicalType::Impl::Decimal::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Decimal", "precision": )" << precision_ << R"(, "scale": )"
       << scale_ << "}";
  return json.str();
}

format::LogicalType LogicalType::Impl::Decimal::ToThrift() const {
  format::LogicalType type;
  format::DecimalType decimal_type;
  decimal_type.__set_precision(precision_);
  decimal_type.__set_scale(scale_);
  type.__set_DECIMAL(decimal_type);
  return type;
}

bool LogicalType::Impl::Decimal::Equals(const LogicalType& other) const {
  bool eq = false;
  if (other.is_decimal()) {
    const auto& other_decimal = checked_cast<const DecimalLogicalType&>(other);
    eq = (precision_ == other_decimal.precision() && scale_ == other_decimal.scale());
  }
  return eq;
}

std::shared_ptr<const LogicalType> DecimalLogicalType::Make(int32_t precision,
                                                            int32_t scale) {
  if (precision < 1) {
    throw ParquetException(
        "Precision must be greater than or equal to 1 for Decimal logical type");
  }
  if (scale < 0 || scale > precision) {
    throw ParquetException(
        "Scale must be a non-negative integer that does not exceed precision for "
        "Decimal logical type");
  }
  auto* logical_type = new DecimalLogicalType();
  logical_type->impl_.reset(new LogicalType::Impl::Decimal(precision, scale));
  return std::shared_ptr<const LogicalType>(logical_type);
}

int32_t DecimalLogicalType::precision() const {
  return (dynamic_cast<const LogicalType::Impl::Decimal&>(*impl_)).precision();
}

int32_t DecimalLogicalType::scale() const {
  return (dynamic_cast<const LogicalType::Impl::Decimal&>(*impl_)).scale();
}

class LogicalType::Impl::Date final : public LogicalType::Impl::SimpleCompatible,
                                      public LogicalType::Impl::SimpleApplicable {
 public:
  friend class DateLogicalType;

  OVERRIDE_TOSTRING(Date)
  OVERRIDE_TOTHRIFT(DateType, DATE)

 private:
  Date()
      : LogicalType::Impl(LogicalType::Type::DATE, SortOrder::SIGNED),
        LogicalType::Impl::SimpleCompatible(ConvertedType::DATE),
        LogicalType::Impl::SimpleApplicable(parquet::Type::INT32) {}
};

GENERATE_MAKE(Date)

#define time_unit_string(u___)                    \
  ((u___) == LogicalType::TimeUnit::MILLIS        \
       ? "milliseconds"                           \
       : ((u___) == LogicalType::TimeUnit::MICROS \
              ? "microseconds"                    \
              : ((u___) == LogicalType::TimeUnit::NANOS ? "nanoseconds" : "unknown")))

class LogicalType::Impl::Time final : public LogicalType::Impl::Compatible,
                                      public LogicalType::Impl::Applicable {
 public:
  friend class TimeLogicalType;

  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override;
  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalType& other) const override;

  bool is_adjusted_to_utc() const { return adjusted_; }
  LogicalType::TimeUnit::unit time_unit() const { return unit_; }

 private:
  Time(bool a, LogicalType::TimeUnit::unit u)
      : LogicalType::Impl(LogicalType::Type::TIME, SortOrder::SIGNED),
        adjusted_(a),
        unit_(u) {}
  bool adjusted_ = false;
  LogicalType::TimeUnit::unit unit_;
};

bool LogicalType::Impl::Time::is_applicable(parquet::Type::type primitive_type,
                                            int32_t primitive_length) const {
  return (primitive_type == parquet::Type::INT32 &&
          unit_ == LogicalType::TimeUnit::MILLIS) ||
         (primitive_type == parquet::Type::INT64 &&
          (unit_ == LogicalType::TimeUnit::MICROS ||
           unit_ == LogicalType::TimeUnit::NANOS));
}

bool LogicalType::Impl::Time::is_compatible(
    ConvertedType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  if (converted_decimal_metadata.isset) {
    return false;
  } else if (adjusted_ && unit_ == LogicalType::TimeUnit::MILLIS) {
    return converted_type == ConvertedType::TIME_MILLIS;
  } else if (adjusted_ && unit_ == LogicalType::TimeUnit::MICROS) {
    return converted_type == ConvertedType::TIME_MICROS;
  } else {
    return (converted_type == ConvertedType::NONE) ||
           (converted_type == ConvertedType::NA);
  }
}

ConvertedType::type LogicalType::Impl::Time::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  reset_decimal_metadata(out_decimal_metadata);
  if (adjusted_) {
    if (unit_ == LogicalType::TimeUnit::MILLIS) {
      return ConvertedType::TIME_MILLIS;
    } else if (unit_ == LogicalType::TimeUnit::MICROS) {
      return ConvertedType::TIME_MICROS;
    }
  }
  return ConvertedType::NONE;
}

std::string LogicalType::Impl::Time::ToString() const {
  std::stringstream type;
  type << "Time(isAdjustedToUTC=" << std::boolalpha << adjusted_
       << ", timeUnit=" << time_unit_string(unit_) << ")";
  return type.str();
}

std::string LogicalType::Impl::Time::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Time", "isAdjustedToUTC": )" << std::boolalpha << adjusted_
       << R"(, "timeUnit": ")" << time_unit_string(unit_) << R"("})";
  return json.str();
}

format::LogicalType LogicalType::Impl::Time::ToThrift() const {
  format::LogicalType type;
  format::TimeType time_type;
  format::TimeUnit time_unit;
  DCHECK(unit_ != LogicalType::TimeUnit::UNKNOWN);
  if (unit_ == LogicalType::TimeUnit::MILLIS) {
    format::MilliSeconds millis;
    time_unit.__set_MILLIS(millis);
  } else if (unit_ == LogicalType::TimeUnit::MICROS) {
    format::MicroSeconds micros;
    time_unit.__set_MICROS(micros);
  } else if (unit_ == LogicalType::TimeUnit::NANOS) {
    format::NanoSeconds nanos;
    time_unit.__set_NANOS(nanos);
  }
  time_type.__set_isAdjustedToUTC(adjusted_);
  time_type.__set_unit(time_unit);
  type.__set_TIME(time_type);
  return type;
}

bool LogicalType::Impl::Time::Equals(const LogicalType& other) const {
  bool eq = false;
  if (other.is_time()) {
    const auto& other_time = checked_cast<const TimeLogicalType&>(other);
    eq =
        (adjusted_ == other_time.is_adjusted_to_utc() && unit_ == other_time.time_unit());
  }
  return eq;
}

std::shared_ptr<const LogicalType> TimeLogicalType::Make(
    bool is_adjusted_to_utc, LogicalType::TimeUnit::unit time_unit) {
  if (time_unit == LogicalType::TimeUnit::MILLIS ||
      time_unit == LogicalType::TimeUnit::MICROS ||
      time_unit == LogicalType::TimeUnit::NANOS) {
    auto* logical_type = new TimeLogicalType();
    logical_type->impl_.reset(new LogicalType::Impl::Time(is_adjusted_to_utc, time_unit));
    return std::shared_ptr<const LogicalType>(logical_type);
  } else {
    throw ParquetException(
        "TimeUnit must be one of MILLIS, MICROS, or NANOS for Time logical type");
  }
}

bool TimeLogicalType::is_adjusted_to_utc() const {
  return (dynamic_cast<const LogicalType::Impl::Time&>(*impl_)).is_adjusted_to_utc();
}

LogicalType::TimeUnit::unit TimeLogicalType::time_unit() const {
  return (dynamic_cast<const LogicalType::Impl::Time&>(*impl_)).time_unit();
}

class LogicalType::Impl::Timestamp final : public LogicalType::Impl::Compatible,
                                           public LogicalType::Impl::SimpleApplicable {
 public:
  friend class TimestampLogicalType;

  bool is_serialized() const override;
  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalType& other) const override;

  bool is_adjusted_to_utc() const { return adjusted_; }
  LogicalType::TimeUnit::unit time_unit() const { return unit_; }

  bool is_from_converted_type() const { return is_from_converted_type_; }
  bool force_set_converted_type() const { return force_set_converted_type_; }

 private:
  Timestamp(bool adjusted, LogicalType::TimeUnit::unit unit, bool is_from_converted_type,
            bool force_set_converted_type)
      : LogicalType::Impl(LogicalType::Type::TIMESTAMP, SortOrder::SIGNED),
        LogicalType::Impl::SimpleApplicable(parquet::Type::INT64),
        adjusted_(adjusted),
        unit_(unit),
        is_from_converted_type_(is_from_converted_type),
        force_set_converted_type_(force_set_converted_type) {}
  bool adjusted_ = false;
  LogicalType::TimeUnit::unit unit_;
  bool is_from_converted_type_ = false;
  bool force_set_converted_type_ = false;
};

bool LogicalType::Impl::Timestamp::is_serialized() const {
  return !is_from_converted_type_;
}

bool LogicalType::Impl::Timestamp::is_compatible(
    ConvertedType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  if (converted_decimal_metadata.isset) {
    return false;
  } else if (unit_ == LogicalType::TimeUnit::MILLIS) {
    if (adjusted_ || force_set_converted_type_) {
      return converted_type == ConvertedType::TIMESTAMP_MILLIS;
    } else {
      return (converted_type == ConvertedType::NONE) ||
             (converted_type == ConvertedType::NA);
    }
  } else if (unit_ == LogicalType::TimeUnit::MICROS) {
    if (adjusted_ || force_set_converted_type_) {
      return converted_type == ConvertedType::TIMESTAMP_MICROS;
    } else {
      return (converted_type == ConvertedType::NONE) ||
             (converted_type == ConvertedType::NA);
    }
  } else {
    return (converted_type == ConvertedType::NONE) ||
           (converted_type == ConvertedType::NA);
  }
}

ConvertedType::type LogicalType::Impl::Timestamp::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  reset_decimal_metadata(out_decimal_metadata);
  if (adjusted_ || force_set_converted_type_) {
    if (unit_ == LogicalType::TimeUnit::MILLIS) {
      return ConvertedType::TIMESTAMP_MILLIS;
    } else if (unit_ == LogicalType::TimeUnit::MICROS) {
      return ConvertedType::TIMESTAMP_MICROS;
    }
  }
  return ConvertedType::NONE;
}

std::string LogicalType::Impl::Timestamp::ToString() const {
  std::stringstream type;
  type << "Timestamp(isAdjustedToUTC=" << std::boolalpha << adjusted_
       << ", timeUnit=" << time_unit_string(unit_)
       << ", is_from_converted_type=" << is_from_converted_type_
       << ", force_set_converted_type=" << force_set_converted_type_ << ")";
  return type.str();
}

std::string LogicalType::Impl::Timestamp::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Timestamp", "isAdjustedToUTC": )" << std::boolalpha << adjusted_
       << R"(, "timeUnit": ")" << time_unit_string(unit_) << R"(")"
       << R"(, "is_from_converted_type": )" << is_from_converted_type_
       << R"(, "force_set_converted_type": )" << force_set_converted_type_ << R"(})";
  return json.str();
}

format::LogicalType LogicalType::Impl::Timestamp::ToThrift() const {
  format::LogicalType type;
  format::TimestampType timestamp_type;
  format::TimeUnit time_unit;
  DCHECK(unit_ != LogicalType::TimeUnit::UNKNOWN);
  if (unit_ == LogicalType::TimeUnit::MILLIS) {
    format::MilliSeconds millis;
    time_unit.__set_MILLIS(millis);
  } else if (unit_ == LogicalType::TimeUnit::MICROS) {
    format::MicroSeconds micros;
    time_unit.__set_MICROS(micros);
  } else if (unit_ == LogicalType::TimeUnit::NANOS) {
    format::NanoSeconds nanos;
    time_unit.__set_NANOS(nanos);
  }
  timestamp_type.__set_isAdjustedToUTC(adjusted_);
  timestamp_type.__set_unit(time_unit);
  type.__set_TIMESTAMP(timestamp_type);
  return type;
}

bool LogicalType::Impl::Timestamp::Equals(const LogicalType& other) const {
  bool eq = false;
  if (other.is_timestamp()) {
    const auto& other_timestamp = checked_cast<const TimestampLogicalType&>(other);
    eq = (adjusted_ == other_timestamp.is_adjusted_to_utc() &&
          unit_ == other_timestamp.time_unit());
  }
  return eq;
}

std::shared_ptr<const LogicalType> TimestampLogicalType::Make(
    bool is_adjusted_to_utc, LogicalType::TimeUnit::unit time_unit,
    bool is_from_converted_type, bool force_set_converted_type) {
  if (time_unit == LogicalType::TimeUnit::MILLIS ||
      time_unit == LogicalType::TimeUnit::MICROS ||
      time_unit == LogicalType::TimeUnit::NANOS) {
    auto* logical_type = new TimestampLogicalType();
    logical_type->impl_.reset(new LogicalType::Impl::Timestamp(
        is_adjusted_to_utc, time_unit, is_from_converted_type, force_set_converted_type));
    return std::shared_ptr<const LogicalType>(logical_type);
  } else {
    throw ParquetException(
        "TimeUnit must be one of MILLIS, MICROS, or NANOS for Timestamp logical type");
  }
}

bool TimestampLogicalType::is_adjusted_to_utc() const {
  return (dynamic_cast<const LogicalType::Impl::Timestamp&>(*impl_)).is_adjusted_to_utc();
}

LogicalType::TimeUnit::unit TimestampLogicalType::time_unit() const {
  return (dynamic_cast<const LogicalType::Impl::Timestamp&>(*impl_)).time_unit();
}

bool TimestampLogicalType::is_from_converted_type() const {
  return (dynamic_cast<const LogicalType::Impl::Timestamp&>(*impl_))
      .is_from_converted_type();
}

bool TimestampLogicalType::force_set_converted_type() const {
  return (dynamic_cast<const LogicalType::Impl::Timestamp&>(*impl_))
      .force_set_converted_type();
}

class LogicalType::Impl::Interval final : public LogicalType::Impl::SimpleCompatible,
                                          public LogicalType::Impl::TypeLengthApplicable {
 public:
  friend class IntervalLogicalType;

  OVERRIDE_TOSTRING(Interval)
  // TODO(tpboudreau): uncomment the following line to enable serialization after
  // parquet.thrift recognizes IntervalType as a ConvertedType
  // OVERRIDE_TOTHRIFT(IntervalType, INTERVAL)

 private:
  Interval()
      : LogicalType::Impl(LogicalType::Type::INTERVAL, SortOrder::UNKNOWN),
        LogicalType::Impl::SimpleCompatible(ConvertedType::INTERVAL),
        LogicalType::Impl::TypeLengthApplicable(parquet::Type::FIXED_LEN_BYTE_ARRAY, 12) {
  }
};

GENERATE_MAKE(Interval)

class LogicalType::Impl::Int final : public LogicalType::Impl::Compatible,
                                     public LogicalType::Impl::Applicable {
 public:
  friend class IntLogicalType;

  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override;
  bool is_compatible(ConvertedType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  ConvertedType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalType& other) const override;

  int bit_width() const { return width_; }
  bool is_signed() const { return signed_; }

 private:
  Int(int w, bool s)
      : LogicalType::Impl(LogicalType::Type::INT,
                          (s ? SortOrder::SIGNED : SortOrder::UNSIGNED)),
        width_(w),
        signed_(s) {}
  int width_ = 0;
  bool signed_ = false;
};

bool LogicalType::Impl::Int::is_applicable(parquet::Type::type primitive_type,
                                           int32_t primitive_length) const {
  return (primitive_type == parquet::Type::INT32 && width_ <= 32) ||
         (primitive_type == parquet::Type::INT64 && width_ == 64);
}

bool LogicalType::Impl::Int::is_compatible(
    ConvertedType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  if (converted_decimal_metadata.isset) {
    return false;
  } else if (signed_ && width_ == 8) {
    return converted_type == ConvertedType::INT_8;
  } else if (signed_ && width_ == 16) {
    return converted_type == ConvertedType::INT_16;
  } else if (signed_ && width_ == 32) {
    return converted_type == ConvertedType::INT_32;
  } else if (signed_ && width_ == 64) {
    return converted_type == ConvertedType::INT_64;
  } else if (!signed_ && width_ == 8) {
    return converted_type == ConvertedType::UINT_8;
  } else if (!signed_ && width_ == 16) {
    return converted_type == ConvertedType::UINT_16;
  } else if (!signed_ && width_ == 32) {
    return converted_type == ConvertedType::UINT_32;
  } else if (!signed_ && width_ == 64) {
    return converted_type == ConvertedType::UINT_64;
  } else {
    return false;
  }
}

ConvertedType::type LogicalType::Impl::Int::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  reset_decimal_metadata(out_decimal_metadata);
  if (signed_) {
    switch (width_) {
      case 8:
        return ConvertedType::INT_8;
      case 16:
        return ConvertedType::INT_16;
      case 32:
        return ConvertedType::INT_32;
      case 64:
        return ConvertedType::INT_64;
    }
  } else {  // unsigned
    switch (width_) {
      case 8:
        return ConvertedType::UINT_8;
      case 16:
        return ConvertedType::UINT_16;
      case 32:
        return ConvertedType::UINT_32;
      case 64:
        return ConvertedType::UINT_64;
    }
  }
  return ConvertedType::NONE;
}

std::string LogicalType::Impl::Int::ToString() const {
  std::stringstream type;
  type << "Int(bitWidth=" << width_ << ", isSigned=" << std::boolalpha << signed_ << ")";
  return type.str();
}

std::string LogicalType::Impl::Int::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Int", "bitWidth": )" << width_ << R"(, "isSigned": )"
       << std::boolalpha << signed_ << "}";
  return json.str();
}

format::LogicalType LogicalType::Impl::Int::ToThrift() const {
  format::LogicalType type;
  format::IntType int_type;
  DCHECK(width_ == 64 || width_ == 32 || width_ == 16 || width_ == 8);
  int_type.__set_bitWidth(static_cast<int8_t>(width_));
  int_type.__set_isSigned(signed_);
  type.__set_INTEGER(int_type);
  return type;
}

bool LogicalType::Impl::Int::Equals(const LogicalType& other) const {
  bool eq = false;
  if (other.is_int()) {
    const auto& other_int = checked_cast<const IntLogicalType&>(other);
    eq = (width_ == other_int.bit_width() && signed_ == other_int.is_signed());
  }
  return eq;
}

std::shared_ptr<const LogicalType> IntLogicalType::Make(int bit_width, bool is_signed) {
  if (bit_width == 8 || bit_width == 16 || bit_width == 32 || bit_width == 64) {
    auto* logical_type = new IntLogicalType();
    logical_type->impl_.reset(new LogicalType::Impl::Int(bit_width, is_signed));
    return std::shared_ptr<const LogicalType>(logical_type);
  } else {
    throw ParquetException(
        "Bit width must be exactly 8, 16, 32, or 64 for Int logical type");
  }
}

int IntLogicalType::bit_width() const {
  return (dynamic_cast<const LogicalType::Impl::Int&>(*impl_)).bit_width();
}

bool IntLogicalType::is_signed() const {
  return (dynamic_cast<const LogicalType::Impl::Int&>(*impl_)).is_signed();
}

class LogicalType::Impl::Null final : public LogicalType::Impl::Incompatible,
                                      public LogicalType::Impl::UniversalApplicable {
 public:
  friend class NullLogicalType;

  OVERRIDE_TOSTRING(Null)
  OVERRIDE_TOTHRIFT(NullType, UNKNOWN)

 private:
  Null() : LogicalType::Impl(LogicalType::Type::NIL, SortOrder::UNKNOWN) {}
};

GENERATE_MAKE(Null)

class LogicalType::Impl::JSON final : public LogicalType::Impl::SimpleCompatible,
                                      public LogicalType::Impl::SimpleApplicable {
 public:
  friend class JSONLogicalType;

  OVERRIDE_TOSTRING(JSON)
  OVERRIDE_TOTHRIFT(JsonType, JSON)

 private:
  JSON()
      : LogicalType::Impl(LogicalType::Type::JSON, SortOrder::UNSIGNED),
        LogicalType::Impl::SimpleCompatible(ConvertedType::JSON),
        LogicalType::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

GENERATE_MAKE(JSON)

class LogicalType::Impl::BSON final : public LogicalType::Impl::SimpleCompatible,
                                      public LogicalType::Impl::SimpleApplicable {
 public:
  friend class BSONLogicalType;

  OVERRIDE_TOSTRING(BSON)
  OVERRIDE_TOTHRIFT(BsonType, BSON)

 private:
  BSON()
      : LogicalType::Impl(LogicalType::Type::BSON, SortOrder::UNSIGNED),
        LogicalType::Impl::SimpleCompatible(ConvertedType::BSON),
        LogicalType::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

GENERATE_MAKE(BSON)

class LogicalType::Impl::UUID final : public LogicalType::Impl::Incompatible,
                                      public LogicalType::Impl::TypeLengthApplicable {
 public:
  friend class UUIDLogicalType;

  OVERRIDE_TOSTRING(UUID)
  OVERRIDE_TOTHRIFT(UUIDType, UUID)

 private:
  UUID()
      : LogicalType::Impl(LogicalType::Type::UUID, SortOrder::UNSIGNED),
        LogicalType::Impl::TypeLengthApplicable(parquet::Type::FIXED_LEN_BYTE_ARRAY, 16) {
  }
};

GENERATE_MAKE(UUID)

class LogicalType::Impl::No final : public LogicalType::Impl::SimpleCompatible,
                                    public LogicalType::Impl::UniversalApplicable {
 public:
  friend class NoLogicalType;

  OVERRIDE_TOSTRING(None)

 private:
  No()
      : LogicalType::Impl(LogicalType::Type::NONE, SortOrder::UNKNOWN),
        LogicalType::Impl::SimpleCompatible(ConvertedType::NONE) {}
};

GENERATE_MAKE(No)

class LogicalType::Impl::Unknown final : public LogicalType::Impl::SimpleCompatible,
                                         public LogicalType::Impl::UniversalApplicable {
 public:
  friend class UnknownLogicalType;

  OVERRIDE_TOSTRING(Unknown)

 private:
  Unknown()
      : LogicalType::Impl(LogicalType::Type::UNKNOWN, SortOrder::UNKNOWN),
        LogicalType::Impl::SimpleCompatible(ConvertedType::NA) {}
};

GENERATE_MAKE(Unknown)

}  // namespace parquet
