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
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

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

std::string LogicalTypeToString(LogicalType::type t) {
  switch (t) {
    case LogicalType::NONE:
      return "NONE";
    case LogicalType::UTF8:
      return "UTF8";
    case LogicalType::MAP:
      return "MAP";
    case LogicalType::MAP_KEY_VALUE:
      return "MAP_KEY_VALUE";
    case LogicalType::LIST:
      return "LIST";
    case LogicalType::ENUM:
      return "ENUM";
    case LogicalType::DECIMAL:
      return "DECIMAL";
    case LogicalType::DATE:
      return "DATE";
    case LogicalType::TIME_MILLIS:
      return "TIME_MILLIS";
    case LogicalType::TIME_MICROS:
      return "TIME_MICROS";
    case LogicalType::TIMESTAMP_MILLIS:
      return "TIMESTAMP_MILLIS";
    case LogicalType::TIMESTAMP_MICROS:
      return "TIMESTAMP_MICROS";
    case LogicalType::UINT_8:
      return "UINT_8";
    case LogicalType::UINT_16:
      return "UINT_16";
    case LogicalType::UINT_32:
      return "UINT_32";
    case LogicalType::UINT_64:
      return "UINT_64";
    case LogicalType::INT_8:
      return "INT_8";
    case LogicalType::INT_16:
      return "INT_16";
    case LogicalType::INT_32:
      return "INT_32";
    case LogicalType::INT_64:
      return "INT_64";
    case LogicalType::JSON:
      return "JSON";
    case LogicalType::BSON:
      return "BSON";
    case LogicalType::INTERVAL:
      return "INTERVAL";
    case LogicalType::UNDEFINED:
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
SortOrder::type GetSortOrder(LogicalType::type converted, Type::type primitive) {
  if (converted == LogicalType::NONE) return DefaultSortOrder(primitive);
  switch (converted) {
    case LogicalType::INT_8:
    case LogicalType::INT_16:
    case LogicalType::INT_32:
    case LogicalType::INT_64:
    case LogicalType::DATE:
    case LogicalType::TIME_MICROS:
    case LogicalType::TIME_MILLIS:
    case LogicalType::TIMESTAMP_MICROS:
    case LogicalType::TIMESTAMP_MILLIS:
      return SortOrder::SIGNED;
    case LogicalType::UINT_8:
    case LogicalType::UINT_16:
    case LogicalType::UINT_32:
    case LogicalType::UINT_64:
    case LogicalType::ENUM:
    case LogicalType::UTF8:
    case LogicalType::BSON:
    case LogicalType::JSON:
      return SortOrder::UNSIGNED;
    case LogicalType::DECIMAL:
    case LogicalType::LIST:
    case LogicalType::MAP:
    case LogicalType::MAP_KEY_VALUE:
    case LogicalType::INTERVAL:
    case LogicalType::NONE:  // required instead of default
    case LogicalType::NA:    // required instead of default
    case LogicalType::UNDEFINED:
      return SortOrder::UNKNOWN;
  }
  return SortOrder::UNKNOWN;
}

SortOrder::type GetSortOrder(const std::shared_ptr<const LogicalAnnotation>& annotation,
                             Type::type primitive) {
  SortOrder::type o = SortOrder::UNKNOWN;
  if (annotation && annotation->is_valid()) {
    o = (annotation->is_none() ? DefaultSortOrder(primitive) : annotation->sort_order());
  }
  return o;
}

ColumnOrder ColumnOrder::undefined_ = ColumnOrder(ColumnOrder::UNDEFINED);
ColumnOrder ColumnOrder::type_defined_ = ColumnOrder(ColumnOrder::TYPE_DEFINED_ORDER);

// Static methods for LogicalAnnotation class

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::FromConvertedType(
    const LogicalType::type converted_type,
    const schema::DecimalMetadata converted_decimal_metadata) {
  switch (converted_type) {
    case LogicalType::UTF8:
      return StringAnnotation::Make();
    case LogicalType::MAP_KEY_VALUE:
    case LogicalType::MAP:
      return MapAnnotation::Make();
    case LogicalType::LIST:
      return ListAnnotation::Make();
    case LogicalType::ENUM:
      return EnumAnnotation::Make();
    case LogicalType::DECIMAL:
      return DecimalAnnotation::Make(converted_decimal_metadata.precision,
                                     converted_decimal_metadata.scale);
    case LogicalType::DATE:
      return DateAnnotation::Make();
    case LogicalType::TIME_MILLIS:
      return TimeAnnotation::Make(true, LogicalAnnotation::TimeUnit::MILLIS);
    case LogicalType::TIME_MICROS:
      return TimeAnnotation::Make(true, LogicalAnnotation::TimeUnit::MICROS);
    case LogicalType::TIMESTAMP_MILLIS:
      return TimestampAnnotation::Make(true, LogicalAnnotation::TimeUnit::MILLIS);
    case LogicalType::TIMESTAMP_MICROS:
      return TimestampAnnotation::Make(true, LogicalAnnotation::TimeUnit::MICROS);
    case LogicalType::INTERVAL:
      return IntervalAnnotation::Make();
    case LogicalType::INT_8:
      return IntAnnotation::Make(8, true);
    case LogicalType::INT_16:
      return IntAnnotation::Make(16, true);
    case LogicalType::INT_32:
      return IntAnnotation::Make(32, true);
    case LogicalType::INT_64:
      return IntAnnotation::Make(64, true);
    case LogicalType::UINT_8:
      return IntAnnotation::Make(8, false);
    case LogicalType::UINT_16:
      return IntAnnotation::Make(16, false);
    case LogicalType::UINT_32:
      return IntAnnotation::Make(32, false);
    case LogicalType::UINT_64:
      return IntAnnotation::Make(64, false);
    case LogicalType::JSON:
      return JSONAnnotation::Make();
    case LogicalType::BSON:
      return BSONAnnotation::Make();
    case LogicalType::NONE:
      return NoAnnotation::Make();
    case LogicalType::NA:
    case LogicalType::UNDEFINED:
      return UnknownAnnotation::Make();
  }
  return UnknownAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::FromThrift(
    const format::LogicalType& type) {
  if (type.__isset.STRING) {
    return StringAnnotation::Make();
  } else if (type.__isset.MAP) {
    return MapAnnotation::Make();
  } else if (type.__isset.LIST) {
    return ListAnnotation::Make();
  } else if (type.__isset.ENUM) {
    return EnumAnnotation::Make();
  } else if (type.__isset.DECIMAL) {
    return DecimalAnnotation::Make(type.DECIMAL.precision, type.DECIMAL.scale);
  } else if (type.__isset.DATE) {
    return DateAnnotation::Make();
  } else if (type.__isset.TIME) {
    LogicalAnnotation::TimeUnit::unit unit;
    if (type.TIME.unit.__isset.MILLIS) {
      unit = LogicalAnnotation::TimeUnit::MILLIS;
    } else if (type.TIME.unit.__isset.MICROS) {
      unit = LogicalAnnotation::TimeUnit::MICROS;
    } else if (type.TIME.unit.__isset.NANOS) {
      unit = LogicalAnnotation::TimeUnit::NANOS;
    } else {
      unit = LogicalAnnotation::TimeUnit::UNKNOWN;
    }
    return TimeAnnotation::Make(type.TIME.isAdjustedToUTC, unit);
  } else if (type.__isset.TIMESTAMP) {
    LogicalAnnotation::TimeUnit::unit unit;
    if (type.TIMESTAMP.unit.__isset.MILLIS) {
      unit = LogicalAnnotation::TimeUnit::MILLIS;
    } else if (type.TIMESTAMP.unit.__isset.MICROS) {
      unit = LogicalAnnotation::TimeUnit::MICROS;
    } else if (type.TIMESTAMP.unit.__isset.NANOS) {
      unit = LogicalAnnotation::TimeUnit::NANOS;
    } else {
      unit = LogicalAnnotation::TimeUnit::UNKNOWN;
    }
    return TimestampAnnotation::Make(type.TIMESTAMP.isAdjustedToUTC, unit);
    // TODO(tpboudreau): activate the commented code after parquet.thrift
    // recognizes IntervalType as a LogicalType
    //} else if (type.__isset.INTERVAL) {
    //  return IntervalAnnotation::Make();
  } else if (type.__isset.INTEGER) {
    return IntAnnotation::Make(static_cast<int>(type.INTEGER.bitWidth),
                               type.INTEGER.isSigned);
  } else if (type.__isset.UNKNOWN) {
    return NullAnnotation::Make();
  } else if (type.__isset.JSON) {
    return JSONAnnotation::Make();
  } else if (type.__isset.BSON) {
    return BSONAnnotation::Make();
  } else if (type.__isset.UUID) {
    return UUIDAnnotation::Make();
  } else {
    throw ParquetException("Metadata contains Thrift LogicalType that is not recognized");
  }
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::String() {
  return StringAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Map() {
  return MapAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::List() {
  return ListAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Enum() {
  return EnumAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Decimal(int32_t precision,
                                                                    int32_t scale) {
  return DecimalAnnotation::Make(precision, scale);
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Date() {
  return DateAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Time(
    bool is_adjusted_to_utc, LogicalAnnotation::TimeUnit::unit time_unit) {
  return TimeAnnotation::Make(is_adjusted_to_utc, time_unit);
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Timestamp(
    bool is_adjusted_to_utc, LogicalAnnotation::TimeUnit::unit time_unit) {
  return TimestampAnnotation::Make(is_adjusted_to_utc, time_unit);
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Interval() {
  return IntervalAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Int(int bit_width,
                                                                bool is_signed) {
  return IntAnnotation::Make(bit_width, is_signed);
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Null() {
  return NullAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::JSON() {
  return JSONAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::BSON() {
  return BSONAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::UUID() {
  return UUIDAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::None() {
  return NoAnnotation::Make();
}

std::shared_ptr<const LogicalAnnotation> LogicalAnnotation::Unknown() {
  return UnknownAnnotation::Make();
}

/*
 * The annotation implementation classes are built in four layers: (1) the base
 * layer, which establishes the interface and provides generally reusable implementations
 * for the ToJSON() and Equals() methods; (2) an intermediate derived layer for the
 * "compatibility" methods, which provides implementations for is_compatible() and
 * ToConvertedType(); (3) another intermediate layer for the "applicability" methods
 * that provides several implementations for the is_applicable() method; and (4) the
 * final derived classes, one for each annotation type, which supply implementations
 * for those methods that remain virtual (usually just ToString() and ToThrift()) or
 * otherwise need to be overridden.
 */

// LogicalAnnotationImpl base class

class LogicalAnnotation::Impl {
 public:
  virtual bool is_applicable(parquet::Type::type primitive_type,
                             int32_t primitive_length = -1) const = 0;

  virtual bool is_compatible(LogicalType::type converted_type,
                             schema::DecimalMetadata converted_decimal_metadata = {
                                 false, -1, -1}) const = 0;

  virtual LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const = 0;

  virtual std::string ToString() const = 0;

  virtual std::string ToJSON() const {
    std::stringstream json;
    json << R"({"Type": ")" << ToString() << R"("})";
    return json.str();
  }

  virtual format::LogicalType ToThrift() const {
    // annotation types inheriting this method should never be serialized
    std::stringstream ss;
    ss << "Annotation type " << ToString() << " should not be serialized";
    throw ParquetException(ss.str());
  }

  virtual bool Equals(const LogicalAnnotation& other) const {
    return other.type() == type_;
  }

  LogicalAnnotation::Type::type type() const { return type_; }

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
  Impl(LogicalAnnotation::Type::type t, SortOrder::type o) : type_(t), order_(o) {}
  Impl() = default;

 private:
  LogicalAnnotation::Type::type type_ = LogicalAnnotation::Type::UNKNOWN;
  SortOrder::type order_ = SortOrder::UNKNOWN;
};

// Special methods for public LogicalAnnotation class

LogicalAnnotation::LogicalAnnotation() = default;
LogicalAnnotation::~LogicalAnnotation() noexcept = default;

// Delegating methods for public LogicalAnnotation class

bool LogicalAnnotation::is_applicable(parquet::Type::type primitive_type,
                                      int32_t primitive_length) const {
  return impl_->is_applicable(primitive_type, primitive_length);
}

bool LogicalAnnotation::is_compatible(
    LogicalType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  return impl_->is_compatible(converted_type, converted_decimal_metadata);
}

LogicalType::type LogicalAnnotation::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  return impl_->ToConvertedType(out_decimal_metadata);
}

std::string LogicalAnnotation::ToString() const { return impl_->ToString(); }

std::string LogicalAnnotation::ToJSON() const { return impl_->ToJSON(); }

format::LogicalType LogicalAnnotation::ToThrift() const { return impl_->ToThrift(); }

bool LogicalAnnotation::Equals(const LogicalAnnotation& other) const {
  return impl_->Equals(other);
}

LogicalAnnotation::Type::type LogicalAnnotation::type() const { return impl_->type(); }

SortOrder::type LogicalAnnotation::sort_order() const { return impl_->sort_order(); }

// Type checks for public LogicalAnnotation class

bool LogicalAnnotation::is_string() const {
  return impl_->type() == LogicalAnnotation::Type::STRING;
}
bool LogicalAnnotation::is_map() const {
  return impl_->type() == LogicalAnnotation::Type::MAP;
}
bool LogicalAnnotation::is_list() const {
  return impl_->type() == LogicalAnnotation::Type::LIST;
}
bool LogicalAnnotation::is_enum() const {
  return impl_->type() == LogicalAnnotation::Type::ENUM;
}
bool LogicalAnnotation::is_decimal() const {
  return impl_->type() == LogicalAnnotation::Type::DECIMAL;
}
bool LogicalAnnotation::is_date() const {
  return impl_->type() == LogicalAnnotation::Type::DATE;
}
bool LogicalAnnotation::is_time() const {
  return impl_->type() == LogicalAnnotation::Type::TIME;
}
bool LogicalAnnotation::is_timestamp() const {
  return impl_->type() == LogicalAnnotation::Type::TIMESTAMP;
}
bool LogicalAnnotation::is_interval() const {
  return impl_->type() == LogicalAnnotation::Type::INTERVAL;
}
bool LogicalAnnotation::is_int() const {
  return impl_->type() == LogicalAnnotation::Type::INT;
}
bool LogicalAnnotation::is_null() const {
  return impl_->type() == LogicalAnnotation::Type::NIL;
}
bool LogicalAnnotation::is_JSON() const {
  return impl_->type() == LogicalAnnotation::Type::JSON;
}
bool LogicalAnnotation::is_BSON() const {
  return impl_->type() == LogicalAnnotation::Type::BSON;
}
bool LogicalAnnotation::is_UUID() const {
  return impl_->type() == LogicalAnnotation::Type::UUID;
}
bool LogicalAnnotation::is_none() const {
  return impl_->type() == LogicalAnnotation::Type::NONE;
}
bool LogicalAnnotation::is_valid() const {
  return impl_->type() != LogicalAnnotation::Type::UNKNOWN;
}
bool LogicalAnnotation::is_invalid() const { return !is_valid(); }
bool LogicalAnnotation::is_nested() const {
  return (impl_->type() == LogicalAnnotation::Type::LIST) ||
         (impl_->type() == LogicalAnnotation::Type::MAP);
}
bool LogicalAnnotation::is_nonnested() const { return !is_nested(); }
bool LogicalAnnotation::is_serialized() const {
  return !((impl_->type() == LogicalAnnotation::Type::NONE) ||
           (impl_->type() == LogicalAnnotation::Type::UNKNOWN));
}

// LogicalAnnotationImpl intermediate "compatibility" classes

class LogicalAnnotation::Impl::Compatible : public virtual LogicalAnnotation::Impl {
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

// For logical annotation types that always translate to the same converted type
class LogicalAnnotation::Impl::SimpleCompatible
    : public virtual LogicalAnnotation::Impl::Compatible {
 public:
  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override {
    return (converted_type == converted_type_) && !converted_decimal_metadata.isset;
  }

  LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override {
    reset_decimal_metadata(out_decimal_metadata);
    return converted_type_;
  }

 protected:
  explicit SimpleCompatible(LogicalType::type c) : converted_type_(c) {}

 private:
  LogicalType::type converted_type_ = LogicalType::NA;
};

// For logical annotations that have no corresponding converted type
class LogicalAnnotation::Impl::Incompatible : public virtual LogicalAnnotation::Impl {
 public:
  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override {
    return (converted_type == LogicalType::NONE || converted_type == LogicalType::NA) &&
           !converted_decimal_metadata.isset;
  }

  LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override {
    reset_decimal_metadata(out_decimal_metadata);
    return LogicalType::NONE;
  }

 protected:
  Incompatible() = default;
};

// LogicalAnnotationImpl intermediate "applicability" classes

class LogicalAnnotation::Impl::Applicable : public virtual LogicalAnnotation::Impl {
 protected:
  Applicable() = default;
};

// For logical annotations that can apply only to a single
// physical type
class LogicalAnnotation::Impl::SimpleApplicable
    : public virtual LogicalAnnotation::Impl::Applicable {
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

// For logical annotations that can apply only to a particular
// physical type and physical length combination
class LogicalAnnotation::Impl::TypeLengthApplicable
    : public virtual LogicalAnnotation::Impl::Applicable {
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

// For logical annotations that can apply to any physical type
class LogicalAnnotation::Impl::UniversalApplicable
    : public virtual LogicalAnnotation::Impl::Applicable {
 public:
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override {
    return true;
  }

 protected:
  UniversalApplicable() = default;
};

// For logical annotations that can never apply to any primitive
// physical type
class LogicalAnnotation::Impl::Inapplicable : public virtual LogicalAnnotation::Impl {
 public:
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override {
    return false;
  }

 protected:
  Inapplicable() = default;
};

// LogicalAnnotation implementation final classes

#define OVERRIDE_TOSTRING(n___) \
  std::string ToString() const override { return #n___; }

#define OVERRIDE_TOTHRIFT(t___, s___)             \
  format::LogicalType ToThrift() const override { \
    format::LogicalType type;                     \
    format::t___ subtype;                         \
    type.__set_##s___(subtype);                   \
    return type;                                  \
  }

class LogicalAnnotation::Impl::String final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::SimpleApplicable {
 public:
  friend class StringAnnotation;

  OVERRIDE_TOSTRING(String)
  OVERRIDE_TOTHRIFT(StringType, STRING)

 private:
  String()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::STRING, SortOrder::UNSIGNED),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::UTF8),
        LogicalAnnotation::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

// Each public annotation class's Make() creation method instantiates a corresponding
// LogicalAnnotation::Impl::* object and installs that implementation in the annotation
// it returns.

#define GENERATE_MAKE(a___)                                           \
  std::shared_ptr<const LogicalAnnotation> a___##Annotation::Make() { \
    auto* annotation = new a___##Annotation();                        \
    annotation->impl_.reset(new LogicalAnnotation::Impl::a___());     \
    return std::shared_ptr<const LogicalAnnotation>(annotation);      \
  }

GENERATE_MAKE(String)

class LogicalAnnotation::Impl::Map final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::Inapplicable {
 public:
  friend class MapAnnotation;

  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override {
    return (converted_type == LogicalType::MAP ||
            converted_type == LogicalType::MAP_KEY_VALUE) &&
           !converted_decimal_metadata.isset;
  }

  OVERRIDE_TOSTRING(Map)
  OVERRIDE_TOTHRIFT(MapType, MAP)

 private:
  Map()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::MAP, SortOrder::UNKNOWN),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::MAP) {}
};

GENERATE_MAKE(Map)

class LogicalAnnotation::Impl::List final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::Inapplicable {
 public:
  friend class ListAnnotation;

  OVERRIDE_TOSTRING(List)
  OVERRIDE_TOTHRIFT(ListType, LIST)

 private:
  List()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::LIST, SortOrder::UNKNOWN),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::LIST) {}
};

GENERATE_MAKE(List)

class LogicalAnnotation::Impl::Enum final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::SimpleApplicable {
 public:
  friend class EnumAnnotation;

  OVERRIDE_TOSTRING(Enum)
  OVERRIDE_TOTHRIFT(EnumType, ENUM)

 private:
  Enum()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::ENUM, SortOrder::UNSIGNED),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::ENUM),
        LogicalAnnotation::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

GENERATE_MAKE(Enum)

// The parameterized annotation types (currently Decimal, Time, Timestamp, and Int)
// generally can't reuse the simple method implementations available in the base and
// intermediate classes and must (re)implement them all

class LogicalAnnotation::Impl::Decimal final
    : public LogicalAnnotation::Impl::Compatible,
      public LogicalAnnotation::Impl::Applicable {
 public:
  friend class DecimalAnnotation;

  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override;
  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalAnnotation& other) const override;

  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }

 private:
  Decimal(int32_t p, int32_t s)
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::DECIMAL, SortOrder::SIGNED),
        precision_(p),
        scale_(s) {}
  int32_t precision_ = -1;
  int32_t scale_ = -1;
};

bool LogicalAnnotation::Impl::Decimal::is_applicable(parquet::Type::type primitive_type,
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

bool LogicalAnnotation::Impl::Decimal::is_compatible(
    LogicalType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  return converted_type == LogicalType::DECIMAL &&
         (converted_decimal_metadata.isset &&
          converted_decimal_metadata.scale == scale_ &&
          converted_decimal_metadata.precision == precision_);
}

LogicalType::type LogicalAnnotation::Impl::Decimal::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  set_decimal_metadata(out_decimal_metadata, true, precision_, scale_);
  return LogicalType::DECIMAL;
}

std::string LogicalAnnotation::Impl::Decimal::ToString() const {
  std::stringstream type;
  type << "Decimal(precision=" << precision_ << ", scale=" << scale_ << ")";
  return type.str();
}

std::string LogicalAnnotation::Impl::Decimal::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Decimal", "precision": )" << precision_ << R"(, "scale": )"
       << scale_ << "}";
  return json.str();
}

format::LogicalType LogicalAnnotation::Impl::Decimal::ToThrift() const {
  format::LogicalType type;
  format::DecimalType decimal_type;
  decimal_type.__set_precision(precision_);
  decimal_type.__set_scale(scale_);
  type.__set_DECIMAL(decimal_type);
  return type;
}

bool LogicalAnnotation::Impl::Decimal::Equals(const LogicalAnnotation& other) const {
  bool eq = false;
  if (other.is_decimal()) {
    const auto& other_decimal = checked_cast<const DecimalAnnotation&>(other);
    eq = (precision_ == other_decimal.precision() && scale_ == other_decimal.scale());
  }
  return eq;
}

std::shared_ptr<const LogicalAnnotation> DecimalAnnotation::Make(int32_t precision,
                                                                 int32_t scale) {
  if (precision < 1) {
    throw ParquetException(
        "Precision must be greater than or equal to 1 for Decimal annotation");
  }
  if (scale < 0 || scale > precision) {
    throw ParquetException(
        "Scale must be a non-negative integer that does not exceed precision for "
        "Decimal annotation");
  }
  auto* annotation = new DecimalAnnotation();
  annotation->impl_.reset(new LogicalAnnotation::Impl::Decimal(precision, scale));
  return std::shared_ptr<const LogicalAnnotation>(annotation);
}

int32_t DecimalAnnotation::precision() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Decimal&>(*impl_)).precision();
}

int32_t DecimalAnnotation::scale() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Decimal&>(*impl_)).scale();
}

class LogicalAnnotation::Impl::Date final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::SimpleApplicable {
 public:
  friend class DateAnnotation;

  OVERRIDE_TOSTRING(Date)
  OVERRIDE_TOTHRIFT(DateType, DATE)

 private:
  Date()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::DATE, SortOrder::SIGNED),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::DATE),
        LogicalAnnotation::Impl::SimpleApplicable(parquet::Type::INT32) {}
};

GENERATE_MAKE(Date)

#define time_unit_string(u___)                                                \
  ((u___) == LogicalAnnotation::TimeUnit::MILLIS                              \
       ? "milliseconds"                                                       \
       : ((u___) == LogicalAnnotation::TimeUnit::MICROS                       \
              ? "microseconds"                                                \
              : ((u___) == LogicalAnnotation::TimeUnit::NANOS ? "nanoseconds" \
                                                              : "unknown")))

class LogicalAnnotation::Impl::Time final : public LogicalAnnotation::Impl::Compatible,
                                            public LogicalAnnotation::Impl::Applicable {
 public:
  friend class TimeAnnotation;

  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override;
  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalAnnotation& other) const override;

  bool is_adjusted_to_utc() const { return adjusted_; }
  LogicalAnnotation::TimeUnit::unit time_unit() const { return unit_; }

 private:
  Time(bool a, LogicalAnnotation::TimeUnit::unit u)
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::TIME, SortOrder::SIGNED),
        adjusted_(a),
        unit_(u) {}
  bool adjusted_ = false;
  LogicalAnnotation::TimeUnit::unit unit_;
};

bool LogicalAnnotation::Impl::Time::is_applicable(parquet::Type::type primitive_type,
                                                  int32_t primitive_length) const {
  return (primitive_type == parquet::Type::INT32 &&
          unit_ == LogicalAnnotation::TimeUnit::MILLIS) ||
         (primitive_type == parquet::Type::INT64 &&
          (unit_ == LogicalAnnotation::TimeUnit::MICROS ||
           unit_ == LogicalAnnotation::TimeUnit::NANOS));
}

bool LogicalAnnotation::Impl::Time::is_compatible(
    LogicalType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  if (converted_decimal_metadata.isset) {
    return false;
  } else if (adjusted_ && unit_ == LogicalAnnotation::TimeUnit::MILLIS) {
    return converted_type == LogicalType::TIME_MILLIS;
  } else if (adjusted_ && unit_ == LogicalAnnotation::TimeUnit::MICROS) {
    return converted_type == LogicalType::TIME_MICROS;
  } else {
    return (converted_type == LogicalType::NONE) || (converted_type == LogicalType::NA);
  }
}

LogicalType::type LogicalAnnotation::Impl::Time::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  reset_decimal_metadata(out_decimal_metadata);
  if (adjusted_) {
    if (unit_ == LogicalAnnotation::TimeUnit::MILLIS) {
      return LogicalType::TIME_MILLIS;
    } else if (unit_ == LogicalAnnotation::TimeUnit::MICROS) {
      return LogicalType::TIME_MICROS;
    }
  }
  return LogicalType::NONE;
}

std::string LogicalAnnotation::Impl::Time::ToString() const {
  std::stringstream type;
  type << "Time(isAdjustedToUTC=" << std::boolalpha << adjusted_
       << ", timeUnit=" << time_unit_string(unit_) << ")";
  return type.str();
}

std::string LogicalAnnotation::Impl::Time::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Time", "isAdjustedToUTC": )" << std::boolalpha << adjusted_
       << R"(, "timeUnit": ")" << time_unit_string(unit_) << R"("})";
  return json.str();
}

format::LogicalType LogicalAnnotation::Impl::Time::ToThrift() const {
  format::LogicalType type;
  format::TimeType time_type;
  format::TimeUnit time_unit;
  DCHECK(unit_ != LogicalAnnotation::TimeUnit::UNKNOWN);
  if (unit_ == LogicalAnnotation::TimeUnit::MILLIS) {
    format::MilliSeconds millis;
    time_unit.__set_MILLIS(millis);
  } else if (unit_ == LogicalAnnotation::TimeUnit::MICROS) {
    format::MicroSeconds micros;
    time_unit.__set_MICROS(micros);
  } else if (unit_ == LogicalAnnotation::TimeUnit::NANOS) {
    format::NanoSeconds nanos;
    time_unit.__set_NANOS(nanos);
  }
  time_type.__set_isAdjustedToUTC(adjusted_);
  time_type.__set_unit(time_unit);
  type.__set_TIME(time_type);
  return type;
}

bool LogicalAnnotation::Impl::Time::Equals(const LogicalAnnotation& other) const {
  bool eq = false;
  if (other.is_time()) {
    const auto& other_time = checked_cast<const TimeAnnotation&>(other);
    eq =
        (adjusted_ == other_time.is_adjusted_to_utc() && unit_ == other_time.time_unit());
  }
  return eq;
}

std::shared_ptr<const LogicalAnnotation> TimeAnnotation::Make(
    bool is_adjusted_to_utc, LogicalAnnotation::TimeUnit::unit time_unit) {
  if (time_unit == LogicalAnnotation::TimeUnit::MILLIS ||
      time_unit == LogicalAnnotation::TimeUnit::MICROS ||
      time_unit == LogicalAnnotation::TimeUnit::NANOS) {
    auto* annotation = new TimeAnnotation();
    annotation->impl_.reset(
        new LogicalAnnotation::Impl::Time(is_adjusted_to_utc, time_unit));
    return std::shared_ptr<const LogicalAnnotation>(annotation);
  } else {
    throw ParquetException(
        "TimeUnit must be one of MILLIS, MICROS, or NANOS for Time annotation");
  }
}

bool TimeAnnotation::is_adjusted_to_utc() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Time&>(*impl_))
      .is_adjusted_to_utc();
}

LogicalAnnotation::TimeUnit::unit TimeAnnotation::time_unit() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Time&>(*impl_)).time_unit();
}

class LogicalAnnotation::Impl::Timestamp final
    : public LogicalAnnotation::Impl::Compatible,
      public LogicalAnnotation::Impl::SimpleApplicable {
 public:
  friend class TimestampAnnotation;

  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalAnnotation& other) const override;

  bool is_adjusted_to_utc() const { return adjusted_; }
  LogicalAnnotation::TimeUnit::unit time_unit() const { return unit_; }

 private:
  Timestamp(bool a, LogicalAnnotation::TimeUnit::unit u)
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::TIMESTAMP, SortOrder::SIGNED),
        LogicalAnnotation::Impl::SimpleApplicable(parquet::Type::INT64),
        adjusted_(a),
        unit_(u) {}
  bool adjusted_ = false;
  LogicalAnnotation::TimeUnit::unit unit_;
};

bool LogicalAnnotation::Impl::Timestamp::is_compatible(
    LogicalType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  if (converted_decimal_metadata.isset) {
    return false;
  } else if (adjusted_ && unit_ == LogicalAnnotation::TimeUnit::MILLIS) {
    return converted_type == LogicalType::TIMESTAMP_MILLIS;
  } else if (adjusted_ && unit_ == LogicalAnnotation::TimeUnit::MICROS) {
    return converted_type == LogicalType::TIMESTAMP_MICROS;
  } else {
    return (converted_type == LogicalType::NONE) || (converted_type == LogicalType::NA);
  }
}

LogicalType::type LogicalAnnotation::Impl::Timestamp::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  reset_decimal_metadata(out_decimal_metadata);
  if (adjusted_) {
    if (unit_ == LogicalAnnotation::TimeUnit::MILLIS) {
      return LogicalType::TIMESTAMP_MILLIS;
    } else if (unit_ == LogicalAnnotation::TimeUnit::MICROS) {
      return LogicalType::TIMESTAMP_MICROS;
    }
  }
  return LogicalType::NONE;
}

std::string LogicalAnnotation::Impl::Timestamp::ToString() const {
  std::stringstream type;
  type << "Timestamp(isAdjustedToUTC=" << std::boolalpha << adjusted_
       << ", timeUnit=" << time_unit_string(unit_) << ")";
  return type.str();
}

std::string LogicalAnnotation::Impl::Timestamp::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Timestamp", "isAdjustedToUTC": )" << std::boolalpha << adjusted_
       << R"(, "timeUnit": ")" << time_unit_string(unit_) << R"("})";
  return json.str();
}

format::LogicalType LogicalAnnotation::Impl::Timestamp::ToThrift() const {
  format::LogicalType type;
  format::TimestampType timestamp_type;
  format::TimeUnit time_unit;
  DCHECK(unit_ != LogicalAnnotation::TimeUnit::UNKNOWN);
  if (unit_ == LogicalAnnotation::TimeUnit::MILLIS) {
    format::MilliSeconds millis;
    time_unit.__set_MILLIS(millis);
  } else if (unit_ == LogicalAnnotation::TimeUnit::MICROS) {
    format::MicroSeconds micros;
    time_unit.__set_MICROS(micros);
  } else if (unit_ == LogicalAnnotation::TimeUnit::NANOS) {
    format::NanoSeconds nanos;
    time_unit.__set_NANOS(nanos);
  }
  timestamp_type.__set_isAdjustedToUTC(adjusted_);
  timestamp_type.__set_unit(time_unit);
  type.__set_TIMESTAMP(timestamp_type);
  return type;
}

bool LogicalAnnotation::Impl::Timestamp::Equals(const LogicalAnnotation& other) const {
  bool eq = false;
  if (other.is_timestamp()) {
    const auto& other_timestamp = checked_cast<const TimestampAnnotation&>(other);
    eq = (adjusted_ == other_timestamp.is_adjusted_to_utc() &&
          unit_ == other_timestamp.time_unit());
  }
  return eq;
}

std::shared_ptr<const LogicalAnnotation> TimestampAnnotation::Make(
    bool is_adjusted_to_utc, LogicalAnnotation::TimeUnit::unit time_unit) {
  if (time_unit == LogicalAnnotation::TimeUnit::MILLIS ||
      time_unit == LogicalAnnotation::TimeUnit::MICROS ||
      time_unit == LogicalAnnotation::TimeUnit::NANOS) {
    auto* annotation = new TimestampAnnotation();
    annotation->impl_.reset(
        new LogicalAnnotation::Impl::Timestamp(is_adjusted_to_utc, time_unit));
    return std::shared_ptr<const LogicalAnnotation>(annotation);
  } else {
    throw ParquetException(
        "TimeUnit must be one of MILLIS, MICROS, or NANOS for Timestamp annotation");
  }
}

bool TimestampAnnotation::is_adjusted_to_utc() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Timestamp&>(*impl_))
      .is_adjusted_to_utc();
}

LogicalAnnotation::TimeUnit::unit TimestampAnnotation::time_unit() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Timestamp&>(*impl_)).time_unit();
}

class LogicalAnnotation::Impl::Interval final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::TypeLengthApplicable {
 public:
  friend class IntervalAnnotation;

  OVERRIDE_TOSTRING(Interval)
  // TODO(tpboudreau): uncomment the following line to enable serialization after
  // parquet.thrift recognizes IntervalType as a LogicalType
  // OVERRIDE_TOTHRIFT(IntervalType, INTERVAL)

 private:
  Interval()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::INTERVAL, SortOrder::UNKNOWN),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::INTERVAL),
        LogicalAnnotation::Impl::TypeLengthApplicable(parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                      12) {}
};

GENERATE_MAKE(Interval)

class LogicalAnnotation::Impl::Int final : public LogicalAnnotation::Impl::Compatible,
                                           public LogicalAnnotation::Impl::Applicable {
 public:
  friend class IntAnnotation;

  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const override;
  bool is_compatible(LogicalType::type converted_type,
                     schema::DecimalMetadata converted_decimal_metadata) const override;
  LogicalType::type ToConvertedType(
      schema::DecimalMetadata* out_decimal_metadata) const override;
  std::string ToString() const override;
  std::string ToJSON() const override;
  format::LogicalType ToThrift() const override;
  bool Equals(const LogicalAnnotation& other) const override;

  int bit_width() const { return width_; }
  bool is_signed() const { return signed_; }

 private:
  Int(int w, bool s)
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::INT,
                                (s ? SortOrder::SIGNED : SortOrder::UNSIGNED)),
        width_(w),
        signed_(s) {}
  int width_ = 0;
  bool signed_ = false;
};

bool LogicalAnnotation::Impl::Int::is_applicable(parquet::Type::type primitive_type,
                                                 int32_t primitive_length) const {
  return (primitive_type == parquet::Type::INT32 && width_ <= 32) ||
         (primitive_type == parquet::Type::INT64 && width_ == 64);
}

bool LogicalAnnotation::Impl::Int::is_compatible(
    LogicalType::type converted_type,
    schema::DecimalMetadata converted_decimal_metadata) const {
  if (converted_decimal_metadata.isset) {
    return false;
  } else if (signed_ && width_ == 8) {
    return converted_type == LogicalType::INT_8;
  } else if (signed_ && width_ == 16) {
    return converted_type == LogicalType::INT_16;
  } else if (signed_ && width_ == 32) {
    return converted_type == LogicalType::INT_32;
  } else if (signed_ && width_ == 64) {
    return converted_type == LogicalType::INT_64;
  } else if (!signed_ && width_ == 8) {
    return converted_type == LogicalType::UINT_8;
  } else if (!signed_ && width_ == 16) {
    return converted_type == LogicalType::UINT_16;
  } else if (!signed_ && width_ == 32) {
    return converted_type == LogicalType::UINT_32;
  } else if (!signed_ && width_ == 64) {
    return converted_type == LogicalType::UINT_64;
  } else {
    return false;
  }
}

LogicalType::type LogicalAnnotation::Impl::Int::ToConvertedType(
    schema::DecimalMetadata* out_decimal_metadata) const {
  reset_decimal_metadata(out_decimal_metadata);
  if (signed_) {
    switch (width_) {
      case 8:
        return LogicalType::INT_8;
      case 16:
        return LogicalType::INT_16;
      case 32:
        return LogicalType::INT_32;
      case 64:
        return LogicalType::INT_64;
    }
  } else {  // unsigned
    switch (width_) {
      case 8:
        return LogicalType::UINT_8;
      case 16:
        return LogicalType::UINT_16;
      case 32:
        return LogicalType::UINT_32;
      case 64:
        return LogicalType::UINT_64;
    }
  }
  return LogicalType::NONE;
}

std::string LogicalAnnotation::Impl::Int::ToString() const {
  std::stringstream type;
  type << "Int(bitWidth=" << width_ << ", isSigned=" << std::boolalpha << signed_ << ")";
  return type.str();
}

std::string LogicalAnnotation::Impl::Int::ToJSON() const {
  std::stringstream json;
  json << R"({"Type": "Int", "bitWidth": )" << width_ << R"(, "isSigned": )"
       << std::boolalpha << signed_ << "}";
  return json.str();
}

format::LogicalType LogicalAnnotation::Impl::Int::ToThrift() const {
  format::LogicalType type;
  format::IntType int_type;
  DCHECK(width_ == 64 || width_ == 32 || width_ == 16 || width_ == 8);
  int_type.__set_bitWidth(static_cast<int8_t>(width_));
  int_type.__set_isSigned(signed_);
  type.__set_INTEGER(int_type);
  return type;
}

bool LogicalAnnotation::Impl::Int::Equals(const LogicalAnnotation& other) const {
  bool eq = false;
  if (other.is_int()) {
    const auto& other_int = checked_cast<const IntAnnotation&>(other);
    eq = (width_ == other_int.bit_width() && signed_ == other_int.is_signed());
  }
  return eq;
}

std::shared_ptr<const LogicalAnnotation> IntAnnotation::Make(int bit_width,
                                                             bool is_signed) {
  if (bit_width == 8 || bit_width == 16 || bit_width == 32 || bit_width == 64) {
    auto* annotation = new IntAnnotation();
    annotation->impl_.reset(new LogicalAnnotation::Impl::Int(bit_width, is_signed));
    return std::shared_ptr<const LogicalAnnotation>(annotation);
  } else {
    throw ParquetException(
        "Bit width must be exactly 8, 16, 32, or 64 for Int annotation");
  }
}

int IntAnnotation::bit_width() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Int&>(*impl_)).bit_width();
}

bool IntAnnotation::is_signed() const {
  return (dynamic_cast<const LogicalAnnotation::Impl::Int&>(*impl_)).is_signed();
}

class LogicalAnnotation::Impl::Null final
    : public LogicalAnnotation::Impl::Incompatible,
      public LogicalAnnotation::Impl::UniversalApplicable {
 public:
  friend class NullAnnotation;

  OVERRIDE_TOSTRING(Null)
  OVERRIDE_TOTHRIFT(NullType, UNKNOWN)

 private:
  Null() : LogicalAnnotation::Impl(LogicalAnnotation::Type::NIL, SortOrder::UNKNOWN) {}
};

GENERATE_MAKE(Null)

class LogicalAnnotation::Impl::JSON final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::SimpleApplicable {
 public:
  friend class JSONAnnotation;

  OVERRIDE_TOSTRING(JSON)
  OVERRIDE_TOTHRIFT(JsonType, JSON)

 private:
  JSON()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::JSON, SortOrder::UNSIGNED),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::JSON),
        LogicalAnnotation::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

GENERATE_MAKE(JSON)

class LogicalAnnotation::Impl::BSON final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::SimpleApplicable {
 public:
  friend class BSONAnnotation;

  OVERRIDE_TOSTRING(BSON)
  OVERRIDE_TOTHRIFT(BsonType, BSON)

 private:
  BSON()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::BSON, SortOrder::UNSIGNED),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::BSON),
        LogicalAnnotation::Impl::SimpleApplicable(parquet::Type::BYTE_ARRAY) {}
};

GENERATE_MAKE(BSON)

class LogicalAnnotation::Impl::UUID final
    : public LogicalAnnotation::Impl::Incompatible,
      public LogicalAnnotation::Impl::TypeLengthApplicable {
 public:
  friend class UUIDAnnotation;

  OVERRIDE_TOSTRING(UUID)
  OVERRIDE_TOTHRIFT(UUIDType, UUID)

 private:
  UUID()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::UUID, SortOrder::UNSIGNED),
        LogicalAnnotation::Impl::TypeLengthApplicable(parquet::Type::FIXED_LEN_BYTE_ARRAY,
                                                      16) {}
};

GENERATE_MAKE(UUID)

class LogicalAnnotation::Impl::No final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::UniversalApplicable {
 public:
  friend class NoAnnotation;

  OVERRIDE_TOSTRING(None)

 private:
  No()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::NONE, SortOrder::UNKNOWN),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::NONE) {}
};

GENERATE_MAKE(No)

class LogicalAnnotation::Impl::Unknown final
    : public LogicalAnnotation::Impl::SimpleCompatible,
      public LogicalAnnotation::Impl::UniversalApplicable {
 public:
  friend class UnknownAnnotation;

  OVERRIDE_TOSTRING(Unknown)

 private:
  Unknown()
      : LogicalAnnotation::Impl(LogicalAnnotation::Type::UNKNOWN, SortOrder::UNKNOWN),
        LogicalAnnotation::Impl::SimpleCompatible(LogicalType::NA) {}
};

GENERATE_MAKE(Unknown)

}  // namespace parquet
