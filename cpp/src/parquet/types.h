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

#ifndef PARQUET_TYPES_H
#define PARQUET_TYPES_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include "parquet/platform.h"

namespace arrow {
namespace util {

class Codec;

}  // namespace util
}  // namespace arrow

namespace parquet {

// ----------------------------------------------------------------------
// Metadata enums to match Thrift metadata
//
// The reason we maintain our own enums is to avoid transitive dependency on
// the compiled Thrift headers (and thus thrift/Thrift.h) for users of the
// public API. After building parquet-cpp, you should not need to include
// Thrift headers in your application. This means some boilerplate to convert
// between our types and Parquet's Thrift types.
//
// We can also add special values like NONE to distinguish between metadata
// values being set and not set. As an example consider ConvertedType and
// CompressionCodec

// Mirrors parquet::Type
struct Type {
  enum type {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
    // Should always be last element.
    UNDEFINED = 8
  };
};

// Mirrors parquet::ConvertedType
struct ConvertedType {
  enum type {
    NONE,
    UTF8,
    MAP,
    MAP_KEY_VALUE,
    LIST,
    ENUM,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    JSON,
    BSON,
    INTERVAL,
    NA = 25,
    // Should always be last element.
    UNDEFINED = 26
  };
};

// forward declaration
namespace format {

class LogicalType;

}

// Mirrors parquet::FieldRepetitionType
struct Repetition {
  enum type { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2, /*Always last*/ UNDEFINED = 3 };
};

// Reference:
// parquet-mr/parquet-hadoop/src/main/java/org/apache/parquet/
//                            format/converter/ParquetMetadataConverter.java
// Sort order for page and column statistics. Types are associated with sort
// orders (e.g., UTF8 columns should use UNSIGNED) and column stats are
// aggregated using a sort order. As of parquet-format version 2.3.1, the
// order used to aggregate stats is always SIGNED and is not stored in the
// Parquet file. These stats are discarded for types that need unsigned.
// See PARQUET-686.
struct SortOrder {
  enum type { SIGNED, UNSIGNED, UNKNOWN };
};

namespace schema {

struct DecimalMetadata {
  bool isset;
  int32_t scale;
  int32_t precision;
};

}  // namespace schema

/// \brief Implementation of parquet.thrift LogicalType types.
class PARQUET_EXPORT LogicalType {
 public:
  struct Type {
    enum type {
      UNKNOWN = 0,
      STRING = 1,
      MAP,
      LIST,
      ENUM,
      DECIMAL,
      DATE,
      TIME,
      TIMESTAMP,
      INTERVAL,
      INT,
      NIL,  // Thrift NullType
      JSON,
      BSON,
      UUID,
      NONE
    };
  };

  struct TimeUnit {
    enum unit { UNKNOWN = 0, MILLIS = 1, MICROS, NANOS };
  };

  /// \brief If possible, return a logical type equivalent to the given legacy
  /// converted type (and decimal metadata if applicable).
  static std::shared_ptr<const LogicalType> FromConvertedType(
      const parquet::ConvertedType::type converted_type,
      const parquet::schema::DecimalMetadata converted_decimal_metadata = {false, -1,
                                                                           -1});

  /// \brief Return the logical type represented by the Thrift intermediary object.
  static std::shared_ptr<const LogicalType> FromThrift(
      const parquet::format::LogicalType& thrift_logical_type);

  /// \brief Return the explicitly requested logical type.
  static std::shared_ptr<const LogicalType> String();
  static std::shared_ptr<const LogicalType> Map();
  static std::shared_ptr<const LogicalType> List();
  static std::shared_ptr<const LogicalType> Enum();
  static std::shared_ptr<const LogicalType> Decimal(int32_t precision, int32_t scale = 0);
  static std::shared_ptr<const LogicalType> Date();
  static std::shared_ptr<const LogicalType> Time(bool is_adjusted_to_utc,
                                                 LogicalType::TimeUnit::unit time_unit);

  /// \brief Create a Timestamp logical type
  /// \param[in] is_adjusted_to_utc set true if the data is UTC-normalized
  /// \param[in] time_unit the resolution of the timestamp
  /// \param[in] is_from_converted_type if true, the timestamp was generated
  /// by translating a legacy converted type of TIMESTAMP_MILLIS or
  /// TIMESTAMP_MICROS. Default is false.
  /// \param[in] force_set_converted_type if true, always set the
  /// legacy ConvertedType TIMESTAMP_MICROS and TIMESTAMP_MILLIS
  /// metadata. Default is false
  static std::shared_ptr<const LogicalType> Timestamp(
      bool is_adjusted_to_utc, LogicalType::TimeUnit::unit time_unit,
      bool is_from_converted_type = false, bool force_set_converted_type = false);

  static std::shared_ptr<const LogicalType> Interval();
  static std::shared_ptr<const LogicalType> Int(int bit_width, bool is_signed);
  static std::shared_ptr<const LogicalType> Null();
  static std::shared_ptr<const LogicalType> JSON();
  static std::shared_ptr<const LogicalType> BSON();
  static std::shared_ptr<const LogicalType> UUID();
  static std::shared_ptr<const LogicalType> None();
  static std::shared_ptr<const LogicalType> Unknown();

  /// \brief Return true if this logical type is consistent with the given underlying
  /// physical type.
  bool is_applicable(parquet::Type::type primitive_type,
                     int32_t primitive_length = -1) const;

  /// \brief Return true if this logical type is equivalent to the given legacy converted
  /// type (and decimal metadata if applicable).
  bool is_compatible(parquet::ConvertedType::type converted_type,
                     parquet::schema::DecimalMetadata converted_decimal_metadata = {
                         false, -1, -1}) const;

  /// \brief If possible, return the legacy converted type (and decimal metadata if
  /// applicable) equivalent to this logical type.
  parquet::ConvertedType::type ToConvertedType(
      parquet::schema::DecimalMetadata* out_decimal_metadata) const;

  /// \brief Return a printable representation of this logical type.
  std::string ToString() const;

  /// \brief Return a JSON representation of this logical type.
  std::string ToJSON() const;

  /// \brief Return a serializable Thrift object for this logical type.
  parquet::format::LogicalType ToThrift() const;

  /// \brief Return true if the given logical type is equivalent to this logical type.
  bool Equals(const LogicalType& other) const;

  /// \brief Return the enumerated type of this logical type.
  LogicalType::Type::type type() const;

  /// \brief Return the appropriate sort order for this logical type.
  SortOrder::type sort_order() const;

  // Type checks ...
  bool is_string() const;
  bool is_map() const;
  bool is_list() const;
  bool is_enum() const;
  bool is_decimal() const;
  bool is_date() const;
  bool is_time() const;
  bool is_timestamp() const;
  bool is_interval() const;
  bool is_int() const;
  bool is_null() const;
  bool is_JSON() const;
  bool is_BSON() const;
  bool is_UUID() const;
  bool is_none() const;
  /// \brief Return true if this logical type is of a known type.
  bool is_valid() const;
  bool is_invalid() const;
  /// \brief Return true if this logical type is suitable for a schema GroupNode.
  bool is_nested() const;
  bool is_nonnested() const;
  /// \brief Return true if this logical type is included in the Thrift output for its
  /// node.
  bool is_serialized() const;

  LogicalType(const LogicalType&) = delete;
  LogicalType& operator=(const LogicalType&) = delete;
  virtual ~LogicalType() noexcept;

 protected:
  LogicalType();

  class Impl;
  std::unique_ptr<const Impl> impl_;
};

/// \brief Allowed for physical type BYTE_ARRAY, must be encoded as UTF-8.
class PARQUET_EXPORT StringLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  StringLogicalType() = default;
};

/// \brief Allowed for group nodes only.
class PARQUET_EXPORT MapLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  MapLogicalType() = default;
};

/// \brief Allowed for group nodes only.
class PARQUET_EXPORT ListLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  ListLogicalType() = default;
};

/// \brief Allowed for physical type BYTE_ARRAY, must be encoded as UTF-8.
class PARQUET_EXPORT EnumLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  EnumLogicalType() = default;
};

/// \brief Allowed for physical type INT32, INT64, FIXED_LEN_BYTE_ARRAY, or BYTE_ARRAY,
/// depending on the precision.
class PARQUET_EXPORT DecimalLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make(int32_t precision, int32_t scale = 0);
  int32_t precision() const;
  int32_t scale() const;

 private:
  DecimalLogicalType() = default;
};

/// \brief Allowed for physical type INT32.
class PARQUET_EXPORT DateLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  DateLogicalType() = default;
};

/// \brief Allowed for physical type INT32 (for MILLIS) or INT64 (for MICROS and NANOS).
class PARQUET_EXPORT TimeLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make(bool is_adjusted_to_utc,
                                                 LogicalType::TimeUnit::unit time_unit);
  bool is_adjusted_to_utc() const;
  LogicalType::TimeUnit::unit time_unit() const;

 private:
  TimeLogicalType() = default;
};

/// \brief Allowed for physical type INT64.
class PARQUET_EXPORT TimestampLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make(bool is_adjusted_to_utc,
                                                 LogicalType::TimeUnit::unit time_unit,
                                                 bool is_from_converted_type = false,
                                                 bool force_set_converted_type = false);
  bool is_adjusted_to_utc() const;
  LogicalType::TimeUnit::unit time_unit() const;

  /// \brief If true, will not set LogicalType in Thrift metadata
  bool is_from_converted_type() const;

  /// \brief If true, will set ConvertedType for micros and millis
  /// resolution in legacy ConvertedType Thrift metadata
  bool force_set_converted_type() const;

 private:
  TimestampLogicalType() = default;
};

/// \brief Allowed for physical type FIXED_LEN_BYTE_ARRAY with length 12
class PARQUET_EXPORT IntervalLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  IntervalLogicalType() = default;
};

/// \brief Allowed for physical type INT32 (for bit widths 8, 16, and 32) and INT64
/// (for bit width 64).
class PARQUET_EXPORT IntLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make(int bit_width, bool is_signed);
  int bit_width() const;
  bool is_signed() const;

 private:
  IntLogicalType() = default;
};

/// \brief Allowed for any physical type.
class PARQUET_EXPORT NullLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  NullLogicalType() = default;
};

/// \brief Allowed for physical type BYTE_ARRAY.
class PARQUET_EXPORT JSONLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  JSONLogicalType() = default;
};

/// \brief Allowed for physical type BYTE_ARRAY.
class PARQUET_EXPORT BSONLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  BSONLogicalType() = default;
};

/// \brief Allowed for physical type FIXED_LEN_BYTE_ARRAY with length 16,
/// must encode raw UUID bytes.
class PARQUET_EXPORT UUIDLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  UUIDLogicalType() = default;
};

/// \brief Allowed for any physical type.
class PARQUET_EXPORT NoLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  NoLogicalType() = default;
};

/// \brief Allowed for any type.
class PARQUET_EXPORT UnknownLogicalType : public LogicalType {
 public:
  static std::shared_ptr<const LogicalType> Make();

 private:
  UnknownLogicalType() = default;
};

// Data encodings. Mirrors parquet::Encoding
struct Encoding {
  enum type {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8,
    UNKNOWN = 999
  };
};

// Compression, mirrors parquet::CompressionCodec
struct Compression {
  enum type { UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD };
};

PARQUET_EXPORT
std::unique_ptr<::arrow::util::Codec> GetCodecFromArrow(Compression::type codec);

struct Encryption {
  enum type { AES_GCM_V1 = 0, AES_GCM_CTR_V1 = 1 };
};

// parquet::PageType
struct PageType {
  enum type { DATA_PAGE, INDEX_PAGE, DICTIONARY_PAGE, DATA_PAGE_V2 };
};

class ColumnOrder {
 public:
  enum type { UNDEFINED, TYPE_DEFINED_ORDER };
  explicit ColumnOrder(ColumnOrder::type column_order) : column_order_(column_order) {}
  // Default to Type Defined Order
  ColumnOrder() : column_order_(type::TYPE_DEFINED_ORDER) {}
  ColumnOrder::type get_order() { return column_order_; }

  static ColumnOrder undefined_;
  static ColumnOrder type_defined_;

 private:
  ColumnOrder::type column_order_;
};

// ----------------------------------------------------------------------

struct ByteArray {
  ByteArray() : len(0), ptr(NULLPTR) {}
  ByteArray(uint32_t len, const uint8_t* ptr) : len(len), ptr(ptr) {}

  ByteArray(::arrow::util::string_view view)  // NOLINT implicit conversion
      : ByteArray(static_cast<uint32_t>(view.size()),
                  reinterpret_cast<const uint8_t*>(view.data())) {}
  uint32_t len;
  const uint8_t* ptr;
};

inline bool operator==(const ByteArray& left, const ByteArray& right) {
  return left.len == right.len &&
         (left.len == 0 || std::memcmp(left.ptr, right.ptr, left.len) == 0);
}

inline bool operator!=(const ByteArray& left, const ByteArray& right) {
  return !(left == right);
}

struct FixedLenByteArray {
  FixedLenByteArray() : ptr(NULLPTR) {}
  explicit FixedLenByteArray(const uint8_t* ptr) : ptr(ptr) {}
  const uint8_t* ptr;
};

using FLBA = FixedLenByteArray;

// Julian day at unix epoch.
//
// The Julian Day Number (JDN) is the integer assigned to a whole solar day in
// the Julian day count starting from noon Universal time, with Julian day
// number 0 assigned to the day starting at noon on Monday, January 1, 4713 BC,
// proleptic Julian calendar (November 24, 4714 BC, in the proleptic Gregorian
// calendar),
constexpr int64_t kJulianToUnixEpochDays = INT64_C(2440588);
constexpr int64_t kSecondsPerDay = INT64_C(60 * 60 * 24);
constexpr int64_t kMillisecondsPerDay = kSecondsPerDay * INT64_C(1000);
constexpr int64_t kMicrosecondsPerDay = kMillisecondsPerDay * INT64_C(1000);
constexpr int64_t kNanosecondsPerDay = kMicrosecondsPerDay * INT64_C(1000);

MANUALLY_ALIGNED_STRUCT(1) Int96 { uint32_t value[3]; };
STRUCT_END(Int96, 12);

inline bool operator==(const Int96& left, const Int96& right) {
  return std::equal(left.value, left.value + 3, right.value);
}

inline bool operator!=(const Int96& left, const Int96& right) { return !(left == right); }

static inline std::string ByteArrayToString(const ByteArray& a) {
  return std::string(reinterpret_cast<const char*>(a.ptr), a.len);
}

static inline void Int96SetNanoSeconds(parquet::Int96& i96, int64_t nanoseconds) {
  std::memcpy(&i96.value, &nanoseconds, sizeof(nanoseconds));
}

static inline int64_t Int96GetNanoSeconds(const parquet::Int96& i96) {
  int64_t days_since_epoch = i96.value[2] - kJulianToUnixEpochDays;
  int64_t nanoseconds = 0;

  memcpy(&nanoseconds, &i96.value, sizeof(int64_t));
  return days_since_epoch * kNanosecondsPerDay + nanoseconds;
}

static inline std::string Int96ToString(const Int96& a) {
  std::ostringstream result;
  std::copy(a.value, a.value + 3, std::ostream_iterator<uint32_t>(result, " "));
  return result.str();
}

static inline std::string FixedLenByteArrayToString(const FixedLenByteArray& a, int len) {
  std::ostringstream result;
  std::copy(a.ptr, a.ptr + len, std::ostream_iterator<uint32_t>(result, " "));
  return result.str();
}

template <Type::type TYPE>
struct type_traits {};

template <>
struct type_traits<Type::BOOLEAN> {
  using value_type = bool;

  static constexpr int value_byte_size = 1;
  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT32> {
  using value_type = int32_t;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "d";
};

template <>
struct type_traits<Type::INT64> {
  using value_type = int64_t;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "ld";
};

template <>
struct type_traits<Type::INT96> {
  using value_type = Int96;

  static constexpr int value_byte_size = 12;
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FLOAT> {
  using value_type = float;

  static constexpr int value_byte_size = 4;
  static constexpr const char* printf_code = "f";
};

template <>
struct type_traits<Type::DOUBLE> {
  using value_type = double;

  static constexpr int value_byte_size = 8;
  static constexpr const char* printf_code = "lf";
};

template <>
struct type_traits<Type::BYTE_ARRAY> {
  using value_type = ByteArray;

  static constexpr int value_byte_size = sizeof(ByteArray);
  static constexpr const char* printf_code = "s";
};

template <>
struct type_traits<Type::FIXED_LEN_BYTE_ARRAY> {
  using value_type = FixedLenByteArray;

  static constexpr int value_byte_size = sizeof(FixedLenByteArray);
  static constexpr const char* printf_code = "s";
};

template <Type::type TYPE>
struct PhysicalType {
  using c_type = typename type_traits<TYPE>::value_type;
  static constexpr Type::type type_num = TYPE;
};

using BooleanType = PhysicalType<Type::BOOLEAN>;
using Int32Type = PhysicalType<Type::INT32>;
using Int64Type = PhysicalType<Type::INT64>;
using Int96Type = PhysicalType<Type::INT96>;
using FloatType = PhysicalType<Type::FLOAT>;
using DoubleType = PhysicalType<Type::DOUBLE>;
using ByteArrayType = PhysicalType<Type::BYTE_ARRAY>;
using FLBAType = PhysicalType<Type::FIXED_LEN_BYTE_ARRAY>;

template <typename Type>
inline std::string format_fwf(int width) {
  std::stringstream ss;
  ss << "%-" << width << type_traits<Type::type_num>::printf_code;
  return ss.str();
}

PARQUET_EXPORT std::string CompressionToString(Compression::type t);

PARQUET_EXPORT std::string EncodingToString(Encoding::type t);

PARQUET_EXPORT std::string ConvertedTypeToString(ConvertedType::type t);

PARQUET_EXPORT std::string TypeToString(Type::type t);

PARQUET_EXPORT std::string FormatStatValue(Type::type parquet_type,
                                           const std::string& val);

/// \deprecated Since 1.5.0
ARROW_DEPRECATED("Use std::string instead of char* as input")
PARQUET_EXPORT std::string FormatStatValue(Type::type parquet_type, const char* val);

PARQUET_EXPORT int GetTypeByteSize(Type::type t);

PARQUET_EXPORT SortOrder::type DefaultSortOrder(Type::type primitive);

PARQUET_EXPORT SortOrder::type GetSortOrder(ConvertedType::type converted,
                                            Type::type primitive);

PARQUET_EXPORT SortOrder::type GetSortOrder(
    const std::shared_ptr<const LogicalType>& logical_type, Type::type primitive);

namespace internal {

PARQUET_EXPORT
int32_t DecimalSize(int32_t precision);

}  // namespace internal
}  // namespace parquet

#endif  // PARQUET_TYPES_H
