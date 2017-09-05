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

#ifndef ARROW_TYPE_H
#define ARROW_TYPE_H

#include <climits>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/status.h"
#include "arrow/type_fwd.h"  // IWYU pragma: export
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor.h"

namespace arrow {

// Data types in this library are all *logical*. They can be expressed as
// either a primitive physical type (bytes or bits of some fixed size), a
// nested type consisting of other data types, or another data type (e.g. a
// timestamp encoded as an int64)
struct Type {
  enum type {
    // A degenerate NULL type represented as 0 bytes/bits
    NA,

    // A boolean value represented as 1 bit
    BOOL,

    // Little-endian integer types
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,

    // 2-byte floating point value
    HALF_FLOAT,

    // 4-byte floating point value
    FLOAT,

    // 8-byte floating point value
    DOUBLE,

    // UTF8 variable-length string as List<Char>
    STRING,

    // Variable-length bytes (no guarantee of UTF8-ness)
    BINARY,

    // Fixed-size binary. Each value occupies the same number of bytes
    FIXED_SIZE_BINARY,

    // int32_t days since the UNIX epoch
    DATE32,

    // int64_t milliseconds since the UNIX epoch
    DATE64,

    // Exact timestamp encoded with int64 since UNIX epoch
    // Default unit millisecond
    TIMESTAMP,

    // Time as signed 32-bit integer, representing either seconds or
    // milliseconds since midnight
    TIME32,

    // Time as signed 64-bit integer, representing either microseconds or
    // nanoseconds since midnight
    TIME64,

    // YEAR_MONTH or DAY_TIME interval in SQL style
    INTERVAL,

    // Precision- and scale-based decimal type. Storage type depends on the
    // parameters.
    DECIMAL,

    // A list of some logical data type
    LIST,

    // Struct of logical types
    STRUCT,

    // Unions of logical types
    UNION,

    // Dictionary aka Category type
    DICTIONARY
  };
};

enum class BufferType : char { DATA, OFFSET, TYPE, VALIDITY };

class BufferDescr {
 public:
  BufferDescr(BufferType type, int bit_width) : type_(type), bit_width_(bit_width) {}

  BufferType type() const { return type_; }
  int bit_width() const { return bit_width_; }

 private:
  BufferType type_;
  int bit_width_;
};

class ARROW_EXPORT DataType {
 public:
  explicit DataType(Type::type id) : id_(id) {}
  virtual ~DataType();

  // Return whether the types are equal
  //
  // Types that are logically convertable from one to another e.g. List<UInt8>
  // and Binary are NOT equal).
  virtual bool Equals(const DataType& other) const;
  bool Equals(const std::shared_ptr<DataType>& other) const;

  std::shared_ptr<Field> child(int i) const { return children_[i]; }

  const std::vector<std::shared_ptr<Field>>& children() const { return children_; }

  int num_children() const { return static_cast<int>(children_.size()); }

  virtual Status Accept(TypeVisitor* visitor) const = 0;

  virtual std::string ToString() const = 0;

  virtual std::vector<BufferDescr> GetBufferLayout() const = 0;

  Type::type id() const { return id_; }

 protected:
  Type::type id_;
  std::vector<std::shared_ptr<Field>> children_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(DataType);
};

// TODO(wesm): Remove this from parquet-cpp
using TypePtr = std::shared_ptr<DataType>;

class ARROW_EXPORT FixedWidthType : public DataType {
 public:
  using DataType::DataType;

  virtual int bit_width() const = 0;

  std::vector<BufferDescr> GetBufferLayout() const override;
};

class ARROW_EXPORT PrimitiveCType : public FixedWidthType {
 public:
  using FixedWidthType::FixedWidthType;
};

class ARROW_EXPORT Integer : public PrimitiveCType {
 public:
  using PrimitiveCType::PrimitiveCType;
  virtual bool is_signed() const = 0;
};

class ARROW_EXPORT FloatingPoint : public PrimitiveCType {
 public:
  using PrimitiveCType::PrimitiveCType;
  enum Precision { HALF, SINGLE, DOUBLE };
  virtual Precision precision() const = 0;
};

class ARROW_EXPORT NestedType : public DataType {
 public:
  using DataType::DataType;
  static std::string name() { return "nested"; }
};

class NoExtraMeta {};

// A field is a piece of metadata that includes (for now) a name and a data
// type
class ARROW_EXPORT Field {
 public:
  Field(const std::string& name, const std::shared_ptr<DataType>& type,
        bool nullable = true,
        const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr)
      : name_(name), type_(type), nullable_(nullable), metadata_(metadata) {}

  std::shared_ptr<const KeyValueMetadata> metadata() const { return metadata_; }

  /// \deprecated
  Status AddMetadata(const std::shared_ptr<const KeyValueMetadata>& metadata,
                     std::shared_ptr<Field>* out) const;

  std::shared_ptr<Field> AddMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const;
  std::shared_ptr<Field> RemoveMetadata() const;

  bool Equals(const Field& other) const;
  bool Equals(const std::shared_ptr<Field>& other) const;

  std::string ToString() const;

  const std::string& name() const { return name_; }
  std::shared_ptr<DataType> type() const { return type_; }
  bool nullable() const { return nullable_; }

 private:
  // Field name
  std::string name_;

  // The field's data type
  std::shared_ptr<DataType> type_;

  // Fields can be nullable
  bool nullable_;

  // The field's metadata, if any
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

namespace detail {

template <typename DERIVED, typename BASE, Type::type TYPE_ID, typename C_TYPE>
class ARROW_EXPORT CTypeImpl : public BASE {
 public:
  using c_type = C_TYPE;
  static constexpr Type::type type_id = TYPE_ID;

  CTypeImpl() : BASE(TYPE_ID) {}

  int bit_width() const override { return static_cast<int>(sizeof(C_TYPE) * CHAR_BIT); }

  Status Accept(TypeVisitor* visitor) const override {
    return visitor->Visit(*static_cast<const DERIVED*>(this));
  }

  std::string ToString() const override { return std::string(DERIVED::name()); }
};

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
class IntegerTypeImpl : public detail::CTypeImpl<DERIVED, Integer, TYPE_ID, C_TYPE> {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

}  // namespace detail

class ARROW_EXPORT NullType : public DataType, public NoExtraMeta {
 public:
  static constexpr Type::type type_id = Type::NA;

  NullType() : DataType(Type::NA) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "null"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

class ARROW_EXPORT BooleanType : public FixedWidthType, public NoExtraMeta {
 public:
  static constexpr Type::type type_id = Type::BOOL;

  BooleanType() : FixedWidthType(Type::BOOL) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  int bit_width() const override { return 1; }
  static std::string name() { return "bool"; }
};

class ARROW_EXPORT UInt8Type
    : public detail::IntegerTypeImpl<UInt8Type, Type::UINT8, uint8_t> {
 public:
  static std::string name() { return "uint8"; }
};

class ARROW_EXPORT Int8Type
    : public detail::IntegerTypeImpl<Int8Type, Type::INT8, int8_t> {
 public:
  static std::string name() { return "int8"; }
};

class ARROW_EXPORT UInt16Type
    : public detail::IntegerTypeImpl<UInt16Type, Type::UINT16, uint16_t> {
 public:
  static std::string name() { return "uint16"; }
};

class ARROW_EXPORT Int16Type
    : public detail::IntegerTypeImpl<Int16Type, Type::INT16, int16_t> {
 public:
  static std::string name() { return "int16"; }
};

class ARROW_EXPORT UInt32Type
    : public detail::IntegerTypeImpl<UInt32Type, Type::UINT32, uint32_t> {
 public:
  static std::string name() { return "uint32"; }
};

class ARROW_EXPORT Int32Type
    : public detail::IntegerTypeImpl<Int32Type, Type::INT32, int32_t> {
 public:
  static std::string name() { return "int32"; }
};

class ARROW_EXPORT UInt64Type
    : public detail::IntegerTypeImpl<UInt64Type, Type::UINT64, uint64_t> {
 public:
  static std::string name() { return "uint64"; }
};

class ARROW_EXPORT Int64Type
    : public detail::IntegerTypeImpl<Int64Type, Type::INT64, int64_t> {
 public:
  static std::string name() { return "int64"; }
};

class ARROW_EXPORT HalfFloatType
    : public detail::CTypeImpl<HalfFloatType, FloatingPoint, Type::HALF_FLOAT, uint16_t> {
 public:
  Precision precision() const override;
  static std::string name() { return "halffloat"; }
};

class ARROW_EXPORT FloatType
    : public detail::CTypeImpl<FloatType, FloatingPoint, Type::FLOAT, float> {
 public:
  Precision precision() const override;
  static std::string name() { return "float"; }
};

class ARROW_EXPORT DoubleType
    : public detail::CTypeImpl<DoubleType, FloatingPoint, Type::DOUBLE, double> {
 public:
  Precision precision() const override;
  static std::string name() { return "double"; }
};

class ARROW_EXPORT ListType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::LIST;

  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(std::make_shared<Field>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : NestedType(Type::LIST) {
    children_ = {value_field};
  }

  std::shared_ptr<Field> value_field() const { return children_[0]; }

  std::shared_ptr<DataType> value_type() const { return children_[0]->type(); }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "list"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

// BinaryType type is represents lists of 1-byte values.
class ARROW_EXPORT BinaryType : public DataType, public NoExtraMeta {
 public:
  static constexpr Type::type type_id = Type::BINARY;

  BinaryType() : BinaryType(Type::BINARY) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "binary"; }

  std::vector<BufferDescr> GetBufferLayout() const override;

 protected:
  // Allow subclasses to change the logical type.
  explicit BinaryType(Type::type logical_type) : DataType(logical_type) {}
};

// BinaryType type is represents lists of 1-byte values.
class ARROW_EXPORT FixedSizeBinaryType : public FixedWidthType {
 public:
  static constexpr Type::type type_id = Type::FIXED_SIZE_BINARY;

  explicit FixedSizeBinaryType(int32_t byte_width)
      : FixedWidthType(Type::FIXED_SIZE_BINARY), byte_width_(byte_width) {}
  explicit FixedSizeBinaryType(int32_t byte_width, Type::type type_id)
      : FixedWidthType(type_id), byte_width_(byte_width) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "fixed_size_binary"; }

  std::vector<BufferDescr> GetBufferLayout() const override;

  int32_t byte_width() const { return byte_width_; }
  int bit_width() const override;

 protected:
  int32_t byte_width_;
};

// UTF-8 encoded strings
class ARROW_EXPORT StringType : public BinaryType {
 public:
  static constexpr Type::type type_id = Type::STRING;

  StringType() : BinaryType(Type::STRING) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "utf8"; }
};

class ARROW_EXPORT StructType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::STRUCT;

  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields)
      : NestedType(Type::STRUCT) {
    children_ = fields;
  }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "struct"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

class ARROW_EXPORT DecimalType : public FixedSizeBinaryType {
 public:
  static constexpr Type::type type_id = Type::DECIMAL;

  explicit DecimalType(int32_t precision, int32_t scale)
      : FixedSizeBinaryType(16, Type::DECIMAL), precision_(precision), scale_(scale) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "decimal"; }

  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }

 private:
  int32_t precision_;
  int32_t scale_;
};

enum class UnionMode : char { SPARSE, DENSE };

class ARROW_EXPORT UnionType : public NestedType {
 public:
  static constexpr Type::type type_id = Type::UNION;

  UnionType(const std::vector<std::shared_ptr<Field>>& fields,
            const std::vector<uint8_t>& type_codes, UnionMode mode = UnionMode::SPARSE);

  std::string ToString() const override;
  static std::string name() { return "union"; }
  Status Accept(TypeVisitor* visitor) const override;

  std::vector<BufferDescr> GetBufferLayout() const override;

  const std::vector<uint8_t>& type_codes() const { return type_codes_; }

  UnionMode mode() const { return mode_; }

 private:
  UnionMode mode_;

  // The type id used in the data to indicate each data type in the union. For
  // example, the first type in the union might be denoted by the id 5 (instead
  // of 0).
  std::vector<uint8_t> type_codes_;
};

// ----------------------------------------------------------------------
// Date and time types

enum class DateUnit : char { DAY = 0, MILLI = 1 };

class ARROW_EXPORT DateType : public FixedWidthType {
 public:
  DateUnit unit() const { return unit_; }

 protected:
  DateType(Type::type type_id, DateUnit unit);
  DateUnit unit_;
};

/// Date as int32_t days since UNIX epoch
class ARROW_EXPORT Date32Type : public DateType {
 public:
  static constexpr Type::type type_id = Type::DATE32;

  using c_type = int32_t;

  Date32Type();

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

/// Date as int64_t milliseconds since UNIX epoch
class ARROW_EXPORT Date64Type : public DateType {
 public:
  static constexpr Type::type type_id = Type::DATE64;

  using c_type = int64_t;

  Date64Type();

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "date"; }
};

struct TimeUnit {
  enum type { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };
};

static inline std::ostream& operator<<(std::ostream& os, TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      os << "s";
      break;
    case TimeUnit::MILLI:
      os << "ms";
      break;
    case TimeUnit::MICRO:
      os << "us";
      break;
    case TimeUnit::NANO:
      os << "ns";
      break;
  }
  return os;
}

class ARROW_EXPORT TimeType : public FixedWidthType {
 public:
  TimeUnit::type unit() const { return unit_; }

 protected:
  TimeType(Type::type type_id, TimeUnit::type unit);
  TimeUnit::type unit_;
};

class ARROW_EXPORT Time32Type : public TimeType {
 public:
  static constexpr Type::type type_id = Type::TIME32;
  using c_type = int32_t;

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  explicit Time32Type(TimeUnit::type unit = TimeUnit::MILLI);

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

class ARROW_EXPORT Time64Type : public TimeType {
 public:
  static constexpr Type::type type_id = Type::TIME64;
  using c_type = int64_t;

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * CHAR_BIT); }

  explicit Time64Type(TimeUnit::type unit = TimeUnit::MILLI);

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

class ARROW_EXPORT TimestampType : public FixedWidthType {
 public:
  using Unit = TimeUnit;

  typedef int64_t c_type;
  static constexpr Type::type type_id = Type::TIMESTAMP;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  explicit TimestampType(TimeUnit::type unit = TimeUnit::MILLI)
      : FixedWidthType(Type::TIMESTAMP), unit_(unit) {}

  explicit TimestampType(TimeUnit::type unit, const std::string& timezone)
      : FixedWidthType(Type::TIMESTAMP), unit_(unit), timezone_(timezone) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "timestamp"; }

  TimeUnit::type unit() const { return unit_; }
  const std::string& timezone() const { return timezone_; }

 private:
  TimeUnit::type unit_;
  std::string timezone_;
};

class ARROW_EXPORT IntervalType : public FixedWidthType {
 public:
  enum class Unit : char { YEAR_MONTH = 0, DAY_TIME = 1 };

  using c_type = int64_t;
  static constexpr Type::type type_id = Type::INTERVAL;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * CHAR_BIT); }

  explicit IntervalType(Unit unit = Unit::YEAR_MONTH)
      : FixedWidthType(Type::INTERVAL), unit_(unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return name(); }
  static std::string name() { return "date"; }

  Unit unit() const { return unit_; }

 private:
  Unit unit_;
};

// ----------------------------------------------------------------------
// DictionaryType (for categorical or dictionary-encoded data)

class ARROW_EXPORT DictionaryType : public FixedWidthType {
 public:
  static constexpr Type::type type_id = Type::DICTIONARY;

  DictionaryType(const std::shared_ptr<DataType>& index_type,
                 const std::shared_ptr<Array>& dictionary, bool ordered = false);

  int bit_width() const override;

  std::shared_ptr<DataType> index_type() const { return index_type_; }

  std::shared_ptr<Array> dictionary() const;

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "dictionary"; }

  bool ordered() const { return ordered_; }

 private:
  // Must be an integer type (not currently checked)
  std::shared_ptr<DataType> index_type_;
  std::shared_ptr<Array> dictionary_;
  bool ordered_;
};

// ----------------------------------------------------------------------
// Schema

/// \class Schema
/// \brief Sequence of arrow::Field objects describing the columns of a record
/// batch or table data structure
class ARROW_EXPORT Schema {
 public:
  explicit Schema(const std::vector<std::shared_ptr<Field>>& fields,
                  const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr);

  explicit Schema(std::vector<std::shared_ptr<Field>>&& fields,
                  const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr);

  virtual ~Schema() = default;

  /// Returns true if all of the schema fields are equal
  bool Equals(const Schema& other) const;

  /// Return the ith schema element. Does not boundscheck
  std::shared_ptr<Field> field(int i) const { return fields_[i]; }

  /// Returns nullptr if name not found
  std::shared_ptr<Field> GetFieldByName(const std::string& name) const;

  /// Returns -1 if name not found
  int64_t GetFieldIndex(const std::string& name) const;

  const std::vector<std::shared_ptr<Field>>& fields() const { return fields_; }

  /// \brief The custom key-value metadata, if any
  ///
  /// \return metadata may be nullptr
  std::shared_ptr<const KeyValueMetadata> metadata() const;

  /// \brief Render a string representation of the schema suitable for debugging
  std::string ToString() const;

  Status AddField(int i, const std::shared_ptr<Field>& field,
                  std::shared_ptr<Schema>* out) const;
  Status RemoveField(int i, std::shared_ptr<Schema>* out) const;

  /// \deprecated
  Status AddMetadata(const std::shared_ptr<const KeyValueMetadata>& metadata,
                     std::shared_ptr<Schema>* out) const;

  /// \brief Replace key-value metadata with new metadata
  ///
  /// \param[in] metadata new KeyValueMetadata
  /// \return new Schema
  std::shared_ptr<Schema> AddMetadata(
      const std::shared_ptr<const KeyValueMetadata>& metadata) const;

  /// \brief Return copy of Schema without the KeyValueMetadata
  std::shared_ptr<Schema> RemoveMetadata() const;

  /// \brief Return the number of fields (columns) in the schema
  int num_fields() const { return static_cast<int>(fields_.size()); }

 private:
  std::vector<std::shared_ptr<Field>> fields_;
  mutable std::unordered_map<std::string, int> name_to_index_;

  std::shared_ptr<const KeyValueMetadata> metadata_;
};

// ----------------------------------------------------------------------
// Factory functions

/// \brief Make an instance of FixedSizeBinaryType
ARROW_EXPORT
std::shared_ptr<DataType> fixed_size_binary(int32_t byte_width);

/// \brief Make an instance of DecimalType
ARROW_EXPORT
std::shared_ptr<DataType> decimal(int32_t precision, int32_t scale);

/// \brief Make an instance of ListType
ARROW_EXPORT
std::shared_ptr<DataType> list(const std::shared_ptr<Field>& value_type);

/// \brief Make an instance of ListType
ARROW_EXPORT
std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type);

/// \brief Make an instance of TimestampType
ARROW_EXPORT
std::shared_ptr<DataType> timestamp(TimeUnit::type unit);

/// \brief Make an instance of TimestampType
ARROW_EXPORT
std::shared_ptr<DataType> timestamp(TimeUnit::type unit, const std::string& timezone);

/// \brief Create an instance of 32-bit time type
/// Unit can be either SECOND or MILLI
std::shared_ptr<DataType> ARROW_EXPORT time32(TimeUnit::type unit);

/// \brief Create an instance of 64-bit time type
/// Unit can be either MICRO or NANO
std::shared_ptr<DataType> ARROW_EXPORT time64(TimeUnit::type unit);

/// \brief Create an instance of Struct type
std::shared_ptr<DataType> ARROW_EXPORT
struct_(const std::vector<std::shared_ptr<Field>>& fields);

/// \brief Create an instance of Union type
std::shared_ptr<DataType> ARROW_EXPORT
union_(const std::vector<std::shared_ptr<Field>>& child_fields,
       const std::vector<uint8_t>& type_codes, UnionMode mode = UnionMode::SPARSE);

/// \brief Create an instance of Dictionary type
std::shared_ptr<DataType> ARROW_EXPORT
dictionary(const std::shared_ptr<DataType>& index_type,
           const std::shared_ptr<Array>& values, bool ordered = false);

/// \brief Create a Field instance
///
/// \param name the field name
/// \param type the field value type
/// \param nullable whether the values are nullable, default true
/// \param metadata any custom key-value metadata, default nullptr
std::shared_ptr<Field> ARROW_EXPORT field(
    const std::string& name, const std::shared_ptr<DataType>& type, bool nullable = true,
    const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr);

/// \brief Create a Schema instance
///
/// \param fields the schema's fields
/// \param metadata any custom key-value metadata, default nullptr
/// \return schema shared_ptr to Schema
std::shared_ptr<Schema> ARROW_EXPORT
schema(const std::vector<std::shared_ptr<Field>>& fields,
       const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr);

/// \brief Create a Schema instance
///
/// \param fields the schema's fields (rvalue reference)
/// \param metadata any custom key-value metadata, default nullptr
/// \return schema shared_ptr to Schema
std::shared_ptr<Schema> ARROW_EXPORT
schema(std::vector<std::shared_ptr<Field>>&& fields,
       const std::shared_ptr<const KeyValueMetadata>& metadata = nullptr);

// ----------------------------------------------------------------------
//

static inline bool is_integer(Type::type type_id) {
  switch (type_id) {
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_floating(Type::type type_id) {
  switch (type_id) {
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_primitive(Type::type type_id) {
  switch (type_id) {
    case Type::NA:
    case Type::BOOL:
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::UINT64:
    case Type::INT64:
    case Type::HALF_FLOAT:
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::DATE32:
    case Type::DATE64:
    case Type::TIME32:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::INTERVAL:
      return true;
    default:
      break;
  }
  return false;
}

static inline bool is_binary_like(Type::type type_id) {
  switch (type_id) {
    case Type::BINARY:
    case Type::STRING:
      return true;
    default:
      break;
  }
  return false;
}

}  // namespace arrow

#endif  // ARROW_TYPE_H
