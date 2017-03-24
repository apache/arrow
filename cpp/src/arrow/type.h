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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
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

    // Fixed-width binary. Each value occupies the same number of bytes
    FIXED_WIDTH_BINARY,

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

struct ARROW_EXPORT DataType {
  Type::type type;

  std::vector<std::shared_ptr<Field>> children_;

  explicit DataType(Type::type type) : type(type) {}

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

 private:
  DISALLOW_COPY_AND_ASSIGN(DataType);
};

typedef std::shared_ptr<DataType> TypePtr;

struct ARROW_EXPORT FixedWidthType : public DataType {
  using DataType::DataType;

  virtual int bit_width() const = 0;

  std::vector<BufferDescr> GetBufferLayout() const override;
};

struct ARROW_EXPORT IntegerMeta {
  virtual bool is_signed() const = 0;
};

struct ARROW_EXPORT FloatingPointMeta {
  enum Precision { HALF, SINGLE, DOUBLE };
  virtual Precision precision() const = 0;
};

struct ARROW_EXPORT NestedType : public DataType {
  using DataType::DataType;
};

struct NoExtraMeta {};

// A field is a piece of metadata that includes (for now) a name and a data
// type
struct ARROW_EXPORT Field {
  // Field name
  std::string name;

  // The field's data type
  std::shared_ptr<DataType> type;

  // Fields can be nullable
  bool nullable;

  Field(const std::string& name, const std::shared_ptr<DataType>& type,
      bool nullable = true)
      : name(name), type(type), nullable(nullable) {}

  bool Equals(const Field& other) const;
  bool Equals(const std::shared_ptr<Field>& other) const;

  std::string ToString() const;
};

typedef std::shared_ptr<Field> FieldPtr;

struct ARROW_EXPORT PrimitiveCType : public FixedWidthType {
  using FixedWidthType::FixedWidthType;
};

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
struct ARROW_EXPORT CTypeImpl : public PrimitiveCType {
  using c_type = C_TYPE;
  static constexpr Type::type type_id = TYPE_ID;

  CTypeImpl() : PrimitiveCType(TYPE_ID) {}

  int bit_width() const override { return static_cast<int>(sizeof(C_TYPE) * 8); }

  Status Accept(TypeVisitor* visitor) const override {
    return visitor->Visit(*static_cast<const DERIVED*>(this));
  }

  std::string ToString() const override { return std::string(DERIVED::name()); }
};

struct ARROW_EXPORT NullType : public DataType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::NA;

  NullType() : DataType(Type::NA) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "null"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
struct IntegerTypeImpl : public CTypeImpl<DERIVED, TYPE_ID, C_TYPE>, public IntegerMeta {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

struct ARROW_EXPORT BooleanType : public FixedWidthType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::BOOL;

  BooleanType() : FixedWidthType(Type::BOOL) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  int bit_width() const override { return 1; }
  static std::string name() { return "bool"; }
};

struct ARROW_EXPORT UInt8Type : public IntegerTypeImpl<UInt8Type, Type::UINT8, uint8_t> {
  static std::string name() { return "uint8"; }
};

struct ARROW_EXPORT Int8Type : public IntegerTypeImpl<Int8Type, Type::INT8, int8_t> {
  static std::string name() { return "int8"; }
};

struct ARROW_EXPORT UInt16Type
    : public IntegerTypeImpl<UInt16Type, Type::UINT16, uint16_t> {
  static std::string name() { return "uint16"; }
};

struct ARROW_EXPORT Int16Type : public IntegerTypeImpl<Int16Type, Type::INT16, int16_t> {
  static std::string name() { return "int16"; }
};

struct ARROW_EXPORT UInt32Type
    : public IntegerTypeImpl<UInt32Type, Type::UINT32, uint32_t> {
  static std::string name() { return "uint32"; }
};

struct ARROW_EXPORT Int32Type : public IntegerTypeImpl<Int32Type, Type::INT32, int32_t> {
  static std::string name() { return "int32"; }
};

struct ARROW_EXPORT UInt64Type
    : public IntegerTypeImpl<UInt64Type, Type::UINT64, uint64_t> {
  static std::string name() { return "uint64"; }
};

struct ARROW_EXPORT Int64Type : public IntegerTypeImpl<Int64Type, Type::INT64, int64_t> {
  static std::string name() { return "int64"; }
};

struct ARROW_EXPORT HalfFloatType
    : public CTypeImpl<HalfFloatType, Type::HALF_FLOAT, uint16_t>,
      public FloatingPointMeta {
  Precision precision() const override;
  static std::string name() { return "halffloat"; }
};

struct ARROW_EXPORT FloatType : public CTypeImpl<FloatType, Type::FLOAT, float>,
                                public FloatingPointMeta {
  Precision precision() const override;
  static std::string name() { return "float"; }
};

struct ARROW_EXPORT DoubleType : public CTypeImpl<DoubleType, Type::DOUBLE, double>,
                                 public FloatingPointMeta {
  Precision precision() const override;
  static std::string name() { return "double"; }
};

struct ARROW_EXPORT ListType : public NestedType {
  static constexpr Type::type type_id = Type::LIST;

  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(std::make_shared<Field>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : NestedType(Type::LIST) {
    children_ = {value_field};
  }

  std::shared_ptr<Field> value_field() const { return children_[0]; }

  std::shared_ptr<DataType> value_type() const { return children_[0]->type; }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "list"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

// BinaryType type is represents lists of 1-byte values.
struct ARROW_EXPORT BinaryType : public DataType, public NoExtraMeta {
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
class ARROW_EXPORT FixedWidthBinaryType : public FixedWidthType {
 public:
  static constexpr Type::type type_id = Type::FIXED_WIDTH_BINARY;

  explicit FixedWidthBinaryType(int32_t byte_width)
      : FixedWidthType(Type::FIXED_WIDTH_BINARY), byte_width_(byte_width) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  std::vector<BufferDescr> GetBufferLayout() const override;

  int32_t byte_width() const { return byte_width_; }
  int bit_width() const override;

 protected:
  int32_t byte_width_;
};

// UTF-8 encoded strings
struct ARROW_EXPORT StringType : public BinaryType {
  static constexpr Type::type type_id = Type::STRING;

  StringType() : BinaryType(Type::STRING) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "utf8"; }
};

struct ARROW_EXPORT StructType : public NestedType {
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

struct ARROW_EXPORT DecimalType : public DataType {
  static constexpr Type::type type_id = Type::DECIMAL;

  explicit DecimalType(int precision_, int scale_)
      : DataType(Type::DECIMAL), precision(precision_), scale(scale_) {}
  int precision;
  int scale;

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "decimal"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

enum class UnionMode : char { SPARSE, DENSE };

struct ARROW_EXPORT UnionType : public NestedType {
  static constexpr Type::type type_id = Type::UNION;

  UnionType(const std::vector<std::shared_ptr<Field>>& fields,
      const std::vector<uint8_t>& type_codes, UnionMode mode = UnionMode::SPARSE);

  std::string ToString() const override;
  static std::string name() { return "union"; }
  Status Accept(TypeVisitor* visitor) const override;

  std::vector<BufferDescr> GetBufferLayout() const override;

  UnionMode mode;

  // The type id used in the data to indicate each data type in the union. For
  // example, the first type in the union might be denoted by the id 5 (instead
  // of 0).
  std::vector<uint8_t> type_codes;
};

// ----------------------------------------------------------------------
// Date and time types

/// Date as int32_t days since UNIX epoch
struct ARROW_EXPORT Date32Type : public FixedWidthType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::DATE32;

  using c_type = int32_t;

  Date32Type() : FixedWidthType(Type::DATE32) {}

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * 4); }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

/// Date as int64_t milliseconds since UNIX epoch
struct ARROW_EXPORT Date64Type : public FixedWidthType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::DATE64;

  using c_type = int64_t;

  Date64Type() : FixedWidthType(Type::DATE64) {}

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * 8); }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "date"; }
};

enum class TimeUnit : char { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };

static inline std::ostream& operator<<(std::ostream& os, TimeUnit unit) {
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

struct ARROW_EXPORT Time32Type : public FixedWidthType {
  static constexpr Type::type type_id = Type::TIME32;
  using Unit = TimeUnit;
  using c_type = int32_t;

  TimeUnit unit;

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * 4); }

  explicit Time32Type(TimeUnit unit = TimeUnit::MILLI);

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

struct ARROW_EXPORT Time64Type : public FixedWidthType {
  static constexpr Type::type type_id = Type::TIME64;
  using Unit = TimeUnit;
  using c_type = int64_t;

  TimeUnit unit;

  int bit_width() const override { return static_cast<int>(sizeof(c_type) * 8); }

  explicit Time64Type(TimeUnit unit = TimeUnit::MILLI);

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
};

struct ARROW_EXPORT TimestampType : public FixedWidthType {
  using Unit = TimeUnit;

  typedef int64_t c_type;
  static constexpr Type::type type_id = Type::TIMESTAMP;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * 8); }

  explicit TimestampType(TimeUnit unit = TimeUnit::MILLI)
      : FixedWidthType(Type::TIMESTAMP), unit(unit) {}

  explicit TimestampType(TimeUnit unit, const std::string& timezone)
      : FixedWidthType(Type::TIMESTAMP), unit(unit), timezone(timezone) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "timestamp"; }

  TimeUnit unit;
  std::string timezone;
};

struct ARROW_EXPORT IntervalType : public FixedWidthType {
  enum class Unit : char { YEAR_MONTH = 0, DAY_TIME = 1 };

  using c_type = int64_t;
  static constexpr Type::type type_id = Type::INTERVAL;

  int bit_width() const override { return static_cast<int>(sizeof(int64_t) * 8); }

  Unit unit;

  explicit IntervalType(Unit unit = Unit::YEAR_MONTH)
      : FixedWidthType(Type::INTERVAL), unit(unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return name(); }
  static std::string name() { return "date"; }
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

  bool ordered() const { return ordered_; }

 private:
  // Must be an integer type (not currently checked)
  std::shared_ptr<DataType> index_type_;
  std::shared_ptr<Array> dictionary_;
  bool ordered_;
};

// ----------------------------------------------------------------------
// Factory functions

std::shared_ptr<DataType> ARROW_EXPORT fixed_width_binary(int32_t byte_width);

std::shared_ptr<DataType> ARROW_EXPORT list(const std::shared_ptr<Field>& value_type);
std::shared_ptr<DataType> ARROW_EXPORT list(const std::shared_ptr<DataType>& value_type);

std::shared_ptr<DataType> ARROW_EXPORT timestamp(TimeUnit unit);
std::shared_ptr<DataType> ARROW_EXPORT timestamp(
    TimeUnit unit, const std::string& timezone);

/// Unit can be either SECOND or MILLI
std::shared_ptr<DataType> ARROW_EXPORT time32(TimeUnit unit);

/// Unit can be either MICRO or NANO
std::shared_ptr<DataType> ARROW_EXPORT time64(TimeUnit unit);

std::shared_ptr<DataType> ARROW_EXPORT struct_(
    const std::vector<std::shared_ptr<Field>>& fields);

std::shared_ptr<DataType> ARROW_EXPORT union_(
    const std::vector<std::shared_ptr<Field>>& child_fields,
    const std::vector<uint8_t>& type_codes, UnionMode mode = UnionMode::SPARSE);

std::shared_ptr<DataType> ARROW_EXPORT dictionary(
    const std::shared_ptr<DataType>& index_type, const std::shared_ptr<Array>& values);

std::shared_ptr<Field> ARROW_EXPORT field(
    const std::string& name, const std::shared_ptr<DataType>& type, bool nullable = true);

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
