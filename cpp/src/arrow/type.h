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
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

// Data types in this library are all *logical*. They can be expressed as
// either a primitive physical type (bytes or bits of some fixed size), a
// nested type consisting of other data types, or another data type (e.g. a
// timestamp encoded as an int64)
struct Type {
  enum type {
    // A degenerate NULL type represented as 0 bytes/bits
    NA = 0,

    // A boolean value represented as 1 bit
    BOOL = 1,

    // Little-endian integer types
    UINT8 = 2,
    INT8 = 3,
    UINT16 = 4,
    INT16 = 5,
    UINT32 = 6,
    INT32 = 7,
    UINT64 = 8,
    INT64 = 9,

    // 2-byte floating point value
    HALF_FLOAT = 10,

    // 4-byte floating point value
    FLOAT = 11,

    // 8-byte floating point value
    DOUBLE = 12,

    // UTF8 variable-length string as List<Char>
    STRING = 13,

    // Variable-length bytes (no guarantee of UTF8-ness)
    BINARY = 14,

    // By default, int32 days since the UNIX epoch
    DATE = 16,

    // Exact timestamp encoded with int64 since UNIX epoch
    // Default unit millisecond
    TIMESTAMP = 17,

    // Exact time encoded with int64, default unit millisecond
    TIME = 18,

    // YEAR_MONTH or DAY_TIME interval in SQL style
    INTERVAL = 19,

    // Precision- and scale-based decimal type. Storage type depends on the
    // parameters.
    DECIMAL = 20,

    // A list of some logical data type
    LIST = 30,

    // Struct of logical types
    STRUCT = 31,

    // Unions of logical types
    UNION = 32,

    // Timestamp as double seconds since the UNIX epoch
    TIMESTAMP_DOUBLE = 33,

    // Decimal value encoded as a text string
    DECIMAL_TEXT = 34,
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
  virtual bool Equals(const DataType* other) const;

  bool Equals(const std::shared_ptr<DataType>& other) const {
    return Equals(other.get());
  }

  std::shared_ptr<Field> child(int i) const { return children_[i]; }

  const std::vector<std::shared_ptr<Field>>& children() const { return children_; }

  int num_children() const { return children_.size(); }

  virtual Status Accept(TypeVisitor* visitor) const = 0;

  virtual std::string ToString() const = 0;

  virtual std::vector<BufferDescr> GetBufferLayout() const = 0;
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

  // optional dictionary id if the field is dictionary encoded
  // 0 means it's not dictionary encoded
  int64_t dictionary;

  Field(const std::string& name, const std::shared_ptr<DataType>& type,
      bool nullable = true, int64_t dictionary = 0)
      : name(name), type(type), nullable(nullable), dictionary(dictionary) {}

  bool operator==(const Field& other) const { return this->Equals(other); }

  bool operator!=(const Field& other) const { return !this->Equals(other); }

  bool Equals(const Field& other) const {
    return (this == &other) ||
           (this->name == other.name && this->nullable == other.nullable &&
               this->dictionary == dictionary && this->type->Equals(other.type.get()));
  }

  bool Equals(const std::shared_ptr<Field>& other) const { return Equals(*other.get()); }

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

  int bit_width() const override { return sizeof(C_TYPE) * 8; }

  Status Accept(TypeVisitor* visitor) const override {
    return visitor->Visit(*static_cast<const DERIVED*>(this));
  }

  std::string ToString() const override { return std::string(DERIVED::name()); }
};

struct ARROW_EXPORT NullType : public DataType {
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

struct ARROW_EXPORT BooleanType : public FixedWidthType {
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

struct ARROW_EXPORT ListType : public DataType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::LIST;

  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(std::make_shared<Field>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : DataType(Type::LIST) {
    children_ = {value_field};
  }

  std::shared_ptr<Field> value_field() const { return children_[0]; }

  std::shared_ptr<DataType> value_type() const { return children_[0]->type; }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static std::string name() { return "list"; }

  std::vector<BufferDescr> GetBufferLayout() const override;
};

// BinaryType type is reprsents lists of 1-byte values.
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

// UTF encoded strings
struct ARROW_EXPORT StringType : public BinaryType {
  static constexpr Type::type type_id = Type::STRING;

  StringType() : BinaryType(Type::STRING) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "utf8"; }
};

struct ARROW_EXPORT StructType : public DataType, public NoExtraMeta {
  static constexpr Type::type type_id = Type::STRUCT;

  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields)
      : DataType(Type::STRUCT) {
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

struct ARROW_EXPORT UnionType : public DataType {
  static constexpr Type::type type_id = Type::UNION;

  UnionType(const std::vector<std::shared_ptr<Field>>& fields,
      const std::vector<uint8_t>& type_ids, UnionMode mode = UnionMode::SPARSE)
      : DataType(Type::UNION), mode(mode), type_ids(type_ids) {
    children_ = fields;
  }

  std::string ToString() const override;
  static std::string name() { return "union"; }
  Status Accept(TypeVisitor* visitor) const override;

  std::vector<BufferDescr> GetBufferLayout() const override;

  UnionMode mode;

  // The type id used in the data to indicate each data type in the union. For
  // example, the first type in the union might be denoted by the id 5 (instead
  // of 0).
  std::vector<uint8_t> type_ids;
};

struct ARROW_EXPORT DateType : public FixedWidthType {
  static constexpr Type::type type_id = Type::DATE;

  using c_type = int64_t;

  DateType() : FixedWidthType(Type::DATE) {}

  int bit_width() const override { return sizeof(c_type) * 8; }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static std::string name() { return "date"; }
};

enum class TimeUnit : char { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };

struct ARROW_EXPORT TimeType : public FixedWidthType {
  static constexpr Type::type type_id = Type::TIME;
  using Unit = TimeUnit;
  using c_type = int64_t;

  TimeUnit unit;

  int bit_width() const override { return sizeof(c_type) * 8; }

  explicit TimeType(TimeUnit unit = TimeUnit::MILLI)
      : FixedWidthType(Type::TIME), unit(unit) {}
  TimeType(const TimeType& other) : TimeType(other.unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return name(); }
  static std::string name() { return "time"; }
};

struct ARROW_EXPORT TimestampType : public FixedWidthType {
  using Unit = TimeUnit;

  typedef int64_t c_type;
  static constexpr Type::type type_id = Type::TIMESTAMP;

  int bit_width() const override { return sizeof(int64_t) * 8; }

  TimeUnit unit;

  explicit TimestampType(TimeUnit unit = TimeUnit::MILLI)
      : FixedWidthType(Type::TIMESTAMP), unit(unit) {}

  TimestampType(const TimestampType& other) : TimestampType(other.unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return name(); }
  static std::string name() { return "timestamp"; }
};

struct ARROW_EXPORT IntervalType : public FixedWidthType {
  enum class Unit : char { YEAR_MONTH = 0, DAY_TIME = 1 };

  using c_type = int64_t;
  static constexpr Type::type type_id = Type::INTERVAL;

  int bit_width() const override { return sizeof(int64_t) * 8; }

  Unit unit;

  explicit IntervalType(Unit unit = Unit::YEAR_MONTH)
      : FixedWidthType(Type::INTERVAL), unit(unit) {}

  IntervalType(const IntervalType& other) : IntervalType(other.unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return name(); }
  static std::string name() { return "date"; }
};

// Factory functions

std::shared_ptr<DataType> ARROW_EXPORT null();
std::shared_ptr<DataType> ARROW_EXPORT boolean();
std::shared_ptr<DataType> ARROW_EXPORT int8();
std::shared_ptr<DataType> ARROW_EXPORT int16();
std::shared_ptr<DataType> ARROW_EXPORT int32();
std::shared_ptr<DataType> ARROW_EXPORT int64();
std::shared_ptr<DataType> ARROW_EXPORT uint8();
std::shared_ptr<DataType> ARROW_EXPORT uint16();
std::shared_ptr<DataType> ARROW_EXPORT uint32();
std::shared_ptr<DataType> ARROW_EXPORT uint64();
std::shared_ptr<DataType> ARROW_EXPORT float16();
std::shared_ptr<DataType> ARROW_EXPORT float32();
std::shared_ptr<DataType> ARROW_EXPORT float64();
std::shared_ptr<DataType> ARROW_EXPORT utf8();
std::shared_ptr<DataType> ARROW_EXPORT binary();

std::shared_ptr<DataType> ARROW_EXPORT list(const std::shared_ptr<Field>& value_type);
std::shared_ptr<DataType> ARROW_EXPORT list(const std::shared_ptr<DataType>& value_type);

std::shared_ptr<DataType> ARROW_EXPORT date();
std::shared_ptr<DataType> ARROW_EXPORT timestamp(TimeUnit unit);
std::shared_ptr<DataType> ARROW_EXPORT time(TimeUnit unit);

std::shared_ptr<DataType> ARROW_EXPORT struct_(
    const std::vector<std::shared_ptr<Field>>& fields);

std::shared_ptr<DataType> ARROW_EXPORT union_(
    const std::vector<std::shared_ptr<Field>>& child_fields,
    const std::vector<uint8_t>& type_ids, UnionMode mode = UnionMode::SPARSE);

std::shared_ptr<Field> ARROW_EXPORT field(const std::string& name,
    const std::shared_ptr<DataType>& type, bool nullable = true, int64_t dictionary = 0);

}  // namespace arrow

#endif  // ARROW_TYPE_H
