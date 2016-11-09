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

#include "arrow/util/macros.h"
#include "arrow/util/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

struct Field;

// Type forward declarations for the TypeVisitor
struct DataType;
struct NullType;
struct BooleanType;
struct Int8Type;
struct Int16Type;
struct Int32Type;
struct Int64Type;
struct UInt8Type;
struct UInt16Type;
struct UInt32Type;
struct UInt64Type;
struct HalfFloatType;
struct FloatType;
struct DoubleType;
struct StringType;
struct BinaryType;
struct DateType;
struct TimeType;
struct TimestampType;
struct DecimalType;
struct ListType;
struct StructType;
struct DenseUnionType;
struct SparseUnionType;

class TypeVisitor {
 public:
  virtual Status Visit(const NullType& type) = 0;
  virtual Status Visit(const BooleanType& type) = 0;
  virtual Status Visit(const Int8Type& type) = 0;
  virtual Status Visit(const Int16Type& type) = 0;
  virtual Status Visit(const Int32Type& type) = 0;
  virtual Status Visit(const Int64Type& type) = 0;
  virtual Status Visit(const UInt8Type& type) = 0;
  virtual Status Visit(const UInt16Type& type) = 0;
  virtual Status Visit(const UInt32Type& type) = 0;
  virtual Status Visit(const UInt64Type& type) = 0;
  virtual Status Visit(const HalfFloatType& type) = 0;
  virtual Status Visit(const FloatType& type) = 0;
  virtual Status Visit(const DoubleType& type) = 0;
  virtual Status Visit(const StringType& type) = 0;
  virtual Status Visit(const BinaryType& type) = 0;
  virtual Status Visit(const DateType& type) = 0;
  virtual Status Visit(const TimeType& type) = 0;
  virtual Status Visit(const TimestampType& type) = 0;
  virtual Status Visit(const DecimalType& type) = 0;
  virtual Status Visit(const ListType& type) = 0;
  virtual Status Visit(const StructType& type) = 0;
  virtual Status Visit(const DenseUnionType& type) = 0;
  virtual Status Visit(const SparseUnionType& type) = 0;
};

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

    // Timestamp as double seconds since the UNIX epoch
    TIMESTAMP_DOUBLE = 18,

    // Exact time encoded with int64, default unit millisecond
    TIME = 19,

    // Precision- and scale-based decimal type. Storage type depends on the
    // parameters.
    DECIMAL = 20,

    // Decimal value encoded as a text string
    DECIMAL_TEXT = 21,

    // A list of some logical data type
    LIST = 30,

    // Struct of logical types
    STRUCT = 31,

    // Unions of logical types
    DENSE_UNION = 32,
    SPARSE_UNION = 33,
  };
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

  const std::shared_ptr<Field>& child(int i) const { return children_[i]; }

  const std::vector<std::shared_ptr<Field>>& children() const { return children_; }

  int num_children() const { return children_.size(); }

  virtual int bit_width() const { return -1; }

  virtual Status Accept(TypeVisitor* visitor) const = 0;

  virtual std::string ToString() const = 0;
};

typedef std::shared_ptr<DataType> TypePtr;

// A field is a piece of metadata that includes (for now) a name and a data
// type
struct ARROW_EXPORT Field {
  // Field name
  std::string name;

  // The field's data type
  TypePtr type;

  // Fields can be nullable
  bool nullable;

  // optional dictionary id if the field is dictionary encoded
  // 0 means it's not dictionary encoded
  int64_t dictionary;

  Field(const std::string& name, const TypePtr& type, bool nullable = true,
      int64_t dictionary = 0)
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

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
struct ARROW_EXPORT PrimitiveType : public DataType {
  using c_type = C_TYPE;
  static constexpr Type::type type_id = TYPE_ID;

  PrimitiveType() : DataType(TYPE_ID) {}

  int bit_width() const override { return sizeof(C_TYPE) * 8; }

  Status Accept(TypeVisitor* visitor) const override {
    return visitor->Visit(*static_cast<const DERIVED*>(this));
  }

  std::string ToString() const override { return std::string(DERIVED::NAME); }
};

struct ARROW_EXPORT NullType : public DataType {
  static constexpr Type::type type_enum = Type::NA;

  NullType() : DataType(Type::NA) {}

  int bit_width() const override { return 0; }

  Status Accept(TypeVisitor* visitor) const override;

  static const std::string NAME;

  std::string ToString() const override { return NAME; }
};

struct IntegerMeta {
  virtual bool is_signed() const = 0;
};

struct FloatingPointMeta {
  enum Precision { HALF, SINGLE, DOUBLE };
  virtual Precision precision() const = 0;
};

template <typename DERIVED, Type::type TYPE_ID, typename C_TYPE>
struct IntegerTypeImpl : public PrimitiveType<DERIVED, TYPE_ID, C_TYPE>,
                         public IntegerMeta {
  bool is_signed() const override { return std::is_signed<C_TYPE>::value; }
};

struct ARROW_EXPORT BooleanType : public PrimitiveType<BooleanType, Type::BOOL, uint8_t> {
  int bit_width() const override { return 1; }
  static const std::string NAME;
};

struct ARROW_EXPORT UInt8Type : public IntegerTypeImpl<UInt8Type, Type::UINT8, uint8_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT Int8Type : public IntegerTypeImpl<Int8Type, Type::INT8, int8_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT UInt16Type
    : public IntegerTypeImpl<UInt16Type, Type::UINT16, uint16_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT Int16Type : public IntegerTypeImpl<Int16Type, Type::INT16, int16_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT UInt32Type
    : public IntegerTypeImpl<UInt32Type, Type::UINT32, uint32_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT Int32Type : public IntegerTypeImpl<Int32Type, Type::INT32, int32_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT UInt64Type
    : public IntegerTypeImpl<UInt64Type, Type::UINT64, uint64_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT Int64Type : public IntegerTypeImpl<Int64Type, Type::INT64, int64_t> {
  static const std::string NAME;
};

struct ARROW_EXPORT HalfFloatType
    : public PrimitiveType<HalfFloatType, Type::HALF_FLOAT, uint16_t>,
      public FloatingPointMeta {
  Precision precision() const override;
  static const std::string NAME;
};

struct ARROW_EXPORT FloatType : public PrimitiveType<FloatType, Type::FLOAT, float>,
                                public FloatingPointMeta {
  Precision precision() const override;
  static const std::string NAME;
};

struct ARROW_EXPORT DoubleType : public PrimitiveType<DoubleType, Type::DOUBLE, double>,
                                 public FloatingPointMeta {
  Precision precision() const override;
  static const std::string NAME;
};

struct ARROW_EXPORT ListType : public DataType {
  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(std::make_shared<Field>("item", value_type)) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : DataType(Type::LIST) {
    children_ = {value_field};
  }

  const std::shared_ptr<Field>& value_field() const { return children_[0]; }

  const std::shared_ptr<DataType>& value_type() const { return children_[0]->type; }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static const std::string NAME;
};

// BinaryType type is reprsents lists of 1-byte values.
struct ARROW_EXPORT BinaryType : public DataType {
  BinaryType() : BinaryType(Type::BINARY) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;

  static const std::string NAME;

 protected:
  // Allow subclasses to change the logical type.
  explicit BinaryType(Type::type logical_type) : DataType(logical_type) {}
};

// UTF encoded strings
struct ARROW_EXPORT StringType : public BinaryType {
  StringType() : BinaryType(Type::STRING) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static const std::string NAME;
};

struct ARROW_EXPORT StructType : public DataType {
  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields)
      : DataType(Type::STRUCT) {
    children_ = fields;
  }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static const std::string NAME;
};

struct ARROW_EXPORT DecimalType : public DataType {
  explicit DecimalType(int precision_, int scale_)
      : DataType(Type::DECIMAL), precision(precision_), scale(scale_) {}
  int precision;
  int scale;

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static const std::string NAME;
};

template <Type::type T>
struct UnionType : public DataType {
  std::vector<TypePtr> child_types_;

  UnionType() : DataType(T) {}

  const TypePtr& child(int i) const { return child_types_[i]; }
  int num_children() const { return child_types_.size(); }
};

struct DenseUnionType : public UnionType<Type::DENSE_UNION> {
  typedef UnionType<Type::DENSE_UNION> Base;

  explicit DenseUnionType(const std::vector<TypePtr>& child_types) : Base() {
    child_types_ = child_types;
  }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static const std::string NAME;
};

struct SparseUnionType : public UnionType<Type::SPARSE_UNION> {
  typedef UnionType<Type::SPARSE_UNION> Base;

  explicit SparseUnionType(const std::vector<TypePtr>& child_types) : Base() {
    child_types_ = child_types;
  }

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override;
  static const std::string NAME;
};

struct ARROW_EXPORT DateType : public DataType {
  enum class Unit : char { DAY = 0, MONTH = 1, YEAR = 2 };

  Unit unit;

  explicit DateType(Unit unit = Unit::DAY) : DataType(Type::DATE), unit(unit) {}

  DateType(const DateType& other) : DateType(other.unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return NAME; }
  static const std::string NAME;
};

struct ARROW_EXPORT TimeType : public DataType {
  enum class Unit : char { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };

  Unit unit;

  explicit TimeType(Unit unit = Unit::MILLI) : DataType(Type::TIME), unit(unit) {}
  TimeType(const TimeType& other) : TimeType(other.unit) {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return NAME; }
  static const std::string NAME;
};

struct ARROW_EXPORT TimestampType : public DataType {
  enum class Unit : char { SECOND = 0, MILLI = 1, MICRO = 2, NANO = 3 };

  typedef int64_t c_type;
  static constexpr Type::type type_enum = Type::TIMESTAMP;

  int bit_width() const override { return sizeof(int64_t) * 8; }

  Unit unit;

  explicit TimestampType(Unit unit = Unit::MILLI)
      : DataType(Type::TIMESTAMP), unit(unit) {}

  TimestampType(const TimestampType& other) : TimestampType(other.unit) {}
  virtual ~TimestampType() {}

  Status Accept(TypeVisitor* visitor) const override;
  std::string ToString() const override { return NAME; }
  static const std::string NAME;
};

// These will be defined elsewhere
template <typename T>
struct TypeTraits {};

}  // namespace arrow

#endif  // ARROW_TYPE_H
