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

    // 4-byte floating point value
    FLOAT = 10,

    // 8-byte floating point value
    DOUBLE = 11,

    // UTF8 variable-length string as List<Char>
    STRING = 13,

    // Variable-length bytes (no guarantee of UTF8-ness)
    BINARY = 15,

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

    // Union<Null, Int32, Double, String, Bool>
    JSON_SCALAR = 50,

    // User-defined type
    USER = 60
  };
};

struct Field;

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

  int num_children() const { return children_.size(); }

  virtual int value_size() const { return -1; }

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

template <typename Derived>
struct ARROW_EXPORT PrimitiveType : public DataType {
  PrimitiveType() : DataType(Derived::type_enum) {}

  std::string ToString() const override;
};

template <typename Derived>
inline std::string PrimitiveType<Derived>::ToString() const {
  std::string result(static_cast<const Derived*>(this)->name());
  return result;
}

#define PRIMITIVE_DECL(TYPENAME, C_TYPE, ENUM, SIZE, NAME) \
  typedef C_TYPE c_type;                                   \
  static constexpr Type::type type_enum = Type::ENUM;      \
                                                           \
  TYPENAME() : PrimitiveType<TYPENAME>() {}                \
                                                           \
  virtual int value_size() const { return SIZE; }          \
                                                           \
  static const char* name() { return NAME; }

struct ARROW_EXPORT NullType : public PrimitiveType<NullType> {
  PRIMITIVE_DECL(NullType, void, NA, 0, "null");
};

struct ARROW_EXPORT BooleanType : public PrimitiveType<BooleanType> {
  PRIMITIVE_DECL(BooleanType, uint8_t, BOOL, 1, "bool");
};

struct ARROW_EXPORT UInt8Type : public PrimitiveType<UInt8Type> {
  PRIMITIVE_DECL(UInt8Type, uint8_t, UINT8, 1, "uint8");
};

struct ARROW_EXPORT Int8Type : public PrimitiveType<Int8Type> {
  PRIMITIVE_DECL(Int8Type, int8_t, INT8, 1, "int8");
};

struct ARROW_EXPORT UInt16Type : public PrimitiveType<UInt16Type> {
  PRIMITIVE_DECL(UInt16Type, uint16_t, UINT16, 2, "uint16");
};

struct ARROW_EXPORT Int16Type : public PrimitiveType<Int16Type> {
  PRIMITIVE_DECL(Int16Type, int16_t, INT16, 2, "int16");
};

struct ARROW_EXPORT UInt32Type : public PrimitiveType<UInt32Type> {
  PRIMITIVE_DECL(UInt32Type, uint32_t, UINT32, 4, "uint32");
};

struct ARROW_EXPORT Int32Type : public PrimitiveType<Int32Type> {
  PRIMITIVE_DECL(Int32Type, int32_t, INT32, 4, "int32");
};

struct ARROW_EXPORT UInt64Type : public PrimitiveType<UInt64Type> {
  PRIMITIVE_DECL(UInt64Type, uint64_t, UINT64, 8, "uint64");
};

struct ARROW_EXPORT Int64Type : public PrimitiveType<Int64Type> {
  PRIMITIVE_DECL(Int64Type, int64_t, INT64, 8, "int64");
};

struct ARROW_EXPORT FloatType : public PrimitiveType<FloatType> {
  PRIMITIVE_DECL(FloatType, float, FLOAT, 4, "float");
};

struct ARROW_EXPORT DoubleType : public PrimitiveType<DoubleType> {
  PRIMITIVE_DECL(DoubleType, double, DOUBLE, 8, "double");
};

struct ARROW_EXPORT ListType : public DataType {
  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : ListType(value_type, Type::LIST) {}

  explicit ListType(const std::shared_ptr<Field>& value_field) : DataType(Type::LIST) {
    children_ = {value_field};
  }

  const std::shared_ptr<Field>& value_field() const { return children_[0]; }

  const std::shared_ptr<DataType>& value_type() const { return children_[0]->type; }

  static char const* name() { return "list"; }

  std::string ToString() const override;

 protected:
  // Constructor for classes that are implemented as List Arrays.
  ListType(const std::shared_ptr<DataType>& value_type, Type::type logical_type)
      : DataType(logical_type) {
    // TODO ARROW-187 this can technically fail, make a constructor method ?
    children_ = {std::make_shared<Field>("item", value_type)};
  }
};

// BinaryType type is reprsents lists of 1-byte values.
struct ARROW_EXPORT BinaryType : public ListType {
  BinaryType() : BinaryType(Type::BINARY) {}
  static char const* name() { return "binary"; }
  std::string ToString() const override;

 protected:
  // Allow subclasses to change the logical type.
  explicit BinaryType(Type::type logical_type)
      : ListType(std::shared_ptr<DataType>(new UInt8Type()), logical_type) {}
};

// UTF encoded strings
struct ARROW_EXPORT StringType : public BinaryType {
  StringType() : BinaryType(Type::STRING) {}

  static char const* name() { return "string"; }

  std::string ToString() const override;

 protected:
  explicit StringType(Type::type logical_type) : BinaryType(logical_type) {}
};

struct ARROW_EXPORT StructType : public DataType {
  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields)
      : DataType(Type::STRUCT) {
    children_ = fields;
  }

  std::string ToString() const override;
};

// These will be defined elsewhere
template <typename T>
struct type_traits {};

}  // namespace arrow

#endif  // ARROW_TYPE_H
