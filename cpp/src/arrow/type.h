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

    // CHAR(N): fixed-length UTF8 string with length N
    CHAR = 12,

    // UTF8 variable-length string as List<Char>
    STRING = 13,

    // VARCHAR(N): Null-terminated string type embedded in a CHAR(N + 1)
    VARCHAR = 14,

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

struct DataType {
  Type::type type;

  std::vector<std::shared_ptr<Field>> children_;

  explicit DataType(Type::type type) :
      type(type) {}

  virtual ~DataType();

  bool Equals(const DataType* other) {
    // Call with a pointer so more friendly to subclasses
    return this == other || (this->type == other->type);
  }

  bool Equals(const std::shared_ptr<DataType>& other) {
    return Equals(other.get());
  }

  const std::shared_ptr<Field>& child(int i) const {
    return children_[i];
  }

  int num_children() const {
    return children_.size();
  }

  virtual std::string ToString() const = 0;
};

typedef std::shared_ptr<DataType> TypePtr;

// A field is a piece of metadata that includes (for now) a name and a data
// type
struct Field {
  // Field name
  std::string name;

  // The field's data type
  TypePtr type;

  // Fields can be nullable
  bool nullable;

  Field(const std::string& name, const TypePtr& type, bool nullable = true) :
      name(name),
      type(type),
      nullable(nullable) {}

  bool operator==(const Field& other) const {
    return this->Equals(other);
  }

  bool operator!=(const Field& other) const {
    return !this->Equals(other);
  }

  bool Equals(const Field& other) const {
    return (this == &other) || (this->name == other.name &&
        this->nullable == other.nullable &&
        this->type->Equals(other.type.get()));
  }

  bool Equals(const std::shared_ptr<Field>& other) const {
    return Equals(*other.get());
  }

  std::string ToString() const;
};

template <typename Derived>
struct PrimitiveType : public DataType {
  PrimitiveType() : DataType(Derived::type_enum) {}

  std::string ToString() const override;
};

template <typename Derived>
inline std::string PrimitiveType<Derived>::ToString() const {
  std::string result(static_cast<const Derived*>(this)->name());
  return result;
}

#define PRIMITIVE_DECL(TYPENAME, C_TYPE, ENUM, SIZE, NAME)  \
  typedef C_TYPE c_type;                                    \
  static constexpr Type::type type_enum = Type::ENUM;       \
  static constexpr int size = SIZE;                         \
                                                            \
  TYPENAME()                                                \
      : PrimitiveType<TYPENAME>() {}                        \
                                                            \
  static const char* name() {                               \
    return NAME;                                            \
  }

struct NullType : public PrimitiveType<NullType> {
  PRIMITIVE_DECL(NullType, void, NA, 0, "null");
};

struct BooleanType : public PrimitiveType<BooleanType> {
  PRIMITIVE_DECL(BooleanType, uint8_t, BOOL, 1, "bool");
};

struct UInt8Type : public PrimitiveType<UInt8Type> {
  PRIMITIVE_DECL(UInt8Type, uint8_t, UINT8, 1, "uint8");
};

struct Int8Type : public PrimitiveType<Int8Type> {
  PRIMITIVE_DECL(Int8Type, int8_t, INT8, 1, "int8");
};

struct UInt16Type : public PrimitiveType<UInt16Type> {
  PRIMITIVE_DECL(UInt16Type, uint16_t, UINT16, 2, "uint16");
};

struct Int16Type : public PrimitiveType<Int16Type> {
  PRIMITIVE_DECL(Int16Type, int16_t, INT16, 2, "int16");
};

struct UInt32Type : public PrimitiveType<UInt32Type> {
  PRIMITIVE_DECL(UInt32Type, uint32_t, UINT32, 4, "uint32");
};

struct Int32Type : public PrimitiveType<Int32Type> {
  PRIMITIVE_DECL(Int32Type, int32_t, INT32, 4, "int32");
};

struct UInt64Type : public PrimitiveType<UInt64Type> {
  PRIMITIVE_DECL(UInt64Type, uint64_t, UINT64, 8, "uint64");
};

struct Int64Type : public PrimitiveType<Int64Type> {
  PRIMITIVE_DECL(Int64Type, int64_t, INT64, 8, "int64");
};

struct FloatType : public PrimitiveType<FloatType> {
  PRIMITIVE_DECL(FloatType, float, FLOAT, 4, "float");
};

struct DoubleType : public PrimitiveType<DoubleType> {
  PRIMITIVE_DECL(DoubleType, double, DOUBLE, 8, "double");
};

struct ListType : public DataType {
  // List can contain any other logical value type
  explicit ListType(const std::shared_ptr<DataType>& value_type)
      : DataType(Type::LIST) {
    children_ = {std::make_shared<Field>("item", value_type)};
  }

  explicit ListType(const std::shared_ptr<Field>& value_field)
      : DataType(Type::LIST) {
    children_ = {value_field};
  }

  const std::shared_ptr<Field>& value_field() const {
    return children_[0];
  }

  const std::shared_ptr<DataType>& value_type() const {
    return children_[0]->type;
  }

  static char const *name() {
    return "list";
  }

  std::string ToString() const override;
};

// String is a logical type consisting of a physical list of 1-byte values
struct StringType : public DataType {
  StringType();

  static char const *name() {
    return "string";
  }

  std::string ToString() const override;
};

struct StructType : public DataType {
  explicit StructType(const std::vector<std::shared_ptr<Field>>& fields)
      : DataType(Type::STRUCT) {
    children_ = fields;
  }

  std::string ToString() const override;
};

} // namespace arrow

#endif  // ARROW_TYPE_H
