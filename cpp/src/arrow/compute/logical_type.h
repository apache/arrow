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

// Metadata objects for creating well-typed expressions. These are distinct
// from (and higher level than) arrow::DataType as some type parameters (like
// decimal scale and precision) may not be known at expression build time, and
// these are resolved later on evaluation

#pragma once

#include <memory>
#include <string>

#include "arrow/util/visibility.h"

namespace arrow {

class Status;

namespace compute {

class Expr;

/// \brief An object that represents either a single concrete value type or a
/// group of related types, to help with expression type validation and other
/// purposes
class LogicalType {
 public:
  enum Id {
    ANY,
    NUMBER,
    INTEGER,
    SIGNED_INTEGER,
    UNSIGNED_INTEGER,
    FLOATING,
    NULL_,
    BOOL,
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    HALF_FLOAT,
    FLOAT,
    DOUBLE,
    BINARY,
    UTF8,
    DATE,
    TIME,
    TIMESTAMP,
    DECIMAL,
    LIST,
    STRUCT
  };

  Id id() const { return id_; }

  virtual std::string ToString() const = 0;

  /// \brief Check if expression is an instance of this type class
  virtual bool IsInstance(const Expr& expr) const = 0;

 protected:
  explicit LogicalType(Id id) : id_(id) {}
  Id id_;
};

namespace type {

/// \brief Logical type for any value type
class Any : public LogicalType {
 public:
  Any() : LogicalType(LogicalType::ANY) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for null
class Null : public LogicalType {
 public:
  Null() : LogicalType(LogicalType::NULL_) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for concrete boolean
class Bool : public LogicalType {
 public:
  Bool() : LogicalType(LogicalType::BOOL) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for any number (integer or floating point)
class Number : public LogicalType {
 public:
  Number() : Number(LogicalType::NUMBER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Number(Id type_id) : LogicalType(type_id) {}
};

/// \brief Logical type for any integer
class Integer : public Number {
 public:
  Integer() : Integer(LogicalType::INTEGER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Integer(Id type_id) : Number(type_id) {}
};

/// \brief Logical type for any floating point number
class Floating : public Number {
 public:
  Floating() : Floating(LogicalType::FLOATING) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Floating(Id type_id) : Number(type_id) {}
};

/// \brief Logical type for any signed integer
class SignedInteger : public Integer {
 public:
  SignedInteger() : SignedInteger(LogicalType::SIGNED_INTEGER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit SignedInteger(Id type_id) : Integer(type_id) {}
};

/// \brief Logical type for any unsigned integer
class UnsignedInteger : public Integer {
 public:
  UnsignedInteger() : UnsignedInteger(LogicalType::UNSIGNED_INTEGER) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit UnsignedInteger(Id type_id) : Integer(type_id) {}
};

/// \brief Logical type for int8
class Int8 : public SignedInteger {
 public:
  Int8() : SignedInteger(LogicalType::INT8) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for int16
class Int16 : public SignedInteger {
 public:
  Int16() : SignedInteger(LogicalType::INT16) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for int32
class Int32 : public SignedInteger {
 public:
  Int32() : SignedInteger(LogicalType::INT32) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for int64
class Int64 : public SignedInteger {
 public:
  Int64() : SignedInteger(LogicalType::INT64) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint8
class UInt8 : public UnsignedInteger {
 public:
  UInt8() : UnsignedInteger(LogicalType::UINT8) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint16
class UInt16 : public UnsignedInteger {
 public:
  UInt16() : UnsignedInteger(LogicalType::UINT16) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint32
class UInt32 : public UnsignedInteger {
 public:
  UInt32() : UnsignedInteger(LogicalType::UINT32) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for uint64
class UInt64 : public UnsignedInteger {
 public:
  UInt64() : UnsignedInteger(LogicalType::UINT64) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for 16-bit floating point
class HalfFloat : public Floating {
 public:
  HalfFloat() : Floating(LogicalType::HALF_FLOAT) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for 32-bit floating point
class Float : public Floating {
 public:
  Float() : Floating(LogicalType::FLOAT) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for 64-bit floating point
class Double : public Floating {
 public:
  Double() : Floating(LogicalType::DOUBLE) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

/// \brief Logical type for variable-size binary
class Binary : public LogicalType {
 public:
  Binary() : Binary(LogicalType::BINARY) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;

 protected:
  explicit Binary(Id type_id) : LogicalType(type_id) {}
};

/// \brief Logical type for variable-size binary
class Utf8 : public Binary {
 public:
  Utf8() : Binary(LogicalType::UTF8) {}
  bool IsInstance(const Expr& expr) const override;
  std::string ToString() const override;
};

#define SIMPLE_TYPE_FACTORY(NAME, TYPE) \
  static inline std::shared_ptr<LogicalType> NAME() { return std::make_shared<TYPE>(); }

SIMPLE_TYPE_FACTORY(any, Any);
SIMPLE_TYPE_FACTORY(null, Null);
SIMPLE_TYPE_FACTORY(boolean, Any);
SIMPLE_TYPE_FACTORY(number, Number);
SIMPLE_TYPE_FACTORY(floating, Floating);
SIMPLE_TYPE_FACTORY(int8, Int8);
SIMPLE_TYPE_FACTORY(int16, Int16);
SIMPLE_TYPE_FACTORY(int32, Int32);
SIMPLE_TYPE_FACTORY(int64, Int64);
SIMPLE_TYPE_FACTORY(uint8, UInt8);
SIMPLE_TYPE_FACTORY(uint16, UInt16);
SIMPLE_TYPE_FACTORY(uint32, UInt32);
SIMPLE_TYPE_FACTORY(uint64, UInt64);
SIMPLE_TYPE_FACTORY(half_float, HalfFloat);
SIMPLE_TYPE_FACTORY(float_, Float);
SIMPLE_TYPE_FACTORY(double_, Double);

}  // namespace type
}  // namespace compute
}  // namespace arrow
