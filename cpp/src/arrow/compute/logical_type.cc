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

#include "arrow/compute/logical_type.h"

#include <string>

#include "arrow/compute/expression.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

Status LogicalType::FromArrow(const ::arrow::DataType& type, LogicalTypePtr* out) {
  switch (type.id()) {
    case Type::NA:
      *out = type::null();
      break;
    case Type::BOOL:
      *out = type::boolean();
      break;
    case Type::UINT8:
      *out = type::uint8();
      break;
    case Type::INT8:
      *out = type::int8();
      break;
    case Type::UINT16:
      *out = type::uint16();
      break;
    case Type::INT16:
      *out = type::int16();
      break;
    case Type::UINT32:
      *out = type::uint32();
      break;
    case Type::INT32:
      *out = type::int32();
      break;
    case Type::UINT64:
      *out = type::uint64();
      break;
    case Type::INT64:
      *out = type::int64();
      break;
    case Type::HALF_FLOAT:
      *out = type::float16();
      break;
    case Type::FLOAT:
      *out = type::float32();
      break;
    case Type::DOUBLE:
      *out = type::float64();
      break;
    case Type::STRING:
      *out = type::utf8();
      break;
    case Type::BINARY:
      *out = type::binary();
      break;
    default:
      return Status::NotImplemented("Logical expr for ", type.ToString());
  }
  return Status::OK();
}

namespace type {

bool Any::IsInstance(const Expr& expr) const { return InheritsFrom<ValueExpr>(expr); }

std::string Any::ToString() const { return "Any"; }

#define SIMPLE_LOGICAL_TYPE(NAME)                 \
  bool NAME::IsInstance(const Expr& expr) const { \
    return InheritsFrom<value::NAME>(expr);       \
  }                                               \
  std::string NAME::ToString() const { return "" #NAME; }

SIMPLE_LOGICAL_TYPE(Null)
SIMPLE_LOGICAL_TYPE(Bool)
SIMPLE_LOGICAL_TYPE(Number)
SIMPLE_LOGICAL_TYPE(Integer)
SIMPLE_LOGICAL_TYPE(Floating)
SIMPLE_LOGICAL_TYPE(SignedInteger)
SIMPLE_LOGICAL_TYPE(UnsignedInteger)
SIMPLE_LOGICAL_TYPE(Int8)
SIMPLE_LOGICAL_TYPE(Int16)
SIMPLE_LOGICAL_TYPE(Int32)
SIMPLE_LOGICAL_TYPE(Int64)
SIMPLE_LOGICAL_TYPE(UInt8)
SIMPLE_LOGICAL_TYPE(UInt16)
SIMPLE_LOGICAL_TYPE(UInt32)
SIMPLE_LOGICAL_TYPE(UInt64)
SIMPLE_LOGICAL_TYPE(Float16)
SIMPLE_LOGICAL_TYPE(Float32)
SIMPLE_LOGICAL_TYPE(Float64)
SIMPLE_LOGICAL_TYPE(Binary)
SIMPLE_LOGICAL_TYPE(Utf8)

#define SIMPLE_TYPE_FACTORY(NAME, TYPE) \
  LogicalTypePtr NAME() { return std::make_shared<TYPE>(); }

SIMPLE_TYPE_FACTORY(any, Any);
SIMPLE_TYPE_FACTORY(null, Null);
SIMPLE_TYPE_FACTORY(boolean, Bool);
SIMPLE_TYPE_FACTORY(number, Number);
SIMPLE_TYPE_FACTORY(integer, Integer);
SIMPLE_TYPE_FACTORY(signed_integer, SignedInteger);
SIMPLE_TYPE_FACTORY(unsigned_integer, UnsignedInteger);
SIMPLE_TYPE_FACTORY(floating, Floating);
SIMPLE_TYPE_FACTORY(int8, Int8);
SIMPLE_TYPE_FACTORY(int16, Int16);
SIMPLE_TYPE_FACTORY(int32, Int32);
SIMPLE_TYPE_FACTORY(int64, Int64);
SIMPLE_TYPE_FACTORY(uint8, UInt8);
SIMPLE_TYPE_FACTORY(uint16, UInt16);
SIMPLE_TYPE_FACTORY(uint32, UInt32);
SIMPLE_TYPE_FACTORY(uint64, UInt64);
SIMPLE_TYPE_FACTORY(float16, Float16);
SIMPLE_TYPE_FACTORY(float32, Float32);
SIMPLE_TYPE_FACTORY(float64, Float64);
SIMPLE_TYPE_FACTORY(binary, Binary);
SIMPLE_TYPE_FACTORY(utf8, Utf8);

}  // namespace type
}  // namespace compute
}  // namespace arrow
