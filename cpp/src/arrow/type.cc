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

#include "arrow/type.h"

#include <sstream>
#include <string>

namespace arrow {

std::string Field::ToString() const {
  std::stringstream ss;
  ss << this->name << " " << this->type->ToString();
  return ss.str();
}

DataType::~DataType() {}

StringType::StringType(bool nullable)
    : DataType(LogicalType::STRING, nullable) {}

StringType::StringType(const StringType& other)
    : StringType(other.nullable) {}

std::string StringType::ToString() const {
  std::string result(name());
  if (!nullable) {
    result.append(" not null");
  }
  return result;
}

std::string ListType::ToString() const {
  std::stringstream s;
  s << "list<" << value_type->ToString() << ">";
  if (!this->nullable) {
    s << " not null";
  }
  return s.str();
}

std::string StructType::ToString() const {
  std::stringstream s;
  s << "struct<";
  for (size_t i = 0; i < fields_.size(); ++i) {
    if (i > 0) s << ", ";
    const std::shared_ptr<Field>& field = fields_[i];
    s << field->name << ": " << field->type->ToString();
  }
  s << ">";
  if (!nullable) s << " not null";
  return s.str();
}

const std::shared_ptr<NullType> NA = std::make_shared<NullType>();
const std::shared_ptr<BooleanType> BOOL = std::make_shared<BooleanType>();
const std::shared_ptr<UInt8Type> UINT8 = std::make_shared<UInt8Type>();
const std::shared_ptr<UInt16Type> UINT16 = std::make_shared<UInt16Type>();
const std::shared_ptr<UInt32Type> UINT32 = std::make_shared<UInt32Type>();
const std::shared_ptr<UInt64Type> UINT64 = std::make_shared<UInt64Type>();
const std::shared_ptr<Int8Type> INT8 = std::make_shared<Int8Type>();
const std::shared_ptr<Int16Type> INT16 = std::make_shared<Int16Type>();
const std::shared_ptr<Int32Type> INT32 = std::make_shared<Int32Type>();
const std::shared_ptr<Int64Type> INT64 = std::make_shared<Int64Type>();
const std::shared_ptr<FloatType> FLOAT = std::make_shared<FloatType>();
const std::shared_ptr<DoubleType> DOUBLE = std::make_shared<DoubleType>();
const std::shared_ptr<StringType> STRING = std::make_shared<StringType>();

} // namespace arrow
