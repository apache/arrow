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

#include "arrow/util/status.h"

namespace arrow {

std::string Field::ToString() const {
  std::stringstream ss;
  ss << this->name << ": " << this->type->ToString();
  if (!this->nullable) { ss << " not null"; }
  return ss.str();
}

DataType::~DataType() {}

bool DataType::Equals(const DataType* other) const {
  bool equals = other && ((this == other) ||
                             ((this->type == other->type) &&
                                 ((this->num_children() == other->num_children()))));
  if (equals) {
    for (int i = 0; i < num_children(); ++i) {
      // TODO(emkornfield) limit recursion
      if (!children_[i]->Equals(other->children_[i])) { return false; }
    }
  }
  return equals;
}

FloatingPointMeta::Precision HalfFloatType::precision() const {
  return FloatingPointMeta::HALF;
}

FloatingPointMeta::Precision FloatType::precision() const {
  return FloatingPointMeta::SINGLE;
}

FloatingPointMeta::Precision DoubleType::precision() const {
  return FloatingPointMeta::DOUBLE;
}

std::string StringType::ToString() const {
  return std::string("string");
}

std::string ListType::ToString() const {
  std::stringstream s;
  s << "list<" << value_field()->ToString() << ">";
  return s.str();
}

std::string BinaryType::ToString() const {
  return std::string("binary");
}

std::string StructType::ToString() const {
  std::stringstream s;
  s << "struct<";
  for (int i = 0; i < this->num_children(); ++i) {
    if (i > 0) { s << ", "; }
    const std::shared_ptr<Field>& field = this->child(i);
    s << field->name << ": " << field->type->ToString();
  }
  s << ">";
  return s.str();
}

// Visitors and template instantiation

#define ACCEPT_VISITOR(TYPE) \
  Status TYPE::Accept(TypeVisitor* visitor) const { return visitor->Visit(*this); }

ACCEPT_VISITOR(NullType);
ACCEPT_VISITOR(BinaryType);
ACCEPT_VISITOR(StringType);
ACCEPT_VISITOR(ListType);
ACCEPT_VISITOR(StructType);
ACCEPT_VISITOR(DecimalType);
ACCEPT_VISITOR(SparseUnionType);
ACCEPT_VISITOR(DenseUnionType);
ACCEPT_VISITOR(DateType);
ACCEPT_VISITOR(TimeType);
ACCEPT_VISITOR(TimestampType);

const std::string NullType::NAME = "null";
const std::string UInt8Type::NAME = "uint8";
const std::string Int8Type::NAME = "int8";
const std::string UInt16Type::NAME = "uint16";
const std::string Int16Type::NAME = "int16";
const std::string UInt32Type::NAME = "uint32";
const std::string Int32Type::NAME = "int32";
const std::string UInt64Type::NAME = "uint64";
const std::string Int64Type::NAME = "int64";
const std::string HalfFloatType::NAME = "halffloat";
const std::string FloatType::NAME = "float";
const std::string DoubleType::NAME = "double";
const std::string BooleanType::NAME = "bool";
const std::string BinaryType::NAME = "binary";
const std::string StringType::NAME = "utf8";
const std::string DecimalType::NAME = "decimal";
const std::string DateType::NAME = "decimal";
const std::string TimeType::NAME = "time";
const std::string TimestampType::NAME = "timestamp";
const std::string ListType::NAME = "list";
const std::string StructType::NAME = "struct";
const std::string DenseUnionType::NAME = "union";
const std::string SparseUnionType::NAME = "union";

}  // namespace arrow
