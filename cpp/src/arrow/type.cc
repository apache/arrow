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

std::string BooleanType::ToString() const {
  return name();
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

std::string UnionType::ToString() const {
  std::stringstream s;

  if (mode == UnionMode::SPARSE) {
    s << "union[sparse]<";
  } else {
    s << "union[dense]<";
  }

  for (size_t i = 0; i < children_.size(); ++i) {
    if (i) { s << ", "; }
    s << children_[i]->ToString();
  }
  s << ">";
  return s.str();
}

std::string NullType::ToString() const {
  return name();
}

// Visitors and template instantiation

#define ACCEPT_VISITOR(TYPE) \
  Status TYPE::Accept(TypeVisitor* visitor) const { return visitor->Visit(*this); }

ACCEPT_VISITOR(NullType);
ACCEPT_VISITOR(BooleanType);
ACCEPT_VISITOR(BinaryType);
ACCEPT_VISITOR(StringType);
ACCEPT_VISITOR(ListType);
ACCEPT_VISITOR(StructType);
ACCEPT_VISITOR(DecimalType);
ACCEPT_VISITOR(UnionType);
ACCEPT_VISITOR(DateType);
ACCEPT_VISITOR(TimeType);
ACCEPT_VISITOR(TimestampType);
ACCEPT_VISITOR(IntervalType);

#define TYPE_FACTORY(NAME, KLASS)                                        \
  std::shared_ptr<DataType> NAME() {                                     \
    static std::shared_ptr<DataType> result = std::make_shared<KLASS>(); \
    return result;                                                       \
  }

TYPE_FACTORY(null, NullType);
TYPE_FACTORY(boolean, BooleanType);
TYPE_FACTORY(int8, Int8Type);
TYPE_FACTORY(uint8, UInt8Type);
TYPE_FACTORY(int16, Int16Type);
TYPE_FACTORY(uint16, UInt16Type);
TYPE_FACTORY(int32, Int32Type);
TYPE_FACTORY(uint32, UInt32Type);
TYPE_FACTORY(int64, Int64Type);
TYPE_FACTORY(uint64, UInt64Type);
TYPE_FACTORY(float16, HalfFloatType);
TYPE_FACTORY(float32, FloatType);
TYPE_FACTORY(float64, DoubleType);
TYPE_FACTORY(utf8, StringType);
TYPE_FACTORY(binary, BinaryType);
TYPE_FACTORY(date, DateType);

std::shared_ptr<DataType> timestamp(TimeUnit unit) {
  static std::shared_ptr<DataType> result = std::make_shared<TimestampType>();
  return result;
}

std::shared_ptr<DataType> time(TimeUnit unit) {
  static std::shared_ptr<DataType> result = std::make_shared<TimeType>();
  return result;
}

std::shared_ptr<DataType> list(const std::shared_ptr<DataType>& value_type) {
  return std::make_shared<ListType>(value_type);
}

std::shared_ptr<DataType> list(const std::shared_ptr<Field>& value_field) {
  return std::make_shared<ListType>(value_field);
}

std::shared_ptr<DataType> struct_(const std::vector<std::shared_ptr<Field>>& fields) {
  return std::make_shared<StructType>(fields);
}

std::shared_ptr<DataType> ARROW_EXPORT union_(
    const std::vector<std::shared_ptr<Field>>& child_fields,
    const std::vector<uint8_t>& type_ids, UnionMode mode) {
  return std::make_shared<UnionType>(child_fields, type_ids, mode);
}

std::shared_ptr<Field> field(
    const std::string& name, const TypePtr& type, bool nullable, int64_t dictionary) {
  return std::make_shared<Field>(name, type, nullable, dictionary);
}

static const BufferDescr kValidityBuffer(BufferType::VALIDITY, 1);
static const BufferDescr kOffsetBuffer(BufferType::OFFSET, 32);
static const BufferDescr kTypeBuffer(BufferType::TYPE, 32);
static const BufferDescr kBooleanBuffer(BufferType::DATA, 1);
static const BufferDescr kValues64(BufferType::DATA, 64);
static const BufferDescr kValues32(BufferType::DATA, 32);
static const BufferDescr kValues16(BufferType::DATA, 16);
static const BufferDescr kValues8(BufferType::DATA, 8);

std::vector<BufferDescr> FixedWidthType::GetBufferLayout() const {
  return {kValidityBuffer, BufferDescr(BufferType::DATA, bit_width())};
}

std::vector<BufferDescr> NullType::GetBufferLayout() const {
  return {};
}

std::vector<BufferDescr> BinaryType::GetBufferLayout() const {
  return {kValidityBuffer, kOffsetBuffer, kValues8};
}

std::vector<BufferDescr> ListType::GetBufferLayout() const {
  return {kValidityBuffer, kOffsetBuffer};
}

std::vector<BufferDescr> StructType::GetBufferLayout() const {
  return {kValidityBuffer, kTypeBuffer};
}

std::vector<BufferDescr> UnionType::GetBufferLayout() const {
  if (mode == UnionMode::SPARSE) {
    return {kValidityBuffer, kTypeBuffer};
  } else {
    return {kValidityBuffer, kTypeBuffer, kOffsetBuffer};
  }
}

std::vector<BufferDescr> DecimalType::GetBufferLayout() const {
  // TODO(wesm)
  return {};
}

}  // namespace arrow
