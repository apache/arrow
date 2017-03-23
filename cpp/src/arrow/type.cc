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

#include "arrow/array.h"
#include "arrow/compare.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/visitor.h"

namespace arrow {

bool Field::Equals(const Field& other) const {
  return (this == &other) ||
         (this->name == other.name && this->nullable == other.nullable &&
             this->type->Equals(*other.type.get()));
}

bool Field::Equals(const std::shared_ptr<Field>& other) const {
  return Equals(*other.get());
}

std::string Field::ToString() const {
  std::stringstream ss;
  ss << this->name << ": " << this->type->ToString();
  if (!this->nullable) { ss << " not null"; }
  return ss.str();
}

DataType::~DataType() {}

bool DataType::Equals(const DataType& other) const {
  bool are_equal = false;
  Status error = TypeEquals(*this, other, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Types not comparable: " << error.ToString(); }
  return are_equal;
}

bool DataType::Equals(const std::shared_ptr<DataType>& other) const {
  if (!other) { return false; }
  return Equals(*other.get());
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

int FixedWidthBinaryType::bit_width() const {
  return 8 * byte_width();
}

std::string FixedWidthBinaryType::ToString() const {
  std::stringstream ss;
  ss << "fixed_width_binary[" << byte_width_ << "]";
  return ss.str();
}

std::string StructType::ToString() const {
  std::stringstream s;
  s << "struct<";
  for (int i = 0; i < this->num_children(); ++i) {
    if (i > 0) { s << ", "; }
    std::shared_ptr<Field> field = this->child(i);
    s << field->name << ": " << field->type->ToString();
  }
  s << ">";
  return s.str();
}

std::string Date64Type::ToString() const {
  return std::string("date64[ms]");
}

std::string Date32Type::ToString() const {
  return std::string("date32[day]");
}

// ----------------------------------------------------------------------
// Time types

Time32Type::Time32Type(TimeUnit unit) : FixedWidthType(Type::TIME32), unit(unit) {
  DCHECK(unit == TimeUnit::SECOND || unit == TimeUnit::MILLI)
      << "Must be seconds or milliseconds";
}

std::string Time32Type::ToString() const {
  std::stringstream ss;
  ss << "time32[" << this->unit << "]";
  return ss.str();
}

Time64Type::Time64Type(TimeUnit unit) : FixedWidthType(Type::TIME64), unit(unit) {
  DCHECK(unit == TimeUnit::MICRO || unit == TimeUnit::NANO)
      << "Must be microseconds or nanoseconds";
}

std::string Time64Type::ToString() const {
  std::stringstream ss;
  ss << "time64[" << this->unit << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Timestamp types

std::string TimestampType::ToString() const {
  std::stringstream ss;
  ss << "timestamp[" << this->unit;
  if (this->timezone.size() > 0) { ss << ", tz=" << this->timezone; }
  ss << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Union type

UnionType::UnionType(const std::vector<std::shared_ptr<Field>>& fields,
    const std::vector<uint8_t>& type_codes, UnionMode mode)
    : NestedType(Type::UNION), mode(mode), type_codes(type_codes) {
  children_ = fields;
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
    s << children_[i]->ToString() << "=" << static_cast<int>(type_codes[i]);
  }
  s << ">";
  return s.str();
}

// ----------------------------------------------------------------------
// DictionaryType

DictionaryType::DictionaryType(const std::shared_ptr<DataType>& index_type,
    const std::shared_ptr<Array>& dictionary, bool ordered)
    : FixedWidthType(Type::DICTIONARY),
      index_type_(index_type),
      dictionary_(dictionary),
      ordered_(ordered) {}

int DictionaryType::bit_width() const {
  return static_cast<const FixedWidthType*>(index_type_.get())->bit_width();
}

std::shared_ptr<Array> DictionaryType::dictionary() const {
  return dictionary_;
}

std::string DictionaryType::ToString() const {
  std::stringstream ss;
  ss << "dictionary<values=" << dictionary_->type()->ToString()
     << ", indices=" << index_type_->ToString() << ">";
  return ss.str();
}

// ----------------------------------------------------------------------
// Null type

std::string NullType::ToString() const {
  return name();
}

// ----------------------------------------------------------------------
// Visitors and factory functions

#define ACCEPT_VISITOR(TYPE) \
  Status TYPE::Accept(TypeVisitor* visitor) const { return visitor->Visit(*this); }

ACCEPT_VISITOR(NullType);
ACCEPT_VISITOR(BooleanType);
ACCEPT_VISITOR(BinaryType);
ACCEPT_VISITOR(FixedWidthBinaryType);
ACCEPT_VISITOR(StringType);
ACCEPT_VISITOR(ListType);
ACCEPT_VISITOR(StructType);
ACCEPT_VISITOR(DecimalType);
ACCEPT_VISITOR(UnionType);
ACCEPT_VISITOR(Date32Type);
ACCEPT_VISITOR(Date64Type);
ACCEPT_VISITOR(Time32Type);
ACCEPT_VISITOR(Time64Type);
ACCEPT_VISITOR(TimestampType);
ACCEPT_VISITOR(IntervalType);
ACCEPT_VISITOR(DictionaryType);

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
TYPE_FACTORY(date64, Date64Type);
TYPE_FACTORY(date32, Date32Type);

std::shared_ptr<DataType> fixed_width_binary(int32_t byte_width) {
  return std::make_shared<FixedWidthBinaryType>(byte_width);
}

std::shared_ptr<DataType> timestamp(TimeUnit unit) {
  return std::make_shared<TimestampType>(unit);
}

std::shared_ptr<DataType> timestamp(TimeUnit unit, const std::string& timezone) {
  return std::make_shared<TimestampType>(unit, timezone);
}

std::shared_ptr<DataType> time32(TimeUnit unit) {
  return std::make_shared<Time32Type>(unit);
}

std::shared_ptr<DataType> time64(TimeUnit unit) {
  return std::make_shared<Time64Type>(unit);
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

std::shared_ptr<DataType> union_(const std::vector<std::shared_ptr<Field>>& child_fields,
    const std::vector<uint8_t>& type_codes, UnionMode mode) {
  return std::make_shared<UnionType>(child_fields, type_codes, mode);
}

std::shared_ptr<DataType> dictionary(const std::shared_ptr<DataType>& index_type,
    const std::shared_ptr<Array>& dict_values) {
  return std::make_shared<DictionaryType>(index_type, dict_values);
}

std::shared_ptr<Field> field(
    const std::string& name, const TypePtr& type, bool nullable) {
  return std::make_shared<Field>(name, type, nullable);
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

std::vector<BufferDescr> FixedWidthBinaryType::GetBufferLayout() const {
  return {kValidityBuffer, BufferDescr(BufferType::DATA, byte_width_ * 8)};
}

std::vector<BufferDescr> ListType::GetBufferLayout() const {
  return {kValidityBuffer, kOffsetBuffer};
}

std::vector<BufferDescr> StructType::GetBufferLayout() const {
  return {kValidityBuffer};
}

std::vector<BufferDescr> UnionType::GetBufferLayout() const {
  if (mode == UnionMode::SPARSE) {
    return {kValidityBuffer, kTypeBuffer};
  } else {
    return {kValidityBuffer, kTypeBuffer, kOffsetBuffer};
  }
}

std::string DecimalType::ToString() const {
  std::stringstream s;
  s << "decimal(" << precision << ", " << scale << ")";
  return s.str();
}

std::vector<BufferDescr> DecimalType::GetBufferLayout() const {
  // TODO(wesm)
  return {};
}

}  // namespace arrow
