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

#include <climits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/compare.h"
#include "arrow/status.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/visitor.h"

namespace arrow {

std::shared_ptr<Field> Field::AddMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  return std::make_shared<Field>(name_, type_, nullable_, metadata);
}

std::shared_ptr<Field> Field::RemoveMetadata() const {
  return std::make_shared<Field>(name_, type_, nullable_);
}

std::vector<std::shared_ptr<Field>> Field::Flatten() const {
  std::vector<std::shared_ptr<Field>> flattened;
  if (type_->id() == Type::STRUCT) {
    for (const auto& child : type_->children()) {
      auto flattened_child = std::make_shared<Field>(*child);
      flattened.push_back(flattened_child);
      flattened_child->name_.insert(0, name() + ".");
      flattened_child->nullable_ |= nullable_;
    }
  } else {
    flattened.push_back(std::make_shared<Field>(*this));
  }
  return flattened;
}

bool Field::Equals(const Field& other) const {
  if (this == &other) {
    return true;
  }
  if (this->name_ == other.name_ && this->nullable_ == other.nullable_ &&
      this->type_->Equals(*other.type_.get())) {
    if (metadata_ == nullptr && other.metadata_ == nullptr) {
      return true;
    } else if ((metadata_ == nullptr) ^ (other.metadata_ == nullptr)) {
      return false;
    } else {
      return metadata_->Equals(*other.metadata_);
    }
  }
  return false;
}

bool Field::Equals(const std::shared_ptr<Field>& other) const {
  return Equals(*other.get());
}

std::string Field::ToString() const {
  std::stringstream ss;
  ss << this->name_ << ": " << this->type_->ToString();
  if (!this->nullable_) {
    ss << " not null";
  }
  return ss.str();
}

DataType::~DataType() {}

bool DataType::Equals(const DataType& other) const { return TypeEquals(*this, other); }

bool DataType::Equals(const std::shared_ptr<DataType>& other) const {
  if (!other) {
    return false;
  }
  return Equals(*other.get());
}

std::string BooleanType::ToString() const { return name(); }

FloatingPoint::Precision HalfFloatType::precision() const { return FloatingPoint::HALF; }

FloatingPoint::Precision FloatType::precision() const { return FloatingPoint::SINGLE; }

FloatingPoint::Precision DoubleType::precision() const { return FloatingPoint::DOUBLE; }

std::string StringType::ToString() const { return std::string("string"); }

std::string ListType::ToString() const {
  std::stringstream s;
  s << "list<" << value_field()->ToString() << ">";
  return s.str();
}

std::string BinaryType::ToString() const { return std::string("binary"); }

int FixedSizeBinaryType::bit_width() const { return CHAR_BIT * byte_width(); }

std::string FixedSizeBinaryType::ToString() const {
  std::stringstream ss;
  ss << "fixed_size_binary[" << byte_width_ << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Date types

DateType::DateType(Type::type type_id, DateUnit unit)
    : FixedWidthType(type_id), unit_(unit) {}

Date32Type::Date32Type() : DateType(Type::DATE32, DateUnit::DAY) {}

Date64Type::Date64Type() : DateType(Type::DATE64, DateUnit::MILLI) {}

std::string Date64Type::ToString() const { return std::string("date64[ms]"); }

std::string Date32Type::ToString() const { return std::string("date32[day]"); }

// ----------------------------------------------------------------------
// Time types

TimeType::TimeType(Type::type type_id, TimeUnit::type unit)
    : FixedWidthType(type_id), unit_(unit) {}

Time32Type::Time32Type(TimeUnit::type unit) : TimeType(Type::TIME32, unit) {
  DCHECK(unit == TimeUnit::SECOND || unit == TimeUnit::MILLI)
      << "Must be seconds or milliseconds";
}

std::string Time32Type::ToString() const {
  std::stringstream ss;
  ss << "time32[" << this->unit_ << "]";
  return ss.str();
}

Time64Type::Time64Type(TimeUnit::type unit) : TimeType(Type::TIME64, unit) {
  DCHECK(unit == TimeUnit::MICRO || unit == TimeUnit::NANO)
      << "Must be microseconds or nanoseconds";
}

std::string Time64Type::ToString() const {
  std::stringstream ss;
  ss << "time64[" << this->unit_ << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Timestamp types

std::string TimestampType::ToString() const {
  std::stringstream ss;
  ss << "timestamp[" << this->unit_;
  if (this->timezone_.size() > 0) {
    ss << ", tz=" << this->timezone_;
  }
  ss << "]";
  return ss.str();
}

// ----------------------------------------------------------------------
// Union type

UnionType::UnionType(const std::vector<std::shared_ptr<Field>>& fields,
                     const std::vector<uint8_t>& type_codes, UnionMode::type mode)
    : NestedType(Type::UNION), mode_(mode), type_codes_(type_codes) {
  children_ = fields;
}

std::string UnionType::ToString() const {
  std::stringstream s;

  if (mode_ == UnionMode::SPARSE) {
    s << "union[sparse]<";
  } else {
    s << "union[dense]<";
  }

  for (size_t i = 0; i < children_.size(); ++i) {
    if (i) {
      s << ", ";
    }
    s << children_[i]->ToString() << "=" << static_cast<int>(type_codes_[i]);
  }
  s << ">";
  return s.str();
}

// ----------------------------------------------------------------------
// Struct type

std::string StructType::ToString() const {
  std::stringstream s;
  s << "struct<";
  for (int i = 0; i < this->num_children(); ++i) {
    if (i > 0) {
      s << ", ";
    }
    std::shared_ptr<Field> field = this->child(i);
    s << field->name() << ": " << field->type()->ToString();
  }
  s << ">";
  return s.str();
}

std::shared_ptr<Field> StructType::GetChildByName(const std::string& name) const {
  int i = GetChildIndex(name);
  return i == -1 ? nullptr : children_[i];
}

int StructType::GetChildIndex(const std::string& name) const {
  if (children_.size() > 0 && name_to_index_.size() == 0) {
    for (size_t i = 0; i < children_.size(); ++i) {
      name_to_index_[children_[i]->name()] = static_cast<int>(i);
    }
  }

  auto it = name_to_index_.find(name);
  if (it == name_to_index_.end()) {
    return -1;
  } else {
    return it->second;
  }
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

std::shared_ptr<Array> DictionaryType::dictionary() const { return dictionary_; }

std::string DictionaryType::ToString() const {
  std::stringstream ss;
  ss << "dictionary<values=" << dictionary_->type()->ToString()
     << ", indices=" << index_type_->ToString() << ", ordered=" << ordered_ << ">";
  return ss.str();
}

// ----------------------------------------------------------------------
// Null type

std::string NullType::ToString() const { return name(); }

// ----------------------------------------------------------------------
// Schema implementation

Schema::Schema(const std::vector<std::shared_ptr<Field>>& fields,
               const std::shared_ptr<const KeyValueMetadata>& metadata)
    : fields_(fields), metadata_(metadata) {}

Schema::Schema(std::vector<std::shared_ptr<Field>>&& fields,
               const std::shared_ptr<const KeyValueMetadata>& metadata)
    : fields_(std::move(fields)), metadata_(metadata) {}

bool Schema::Equals(const Schema& other) const {
  if (this == &other) {
    return true;
  }

  if (num_fields() != other.num_fields()) {
    return false;
  }
  for (int i = 0; i < num_fields(); ++i) {
    if (!field(i)->Equals(*other.field(i).get())) {
      return false;
    }
  }
  return true;
}

std::shared_ptr<Field> Schema::GetFieldByName(const std::string& name) const {
  int64_t i = GetFieldIndex(name);
  return i == -1 ? nullptr : fields_[i];
}

int64_t Schema::GetFieldIndex(const std::string& name) const {
  if (fields_.size() > 0 && name_to_index_.size() == 0) {
    for (size_t i = 0; i < fields_.size(); ++i) {
      name_to_index_[fields_[i]->name()] = static_cast<int>(i);
    }
  }

  auto it = name_to_index_.find(name);
  if (it == name_to_index_.end()) {
    return -1;
  } else {
    return it->second;
  }
}

Status Schema::AddField(int i, const std::shared_ptr<Field>& field,
                        std::shared_ptr<Schema>* out) const {
  if (i < 0 || i > this->num_fields()) {
    return Status::Invalid("Invalid column index to add field.");
  }

  *out =
      std::make_shared<Schema>(internal::AddVectorElement(fields_, i, field), metadata_);
  return Status::OK();
}

std::shared_ptr<Schema> Schema::AddMetadata(
    const std::shared_ptr<const KeyValueMetadata>& metadata) const {
  return std::make_shared<Schema>(fields_, metadata);
}

std::shared_ptr<const KeyValueMetadata> Schema::metadata() const { return metadata_; }

std::shared_ptr<Schema> Schema::RemoveMetadata() const {
  return std::make_shared<Schema>(fields_);
}

Status Schema::RemoveField(int i, std::shared_ptr<Schema>* out) const {
  if (i < 0 || i >= this->num_fields()) {
    return Status::Invalid("Invalid column index to remove field.");
  }

  *out = std::make_shared<Schema>(internal::DeleteVectorElement(fields_, i), metadata_);
  return Status::OK();
}

std::string Schema::ToString() const {
  std::stringstream buffer;

  int i = 0;
  for (auto field : fields_) {
    if (i > 0) {
      buffer << std::endl;
    }
    buffer << field->ToString();
    ++i;
  }

  if (metadata_) {
    buffer << "\n-- metadata --";
    for (int64_t i = 0; i < metadata_->size(); ++i) {
      buffer << "\n" << metadata_->key(i) << ": " << metadata_->value(i);
    }
  }

  return buffer.str();
}

std::shared_ptr<Schema> schema(const std::vector<std::shared_ptr<Field>>& fields,
                               const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return std::make_shared<Schema>(fields, metadata);
}

std::shared_ptr<Schema> schema(std::vector<std::shared_ptr<Field>>&& fields,
                               const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return std::make_shared<Schema>(std::move(fields), metadata);
}

// ----------------------------------------------------------------------
// Visitors and factory functions

#define ACCEPT_VISITOR(TYPE) \
  Status TYPE::Accept(TypeVisitor* visitor) const { return visitor->Visit(*this); }

ACCEPT_VISITOR(NullType);
ACCEPT_VISITOR(BooleanType);
ACCEPT_VISITOR(BinaryType);
ACCEPT_VISITOR(FixedSizeBinaryType);
ACCEPT_VISITOR(StringType);
ACCEPT_VISITOR(ListType);
ACCEPT_VISITOR(StructType);
ACCEPT_VISITOR(Decimal128Type);
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

std::shared_ptr<DataType> fixed_size_binary(int32_t byte_width) {
  return std::make_shared<FixedSizeBinaryType>(byte_width);
}

std::shared_ptr<DataType> timestamp(TimeUnit::type unit) {
  return std::make_shared<TimestampType>(unit);
}

std::shared_ptr<DataType> timestamp(TimeUnit::type unit, const std::string& timezone) {
  return std::make_shared<TimestampType>(unit, timezone);
}

std::shared_ptr<DataType> time32(TimeUnit::type unit) {
  return std::make_shared<Time32Type>(unit);
}

std::shared_ptr<DataType> time64(TimeUnit::type unit) {
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
                                 const std::vector<uint8_t>& type_codes,
                                 UnionMode::type mode) {
  return std::make_shared<UnionType>(child_fields, type_codes, mode);
}

std::shared_ptr<DataType> union_(const std::vector<std::shared_ptr<Array>>& children,
                                 UnionMode::type mode) {
  std::vector<std::shared_ptr<Field>> types;
  std::vector<uint8_t> type_codes;
  uint8_t counter = 0;
  for (const auto& child : children) {
    types.push_back(field(std::to_string(counter), child->type()));
    type_codes.push_back(counter);
    counter++;
  }
  return union_(types, type_codes, mode);
}

std::shared_ptr<DataType> dictionary(const std::shared_ptr<DataType>& index_type,
                                     const std::shared_ptr<Array>& dict_values,
                                     bool ordered) {
  return std::make_shared<DictionaryType>(index_type, dict_values, ordered);
}

std::shared_ptr<Field> field(const std::string& name,
                             const std::shared_ptr<DataType>& type, bool nullable,
                             const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return std::make_shared<Field>(name, type, nullable, metadata);
}

std::shared_ptr<DataType> decimal(int32_t precision, int32_t scale) {
  return std::make_shared<Decimal128Type>(precision, scale);
}

std::string Decimal128Type::ToString() const {
  std::stringstream s;
  s << "decimal(" << precision_ << ", " << scale_ << ")";
  return s.str();
}

}  // namespace arrow
