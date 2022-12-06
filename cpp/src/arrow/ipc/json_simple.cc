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

#include <cstdint>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/array_dict.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/chunked_array.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

#include "arrow/json/rapidjson_defs.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/reader.h>
#include <rapidjson/writer.h>

namespace rj = arrow::rapidjson;

namespace arrow {

using internal::ParseValue;

namespace ipc {
namespace internal {
namespace json {

using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

namespace {

constexpr auto kParseFlags = rj::kParseFullPrecisionFlag | rj::kParseNanAndInfFlag;

const char* JsonTypeName(rj::Type json_type) {
  switch (json_type) {
    case rapidjson::kNullType:
      return "null";
    case rapidjson::kFalseType:
      return "false";
    case rapidjson::kTrueType:
      return "true";
    case rapidjson::kObjectType:
      return "object";
    case rapidjson::kArrayType:
      return "array";
    case rapidjson::kStringType:
      return "string";
    case rapidjson::kNumberType:
      return "number";
    default:
      return "unknown";
  }
}

Status JSONTypeError(const char* expected_type, rj::Type json_type) {
  return Status::Invalid("Expected ", expected_type, " or null, got JSON type ",
                         JsonTypeName(json_type));
}

class Converter {
 public:
  virtual ~Converter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status AppendValue(const rj::Value& json_obj) = 0;

  Status AppendNull() { return this->builder()->AppendNull(); }

  virtual Status AppendValues(const rj::Value& json_array) = 0;

  virtual std::shared_ptr<ArrayBuilder> builder() = 0;

  virtual Status Finish(std::shared_ptr<Array>* out) {
    auto builder = this->builder();
    if (builder->length() == 0) {
      // Make sure the builder was initialized
      RETURN_NOT_OK(builder->Resize(1));
    }
    return builder->Finish(out);
  }

 protected:
  std::shared_ptr<DataType> type_;
};

Status GetConverter(const std::shared_ptr<DataType>&, std::shared_ptr<Converter>* out);

// CRTP
template <class Derived>
class ConcreteConverter : public Converter {
 public:
  Status AppendValues(const rj::Value& json_array) override {
    auto self = static_cast<Derived*>(this);
    if (!json_array.IsArray()) {
      return JSONTypeError("array", json_array.GetType());
    }
    auto size = json_array.Size();
    for (uint32_t i = 0; i < size; ++i) {
      RETURN_NOT_OK(self->AppendValue(json_array[i]));
    }
    return Status::OK();
  }

  const std::shared_ptr<DataType>& value_type() {
    if (type_->id() != Type::DICTIONARY) {
      return type_;
    }
    return checked_cast<const DictionaryType&>(*type_).value_type();
  }

  template <typename BuilderType>
  Status MakeConcreteBuilder(std::shared_ptr<BuilderType>* out) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(default_memory_pool(), this->type_, &builder));
    *out = checked_pointer_cast<BuilderType>(std::move(builder));
    DCHECK(*out);
    return Status::OK();
  }
};

// ------------------------------------------------------------------------
// Converter for null arrays

class NullConverter final : public ConcreteConverter<NullConverter> {
 public:
  explicit NullConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<NullBuilder>();
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    return JSONTypeError("null", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<NullBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for boolean arrays

class BooleanConverter final : public ConcreteConverter<BooleanConverter> {
 public:
  explicit BooleanConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<BooleanBuilder>();
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    if (json_obj.IsBool()) {
      return builder_->Append(json_obj.GetBool());
    }
    if (json_obj.IsInt()) {
      return builder_->Append(json_obj.GetInt() != 0);
    }
    return JSONTypeError("boolean", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BooleanBuilder> builder_;
};

// ------------------------------------------------------------------------
// Helpers for numeric converters

// Convert single signed integer value (also {Date,Time}{32,64} and Timestamp)
template <typename T>
enable_if_physical_signed_integer<T, Status> ConvertNumber(const rj::Value& json_obj,
                                                           const DataType& type,
                                                           typename T::c_type* out) {
  if (json_obj.IsInt64()) {
    int64_t v64 = json_obj.GetInt64();
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("signed int", json_obj.GetType());
  }
}

// Convert single unsigned integer value
template <typename T>
enable_if_physical_unsigned_integer<T, Status> ConvertNumber(const rj::Value& json_obj,
                                                             const DataType& type,
                                                             typename T::c_type* out) {
  if (json_obj.IsUint64()) {
    uint64_t v64 = json_obj.GetUint64();
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("unsigned int", json_obj.GetType());
  }
}

// Convert single floating point value
template <typename T>
enable_if_physical_floating_point<T, Status> ConvertNumber(const rj::Value& json_obj,
                                                           const DataType& type,
                                                           typename T::c_type* out) {
  if (json_obj.IsNumber()) {
    *out = static_cast<typename T::c_type>(json_obj.GetDouble());
    return Status::OK();
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("number", json_obj.GetType());
  }
}

// ------------------------------------------------------------------------
// Converter for int arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class IntegerConverter final
    : public ConcreteConverter<IntegerConverter<Type, BuilderType>> {
  using c_type = typename Type::c_type;

  static constexpr auto is_signed = std::is_signed<c_type>::value;

 public:
  explicit IntegerConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    c_type value;
    RETURN_NOT_OK(ConvertNumber<Type>(json_obj, *this->type_, &value));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for float arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class FloatConverter final : public ConcreteConverter<FloatConverter<Type, BuilderType>> {
  using c_type = typename Type::c_type;

 public:
  explicit FloatConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    c_type value;
    RETURN_NOT_OK(ConvertNumber<Type>(json_obj, *this->type_, &value));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for decimal arrays

template <typename DecimalSubtype, typename DecimalValue, typename BuilderType>
class DecimalConverter final
    : public ConcreteConverter<
          DecimalConverter<DecimalSubtype, DecimalValue, BuilderType>> {
 public:
  explicit DecimalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    decimal_type_ = &checked_cast<const DecimalSubtype&>(*this->value_type());
  }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    if (json_obj.IsString()) {
      int32_t precision, scale;
      DecimalValue d;
      auto view = std::string_view(json_obj.GetString(), json_obj.GetStringLength());
      RETURN_NOT_OK(DecimalValue::FromString(view, &d, &precision, &scale));
      if (scale != decimal_type_->scale()) {
        return Status::Invalid("Invalid scale for decimal: expected ",
                               decimal_type_->scale(), ", got ", scale);
      }
      return builder_->Append(d);
    }
    return JSONTypeError("decimal string", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
  const DecimalSubtype* decimal_type_;
};

template <typename BuilderType = typename TypeTraits<Decimal128Type>::BuilderType>
using Decimal128Converter = DecimalConverter<Decimal128Type, Decimal128, BuilderType>;
template <typename BuilderType = typename TypeTraits<Decimal256Type>::BuilderType>
using Decimal256Converter = DecimalConverter<Decimal256Type, Decimal256, BuilderType>;

// ------------------------------------------------------------------------
// Converter for timestamp arrays

class TimestampConverter final : public ConcreteConverter<TimestampConverter> {
 public:
  explicit TimestampConverter(const std::shared_ptr<DataType>& type)
      : timestamp_type_{checked_cast<const TimestampType*>(type.get())} {
    this->type_ = type;
    builder_ = std::make_shared<TimestampBuilder>(type, default_memory_pool());
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    int64_t value;
    if (json_obj.IsNumber()) {
      RETURN_NOT_OK(ConvertNumber<Int64Type>(json_obj, *this->type_, &value));
    } else if (json_obj.IsString()) {
      std::string_view view(json_obj.GetString(), json_obj.GetStringLength());
      if (!ParseValue(*timestamp_type_, view.data(), view.size(), &value)) {
        return Status::Invalid("couldn't parse timestamp from ", view);
      }
    } else {
      return JSONTypeError("timestamp", json_obj.GetType());
    }
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  const TimestampType* timestamp_type_;
  std::shared_ptr<TimestampBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for day-time interval arrays

class DayTimeIntervalConverter final
    : public ConcreteConverter<DayTimeIntervalConverter> {
 public:
  explicit DayTimeIntervalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<DayTimeIntervalBuilder>(default_memory_pool());
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    DayTimeIntervalType::DayMilliseconds value;
    if (!json_obj.IsArray()) {
      return JSONTypeError("array", json_obj.GetType());
    }
    if (json_obj.Size() != 2) {
      return Status::Invalid(
          "day time interval pair must have exactly two elements, had ", json_obj.Size());
    }
    RETURN_NOT_OK(ConvertNumber<Int32Type>(json_obj[0], *this->type_, &value.days));
    RETURN_NOT_OK(
        ConvertNumber<Int32Type>(json_obj[1], *this->type_, &value.milliseconds));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<DayTimeIntervalBuilder> builder_;
};

class MonthDayNanoIntervalConverter final
    : public ConcreteConverter<MonthDayNanoIntervalConverter> {
 public:
  explicit MonthDayNanoIntervalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<MonthDayNanoIntervalBuilder>(default_memory_pool());
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    MonthDayNanoIntervalType::MonthDayNanos value;
    if (!json_obj.IsArray()) {
      return JSONTypeError("array", json_obj.GetType());
    }
    if (json_obj.Size() != 3) {
      return Status::Invalid(
          "month_day_nano_interval  must have exactly 3 elements, had ", json_obj.Size());
    }
    RETURN_NOT_OK(ConvertNumber<Int32Type>(json_obj[0], *this->type_, &value.months));
    RETURN_NOT_OK(ConvertNumber<Int32Type>(json_obj[1], *this->type_, &value.days));
    RETURN_NOT_OK(
        ConvertNumber<Int64Type>(json_obj[2], *this->type_, &value.nanoseconds));

    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<MonthDayNanoIntervalBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for binary and string arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class StringConverter final
    : public ConcreteConverter<StringConverter<Type, BuilderType>> {
 public:
  explicit StringConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    if (json_obj.IsString()) {
      auto view = std::string_view(json_obj.GetString(), json_obj.GetStringLength());
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.GetType());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for fixed-size binary arrays

template <typename BuilderType = typename TypeTraits<FixedSizeBinaryType>::BuilderType>
class FixedSizeBinaryConverter final
    : public ConcreteConverter<FixedSizeBinaryConverter<BuilderType>> {
 public:
  explicit FixedSizeBinaryConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
  }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    if (json_obj.IsString()) {
      auto view = std::string_view(json_obj.GetString(), json_obj.GetStringLength());
      if (view.length() != static_cast<size_t>(builder_->byte_width())) {
        std::stringstream ss;
        ss << "Invalid string length " << view.length() << " in JSON input for "
           << this->type_->ToString();
        return Status::Invalid(ss.str());
      }
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.GetType());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for list arrays

template <typename TYPE>
class ListConverter final : public ConcreteConverter<ListConverter<TYPE>> {
 public:
  using BuilderType = typename TypeTraits<TYPE>::BuilderType;

  explicit ListConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override {
    const auto& list_type = checked_cast<const TYPE&>(*this->type_);
    RETURN_NOT_OK(GetConverter(list_type.value_type(), &child_converter_));
    auto child_builder = child_converter_->builder();
    builder_ =
        std::make_shared<BuilderType>(default_memory_pool(), child_builder, this->type_);
    return Status::OK();
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    // Extend the child converter with this JSON array
    return child_converter_->AppendValues(json_obj);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
  std::shared_ptr<Converter> child_converter_;
};

// ------------------------------------------------------------------------
// Converter for map arrays

class MapConverter final : public ConcreteConverter<MapConverter> {
 public:
  explicit MapConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    const auto& map_type = checked_cast<const MapType&>(*type_);
    RETURN_NOT_OK(GetConverter(map_type.key_type(), &key_converter_));
    RETURN_NOT_OK(GetConverter(map_type.item_type(), &item_converter_));
    auto key_builder = key_converter_->builder();
    auto item_builder = item_converter_->builder();
    builder_ = std::make_shared<MapBuilder>(default_memory_pool(), key_builder,
                                            item_builder, type_);
    return Status::OK();
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    if (!json_obj.IsArray()) {
      return JSONTypeError("array", json_obj.GetType());
    }
    auto size = json_obj.Size();
    for (uint32_t i = 0; i < size; ++i) {
      const auto& json_pair = json_obj[i];
      if (!json_pair.IsArray()) {
        return JSONTypeError("array", json_pair.GetType());
      }
      if (json_pair.Size() != 2) {
        return Status::Invalid("key item pair must have exactly two elements, had ",
                               json_pair.Size());
      }
      if (json_pair[0].IsNull()) {
        return Status::Invalid("null key is invalid");
      }
      RETURN_NOT_OK(key_converter_->AppendValue(json_pair[0]));
      RETURN_NOT_OK(item_converter_->AppendValue(json_pair[1]));
    }
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<MapBuilder> builder_;
  std::shared_ptr<Converter> key_converter_, item_converter_;
};

// ------------------------------------------------------------------------
// Converter for fixed size list arrays

class FixedSizeListConverter final : public ConcreteConverter<FixedSizeListConverter> {
 public:
  explicit FixedSizeListConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    const auto& list_type = checked_cast<const FixedSizeListType&>(*type_);
    list_size_ = list_type.list_size();
    RETURN_NOT_OK(GetConverter(list_type.value_type(), &child_converter_));
    auto child_builder = child_converter_->builder();
    builder_ = std::make_shared<FixedSizeListBuilder>(default_memory_pool(),
                                                      child_builder, type_);
    return Status::OK();
  }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    // Extend the child converter with this JSON array
    RETURN_NOT_OK(child_converter_->AppendValues(json_obj));
    if (json_obj.GetArray().Size() != static_cast<rj::SizeType>(list_size_)) {
      return Status::Invalid("incorrect list size ", json_obj.GetArray().Size());
    }
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  int32_t list_size_;
  std::shared_ptr<FixedSizeListBuilder> builder_;
  std::shared_ptr<Converter> child_converter_;
};

// ------------------------------------------------------------------------
// Converter for struct arrays

class StructConverter final : public ConcreteConverter<StructConverter> {
 public:
  explicit StructConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;
    for (const auto& field : type_->fields()) {
      std::shared_ptr<Converter> child_converter;
      RETURN_NOT_OK(GetConverter(field->type(), &child_converter));
      child_converters_.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }
    builder_ = std::make_shared<StructBuilder>(type_, default_memory_pool(),
                                               std::move(child_builders));
    return Status::OK();
  }

  // Append a JSON value that is either an array of N elements in order
  // or an object mapping struct names to values (omitted struct members
  // are mapped to null).
  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    if (json_obj.IsArray()) {
      auto size = json_obj.Size();
      auto expected_size = static_cast<uint32_t>(type_->num_fields());
      if (size != expected_size) {
        return Status::Invalid("Expected array of size ", expected_size,
                               ", got array of size ", size);
      }
      for (uint32_t i = 0; i < size; ++i) {
        RETURN_NOT_OK(child_converters_[i]->AppendValue(json_obj[i]));
      }
      return builder_->Append();
    }
    if (json_obj.IsObject()) {
      auto remaining = json_obj.MemberCount();
      auto num_children = type_->num_fields();
      for (int32_t i = 0; i < num_children; ++i) {
        const auto& field = type_->field(i);
        auto it = json_obj.FindMember(field->name());
        if (it != json_obj.MemberEnd()) {
          --remaining;
          RETURN_NOT_OK(child_converters_[i]->AppendValue(it->value));
        } else {
          RETURN_NOT_OK(child_converters_[i]->AppendNull());
        }
      }
      if (remaining > 0) {
        rj::StringBuffer sb;
        rj::Writer<rj::StringBuffer> writer(sb);
        json_obj.Accept(writer);
        return Status::Invalid("Unexpected members in JSON object for type ",
                               type_->ToString(), " Object: ", sb.GetString());
      }
      return builder_->Append();
    }
    return JSONTypeError("array or object", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<StructBuilder> builder_;
  std::vector<std::shared_ptr<Converter>> child_converters_;
};

// ------------------------------------------------------------------------
// Converter for union arrays

class UnionConverter final : public ConcreteConverter<UnionConverter> {
 public:
  explicit UnionConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    auto union_type = checked_cast<const UnionType*>(type_.get());
    mode_ = union_type->mode();
    type_id_to_child_num_.clear();
    type_id_to_child_num_.resize(union_type->max_type_code() + 1, -1);
    int child_i = 0;
    for (auto type_id : union_type->type_codes()) {
      type_id_to_child_num_[type_id] = child_i++;
    }
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;
    for (const auto& field : type_->fields()) {
      std::shared_ptr<Converter> child_converter;
      RETURN_NOT_OK(GetConverter(field->type(), &child_converter));
      child_converters_.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }
    if (mode_ == UnionMode::DENSE) {
      builder_ = std::make_shared<DenseUnionBuilder>(default_memory_pool(),
                                                     std::move(child_builders), type_);
    } else {
      builder_ = std::make_shared<SparseUnionBuilder>(default_memory_pool(),
                                                      std::move(child_builders), type_);
    }
    return Status::OK();
  }

  // Append a JSON value that must be a 2-long array, containing the type_id
  // and value of the UnionArray's slot.
  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return this->AppendNull();
    }
    if (!json_obj.IsArray()) {
      return JSONTypeError("array", json_obj.GetType());
    }
    if (json_obj.Size() != 2) {
      return Status::Invalid("Expected [type_id, value] pair, got array of size ",
                             json_obj.Size());
    }
    const auto& id_obj = json_obj[0];
    if (!id_obj.IsInt()) {
      return JSONTypeError("int", id_obj.GetType());
    }

    auto id = static_cast<int8_t>(id_obj.GetInt());
    auto child_num = type_id_to_child_num_[id];
    if (child_num == -1) {
      return Status::Invalid("type_id ", id, " not found in ", *type_);
    }

    auto child_converter = child_converters_[child_num];
    if (mode_ == UnionMode::SPARSE) {
      RETURN_NOT_OK(checked_cast<SparseUnionBuilder&>(*builder_).Append(id));
      for (auto&& other_converter : child_converters_) {
        if (other_converter != child_converter) {
          RETURN_NOT_OK(other_converter->AppendNull());
        }
      }
    } else {
      RETURN_NOT_OK(checked_cast<DenseUnionBuilder&>(*builder_).Append(id));
    }
    return child_converter->AppendValue(json_obj[1]);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  UnionMode::type mode_;
  std::shared_ptr<ArrayBuilder> builder_;
  std::vector<std::shared_ptr<Converter>> child_converters_;
  std::vector<int8_t> type_id_to_child_num_;
};

// ------------------------------------------------------------------------
// General conversion functions

Status ConversionNotImplemented(const std::shared_ptr<DataType>& type) {
  return Status::NotImplemented("JSON conversion to ", type->ToString(),
                                " not implemented");
}

Status GetDictConverter(const std::shared_ptr<DataType>& type,
                        std::shared_ptr<Converter>* out) {
  std::shared_ptr<Converter> res;

  const auto value_type = checked_cast<const DictionaryType&>(*type).value_type();

#define SIMPLE_CONVERTER_CASE(ID, CLASS, TYPE)                    \
  case ID:                                                        \
    res = std::make_shared<CLASS<DictionaryBuilder<TYPE>>>(type); \
    break;

#define PARAM_CONVERTER_CASE(ID, CLASS, TYPE)                           \
  case ID:                                                              \
    res = std::make_shared<CLASS<TYPE, DictionaryBuilder<TYPE>>>(type); \
    break;

  switch (value_type->id()) {
    PARAM_CONVERTER_CASE(Type::INT8, IntegerConverter, Int8Type)
    PARAM_CONVERTER_CASE(Type::INT16, IntegerConverter, Int16Type)
    PARAM_CONVERTER_CASE(Type::INT32, IntegerConverter, Int32Type)
    PARAM_CONVERTER_CASE(Type::INT64, IntegerConverter, Int64Type)
    PARAM_CONVERTER_CASE(Type::UINT8, IntegerConverter, UInt8Type)
    PARAM_CONVERTER_CASE(Type::UINT16, IntegerConverter, UInt16Type)
    PARAM_CONVERTER_CASE(Type::UINT32, IntegerConverter, UInt32Type)
    PARAM_CONVERTER_CASE(Type::UINT64, IntegerConverter, UInt64Type)
    PARAM_CONVERTER_CASE(Type::FLOAT, FloatConverter, FloatType)
    PARAM_CONVERTER_CASE(Type::DOUBLE, FloatConverter, DoubleType)
    PARAM_CONVERTER_CASE(Type::STRING, StringConverter, StringType)
    PARAM_CONVERTER_CASE(Type::BINARY, StringConverter, BinaryType)
    PARAM_CONVERTER_CASE(Type::LARGE_STRING, StringConverter, LargeStringType)
    PARAM_CONVERTER_CASE(Type::LARGE_BINARY, StringConverter, LargeBinaryType)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter,
                          FixedSizeBinaryType)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL128, Decimal128Converter, Decimal128Type)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL256, Decimal256Converter, Decimal256Type)
    default:
      return ConversionNotImplemented(type);
  }

#undef SIMPLE_CONVERTER_CASE
#undef PARAM_CONVERTER_CASE

  RETURN_NOT_OK(res->Init());
  *out = res;
  return Status::OK();
}

Status GetConverter(const std::shared_ptr<DataType>& type,
                    std::shared_ptr<Converter>* out) {
  if (type->id() == Type::DICTIONARY) {
    return GetDictConverter(type, out);
  }

  std::shared_ptr<Converter> res;

#define SIMPLE_CONVERTER_CASE(ID, CLASS) \
  case ID:                               \
    res = std::make_shared<CLASS>(type); \
    break;

  switch (type->id()) {
    SIMPLE_CONVERTER_CASE(Type::INT8, IntegerConverter<Int8Type>)
    SIMPLE_CONVERTER_CASE(Type::INT16, IntegerConverter<Int16Type>)
    SIMPLE_CONVERTER_CASE(Type::INT32, IntegerConverter<Int32Type>)
    SIMPLE_CONVERTER_CASE(Type::INT64, IntegerConverter<Int64Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT8, IntegerConverter<UInt8Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT16, IntegerConverter<UInt16Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT32, IntegerConverter<UInt32Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT64, IntegerConverter<UInt64Type>)
    SIMPLE_CONVERTER_CASE(Type::TIMESTAMP, TimestampConverter)
    SIMPLE_CONVERTER_CASE(Type::DATE32, IntegerConverter<Date32Type>)
    SIMPLE_CONVERTER_CASE(Type::DATE64, IntegerConverter<Date64Type>)
    SIMPLE_CONVERTER_CASE(Type::TIME32, IntegerConverter<Time32Type>)
    SIMPLE_CONVERTER_CASE(Type::TIME64, IntegerConverter<Time64Type>)
    SIMPLE_CONVERTER_CASE(Type::DURATION, IntegerConverter<DurationType>)
    SIMPLE_CONVERTER_CASE(Type::NA, NullConverter)
    SIMPLE_CONVERTER_CASE(Type::BOOL, BooleanConverter)
    SIMPLE_CONVERTER_CASE(Type::HALF_FLOAT, IntegerConverter<HalfFloatType>)
    SIMPLE_CONVERTER_CASE(Type::FLOAT, FloatConverter<FloatType>)
    SIMPLE_CONVERTER_CASE(Type::DOUBLE, FloatConverter<DoubleType>)
    SIMPLE_CONVERTER_CASE(Type::LIST, ListConverter<ListType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_LIST, ListConverter<LargeListType>)
    SIMPLE_CONVERTER_CASE(Type::MAP, MapConverter)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_LIST, FixedSizeListConverter)
    SIMPLE_CONVERTER_CASE(Type::STRUCT, StructConverter)
    SIMPLE_CONVERTER_CASE(Type::STRING, StringConverter<StringType>)
    SIMPLE_CONVERTER_CASE(Type::BINARY, StringConverter<BinaryType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_STRING, StringConverter<LargeStringType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_BINARY, StringConverter<LargeBinaryType>)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL128, Decimal128Converter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL256, Decimal256Converter<>)
    SIMPLE_CONVERTER_CASE(Type::SPARSE_UNION, UnionConverter)
    SIMPLE_CONVERTER_CASE(Type::DENSE_UNION, UnionConverter)
    SIMPLE_CONVERTER_CASE(Type::INTERVAL_MONTHS, IntegerConverter<MonthIntervalType>)
    SIMPLE_CONVERTER_CASE(Type::INTERVAL_DAY_TIME, DayTimeIntervalConverter)
    SIMPLE_CONVERTER_CASE(Type::INTERVAL_MONTH_DAY_NANO, MonthDayNanoIntervalConverter)
    default:
      return ConversionNotImplemented(type);
  }

#undef SIMPLE_CONVERTER_CASE

  RETURN_NOT_OK(res->Init());
  *out = res;
  return Status::OK();
}

}  // namespace

Result<std::shared_ptr<Array>> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                             std::string_view json_string) {
  std::shared_ptr<Converter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));

  rj::Document json_doc;
  json_doc.Parse<kParseFlags>(json_string.data(), json_string.length());
  if (json_doc.HasParseError()) {
    return Status::Invalid("JSON parse error at offset ", json_doc.GetErrorOffset(), ": ",
                           GetParseError_En(json_doc.GetParseError()));
  }

  // The JSON document should be an array, append it
  RETURN_NOT_OK(converter->AppendValues(json_doc));
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(converter->Finish(&out));
  return out;
}

Result<std::shared_ptr<Array>> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                             const std::string& json_string) {
  return ArrayFromJSON(type, std::string_view(json_string));
}

Result<std::shared_ptr<Array>> ArrayFromJSON(const std::shared_ptr<DataType>& type,
                                             const char* json_string) {
  return ArrayFromJSON(type, std::string_view(json_string));
}

Status ChunkedArrayFromJSON(const std::shared_ptr<DataType>& type,
                            const std::vector<std::string>& json_strings,
                            std::shared_ptr<ChunkedArray>* out) {
  ArrayVector out_chunks;
  out_chunks.reserve(json_strings.size());
  for (const std::string& chunk_json : json_strings) {
    out_chunks.emplace_back();
    ARROW_ASSIGN_OR_RAISE(out_chunks.back(), ArrayFromJSON(type, chunk_json));
  }
  *out = std::make_shared<ChunkedArray>(std::move(out_chunks), type);
  return Status::OK();
}

Status DictArrayFromJSON(const std::shared_ptr<DataType>& type,
                         std::string_view indices_json, std::string_view dictionary_json,
                         std::shared_ptr<Array>* out) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("DictArrayFromJSON requires dictionary type, got ", *type);
  }

  const auto& dictionary_type = checked_cast<const DictionaryType&>(*type);

  ARROW_ASSIGN_OR_RAISE(auto indices,
                        ArrayFromJSON(dictionary_type.index_type(), indices_json));
  ARROW_ASSIGN_OR_RAISE(auto dictionary,
                        ArrayFromJSON(dictionary_type.value_type(), dictionary_json));

  return DictionaryArray::FromArrays(type, std::move(indices), std::move(dictionary))
      .Value(out);
}

Status ScalarFromJSON(const std::shared_ptr<DataType>& type, std::string_view json_string,
                      std::shared_ptr<Scalar>* out) {
  std::shared_ptr<Converter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));

  rj::Document json_doc;
  json_doc.Parse<kParseFlags>(json_string.data(), json_string.length());
  if (json_doc.HasParseError()) {
    return Status::Invalid("JSON parse error at offset ", json_doc.GetErrorOffset(), ": ",
                           GetParseError_En(json_doc.GetParseError()));
  }

  std::shared_ptr<Array> array;
  RETURN_NOT_OK(converter->AppendValue(json_doc));
  RETURN_NOT_OK(converter->Finish(&array));
  DCHECK_EQ(array->length(), 1);
  return array->GetScalar(0).Value(out);
}

Status DictScalarFromJSON(const std::shared_ptr<DataType>& type,
                          std::string_view index_json, std::string_view dictionary_json,
                          std::shared_ptr<Scalar>* out) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("DictScalarFromJSON requires dictionary type, got ", *type);
  }

  const auto& dictionary_type = checked_cast<const DictionaryType&>(*type);

  std::shared_ptr<Scalar> index;
  std::shared_ptr<Array> dictionary;
  RETURN_NOT_OK(ScalarFromJSON(dictionary_type.index_type(), index_json, &index));
  ARROW_ASSIGN_OR_RAISE(dictionary,
                        ArrayFromJSON(dictionary_type.value_type(), dictionary_json));

  *out = DictionaryScalar::Make(std::move(index), std::move(dictionary));
  return Status::OK();
}

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
