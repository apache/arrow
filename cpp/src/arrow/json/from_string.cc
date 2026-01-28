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

#include <cmath>
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
#include "arrow/json/from_string.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/float16.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/value_parsing.h"

#include <simdjson.h>

namespace sj = simdjson::ondemand;

namespace arrow {

using internal::ParseValue;
using util::Float16;

namespace json {

using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

namespace {

// TODO.TAE constexpr auto kParseFlags = rj::kParseFullPrecisionFlag | rj::kParseNanAndInfFlag;

const char* JsonTypeName(sj::json_type type) {
    switch (type) {
        case sj::json_type::array: return "array"; break;
        case sj::json_type::object: return "object"; break;
        case sj::json_type::number: return "number"; break;
        case sj::json_type::string: return "string"; break;
        case sj::json_type::boolean: return "boolean"; break;
        case sj::json_type::null: return "null"; break;
        default: Unreachable();
    }
}

Status JSONTypeError(const char* expected_type, sj::json_type json_type) {
  return Status::Invalid("Expected ", expected_type, " or null, got JSON type ",
                         JsonTypeName(json_type));
}

class JSONConverter {
 public:
  virtual ~JSONConverter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status AppendValue(sj::value& json_obj) = 0;

  Status AppendNull() { return this->builder()->AppendNull(); }

  // TODO.TAE returns the number of elements that were appended
  virtual Result<int32_t> AppendValues(sj::array& json_array) = 0;

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

Status GetConverter(const std::shared_ptr<DataType>&,
                    std::shared_ptr<JSONConverter>* out);

// CRTP
template <class Derived>
class ConcreteConverter : public JSONConverter {
 public:
  Result<int32_t> AppendValues(sj::array& json_array) final {
    auto self = static_cast<Derived*>(this);
    size_t num_elements = 0;
    for (auto element : json_array) {
      sj::value value;
      if(element.get(value) != simdjson::SUCCESS){
        return Status::Invalid("Could not iterate elements of array: ", json_array.raw_json());
      }
      RETURN_NOT_OK(self->AppendValue(value));
      num_elements++;
    }
    return num_elements;
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return AppendNull();
    }
    return JSONTypeError("null", json_obj.type());
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return AppendNull();
    }
    bool bool_value;
    if (json_obj.get(bool_value) == simdjson::SUCCESS) {
      return builder_->Append(bool_value);
    }
    int64_t int_value;
    if (json_obj.get(int_value) == simdjson::SUCCESS) {
      return builder_->Append(int_value != 0);
    }
    return JSONTypeError("boolean", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BooleanBuilder> builder_;
};

// ------------------------------------------------------------------------
// Helpers for numeric converters

// Convert single signed integer value (also {Date,Time}{32,64} and Timestamp)
template <typename T>
enable_if_physical_signed_integer<T, Status> ConvertNumber(sj::value& json_obj,
                                                           const DataType& type,
                                                           typename T::c_type* out) {
  
  int64_t v64;
  if (json_obj.get(v64) == simdjson::SUCCESS) {
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("int", json_obj.type());
  }
}

// Convert single unsigned integer value
template <typename T>
enable_if_unsigned_integer<T, Status> ConvertNumber(sj::value& json_obj,
                                                    const DataType& type,
                                                    typename T::c_type* out) {
  
  uint64_t v64;
  if (json_obj.get(v64) == simdjson::SUCCESS) {
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("unsigned int", json_obj.type());
  }
}

// Convert float16/HalfFloatType
template <typename T>
enable_if_half_float<T, Status> ConvertNumber(sj::value& json_obj,
                                              const DataType& type, uint16_t* out) {
  double f64;
  if (json_obj.get(f64) == simdjson::SUCCESS) {
    *out = Float16(f64).bits();
    return Status::OK();
  }
  uint64_t u64t;
  if (json_obj.get(u64t)) {
    auto f64 = static_cast<double>(u64t);
    *out = Float16(f64).bits();
    return Status::OK();
  }
  int64_t i64t;
  if (json_obj.get(i64t)) {
    auto f64 = static_cast<double>(i64t);
    *out = Float16(f64).bits();
    return Status::OK();
  }
  std::string_view str;
  if (json_obj.get(str)) {
    if(str == "NaN") {
      *out = Float16(std::numeric_limits<double>::quiet_NaN()).bits();
      return Status::OK();
    }
    else if (str == "Inf" || str == "Infinity") {
      *out = Float16(std::numeric_limits<double>::infinity()).bits();
      return Status::OK();
    }
    else if (str == "-Inf" || str == "-Infinity") {
      *out = Float16(-std::numeric_limits<double>::infinity()).bits();
      return Status::OK();
    }
  }
  *out = static_cast<uint16_t>(0);
  return JSONTypeError("number", json_obj.type());
}

// Convert single floating point value
template <typename T>
enable_if_physical_floating_point<T, Status> ConvertNumber(sj::value& json_obj,
                                                           const DataType& type,
                                                           typename T::c_type* out) {
  sj::number number;
  if (json_obj.get(number) == simdjson::SUCCESS) {
    *out = static_cast<typename T::c_type>(number.as_double());
    return Status::OK();
  }
  std::string_view str;
  if (json_obj.get(str)) {
    if(str == "NaN") {
      *out = static_cast<typename T::c_type>(std::numeric_limits<double>::quiet_NaN());
      return Status::OK();
    }
    else if (str == "Inf" || str == "Infinity") {
      *out = static_cast<typename T::c_type>(std::numeric_limits<double>::infinity());
      return Status::OK();
    }
    else if (str == "-Inf" || str == "-Infinity") {
      *out = static_cast<typename T::c_type>(-std::numeric_limits<double>::infinity());
      return Status::OK();
    }
  }
  *out = static_cast<typename T::c_type>(0);
  return JSONTypeError("number", json_obj.type());
}


// ------------------------------------------------------------------------
// Helper to process a JSON array with exactly N elements, calling a handler for each.
// Each handler is a callable taking sj::value& and returning Status.
template <typename... Handlers>
Status ProcessJsonArrayElements(sj::array& json_array, const char* error_context,
                                Handlers&&... handlers) {
  constexpr size_t expected_size = sizeof...(Handlers);
  auto it = json_array.begin();
  auto end = json_array.end();

  size_t index = 0;
  Status result = Status::OK();

  auto process_one = [&](auto&& handler) -> bool {
    if (!result.ok()) return false;

    if (it == end) {
      result = Status::Invalid(error_context, " must have exactly ", expected_size,
                               " elements, had ", index);
      return false;
    }

    sj::value element;
    auto error = (*it).get(element);
    if (error) {
      result = Status::Invalid("Failed to get element ", index, " from ", error_context);
      return false;
    }

    result = handler(element);
    ++it;
    ++index;
    return result.ok();
  };

  // Use fold expression to process all handlers in order
  (process_one(std::forward<Handlers>(handlers)) && ...);

  if (!result.ok()) return result;

  if (it != end) {
    return Status::Invalid(error_context, " must have exactly ", expected_size,
                           " elements, had more");
  }
  return Status::OK();
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    std::string_view string_value;
    if (json_obj.get<std::string_view>(string_value) == simdjson::SUCCESS) {
      int32_t precision, scale;
      DecimalValue d;
      RETURN_NOT_OK(DecimalValue::FromString(string_value, &d, &precision, &scale));
      if (scale != decimal_type_->scale()) {
        return Status::Invalid("Invalid scale for decimal: expected ",
                               decimal_type_->scale(), ", got ", scale);
      }
      return builder_->Append(d);
    }
    return JSONTypeError("decimal string", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
  const DecimalSubtype* decimal_type_;
};

template <typename BuilderType = typename TypeTraits<Decimal32Type>::BuilderType>
using Decimal32Converter = DecimalConverter<Decimal32Type, Decimal32, BuilderType>;
template <typename BuilderType = typename TypeTraits<Decimal64Type>::BuilderType>
using Decimal64Converter = DecimalConverter<Decimal64Type, Decimal64, BuilderType>;
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    int64_t value;
    std::string_view view;
    if (json_obj.get(view) == simdjson::SUCCESS) {
      if (!ParseValue(*timestamp_type_, view.data(), view.size(), &value)) {
        return Status::Invalid("couldn't parse timestamp from ", view);
      }
    }
    // TODO.TAE invert the check order
    RETURN_NOT_OK(ConvertNumber<Int64Type>(json_obj, *this->type_, &value));
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }

    sj::array array;
    if (json_obj.get(array) != simdjson::SUCCESS) {
      return JSONTypeError("array", json_obj.type());
    }

    DayTimeIntervalType::DayMilliseconds value;
    RETURN_NOT_OK(ProcessJsonArrayElements(
        array, "day-time interval",
        [this, &value](sj::value& elem) {
          return ConvertNumber<Int32Type>(elem, *this->type_, &value.days);
        },
        [this, &value](sj::value& elem) {
          return ConvertNumber<Int32Type>(elem, *this->type_, &value.milliseconds);
        }));
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }

    sj::array array;
    if (json_obj.get(array) != simdjson::SUCCESS) {
      return JSONTypeError("array", json_obj.type());
    }

    MonthDayNanoIntervalType::MonthDayNanos value;
    RETURN_NOT_OK(ProcessJsonArrayElements(
        array, "month-day-nano interval",
        [this, &value](sj::value& elem) {
          return ConvertNumber<Int32Type>(elem, *this->type_, &value.months);
        },
        [this, &value](sj::value& elem) {
          return ConvertNumber<Int32Type>(elem, *this->type_, &value.days);
        },
        [this, &value](sj::value& elem) {
          return ConvertNumber<Int64Type>(elem, *this->type_, &value.nanoseconds);
        }));
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    std::string_view view;
    if (json_obj.get(view) == simdjson::SUCCESS) {
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.type());
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    std::string_view view;
    if (json_obj.get(view) == simdjson::SUCCESS) {
      if (view.length() != static_cast<size_t>(builder_->byte_width())) {
        std::stringstream ss;
        ss << "Invalid string length " << view.length() << " in JSON input for "
           << this->type_->ToString();
        return Status::Invalid(ss.str());
      }
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.type());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
};

// ------------------------------------------------------------------------
// Converter for list arrays

template <typename TYPE>
class VarLengthListLikeConverter final
    : public ConcreteConverter<VarLengthListLikeConverter<TYPE>> {
 public:
  using BuilderType = typename TypeTraits<TYPE>::BuilderType;

  explicit VarLengthListLikeConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
  }

  Status Init() override {
    const auto& var_length_list_like_type = checked_cast<const TYPE&>(*this->type_);
    RETURN_NOT_OK(
        GetConverter(var_length_list_like_type.value_type(), &child_converter_));
    auto child_builder = child_converter_->builder();
    builder_ =
        std::make_shared<BuilderType>(default_memory_pool(), child_builder, this->type_);
    return Status::OK();
  }

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    sj::array array;
    if(json_obj.get(array) != simdjson::SUCCESS){
      return JSONTypeError("array", json_obj.type());
    }
    // Extend the child converter with this JSON array
    ARROW_ASSIGN_OR_RAISE(int32_t size, child_converter_->AppendValues(array));
    return builder_->Append(true, size);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BuilderType> builder_;
  std::shared_ptr<JSONConverter> child_converter_;
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    sj::array array;
    if (json_obj.get(array) != simdjson::SUCCESS) {
      return JSONTypeError("array", json_obj.type());
    }

    for (auto json_pair : array) {
      sj::array json_pair_array;
      if (json_pair.get(json_pair_array) != simdjson::SUCCESS) {
        return JSONTypeError("array", json_pair.type());
      }

      RETURN_NOT_OK(ProcessJsonArrayElements(
          json_pair_array, "key-item pair",
          [this](sj::value& key) {
            if (key.is_null()) {
              return Status::Invalid("null key is invalid");
            }
            return key_converter_->AppendValue(key);
          },
          [this](sj::value& item) { return item_converter_->AppendValue(item); }));
    }
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<MapBuilder> builder_;
  std::shared_ptr<JSONConverter> key_converter_, item_converter_;
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    // Extend the child converter with this JSON array
    sj::array array;
    if(json_obj.get(array) != simdjson::SUCCESS){
      return JSONTypeError("array", json_obj.type());
    }
    ARROW_ASSIGN_OR_RAISE(int32_t size, child_converter_->AppendValues(array));
    if (size != list_size_) {
      return Status::Invalid("incorrect list size ", size);
    }
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  int32_t list_size_;
  std::shared_ptr<FixedSizeListBuilder> builder_;
  std::shared_ptr<JSONConverter> child_converter_;
};

// ------------------------------------------------------------------------
// Converter for struct arrays

class StructConverter final : public ConcreteConverter<StructConverter> {
 public:
  explicit StructConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;
    for (const auto& field : type_->fields()) {
      std::shared_ptr<JSONConverter> child_converter;
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
  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    sj::array array;
    if (json_obj.get(array) == simdjson::SUCCESS) {
      auto expected_size = static_cast<uint32_t>(type_->num_fields());
      uint32_t i = 0;
      for (auto child : array) {
        // TODO.TAE no unsafe
        sj::value child_value = child.value_unsafe();
        RETURN_NOT_OK(child_converters_[i]->AppendValue(child_value));
        ++i;
      }
      if (i != expected_size) {
        return Status::Invalid("Expected array of size ", expected_size,
                               ", got array of size ", i);
      }
      return builder_->Append();
    }
    sj::object object;
    if (json_obj.get(object) == simdjson::SUCCESS) {
      size_t remaining_num_fields_in_json;
      if(object.count_fields().get(remaining_num_fields_in_json) != simdjson::SUCCESS){
        return Status::Invalid("Malformed json object: ", object.raw_json());
      }
      auto num_fields = type_->num_fields();
      for (int32_t i = 0; i < num_fields; ++i) {
        const auto& field = type_->field(i);
        auto it = object.find_field_unordered(field->name());
        sj::value value;
        if (it.get(value) == simdjson::SUCCESS) {
          --remaining_num_fields_in_json;
          RETURN_NOT_OK(child_converters_[i]->AppendValue(value));
        } else {
          RETURN_NOT_OK(child_converters_[i]->AppendNull());
        }
      }
      if (remaining_num_fields_in_json > 0) {
        std::string_view raw_json = json_obj.raw_json();
        return Status::Invalid("Unexpected members in JSON object for type ",
                               type_->ToString(), " Object: ", raw_json);
      }
      return builder_->Append();
    }
    return JSONTypeError("array or object", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<StructBuilder> builder_;
  std::vector<std::shared_ptr<JSONConverter>> child_converters_;
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
      std::shared_ptr<JSONConverter> child_converter;
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
  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }

    sj::array array;
    if (json_obj.get(array) != simdjson::SUCCESS) {
      return JSONTypeError("array", json_obj.type());
    }

    int8_t id = 0;
    std::shared_ptr<JSONConverter> child_converter;

    RETURN_NOT_OK(ProcessJsonArrayElements(
        array, "[type_id, value] pair",
        [this, &id, &child_converter](sj::value& id_elem) {
          int64_t id_value;
          if (id_elem.get(id_value) != simdjson::SUCCESS) {
            return JSONTypeError("int", id_elem.type());
          }
          id = static_cast<int8_t>(id_value);
          auto child_num = type_id_to_child_num_[id];
          if (child_num == -1) {
            return Status::Invalid("type_id ", id, " not found in ", *type_);
          }
          child_converter = child_converters_[child_num];

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
          return Status::OK();
        },
        [&child_converter](sj::value& value_elem) {
          return child_converter->AppendValue(value_elem);
        }));
    return Status::OK();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  UnionMode::type mode_;
  std::shared_ptr<ArrayBuilder> builder_;
  std::vector<std::shared_ptr<JSONConverter>> child_converters_;
  std::vector<int8_t> type_id_to_child_num_;
};

// ------------------------------------------------------------------------
// General conversion functions

Status ConversionNotImplemented(const std::shared_ptr<DataType>& type) {
  return Status::NotImplemented("JSON conversion to ", type->ToString(),
                                " not implemented");
}

Status GetDictConverter(const std::shared_ptr<DataType>& type,
                        std::shared_ptr<JSONConverter>* out) {
  std::shared_ptr<JSONConverter> res;

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
    PARAM_CONVERTER_CASE(Type::STRING_VIEW, StringConverter, StringViewType)
    PARAM_CONVERTER_CASE(Type::BINARY_VIEW, StringConverter, BinaryViewType)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter,
                          FixedSizeBinaryType)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL32, Decimal32Converter, Decimal32Type)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL64, Decimal64Converter, Decimal64Type)
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
                    std::shared_ptr<JSONConverter>* out) {
  if (type->id() == Type::DICTIONARY) {
    return GetDictConverter(type, out);
  }

  std::shared_ptr<JSONConverter> res;

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
    SIMPLE_CONVERTER_CASE(Type::LIST, VarLengthListLikeConverter<ListType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_LIST, VarLengthListLikeConverter<LargeListType>)
    SIMPLE_CONVERTER_CASE(Type::LIST_VIEW, VarLengthListLikeConverter<ListViewType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_LIST_VIEW,
                          VarLengthListLikeConverter<LargeListViewType>)
    SIMPLE_CONVERTER_CASE(Type::MAP, MapConverter)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_LIST, FixedSizeListConverter)
    SIMPLE_CONVERTER_CASE(Type::STRUCT, StructConverter)
    SIMPLE_CONVERTER_CASE(Type::STRING, StringConverter<StringType>)
    SIMPLE_CONVERTER_CASE(Type::BINARY, StringConverter<BinaryType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_STRING, StringConverter<LargeStringType>)
    SIMPLE_CONVERTER_CASE(Type::LARGE_BINARY, StringConverter<LargeBinaryType>)
    SIMPLE_CONVERTER_CASE(Type::STRING_VIEW, StringConverter<StringViewType>)
    SIMPLE_CONVERTER_CASE(Type::BINARY_VIEW, StringConverter<BinaryViewType>)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL32, Decimal32Converter<>)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL64, Decimal64Converter<>)
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

Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                                   std::string_view json_string) {
  std::shared_ptr<JSONConverter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));

  // TODO we should not copy the whole string. Maybe we can move the requirement of padding to users of this function
  simdjson::padded_string padded_string{json_string};

  sj::parser parser;
  sj::document json_doc;
  auto error = parser.iterate(padded_string).get(json_doc);
  if (error) {
    return Status::Invalid("JSON parse error: ", simdjson::error_message(error));
  }

  sj::array array;
  if(json_doc.get(array) != simdjson::SUCCESS){
    return JSONTypeError("array", json_doc.type());
  }

  // The JSON document should be an array, append it
  RETURN_NOT_OK(converter->AppendValues(array));
  std::shared_ptr<Array> out;
  RETURN_NOT_OK(converter->Finish(&out));
  return out;
}

Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                                   const std::string& json_string) {
  return ArrayFromJSONString(type, std::string_view(json_string));
}

Result<std::shared_ptr<Array>> ArrayFromJSONString(const std::shared_ptr<DataType>& type,
                                                   const char* json_string) {
  return ArrayFromJSONString(type, std::string_view(json_string));
}

Result<std::shared_ptr<ChunkedArray>> ChunkedArrayFromJSONString(
    const std::shared_ptr<DataType>& type, const std::vector<std::string>& json_strings) {
  ArrayVector out_chunks;
  out_chunks.reserve(json_strings.size());
  for (const std::string& chunk_json : json_strings) {
    out_chunks.emplace_back();
    ARROW_ASSIGN_OR_RAISE(out_chunks.back(), ArrayFromJSONString(type, chunk_json));
  }
  return std::make_shared<ChunkedArray>(std::move(out_chunks), type);
}

Result<std::shared_ptr<Array>> DictArrayFromJSONString(
    const std::shared_ptr<DataType>& type, std::string_view indices_json,
    std::string_view dictionary_json) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("DictArrayFromJSON requires dictionary type, got ", *type);
  }

  const auto& dictionary_type = checked_cast<const DictionaryType&>(*type);

  ARROW_ASSIGN_OR_RAISE(auto indices,
                        ArrayFromJSONString(dictionary_type.index_type(), indices_json));
  ARROW_ASSIGN_OR_RAISE(auto dictionary, ArrayFromJSONString(dictionary_type.value_type(),
                                                             dictionary_json));
  return DictionaryArray::FromArrays(type, std::move(indices), std::move(dictionary));
}

Result<std::shared_ptr<Scalar>> ScalarFromJSONString(
    const std::shared_ptr<DataType>& type, std::string_view json_string) {
  std::shared_ptr<JSONConverter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));

  // TODO we should not copy the whole string. Maybe we can move the requirement of padding to users of this function
  simdjson::padded_string padded_string{json_string};

  sj::parser parser;
  sj::document json_doc;
  auto error = parser.iterate(padded_string).get(json_doc);
  if (error) {
    return Status::Invalid("JSON parse error: ", simdjson::error_message(error));
  }

  sj::value value;
  if(json_doc.get(value) != simdjson::SUCCESS){
    return JSONTypeError("value", json_doc.type());
  }

  RETURN_NOT_OK(converter->AppendValue(value));

  std::shared_ptr<Array> array;
  RETURN_NOT_OK(converter->Finish(&array));
  DCHECK_EQ(array->length(), 1);
  return array->GetScalar(0);
}

Result<std::shared_ptr<Scalar>> DictScalarFromJSONString(
    const std::shared_ptr<DataType>& type, std::string_view index_json,
    std::string_view dictionary_json) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("DictScalarFromJSONString requires dictionary type, got ",
                             *type);
  }

  const auto& dictionary_type = checked_cast<const DictionaryType&>(*type);

  std::shared_ptr<Array> dictionary;
  ARROW_ASSIGN_OR_RAISE(auto index,
                        ScalarFromJSONString(dictionary_type.index_type(), index_json));
  ARROW_ASSIGN_OR_RAISE(
      dictionary, ArrayFromJSONString(dictionary_type.value_type(), dictionary_json));

  return DictionaryScalar::Make(std::move(index), std::move(dictionary));
}

}  // namespace json
}  // namespace arrow
