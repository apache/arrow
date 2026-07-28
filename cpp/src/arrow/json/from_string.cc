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

#include <cctype>
#include <cmath>
#include <cstdint>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
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

const char* JsonTypeName(sj::json_type type) {
  switch (type) {
    case sj::json_type::array:
      return "array";
    case sj::json_type::object:
      return "object";
    case sj::json_type::number:
      return "number";
    case sj::json_type::string:
      return "string";
    case sj::json_type::boolean:
      return "boolean";
    case sj::json_type::null:
      return "null";
    default:
      return "unknown";
  }
}

template <typename>
inline constexpr bool kAlwaysFalse = false;

template <typename SimdjsonClass>
const char* JsonTypeName() {
  constexpr bool isNumber = std::is_same_v<SimdjsonClass, int64_t> ||
                            std::is_same_v<SimdjsonClass, uint64_t> ||
                            std::is_same_v<SimdjsonClass, double>;
  if constexpr (isNumber) {
    return "number";
  } else if constexpr (std::is_same_v<SimdjsonClass, sj::array>) {
    return "array";
  } else if constexpr (std::is_same_v<SimdjsonClass, sj::object>) {
    return "object";
  } else if constexpr (std::is_same_v<SimdjsonClass, std::string_view>) {
    return "string";
  } else if constexpr (std::is_same_v<SimdjsonClass, bool>) {
    return "boolean";
  } else if constexpr (std::is_same_v<SimdjsonClass, std::monostate>) {
    return "null";
  } else {
    static_assert(kAlwaysFalse<SimdjsonClass>, "unmapped simdjson value type");
  }
}

template <typename SimdjsonValueType>
Result<SimdjsonValueType> GetAs(sj::value& value) {
  SimdjsonValueType typed_value{};
  simdjson::error_code error_code;
  if constexpr (std::is_same_v<SimdjsonValueType, std::monostate>) {
    // simdjson has no get<>() for null; probe it explicitly
    bool is_null;
    error_code = value.is_null().get(is_null);
    if (error_code == simdjson::SUCCESS && !is_null) {
      error_code = simdjson::INCORRECT_TYPE;
    }
  } else {
    error_code = value.get(typed_value);
  }
  if (error_code != simdjson::SUCCESS) {
    sj::json_type json_type;
    if (value.type().get(json_type) != simdjson::SUCCESS) {
      return Status::Invalid("Expected ", JsonTypeName<SimdjsonValueType>(),
                             " or null, got malformed JSON value");
    }
    return Status::Invalid("Expected ", JsonTypeName<SimdjsonValueType>(),
                           " or null, got JSON type ", JsonTypeName(json_type));
  }
  return typed_value;
}

template <typename SimdjsonValueType>
Result<SimdjsonValueType> Get(simdjson::simdjson_result<SimdjsonValueType> element,
                              std::string_view error) {
  SimdjsonValueType typed_value;
  if (auto error_code = std::move(element).get(typed_value);
      error_code != simdjson::SUCCESS) {
    return Status::Invalid(error, simdjson::error_message(error_code));
  }
  return typed_value;
}

class JSONConverter {
 public:
  virtual ~JSONConverter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status AppendValue(sj::value& json_obj) = 0;

  Status AppendNull() { return this->builder()->AppendNull(); }

  /// Append all elements of `json_array` to this converter's builder and
  /// return the number of elements that were appended. The count lets callers
  /// with a size expectation (e.g. fixed-size lists or single scalars)
  /// validate it without a second pass over the forward-only JSON input. Note
  /// that it only counts the elements this call consumed; callers parsing a
  /// whole document must check `document::at_end()` to detect trailing input.
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
    int32_t num_elements = 0;
    for (auto element : json_array) {
      ARROW_ASSIGN_OR_RAISE(
          auto value,
          Get<sj::value>(element, "Could not iterate elements of JSON array: "));
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
    ARROW_RETURN_NOT_OK(GetAs<std::monostate>(json_obj));
    return AppendNull();
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
    int64_t int_value;
    if (json_obj.get(int_value) == simdjson::SUCCESS) {
      return builder_->Append(int_value != 0);
    }
    ARROW_ASSIGN_OR_RAISE(bool bool_value, GetAs<bool>(json_obj));
    return builder_->Append(bool_value);
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
  *out = static_cast<typename T::c_type>(0);
  ARROW_ASSIGN_OR_RAISE(int64_t v64, GetAs<int64_t>(json_obj));
  *out = static_cast<typename T::c_type>(v64);
  if (*out == v64) {
    return Status::OK();
  } else {
    return Status::Invalid("Value ", v64, " out of bounds for ", type);
  }
}

// Convert single unsigned integer value
template <typename T>
enable_if_unsigned_integer<T, Status> ConvertNumber(sj::value& json_obj,
                                                    const DataType& type,
                                                    typename T::c_type* out) {
  *out = static_cast<typename T::c_type>(0);
  ARROW_ASSIGN_OR_RAISE(uint64_t v64, GetAs<uint64_t>(json_obj));
  *out = static_cast<typename T::c_type>(v64);
  if (*out == v64) {
    return Status::OK();
  } else {
    return Status::Invalid("Value ", v64, " out of bounds for ", type);
  }
}

// Match the std::string_view against NaN, Inf, Infinity with optional leading minus
std::optional<double> NonFiniteDoubleFromString(std::string_view str) {
  if (str == "NaN") {
    return std::numeric_limits<double>::quiet_NaN();
  } else if (str == "-NaN") {
    return -std::numeric_limits<double>::quiet_NaN();
  } else if (str == "Inf" || str == "Infinity") {
    return std::numeric_limits<double>::infinity();
  } else if (str == "-Inf" || str == "-Infinity") {
    return -std::numeric_limits<double>::infinity();
  } else {
    return std::nullopt;
  }
}

std::optional<double> NonFiniteDoubleFromRawToken(sj::value& json_obj) {
  std::string_view token = json_obj.raw_json_token();
  // The raw token includes any trailing whitespace up to the next token
  while (!token.empty() && std::isspace(static_cast<unsigned char>(token.back()))) {
    token.remove_suffix(1);
  }
  return NonFiniteDoubleFromString(token);
}

// Convert float16/HalfFloatType
template <typename T>
enable_if_half_float<T, Status> ConvertNumber(sj::value& json_obj, const DataType& type,
                                              uint16_t* out) {
  *out = static_cast<uint16_t>(0);
  if (auto f64 = NonFiniteDoubleFromRawToken(json_obj); f64.has_value()) {
    *out = Float16(f64.value()).bits();
    return Status::OK();
  }
  ARROW_ASSIGN_OR_RAISE(auto f64, GetAs<double>(json_obj));
  *out = Float16(f64).bits();
  return arrow::Status::OK();
}

// Convert single floating point value
template <typename T>
enable_if_physical_floating_point<T, Status> ConvertNumber(sj::value& json_obj,
                                                           const DataType& type,
                                                           typename T::c_type* out) {
  *out = static_cast<typename T::c_type>(0);
  if (auto f64 = NonFiniteDoubleFromRawToken(json_obj); f64.has_value()) {
    *out = static_cast<typename T::c_type>(f64.value());
    return Status::OK();
  }
  ARROW_ASSIGN_OR_RAISE(auto f64, GetAs<double>(json_obj));
  *out = static_cast<typename T::c_type>(f64);
  return arrow::Status::OK();
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

  auto process_one = [&](auto&& handler) -> arrow::Status {
    if (it == end) {
      return Status::Invalid(error_context, " must have exactly ", expected_size,
                             " elements, had more");
    }

    sj::value element;
    auto error = (*it).get(element);
    if (error) {
      return Status::Invalid("Failed to get element ", index, " from ", error_context,
                             ": ", simdjson::error_message(error));
    }

    auto result = handler(element);
    ++it;
    ++index;
    return result;
  };

  // Use fold expression to process all handlers in order
  auto result = (process_one(std::forward<Handlers>(handlers)) & ...);

  if (!result.ok()) return result;

  if (it != end) {
    return Status::Invalid(error_context, " must have exactly ", expected_size,
                           " elements, had ", index);
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
    ARROW_ASSIGN_OR_RAISE(auto string_value, GetAs<std::string_view>(json_obj));
    int32_t precision, scale;
    DecimalValue d;
    RETURN_NOT_OK(DecimalValue::FromString(string_value, &d, &precision, &scale));
    if (scale != decimal_type_->scale()) {
      return Status::Invalid("Invalid scale for decimal: expected ",
                             decimal_type_->scale(), ", got ", scale);
    }
    return builder_->Append(d);
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
    } else {
      RETURN_NOT_OK(ConvertNumber<Int64Type>(json_obj, *this->type_, &value));
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

  Status AppendValue(sj::value& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }

    ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));

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

    ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));

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

    ARROW_ASSIGN_OR_RAISE(auto view, GetAs<std::string_view>(json_obj));
    return builder_->Append(view);
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
    ARROW_ASSIGN_OR_RAISE(auto view, GetAs<std::string_view>(json_obj));
    if (view.length() != static_cast<size_t>(builder_->byte_width())) {
      std::stringstream ss;
      ss << "Invalid string length " << view.length() << " in JSON input for "
         << this->type_->ToString();
      return Status::Invalid(ss.str());
    }
    return builder_->Append(view);
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
    ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));
    size_t num_elements;
    if (array.count_elements().get(num_elements) != simdjson::SUCCESS) {
      return Status::Invalid("Malformed JSON array for type ", this->type_->ToString());
    }
    // Extend the child converter with this JSON array
    ARROW_RETURN_NOT_OK(builder_->Append(true, static_cast<int64_t>(num_elements)));
    return child_converter_->AppendValues(array).status();
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
    ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));

    for (auto json_pair_result : array) {
      ARROW_ASSIGN_OR_RAISE(
          auto json_pair,
          Get<sj::value>(json_pair_result, "Could not iterate elements of JSON array: "));
      ARROW_ASSIGN_OR_RAISE(auto json_pair_array, GetAs<sj::array>(json_pair));

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
    ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));
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
    for (int32_t i = 0; i < type_->num_fields(); ++i) {
      field_index_.emplace(type_->field(i)->name(), i);
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
      auto expected_size = static_cast<size_t>(type_->num_fields());
      size_t size;
      if (array.count_elements().get(size) != simdjson::SUCCESS) {
        return Status::Invalid("Malformed JSON array for type ", type_->ToString());
      }
      if (size != expected_size) {
        return Status::Invalid("Expected array of size ", expected_size,
                               ", got array of size ", size);
      }
      size_t i = 0;
      for (auto child : array) {
        ARROW_ASSIGN_OR_RAISE(
            auto child_value,
            Get<sj::value>(child, "Could not iterate elements of JSON array: "));
        RETURN_NOT_OK(child_converters_[i]->AppendValue(child_value));
        ++i;
      }
      return builder_->Append();
    }
    ARROW_ASSIGN_OR_RAISE(auto object, GetAs<sj::object>(json_obj));
    // Iterate the object fields in JSON order (the on-demand API is
    // forward-only, so per-field lookups would be quadratic and would also
    // compare against raw, still-escaped keys). Fields absent from the JSON
    // are appended as null afterwards.
    auto num_fields = type_->num_fields();
    std::vector<bool> field_seen(num_fields, false);
    for (auto field_result : object) {
      ARROW_ASSIGN_OR_RAISE(
          auto field, Get<sj::field>(field_result, "Error getting field of object: "));
      std::string_view key;
      if (field.unescaped_key(/*allow_replacement=*/false).get(key) !=
          simdjson::SUCCESS) {
        return Status::Invalid("Malformed key in JSON object for type ",
                               type_->ToString());
      }
      auto it = field_index_.find(key);
      if (it == field_index_.end()) {
        return Status::Invalid("Unexpected member \"", key, "\" in JSON object for type ",
                               type_->ToString());
      }
      const int32_t field_num = it->second;
      if (field_seen[field_num]) {
        return Status::Invalid("Duplicate member \"", key, "\" in JSON object for type ",
                               type_->ToString());
      }
      field_seen[field_num] = true;
      sj::value value = field.value();
      RETURN_NOT_OK(child_converters_[field_num]->AppendValue(value));
    }
    for (int32_t i = 0; i < num_fields; ++i) {
      if (!field_seen[i]) {
        RETURN_NOT_OK(child_converters_[i]->AppendNull());
      }
    }
    return builder_->Append();
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<StructBuilder> builder_;
  std::vector<std::shared_ptr<JSONConverter>> child_converters_;
  // keys are views into the field names owned by type_
  std::unordered_map<std::string_view, int32_t> field_index_;
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

    ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));

    int8_t id = 0;
    std::shared_ptr<JSONConverter> child_converter;

    RETURN_NOT_OK(ProcessJsonArrayElements(
        array, "[type_id, value] pair",
        [this, &id, &child_converter](sj::value& id_elem) {
          ARROW_ASSIGN_OR_RAISE(auto id_value, GetAs<int64_t>(id_elem));
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

  simdjson::padded_string padded_string{json_string};

  sj::parser parser;
  sj::document json_doc;
  auto error = parser.iterate(padded_string).get(json_doc);
  if (error) {
    return Status::Invalid("JSON parse error: ", simdjson::error_message(error));
  }
  sj::value json_obj;
  if (auto error_code = json_doc.get_value().get(json_obj);
      error_code != simdjson::SUCCESS) {
    return Status::Invalid("JSON parse error: ", simdjson::error_message(error_code));
  }
  ARROW_ASSIGN_OR_RAISE(auto array, GetAs<sj::array>(json_obj));

  // The JSON document should be an array, append it
  RETURN_NOT_OK(converter->AppendValues(array));
  // The parser is lazy and does not look past the end of the array by itself
  if (!json_doc.at_end()) {
    return Status::Invalid("JSON parse error: trailing content after JSON array");
  }
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

  simdjson::padded_string padded_string{
      arrow::internal::JoinToString("[", json_string, "]")};

  sj::parser parser;
  sj::document json_doc;
  auto error = parser.iterate(padded_string).get(json_doc);
  if (error) {
    return Status::Invalid("JSON parse error: ", simdjson::error_message(error));
  }

  sj::value json_obj;
  if (auto error_code = json_doc.get_value().get(json_obj);
      error_code != simdjson::SUCCESS) {
    return Status::Invalid("JSON parse error: ", simdjson::error_message(error_code));
  }
  ARROW_ASSIGN_OR_RAISE(auto singleton_array, GetAs<sj::array>(json_obj));

  ARROW_ASSIGN_OR_RAISE(int32_t num_elements, converter->AppendValues(singleton_array));
  if (num_elements != 1) {
    return Status::Invalid("Expected exactly one JSON value, got ", num_elements,
                           " values");
  }
  // The parser is lazy and does not look past the end of the wrapped value by
  // itself, e.g. the input "1] , [2" would otherwise be accepted as 1
  if (!json_doc.at_end()) {
    return Status::Invalid("JSON parse error: trailing content after JSON value");
  }

  std::shared_ptr<Array> array;
  RETURN_NOT_OK(converter->Finish(&array));
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
