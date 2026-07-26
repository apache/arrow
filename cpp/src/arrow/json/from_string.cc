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
#include <cstdio>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <simdjson.h>

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
#include "arrow/scalar.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/float16.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::ParseValue;
using util::Float16;

namespace json {

using ::arrow::internal::checked_cast;
using ::arrow::internal::checked_pointer_cast;

namespace {

// Lazy fallback sanitizer for legacy non-standard JSON inputs accepted by RapidJSON
// (unquoted NaN/Inf tokens and raw non-UTF-8 bytes in binary string literals).
std::string SanitizeNonStandardJson(std::string_view input) {
  std::string result;
  result.reserve(input.size() + 16);

  bool in_string = false;
  bool escaped = false;

  for (size_t i = 0; i < input.size(); ++i) {
    char c = input[i];

    if (in_string) {
      if (escaped) {
        result.push_back(c);
        escaped = false;
      } else if (c == '\\') {
        result.push_back(c);
        escaped = true;
      } else if (c == '"') {
        result.push_back(c);
        in_string = false;
      } else {
        unsigned char uc = static_cast<unsigned char>(c);
        if (uc >= 0x80) {
          size_t seq_len = 0;
          if ((uc & 0xE0) == 0xC0)
            seq_len = 2;
          else if ((uc & 0xF0) == 0xE0)
            seq_len = 3;
          else if ((uc & 0xF8) == 0xF0)
            seq_len = 4;

          bool valid_utf8 = false;
          if (seq_len > 1 && i + seq_len <= input.size()) {
            valid_utf8 = true;
            for (size_t k = 1; k < seq_len; ++k) {
              if ((static_cast<unsigned char>(input[i + k]) & 0xC0) != 0x80) {
                valid_utf8 = false;
                break;
              }
            }
          }

          if (valid_utf8) {
            for (size_t k = 0; k < seq_len; ++k) {
              result.push_back(input[i + k]);
            }
            i += seq_len - 1;
          } else {
            // Encode raw non-UTF-8 byte into Private Use Area U+E080..U+E0FF (valid
            // UTF-8)
            uint32_t val = uc - 0x80;
            result.push_back(static_cast<char>(0xEE));
            result.push_back(static_cast<char>(0x82 + (val >> 6)));
            result.push_back(static_cast<char>(0x80 + (val & 0x3F)));
          }
        } else {
          result.push_back(c);
        }
      }
    } else {
      if (c == '"') {
        result.push_back(c);
        in_string = true;
      } else {
        if ((c == 'N' || c == 'I' || c == '+' || c == '-') &&
            (i == 0 || (!std::isalnum(static_cast<unsigned char>(input[i - 1])) &&
                        input[i - 1] != '_'))) {
          std::string_view rem = input.substr(i);
          if (rem.starts_with("NaN")) {
            result.append("\"NaN\"");
            i += 2;
            continue;
          } else if (rem.starts_with("Infinity")) {
            result.append("\"Infinity\"");
            i += 7;
            continue;
          } else if (rem.starts_with("Inf")) {
            result.append("\"Inf\"");
            i += 2;
            continue;
          } else if (rem.starts_with("-Infinity")) {
            result.append("\"-Infinity\"");
            i += 8;
            continue;
          } else if (rem.starts_with("-Inf")) {
            result.append("\"-Inf\"");
            i += 3;
            continue;
          } else if (rem.starts_with("+Infinity")) {
            result.append("\"+Infinity\"");
            i += 8;
            continue;
          } else if (rem.starts_with("+Inf")) {
            result.append("\"+Inf\"");
            i += 3;
            continue;
          }
        }
        result.push_back(c);
      }
    }
  }
  return result;
}

std::string DecodeBinaryString(std::string_view view) {
  std::string out;
  out.reserve(view.size());
  for (size_t i = 0; i < view.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(view[i]);
    if (c == 0xEE && i + 2 < view.size()) {
      unsigned char c2 = static_cast<unsigned char>(view[i + 1]);
      unsigned char c3 = static_cast<unsigned char>(view[i + 2]);
      if ((c2 == 0x82 || c2 == 0x83) && (c3 & 0xC0) == 0x80) {
        uint32_t val = ((c2 - 0x82) << 6) | (c3 - 0x80);
        if (val <= 0x7F) {
          out.push_back(static_cast<char>(0x80 + val));
          i += 2;
          continue;
        }
      }
    }
    out.push_back(c);
  }
  return out;
}

const char* JsonTypeName(simdjson::dom::element_type type) {
  switch (type) {
    case simdjson::dom::element_type::NULL_VALUE:
      return "null";
    case simdjson::dom::element_type::BOOL:
      return "boolean";
    case simdjson::dom::element_type::INT64:
      return "signed int";
    case simdjson::dom::element_type::UINT64:
      return "unsigned int";
    case simdjson::dom::element_type::DOUBLE:
      return "number";
    case simdjson::dom::element_type::STRING:
      return "string";
    case simdjson::dom::element_type::ARRAY:
      return "array";
    case simdjson::dom::element_type::OBJECT:
      return "object";
    default:
      return "unknown";
  }
}

Status JSONTypeError(const char* expected_type, simdjson::dom::element_type json_type) {
  return Status::Invalid("Expected ", expected_type, " or null, got JSON type ",
                         JsonTypeName(json_type));
}

class JSONConverter {
 public:
  virtual ~JSONConverter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status AppendValue(const simdjson::dom::element& json_obj) = 0;

  Status AppendNull() { return this->builder()->AppendNull(); }

  virtual Status AppendValues(const simdjson::dom::element& json_array) = 0;

  virtual std::shared_ptr<ArrayBuilder> builder() = 0;

  virtual Status Finish(std::shared_ptr<Array>* out) {
    auto builder = this->builder();
    if (builder->length() == 0) {
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
  Result<uint64_t> SizeOfJSONArray(const simdjson::dom::element& json_obj) {
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    return json_obj.get_array().size();
  }

  Status AppendValues(const simdjson::dom::element& json_array) final {
    auto self = static_cast<Derived*>(this);
    if (!json_array.is_array()) {
      return JSONTypeError("array", json_array.type());
    }
    for (simdjson::dom::element elem : json_array.get_array()) {
      RETURN_NOT_OK(self->AppendValue(elem));
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return AppendNull();
    }
    if (json_obj.is_bool()) {
      return builder_->Append(json_obj.get_bool().value());
    }
    if (json_obj.is_int64()) {
      return builder_->Append(json_obj.get_int64().value() != 0);
    }
    if (json_obj.is_uint64()) {
      return builder_->Append(json_obj.get_uint64().value() != 0);
    }
    return JSONTypeError("boolean", json_obj.type());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BooleanBuilder> builder_;
};

// ------------------------------------------------------------------------
// Helpers for numeric converters

template <typename T>
enable_if_physical_signed_integer<T, Status> ConvertNumber(
    const simdjson::dom::element& json_obj, const DataType& type,
    typename T::c_type* out) {
  if (json_obj.is_int64()) {
    int64_t v64 = json_obj.get_int64().value();
    *out = static_cast<typename T::c_type>(v64);
    if (static_cast<int64_t>(*out) == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else if (json_obj.is_uint64()) {
    uint64_t u64 = json_obj.get_uint64().value();
    *out = static_cast<typename T::c_type>(u64);
    if (u64 <= static_cast<uint64_t>(std::numeric_limits<typename T::c_type>::max())) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", u64, " out of bounds for ", type);
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("signed int", json_obj.type());
  }
}

template <typename T>
enable_if_unsigned_integer<T, Status> ConvertNumber(
    const simdjson::dom::element& json_obj, const DataType& type,
    typename T::c_type* out) {
  if (json_obj.is_uint64()) {
    uint64_t v64 = json_obj.get_uint64().value();
    *out = static_cast<typename T::c_type>(v64);
    if (static_cast<uint64_t>(*out) == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else if (json_obj.is_int64()) {
    int64_t v64 = json_obj.get_int64().value();
    if (v64 >= 0) {
      uint64_t u64 = static_cast<uint64_t>(v64);
      *out = static_cast<typename T::c_type>(u64);
      if (static_cast<uint64_t>(*out) == u64) {
        return Status::OK();
      } else {
        return Status::Invalid("Value ", v64, " out of bounds for ", type);
      }
    } else {
      *out = static_cast<typename T::c_type>(0);
      return Status::Invalid("Value ", v64, " out of bounds for ", type);
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("unsigned int", json_obj.type());
  }
}

template <typename T>
enable_if_half_float<T, Status> ConvertNumber(const simdjson::dom::element& json_obj,
                                              const DataType& type, uint16_t* out) {
  if (json_obj.is_double()) {
    double f64 = json_obj.get_double().value();
    *out = Float16(f64).bits();
    return Status::OK();
  } else if (json_obj.is_uint64()) {
    uint64_t u64 = json_obj.get_uint64().value();
    double f64 = static_cast<double>(u64);
    *out = Float16(f64).bits();
    return Status::OK();
  } else if (json_obj.is_int64()) {
    int64_t i64 = json_obj.get_int64().value();
    double f64 = static_cast<double>(i64);
    *out = Float16(f64).bits();
    return Status::OK();
  } else if (json_obj.is_string()) {
    std::string_view str = json_obj.get_string().value();
    if (str == "NaN") {
      *out = Float16(std::numeric_limits<float>::quiet_NaN()).bits();
      return Status::OK();
    } else if (str == "Inf" || str == "Infinity" || str == "+Inf" || str == "+Infinity") {
      *out = Float16(std::numeric_limits<float>::infinity()).bits();
      return Status::OK();
    } else if (str == "-Inf" || str == "-Infinity") {
      *out = Float16(-std::numeric_limits<float>::infinity()).bits();
      return Status::OK();
    }
  }
  *out = static_cast<uint16_t>(0);
  return JSONTypeError("unsigned int", json_obj.type());
}

template <typename T>
enable_if_physical_floating_point<T, Status> ConvertNumber(
    const simdjson::dom::element& json_obj, const DataType& type,
    typename T::c_type* out) {
  if (json_obj.is_double()) {
    *out = static_cast<typename T::c_type>(json_obj.get_double().value());
    return Status::OK();
  } else if (json_obj.is_int64()) {
    *out = static_cast<typename T::c_type>(json_obj.get_int64().value());
    return Status::OK();
  } else if (json_obj.is_uint64()) {
    *out = static_cast<typename T::c_type>(json_obj.get_uint64().value());
    return Status::OK();
  } else if (json_obj.is_string()) {
    std::string_view str = json_obj.get_string().value();
    if (str == "NaN") {
      *out = std::numeric_limits<typename T::c_type>::quiet_NaN();
      return Status::OK();
    } else if (str == "Inf" || str == "Infinity" || str == "+Inf" || str == "+Infinity") {
      *out = std::numeric_limits<typename T::c_type>::infinity();
      return Status::OK();
    } else if (str == "-Inf" || str == "-Infinity") {
      *out = -std::numeric_limits<typename T::c_type>::infinity();
      return Status::OK();
    }
  }
  *out = static_cast<typename T::c_type>(0);
  return JSONTypeError("number", json_obj.type());
}

// ------------------------------------------------------------------------
// Converter for int arrays

template <typename Type, typename BuilderType = typename TypeTraits<Type>::BuilderType>
class IntegerConverter final
    : public ConcreteConverter<IntegerConverter<Type, BuilderType>> {
  using c_type = typename Type::c_type;

 public:
  explicit IntegerConverter(const std::shared_ptr<DataType>& type) { this->type_ = type; }

  Status Init() override { return this->MakeConcreteBuilder(&builder_); }

  Status AppendValue(const simdjson::dom::element& json_obj) override {
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_string()) {
      int32_t precision, scale;
      DecimalValue d;
      std::string_view view = json_obj.get_string().value();
      RETURN_NOT_OK(DecimalValue::FromString(view, &d, &precision, &scale));
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    int64_t value;
    if (json_obj.is_int64() || json_obj.is_uint64() || json_obj.is_double()) {
      RETURN_NOT_OK(ConvertNumber<Int64Type>(json_obj, *this->type_, &value));
    } else if (json_obj.is_string()) {
      std::string_view view = json_obj.get_string().value();
      if (!ParseValue(*timestamp_type_, view.data(), view.size(), &value)) {
        return Status::Invalid("couldn't parse timestamp from ", view);
      }
    } else {
      return JSONTypeError("timestamp", json_obj.type());
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    simdjson::dom::array arr = json_obj.get_array();
    if (arr.size() != 2) {
      return Status::Invalid(
          "day time interval pair must have exactly two elements, had ", arr.size());
    }
    DayTimeIntervalType::DayMilliseconds value;
    RETURN_NOT_OK(ConvertNumber<Int32Type>(arr.at(0).value(), *this->type_, &value.days));
    RETURN_NOT_OK(
        ConvertNumber<Int32Type>(arr.at(1).value(), *this->type_, &value.milliseconds));
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    simdjson::dom::array arr = json_obj.get_array();
    if (arr.size() != 3) {
      return Status::Invalid(
          "month_day_nano_interval  must have exactly 3 elements, had ", arr.size());
    }
    MonthDayNanoIntervalType::MonthDayNanos value;
    RETURN_NOT_OK(
        ConvertNumber<Int32Type>(arr.at(0).value(), *this->type_, &value.months));
    RETURN_NOT_OK(ConvertNumber<Int32Type>(arr.at(1).value(), *this->type_, &value.days));
    RETURN_NOT_OK(
        ConvertNumber<Int64Type>(arr.at(2).value(), *this->type_, &value.nanoseconds));

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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_string()) {
      std::string_view view = json_obj.get_string().value();
      if constexpr (std::is_same_v<Type, BinaryType> ||
                    std::is_same_v<Type, LargeBinaryType> ||
                    std::is_same_v<Type, BinaryViewType>) {
        std::string decoded = DecodeBinaryString(view);
        return builder_->Append(decoded);
      } else {
        return builder_->Append(view);
      }
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_string()) {
      std::string_view view = json_obj.get_string().value();
      std::string decoded = DecodeBinaryString(view);
      if (decoded.length() != static_cast<size_t>(builder_->byte_width())) {
        std::stringstream ss;
        ss << "Invalid string length " << decoded.length() << " in JSON input for "
           << this->type_->ToString();
        return Status::Invalid(ss.str());
      }
      return builder_->Append(decoded);
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    ARROW_ASSIGN_OR_RAISE(auto size, this->SizeOfJSONArray(json_obj));
    RETURN_NOT_OK(builder_->Append(true, static_cast<int64_t>(size)));
    return child_converter_->AppendValues(json_obj);
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    simdjson::dom::array arr = json_obj.get_array();
    for (simdjson::dom::element json_pair : arr) {
      if (!json_pair.is_array()) {
        return JSONTypeError("array", json_pair.type());
      }
      simdjson::dom::array pair_arr = json_pair.get_array();
      if (pair_arr.size() != 2) {
        return Status::Invalid("key item pair must have exactly two elements, had ",
                               pair_arr.size());
      }
      if (pair_arr.at(0).value().is_null()) {
        return Status::Invalid("null key is invalid");
      }
      RETURN_NOT_OK(key_converter_->AppendValue(pair_arr.at(0).value()));
      RETURN_NOT_OK(item_converter_->AppendValue(pair_arr.at(1).value()));
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    RETURN_NOT_OK(child_converter_->AppendValues(json_obj));
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    if (json_obj.get_array().size() != static_cast<size_t>(list_size_)) {
      return Status::Invalid("incorrect list size ", json_obj.get_array().size());
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (json_obj.is_array()) {
      simdjson::dom::array arr = json_obj.get_array();
      auto size = arr.size();
      auto expected_size = static_cast<size_t>(type_->num_fields());
      if (size != expected_size) {
        return Status::Invalid("Expected array of size ", expected_size,
                               ", got array of size ", size);
      }
      uint32_t i = 0;
      for (simdjson::dom::element elem : arr) {
        RETURN_NOT_OK(child_converters_[i++]->AppendValue(elem));
      }
      return builder_->Append();
    }
    if (json_obj.is_object()) {
      simdjson::dom::object obj = json_obj.get_object();
      auto remaining = obj.size();
      auto num_children = type_->num_fields();
      for (int32_t i = 0; i < num_children; ++i) {
        const auto& field = type_->field(i);
        auto val_result = obj[field->name()];
        if (val_result.error() == simdjson::SUCCESS) {
          --remaining;
          RETURN_NOT_OK(child_converters_[i]->AppendValue(val_result.value()));
        } else {
          RETURN_NOT_OK(child_converters_[i]->AppendNull());
        }
      }
      if (remaining > 0) {
        return Status::Invalid("Unexpected members in JSON object for type ",
                               type_->ToString(),
                               " Object: ", simdjson::to_string(json_obj));
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

  Status AppendValue(const simdjson::dom::element& json_obj) override {
    if (json_obj.is_null()) {
      return this->AppendNull();
    }
    if (!json_obj.is_array()) {
      return JSONTypeError("array", json_obj.type());
    }
    simdjson::dom::array arr = json_obj.get_array();
    if (arr.size() != 2) {
      return Status::Invalid("Expected [type_id, value] pair, got array of size ",
                             arr.size());
    }
    const auto& id_obj = arr.at(0).value();
    if (!id_obj.is_int64() && !id_obj.is_uint64()) {
      return JSONTypeError("int", id_obj.type());
    }

    int8_t id = 0;
    if (id_obj.is_int64()) {
      id = static_cast<int8_t>(id_obj.get_int64().value());
    } else {
      id = static_cast<int8_t>(id_obj.get_uint64().value());
    }

    if (id < 0 || id >= static_cast<int8_t>(type_id_to_child_num_.size())) {
      return Status::Invalid("type_id ", id, " not found in ", *type_);
    }

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
    return child_converter->AppendValue(arr.at(1).value());
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

  simdjson::dom::parser parser;
  simdjson::padded_string padded(json_string);
  simdjson::dom::element json_doc;
  auto err = parser.parse(padded).get(json_doc);
  if (err) {
    if (err == simdjson::TAPE_ERROR || err == simdjson::UTF8_ERROR) {
      std::string preprocessed = SanitizeNonStandardJson(json_string);
      simdjson::padded_string padded_preprocessed(preprocessed);
      err = parser.parse(padded_preprocessed).get(json_doc);
    }
    if (err) {
      return Status::Invalid("JSON parse error: ", simdjson::error_message(err));
    }
  }

  RETURN_NOT_OK(converter->AppendValues(json_doc));
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

  simdjson::dom::parser parser;
  simdjson::padded_string padded(json_string);
  simdjson::dom::element json_doc;
  auto err = parser.parse(padded).get(json_doc);
  if (err) {
    if (err == simdjson::TAPE_ERROR || err == simdjson::UTF8_ERROR) {
      std::string preprocessed = SanitizeNonStandardJson(json_string);
      simdjson::padded_string padded_preprocessed(preprocessed);
      err = parser.parse(padded_preprocessed).get(json_doc);
    }
    if (err) {
      return Status::Invalid("JSON parse error: ", simdjson::error_message(err));
    }
  }

  std::shared_ptr<Array> array;
  RETURN_NOT_OK(converter->AppendValue(json_doc));
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
