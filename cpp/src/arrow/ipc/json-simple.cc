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
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/ipc/json-internal.h"
#include "arrow/ipc/json-simple.h"
#include "arrow/memory_pool.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace ipc {
namespace internal {
namespace json {

using ::arrow::internal::checked_cast;

static constexpr auto kParseFlags = rj::kParseFullPrecisionFlag | rj::kParseNanAndInfFlag;

static Status JSONTypeError(const char* expected_type, rj::Type json_type) {
  return Status::Invalid("Expected ", expected_type, " or null, got JSON type ",
                         json_type);
}

class Converter {
 public:
  virtual ~Converter() = default;

  virtual Status Init() { return Status::OK(); }

  virtual Status AppendValue(const rj::Value& json_obj) = 0;

  virtual Status AppendNull() = 0;

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
};

// ------------------------------------------------------------------------
// Converter for null arrays

class NullConverter final : public ConcreteConverter<NullConverter> {
 public:
  explicit NullConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<NullBuilder>();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

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

  Status AppendNull() override { return builder_->AppendNull(); }

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
typename std::enable_if<is_signed_integer<T>::value || is_date<T>::value ||
                            is_time<T>::value || is_timestamp<T>::value,
                        Status>::type
ConvertNumber(const rj::Value& json_obj, typename T::c_type* out) {
  if (json_obj.IsInt64()) {
    int64_t v64 = json_obj.GetInt64();
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ",
                             TypeTraits<T>::type_singleton());
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("signed int", json_obj.GetType());
  }
}

// Convert single unsigned integer value
template <typename T>
enable_if_unsigned_integer<T, Status> ConvertNumber(const rj::Value& json_obj,
                                                    typename T::c_type* out) {
  if (json_obj.IsUint64()) {
    uint64_t v64 = json_obj.GetUint64();
    *out = static_cast<typename T::c_type>(v64);
    if (*out == v64) {
      return Status::OK();
    } else {
      return Status::Invalid("Value ", v64, " out of bounds for ",
                             TypeTraits<T>::type_singleton());
    }
  } else {
    *out = static_cast<typename T::c_type>(0);
    return JSONTypeError("unsigned int", json_obj.GetType());
  }
}

// Convert single floating point value
template <typename T>
enable_if_floating_point<T, Status> ConvertNumber(const rj::Value& json_obj,
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

template <typename Type>
class IntegerConverter final : public ConcreteConverter<IntegerConverter<Type>> {
  using c_type = typename Type::c_type;
  static constexpr auto is_signed = std::is_signed<c_type>::value;

 public:
  explicit IntegerConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<NumericBuilder<Type>>();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    c_type value;
    RETURN_NOT_OK(ConvertNumber<Type>(json_obj, &value));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<NumericBuilder<Type>> builder_;
};

// ------------------------------------------------------------------------
// Converter for float arrays

template <typename Type>
class FloatConverter final : public ConcreteConverter<FloatConverter<Type>> {
  using c_type = typename Type::c_type;

 public:
  explicit FloatConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<NumericBuilder<Type>>();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    c_type value;
    RETURN_NOT_OK(ConvertNumber<Type>(json_obj, &value));
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<NumericBuilder<Type>> builder_;
};

// ------------------------------------------------------------------------
// Converter for decimal arrays

class DecimalConverter final : public ConcreteConverter<DecimalConverter> {
 public:
  explicit DecimalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    decimal_type_ = checked_cast<Decimal128Type*>(type.get());
    builder_ = std::make_shared<DecimalBuilder>(type);
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    if (json_obj.IsString()) {
      int32_t precision, scale;
      Decimal128 d;
      auto view = util::string_view(json_obj.GetString(), json_obj.GetStringLength());
      RETURN_NOT_OK(Decimal128::FromString(view, &d, &precision, &scale));
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
  std::shared_ptr<DecimalBuilder> builder_;
  Decimal128Type* decimal_type_;
};

// ------------------------------------------------------------------------
// Converter for timestamp arrays

class TimestampConverter final : public ConcreteConverter<TimestampConverter> {
 public:
  explicit TimestampConverter(const std::shared_ptr<DataType>& type)
      : from_string_(type) {
    this->type_ = type;
    builder_ = std::make_shared<TimestampBuilder>(type, default_memory_pool());
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    int64_t value;
    if (json_obj.IsNumber()) {
      RETURN_NOT_OK(ConvertNumber<Int64Type>(json_obj, &value));
    } else if (json_obj.IsString()) {
      auto view = util::string_view(json_obj.GetString(), json_obj.GetStringLength());
      if (!from_string_(view.data(), view.size(), &value)) {
        return Status::Invalid("couldn't parse timestamp from ", view);
      }
    } else {
      return JSONTypeError("timestamp", json_obj.GetType());
    }
    return builder_->Append(value);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  ::arrow::internal::StringConverter<TimestampType> from_string_;
  std::shared_ptr<TimestampBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for binary and string arrays

class StringConverter final : public ConcreteConverter<StringConverter> {
 public:
  explicit StringConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<BinaryBuilder>(type, default_memory_pool());
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    if (json_obj.IsString()) {
      auto view = util::string_view(json_obj.GetString(), json_obj.GetStringLength());
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.GetType());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<BinaryBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for fixed-size binary arrays

class FixedSizeBinaryConverter final
    : public ConcreteConverter<FixedSizeBinaryConverter> {
 public:
  explicit FixedSizeBinaryConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<FixedSizeBinaryBuilder>(type, default_memory_pool());
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    if (json_obj.IsString()) {
      auto view = util::string_view(json_obj.GetString(), json_obj.GetStringLength());
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
  std::shared_ptr<FixedSizeBinaryBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for list arrays

class ListConverter final : public ConcreteConverter<ListConverter> {
 public:
  explicit ListConverter(const std::shared_ptr<DataType>& type) { type_ = type; }

  Status Init() override {
    const auto& list_type = checked_cast<const ListType&>(*type_);
    RETURN_NOT_OK(GetConverter(list_type.value_type(), &child_converter_));
    auto child_builder = child_converter_->builder();
    builder_ = std::make_shared<ListBuilder>(default_memory_pool(), child_builder, type_);
    return Status::OK();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    // Extend the child converter with this JSON array
    return child_converter_->AppendValues(json_obj);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 private:
  std::shared_ptr<ListBuilder> builder_;
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

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
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

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
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
    for (const auto& field : type_->children()) {
      std::shared_ptr<Converter> child_converter;
      RETURN_NOT_OK(GetConverter(field->type(), &child_converter));
      child_converters_.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }
    builder_ = std::make_shared<StructBuilder>(type_, default_memory_pool(),
                                               std::move(child_builders));
    return Status::OK();
  }

  Status AppendNull() override {
    for (auto& converter : child_converters_) {
      RETURN_NOT_OK(converter->AppendNull());
    }
    return builder_->AppendNull();
  }

  // Append a JSON value that is either an array of N elements in order
  // or an object mapping struct names to values (omitted struct members
  // are mapped to null).
  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return AppendNull();
    }
    if (json_obj.IsArray()) {
      auto size = json_obj.Size();
      auto expected_size = static_cast<uint32_t>(type_->num_children());
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
      auto num_children = type_->num_children();
      for (int32_t i = 0; i < num_children; ++i) {
        const auto& field = type_->child(i);
        auto it = json_obj.FindMember(field->name());
        if (it != json_obj.MemberEnd()) {
          --remaining;
          RETURN_NOT_OK(child_converters_[i]->AppendValue(it->value));
        } else {
          RETURN_NOT_OK(child_converters_[i]->AppendNull());
        }
      }
      if (remaining > 0) {
        return Status::Invalid("Unexpected members in JSON object for type ",
                               type_->ToString());
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
// General conversion functions

Status GetConverter(const std::shared_ptr<DataType>& type,
                    std::shared_ptr<Converter>* out) {
  std::shared_ptr<Converter> res;

#define SIMPLE_CONVERTER_CASE(ID, CLASS) \
  case ID:                               \
    res = std::make_shared<CLASS>(type); \
    break;

  switch (type->id()) {
    SIMPLE_CONVERTER_CASE(Type::INT8, IntegerConverter<Int8Type>)
    SIMPLE_CONVERTER_CASE(Type::INT16, IntegerConverter<Int16Type>)
    SIMPLE_CONVERTER_CASE(Type::INT32, IntegerConverter<Int32Type>)
    SIMPLE_CONVERTER_CASE(Type::TIME32, IntegerConverter<Int32Type>)
    SIMPLE_CONVERTER_CASE(Type::DATE32, IntegerConverter<Date32Type>)
    SIMPLE_CONVERTER_CASE(Type::INT64, IntegerConverter<Int64Type>)
    SIMPLE_CONVERTER_CASE(Type::TIME64, IntegerConverter<Int64Type>)
    SIMPLE_CONVERTER_CASE(Type::TIMESTAMP, TimestampConverter)
    SIMPLE_CONVERTER_CASE(Type::DATE64, IntegerConverter<Date64Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT8, IntegerConverter<UInt8Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT16, IntegerConverter<UInt16Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT32, IntegerConverter<UInt32Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT64, IntegerConverter<UInt64Type>)
    SIMPLE_CONVERTER_CASE(Type::NA, NullConverter)
    SIMPLE_CONVERTER_CASE(Type::BOOL, BooleanConverter)
    SIMPLE_CONVERTER_CASE(Type::FLOAT, FloatConverter<FloatType>)
    SIMPLE_CONVERTER_CASE(Type::DOUBLE, FloatConverter<DoubleType>)
    SIMPLE_CONVERTER_CASE(Type::LIST, ListConverter)
    SIMPLE_CONVERTER_CASE(Type::MAP, MapConverter)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_LIST, FixedSizeListConverter)
    SIMPLE_CONVERTER_CASE(Type::STRUCT, StructConverter)
    SIMPLE_CONVERTER_CASE(Type::STRING, StringConverter)
    SIMPLE_CONVERTER_CASE(Type::BINARY, StringConverter)
    SIMPLE_CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL, DecimalConverter)
    default: {
      return Status::NotImplemented("JSON conversion to ", type->ToString(),
                                    " not implemented");
    }
  }

#undef SIMPLE_CONVERTER_CASE

  RETURN_NOT_OK(res->Init());
  *out = res;
  return Status::OK();
}

Status ArrayFromJSON(const std::shared_ptr<DataType>& type,
                     const util::string_view& json_string, std::shared_ptr<Array>* out) {
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
  return converter->Finish(out);
}

Status ArrayFromJSON(const std::shared_ptr<DataType>& type,
                     const std::string& json_string, std::shared_ptr<Array>* out) {
  return ArrayFromJSON(type, util::string_view(json_string), out);
}

Status ArrayFromJSON(const std::shared_ptr<DataType>& type, const char* json_string,
                     std::shared_ptr<Array>* out) {
  return ArrayFromJSON(type, util::string_view(json_string), out);
}

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
