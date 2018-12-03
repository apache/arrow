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
#include "arrow/util/string_view.h"

namespace arrow {
namespace ipc {
namespace internal {
namespace json {

using ::arrow::internal::checked_cast;

static constexpr auto kParseFlags = rj::kParseFullPrecisionFlag | rj::kParseNanAndInfFlag;

static Status JSONTypeError(const char* expected_type, rj::Type json_type) {
  std::stringstream ss;
  ss << "Expected " << expected_type << " or null, got type " << json_type;
  return Status::Invalid(ss.str());
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

// TODO : dates and times?
// TODO : binary / fixed size binary?

// ------------------------------------------------------------------------
// Converter for null arrays

class NullConverter : public ConcreteConverter<NullConverter> {
 public:
  explicit NullConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<NullBuilder>();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return builder_->AppendNull();
    }
    return JSONTypeError("null", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  std::shared_ptr<NullBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for boolean arrays

class BooleanConverter : public ConcreteConverter<BooleanConverter> {
 public:
  explicit BooleanConverter(const std::shared_ptr<DataType>& type) {
    type_ = type;
    builder_ = std::make_shared<BooleanBuilder>();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return builder_->AppendNull();
    }
    if (json_obj.IsBool()) {
      return builder_->Append(json_obj.GetBool());
    }
    return JSONTypeError("boolean", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  std::shared_ptr<BooleanBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for int arrays

template <typename Type>
class IntegerConverter : public ConcreteConverter<IntegerConverter<Type>> {
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
      return builder_->AppendNull();
    }
    return AppendNumber(json_obj);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  // Append signed integer value
  template <typename Integer = c_type>
  typename std::enable_if<std::is_signed<Integer>::value, Status>::type AppendNumber(
      const rj::Value& json_obj) {
    if (json_obj.IsInt64()) {
      int64_t v64 = json_obj.GetInt64();
      c_type v = static_cast<c_type>(v64);
      if (v == v64) {
        return builder_->Append(v);
      } else {
        std::stringstream ss;
        ss << "Value " << v64 << " out of bounds for " << this->type_->ToString();
        return Status::Invalid(ss.str());
      }
    } else {
      return JSONTypeError("signed int", json_obj.GetType());
    }
  }

  // Append unsigned integer value
  template <typename Integer = c_type>
  typename std::enable_if<std::is_unsigned<Integer>::value, Status>::type AppendNumber(
      const rj::Value& json_obj) {
    if (json_obj.IsUint64()) {
      uint64_t v64 = json_obj.GetUint64();
      c_type v = static_cast<c_type>(v64);
      if (v == v64) {
        return builder_->Append(v);
      } else {
        std::stringstream ss;
        ss << "Value " << v64 << " out of bounds for " << this->type_->ToString();
        return Status::Invalid(ss.str());
      }
      return builder_->Append(v);
    } else {
      return JSONTypeError("unsigned int", json_obj.GetType());
    }
  }

  std::shared_ptr<NumericBuilder<Type>> builder_;
};

// ------------------------------------------------------------------------
// Converter for float arrays

template <typename Type>
class FloatConverter : public ConcreteConverter<FloatConverter<Type>> {
  using c_type = typename Type::c_type;

 public:
  explicit FloatConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<NumericBuilder<Type>>();
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return builder_->AppendNull();
    }
    if (json_obj.IsNumber()) {
      c_type v = static_cast<c_type>(json_obj.GetDouble());
      return builder_->Append(v);
    } else {
      return JSONTypeError("number", json_obj.GetType());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  std::shared_ptr<NumericBuilder<Type>> builder_;
};

// ------------------------------------------------------------------------
// Converter for decimal arrays

class DecimalConverter : public ConcreteConverter<DecimalConverter> {
 public:
  explicit DecimalConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    decimal_type_ = checked_cast<Decimal128Type*>(type.get());
    builder_ = std::make_shared<DecimalBuilder>(type);
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return builder_->AppendNull();
    }
    if (json_obj.IsString()) {
      int32_t precision, scale;
      Decimal128 d;
      auto view = util::string_view(json_obj.GetString(), json_obj.GetStringLength());
      RETURN_NOT_OK(Decimal128::FromString(view, &d, &precision, &scale));
      if (scale != decimal_type_->scale()) {
        std::stringstream ss;
        ss << "Invalid scale for decimal: expected " << decimal_type_->scale() << ", got "
           << scale;
        return Status::Invalid(ss.str());
      }
      return builder_->Append(d);
    }
    return JSONTypeError("decimal string", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  std::shared_ptr<DecimalBuilder> builder_;
  Decimal128Type* decimal_type_;
};

// ------------------------------------------------------------------------
// Converter for string arrays

class StringConverter : public ConcreteConverter<StringConverter> {
 public:
  explicit StringConverter(const std::shared_ptr<DataType>& type) {
    this->type_ = type;
    builder_ = std::make_shared<BinaryBuilder>(type, default_memory_pool());
  }

  Status AppendNull() override { return builder_->AppendNull(); }

  Status AppendValue(const rj::Value& json_obj) override {
    if (json_obj.IsNull()) {
      return builder_->AppendNull();
    }
    if (json_obj.IsString()) {
      auto view = util::string_view(json_obj.GetString(), json_obj.GetStringLength());
      return builder_->Append(view);
    } else {
      return JSONTypeError("string", json_obj.GetType());
    }
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  std::shared_ptr<BinaryBuilder> builder_;
};

// ------------------------------------------------------------------------
// Converter for list arrays

class ListConverter : public ConcreteConverter<ListConverter> {
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
      return builder_->AppendNull();
    }
    RETURN_NOT_OK(builder_->Append());
    // Extend the child converter with this JSON array
    return child_converter_->AppendValues(json_obj);
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
  std::shared_ptr<ListBuilder> builder_;
  std::shared_ptr<Converter> child_converter_;
};

// ------------------------------------------------------------------------
// Converter for struct arrays

class StructConverter : public ConcreteConverter<StructConverter> {
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
        std::stringstream ss;
        ss << "Expected array of size " << expected_size << ", got array of size "
           << size;
        return Status::Invalid(ss.str());
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
        std::stringstream ss;
        ss << "Unexpected members in JSON object for type " << type_->ToString();
        return Status::Invalid(ss.str());
      }
      return builder_->Append();
    }
    return JSONTypeError("array or object", json_obj.GetType());
  }

  std::shared_ptr<ArrayBuilder> builder() override { return builder_; }

 protected:
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
    SIMPLE_CONVERTER_CASE(Type::INT64, IntegerConverter<Int64Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT8, IntegerConverter<UInt8Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT16, IntegerConverter<UInt16Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT32, IntegerConverter<UInt32Type>)
    SIMPLE_CONVERTER_CASE(Type::UINT64, IntegerConverter<UInt64Type>)
    SIMPLE_CONVERTER_CASE(Type::NA, NullConverter)
    SIMPLE_CONVERTER_CASE(Type::BOOL, BooleanConverter)
    SIMPLE_CONVERTER_CASE(Type::FLOAT, FloatConverter<FloatType>)
    SIMPLE_CONVERTER_CASE(Type::DOUBLE, FloatConverter<DoubleType>)
    SIMPLE_CONVERTER_CASE(Type::LIST, ListConverter)
    SIMPLE_CONVERTER_CASE(Type::STRUCT, StructConverter)
    SIMPLE_CONVERTER_CASE(Type::STRING, StringConverter)
    SIMPLE_CONVERTER_CASE(Type::DECIMAL, DecimalConverter)
    default: {
      std::stringstream ss;
      ss << "JSON conversion to " << type->ToString() << " not implemented";
      return Status::NotImplemented(ss.str());
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
    std::stringstream ss;
    ss << "JSON parse error at offset " << json_doc.GetErrorOffset() << ": "
       << GetParseError_En(json_doc.GetParseError());
    return Status::Invalid(ss.str());
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
