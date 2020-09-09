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

#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

#include "arrow/visitor_inline.h"

namespace arrow {
namespace internal {

using internal::checked_cast;
using internal::checked_pointer_cast;

template <typename T, typename BaseConverter>
class PrimitiveConverter : public BaseConverter {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

  Status Init() override {
    primitive_type_ = checked_cast<const T*>(this->type_.get());
    primitive_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

 protected:
  const T* primitive_type_;
  BuilderType* primitive_builder_;
};

template <typename T, typename BaseConverter>
class ListConverter : public BaseConverter {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

  Status Init() override {
    list_type_ = checked_cast<const T*>(this->type_.get());
    list_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    value_converter_ = this->children_[0];
    return Status::OK();
  }

 protected:
  const T* list_type_;
  BuilderType* list_builder_;
  std::shared_ptr<BaseConverter> value_converter_;
};

template <typename BaseConverter>
class StructConverter : public BaseConverter {
 public:
  Status Init() override {
    struct_type_ = checked_cast<const StructType*>(this->type_.get());
    struct_builder_ = checked_cast<StructBuilder*>(this->builder_.get());
    return Status::OK();
  }

 protected:
  const StructType* struct_type_;
  StructBuilder* struct_builder_;
};

template <typename U, typename BaseConverter>
class DictionaryConverter : public BaseConverter {
 public:
  using BuilderType = DictionaryBuilder<U>;

  Status Init() override {
    dict_type_ = checked_cast<const DictionaryType*>(this->type_.get());
    value_type_ = checked_cast<const U*>(dict_type_->value_type().get());
    value_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

 protected:
  const DictionaryType* dict_type_;
  const U* value_type_;
  BuilderType* value_builder_;
};

template <typename Converter>
struct MakeConverterImpl;

template <typename Input, typename Options, typename Self>
class Converter {
 public:
  using InputType = Input;
  using OptionsType = Options;

  template <typename T>
  using PrimitiveConverter = PrimitiveConverter<T, Self>;
  template <typename T>
  using ListConverter = ListConverter<T, Self>;
  template <typename T>
  using DictionaryConveter = DictionaryConverter<T, Self>;
  using StructConverter = StructConverter<Self>;

  static Result<std::shared_ptr<Self>> Make(std::shared_ptr<DataType> type,
                                            MemoryPool* pool, OptionsType options) {
    std::shared_ptr<Self> out;
    MakeConverterImpl<Self> visitor = {type, pool, options, &out};
    ARROW_RETURN_NOT_OK(VisitTypeInline(*type, &visitor));
    ARROW_RETURN_NOT_OK(out->Init());
    return out;
  }

  virtual ~Converter() = default;

  virtual Status Init() { return Status::OK(); };

  virtual Status Append(InputType value) {
    return Status::NotImplemented("Converter not implemented for type ",
                                  type()->ToString());
  }

  const std::shared_ptr<ArrayBuilder>& builder() const { return builder_; }

  const std::shared_ptr<DataType>& type() const { return type_; }

  OptionsType options() const { return options_; }

  virtual Status Reserve(int64_t additional_capacity) {
    return builder_->Reserve(additional_capacity);
  }

  virtual Status AppendNull() { return builder_->AppendNull(); }

  virtual Result<std::shared_ptr<Array>> ToArray() { return builder_->Finish(); };

  virtual Result<std::shared_ptr<Array>> ToArray(int64_t length) {
    // RETURN_NOT_OK(builder_->Resize(length));
    ARROW_ASSIGN_OR_RAISE(auto arr, this->ToArray());
    return arr->Slice(0, length);
  }

 protected:
  friend struct MakeConverterImpl<Self>;

  std::shared_ptr<DataType> type_;
  std::shared_ptr<ArrayBuilder> builder_;
  std::vector<std::shared_ptr<Self>> children_;
  OptionsType options_;
};

#define DICTIONARY_CASE(TYPE_ENUM, TYPE_CLASS)                                   \
  case Type::TYPE_ENUM:                                                          \
    return Finish<typename Converter::template DictionaryConverter<TYPE_CLASS>>( \
        std::move(builder), {});                                                 \
    break;

template <typename Converter>
struct MakeConverterImpl {
  Status Visit(const NullType& t) {
    using BuilderType = typename TypeTraits<NullType>::BuilderType;
    using ConverterType = typename Converter::template PrimitiveConverter<NullType>;

    auto builder = std::make_shared<BuilderType>(pool);
    return Finish<ConverterType>(std::move(builder), {});
  }

  template <typename T>
  enable_if_t<!is_nested_type<T>::value && !is_interval_type<T>::value &&
                  !is_dictionary_type<T>::value && !is_extension_type<T>::value,
              Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ConverterType = typename Converter::template PrimitiveConverter<T>;

    auto builder = std::make_shared<BuilderType>(type, pool);
    return Finish<ConverterType>(std::move(builder), {});
  }

  template <typename T>
  enable_if_t<is_list_like_type<T>::value && !std::is_same<T, MapType>::value, Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ConverterType = typename Converter::template ListConverter<T>;

    ARROW_ASSIGN_OR_RAISE(auto child_converter,
                          Converter::Make(t.value_type(), pool, options));
    auto builder = std::make_shared<BuilderType>(pool, child_converter->builder(), type);
    return Finish<ConverterType>(std::move(builder), {std::move(child_converter)});
  }

  Status Visit(const MapType& t) {
    using ConverterType = typename Converter::template ListConverter<MapType>;

    // TODO(kszucs): seems like builders not respect field nullability
    std::vector<std::shared_ptr<Field>> struct_fields{t.key_field(), t.item_field()};
    auto struct_type = std::make_shared<StructType>(struct_fields);
    ARROW_ASSIGN_OR_RAISE(auto struct_converter,
                          Converter::Make(struct_type, pool, options));

    auto struct_builder = struct_converter->builder();
    auto key_builder = struct_builder->child_builder(0);
    auto item_builder = struct_builder->child_builder(1);
    auto builder = std::make_shared<MapBuilder>(pool, key_builder, item_builder, type);

    return Finish<ConverterType>(std::move(builder), {std::move(struct_converter)});
  }

  Status Visit(const DictionaryType& t) {
    std::unique_ptr<ArrayBuilder> builder;
    ARROW_RETURN_NOT_OK(MakeDictionaryBuilder(pool, type, NULLPTR, &builder));

    switch (t.value_type()->id()) {
      DICTIONARY_CASE(BOOL, BooleanType);
      DICTIONARY_CASE(INT8, Int8Type);
      DICTIONARY_CASE(INT16, Int16Type);
      DICTIONARY_CASE(INT32, Int32Type);
      DICTIONARY_CASE(INT64, Int64Type);
      DICTIONARY_CASE(UINT8, UInt8Type);
      DICTIONARY_CASE(UINT16, UInt16Type);
      DICTIONARY_CASE(UINT32, UInt32Type);
      DICTIONARY_CASE(UINT64, UInt64Type);
      DICTIONARY_CASE(HALF_FLOAT, HalfFloatType);
      DICTIONARY_CASE(FLOAT, FloatType);
      DICTIONARY_CASE(DOUBLE, DoubleType);
      DICTIONARY_CASE(DATE32, Date32Type);
      DICTIONARY_CASE(DATE64, Date64Type);
      DICTIONARY_CASE(BINARY, BinaryType);
      DICTIONARY_CASE(STRING, StringType);
      DICTIONARY_CASE(FIXED_SIZE_BINARY, FixedSizeBinaryType);
      default:
        return Status::NotImplemented("DictionaryArray converter for type ", t.ToString(),
                                      " not implemented");
    }
  }

  Status Visit(const StructType& t) {
    using ConverterType = typename Converter::StructConverter;

    std::shared_ptr<Converter> child_converter;
    std::vector<std::shared_ptr<Converter>> child_converters;
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;

    for (const auto& field : t.fields()) {
      ARROW_ASSIGN_OR_RAISE(child_converter,
                            Converter::Make(field->type(), pool, options));

      // TODO: use move
      child_converters.push_back(child_converter);
      child_builders.push_back(child_converter->builder());
    }

    auto builder = std::make_shared<StructBuilder>(type, pool, child_builders);
    return Finish<ConverterType>(std::move(builder), std::move(child_converters));
  }

  Status Visit(const DataType& t) { return Status::NotImplemented(t.name()); }

  template <typename ConverterType>
  Status Finish(std::shared_ptr<ArrayBuilder> builder,
                std::vector<std::shared_ptr<Converter>> children) {
    auto converter = new ConverterType();
    converter->type_ = std::move(type);
    converter->builder_ = std::move(builder);
    converter->options_ = options;
    converter->children_ = std::move(children);
    out->reset(converter);
    return Status::OK();
  }

  const std::shared_ptr<DataType> type;
  MemoryPool* pool;
  typename Converter::OptionsType options;
  std::shared_ptr<Converter>* out;
};

template <typename BaseConverter>
class Chunker : public BaseConverter {
 public:
  using Self = Chunker<BaseConverter>;
  using InputType = typename BaseConverter::InputType;

  static Result<std::shared_ptr<Self>> Make(std::shared_ptr<BaseConverter> converter) {
    auto result = std::make_shared<Self>();
    result->type_ = converter->type();
    result->builder_ = converter->builder();
    // result->options_ = converter->options_;
    // result->children_ = converter->children_;
    result->converter_ = std::move(converter);
    return result;
  }

  Status AppendNull() override {
    auto status = converter_->AppendNull();
    if (status.ok()) {
      length_ = this->builder_->length();
    } else if (status.IsCapacityError()) {
      RETURN_NOT_OK(FinishChunk());
      return converter_->AppendNull();
    }
    return status;
  }

  Status Append(InputType value) override {
    auto status = converter_->Append(value);
    if (status.ok()) {
      length_ = this->builder_->length();
    } else if (status.IsCapacityError()) {
      RETURN_NOT_OK(FinishChunk());
      return converter_->Append(value);
    }
    return status;
  }

  Status FinishChunk() {
    ARROW_ASSIGN_OR_RAISE(auto chunk, this->ToArray(length_));
    this->builder_->Reset();
    length_ = 0;
    chunks_.push_back(chunk);
    return Status::OK();
  }

  Result<std::shared_ptr<ChunkedArray>> ToChunkedArray() {
    RETURN_NOT_OK(FinishChunk());
    return std::make_shared<ChunkedArray>(chunks_);
  }

 protected:
  int64_t length_ = 0;
  std::shared_ptr<BaseConverter> converter_;
  std::vector<std::shared_ptr<Array>> chunks_;
};

}  // namespace internal
}  // namespace arrow
