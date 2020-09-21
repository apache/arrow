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

template <typename BaseConverter, template <typename...> class ConverterTrait>
static Result<std::shared_ptr<BaseConverter>> MakeConverter(
    std::shared_ptr<DataType> type, typename BaseConverter::OptionsType options,
    MemoryPool* pool);

template <typename Input, typename Options>
class Converter {
 public:
  using Self = Converter<Input, Options>;
  using InputType = Input;
  using OptionsType = Options;

  virtual ~Converter() = default;

  Status Construct(std::shared_ptr<DataType> type, OptionsType options,
                   MemoryPool* pool) {
    type_ = std::move(type);
    options_ = std::move(options);
    return Init(pool);
  }

  virtual Status Append(InputType value) {
    return Status::NotImplemented("Converter not implemented for type ",
                                  type()->ToString());
  }

  const std::shared_ptr<ArrayBuilder>& builder() const { return builder_; }

  const std::shared_ptr<DataType>& type() const { return type_; }

  OptionsType options() const { return options_; }

  const std::vector<std::shared_ptr<Self>>& children() const { return children_; }

  Status Reserve(int64_t additional_capacity) {
    return builder_->Reserve(additional_capacity);
  }

  Status AppendNull() { return builder_->AppendNull(); }

  virtual Result<std::shared_ptr<Array>> ToArray() { return builder_->Finish(); }

  virtual Result<std::shared_ptr<Array>> ToArray(int64_t length) {
    ARROW_ASSIGN_OR_RAISE(auto arr, this->ToArray());
    return arr->Slice(0, length);
  }

 protected:
  virtual Status Init(MemoryPool* pool) { return Status::OK(); }

  std::shared_ptr<DataType> type_;
  std::shared_ptr<ArrayBuilder> builder_;
  std::vector<std::shared_ptr<Self>> children_;
  OptionsType options_;
};

template <typename T, typename BaseConverter>
class PrimitiveConverter : public BaseConverter {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;

 protected:
  Status Init(MemoryPool* pool) override {
    this->builder_ = std::make_shared<BuilderType>(this->type_, pool);
    this->primitive_type_ = checked_cast<const T*>(this->type_.get());
    this->primitive_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

  const T* primitive_type_;
  BuilderType* primitive_builder_;
};

template <typename T, typename BaseConverter, template <typename...> class ConverterTrait>
class ListConverter : public BaseConverter {
 public:
  using BuilderType = typename TypeTraits<T>::BuilderType;
  using ConverterType = typename ConverterTrait<T>::type;

 protected:
  Status Init(MemoryPool* pool) override {
    list_type_ = checked_cast<const T*>(this->type_.get());
    ARROW_ASSIGN_OR_RAISE(value_converter_,
                          (MakeConverter<BaseConverter, ConverterTrait>(
                              list_type_->value_type(), this->options_, pool)));
    this->builder_ =
        std::make_shared<BuilderType>(pool, value_converter_->builder(), this->type_);
    this->children_ = {value_converter_};
    list_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

  const T* list_type_;
  BuilderType* list_builder_;
  std::shared_ptr<BaseConverter> value_converter_;
};

template <typename BaseConverter, template <typename...> class ConverterTrait>
class StructConverter : public BaseConverter {
 public:
  using ConverterType = typename ConverterTrait<StructType>::type;

 protected:
  Status Init(MemoryPool* pool) override {
    std::shared_ptr<BaseConverter> child_converter;
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;

    struct_type_ = checked_cast<const StructType*>(this->type_.get());
    for (const auto& field : struct_type_->fields()) {
      ARROW_ASSIGN_OR_RAISE(child_converter,
                            (MakeConverter<BaseConverter, ConverterTrait>(
                                field->type(), this->options_, pool)));
      child_builders.push_back(child_converter->builder());
      this->children_.push_back(std::move(child_converter));
    }

    this->builder_ =
        std::make_shared<StructBuilder>(this->type_, pool, std::move(child_builders));
    struct_builder_ = checked_cast<StructBuilder*>(this->builder_.get());

    return Status::OK();
  }

  const StructType* struct_type_;
  StructBuilder* struct_builder_;
};

template <typename U, typename BaseConverter>
class DictionaryConverter : public BaseConverter {
 public:
  using BuilderType = DictionaryBuilder<U>;

 protected:
  Status Init(MemoryPool* pool) override {
    std::unique_ptr<ArrayBuilder> builder;
    ARROW_RETURN_NOT_OK(MakeDictionaryBuilder(pool, this->type_, NULLPTR, &builder));
    this->builder_ = std::move(builder);
    dict_type_ = checked_cast<const DictionaryType*>(this->type_.get());
    value_type_ = checked_cast<const U*>(dict_type_->value_type().get());
    value_builder_ = checked_cast<BuilderType*>(this->builder_.get());
    return Status::OK();
  }

  const DictionaryType* dict_type_;
  const U* value_type_;
  BuilderType* value_builder_;
};

template <typename BaseConverter, template <typename...> class ConverterTrait>
struct MakeConverterImpl {
  template <typename T, typename ConverterType = typename ConverterTrait<T>::type>
  Status Visit(const T&) {
    out.reset(new ConverterType());
    return out->Construct(std::move(type), std::move(options), pool);
  }

  Status Visit(const DictionaryType& t) {
    switch (t.value_type()->id()) {
#define DICTIONARY_CASE(TYPE)                                            \
  case TYPE::type_id:                                                    \
    out = std::make_shared<                                              \
        typename ConverterTrait<DictionaryType>::template type<TYPE>>(); \
    break;
      DICTIONARY_CASE(BooleanType);
      DICTIONARY_CASE(Int8Type);
      DICTIONARY_CASE(Int16Type);
      DICTIONARY_CASE(Int32Type);
      DICTIONARY_CASE(Int64Type);
      DICTIONARY_CASE(UInt8Type);
      DICTIONARY_CASE(UInt16Type);
      DICTIONARY_CASE(UInt32Type);
      DICTIONARY_CASE(UInt64Type);
      DICTIONARY_CASE(FloatType);
      DICTIONARY_CASE(DoubleType);
      DICTIONARY_CASE(BinaryType);
      DICTIONARY_CASE(StringType);
      DICTIONARY_CASE(FixedSizeBinaryType);
      default:
        return Status::NotImplemented("DictionaryArray converter for type ", t.ToString(),
                                      " not implemented");
    }
    return out->Construct(std::move(type), std::move(options), pool);
  }

  Status Visit(const DataType& t) { return Status::NotImplemented(t.name()); }

  std::shared_ptr<DataType> type;
  typename BaseConverter::OptionsType options;
  MemoryPool* pool;
  std::shared_ptr<BaseConverter> out;
};

template <typename BaseConverter, template <typename...> class ConverterTrait>
static Result<std::shared_ptr<BaseConverter>> MakeConverter(
    std::shared_ptr<DataType> type, typename BaseConverter::OptionsType options,
    MemoryPool* pool) {
  MakeConverterImpl<BaseConverter, ConverterTrait> visitor{
      std::move(type), std::move(options), pool, nullptr};
  ARROW_RETURN_NOT_OK(VisitTypeInline(*visitor.type, &visitor));
  return std::move(visitor.out);
}

template <typename Converter>
class Chunker {
 public:
  using InputType = typename Converter::InputType;

  explicit Chunker(std::shared_ptr<Converter> converter)
      : converter_(std::move(converter)) {}

  Status Reserve(int64_t additional_capacity) {
    return converter_->Reserve(additional_capacity);
  }

  Status AppendNull() {
    auto status = converter_->AppendNull();
    if (status.ok()) {
      length_ = converter_->builder()->length();
    } else if (status.IsCapacityError()) {
      ARROW_RETURN_NOT_OK(FinishChunk());
      return converter_->AppendNull();
    }
    return status;
  }

  Status Append(InputType value) {
    auto status = converter_->Append(value);
    if (status.ok()) {
      length_ = converter_->builder()->length();
    } else if (status.IsCapacityError()) {
      ARROW_RETURN_NOT_OK(FinishChunk());
      return Append(value);
    }
    return status;
  }

  Status FinishChunk() {
    ARROW_ASSIGN_OR_RAISE(auto chunk, converter_->ToArray(length_));
    converter_->builder()->Reset();
    length_ = 0;
    chunks_.push_back(chunk);
    return Status::OK();
  }

  Result<std::shared_ptr<ChunkedArray>> ToChunkedArray() {
    ARROW_RETURN_NOT_OK(FinishChunk());
    return std::make_shared<ChunkedArray>(chunks_);
  }

 protected:
  int64_t length_ = 0;
  std::shared_ptr<Converter> converter_;
  std::vector<std::shared_ptr<Array>> chunks_;
};

template <typename T>
static Result<std::shared_ptr<Chunker<T>>> MakeChunker(std::shared_ptr<T> converter) {
  return std::make_shared<Chunker<T>>(std::move(converter));
}

}  // namespace internal
}  // namespace arrow
