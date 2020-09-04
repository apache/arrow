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

using internal::checked_cast;
using internal::checked_pointer_cast;

template <typename Input, typename Options>
class ArrayConverter {
 public:
  using InputType = Input;
  using OptionsType = Options;

  ArrayConverter(const std::shared_ptr<DataType>& type,
                 std::shared_ptr<ArrayBuilder> builder, Options options)
      : sp_type_(type), sp_builder_(builder), options_(options) {}

  virtual ~ArrayConverter() = default;
  const std::shared_ptr<ArrayBuilder>& builder() const { return sp_builder_; }
  const std::shared_ptr<DataType>& type() const { return sp_type_; }
  Options options() const { return options_; }

  virtual Status Init() { return Status::OK(); }
  virtual Status Reserve(int64_t additional_capacity) = 0;
  virtual Status Append(InputType value) = 0;
  virtual Status AppendNull() = 0;
  virtual Status Extend(Input seq, int64_t size) = 0;
  virtual Result<std::shared_ptr<Array>> Finish() = 0;

 protected:
  const std::shared_ptr<DataType> sp_type_;
  std::shared_ptr<ArrayBuilder> sp_builder_;
  Options options_;
};

template <typename T, typename ArrayConverter,
          typename BuilderType = typename TypeTraits<T>::BuilderType>
class TypedArrayConverter : public ArrayConverter {
 public:
  using ArrayConverterType = ArrayConverter;

  TypedArrayConverter(const std::shared_ptr<DataType>& type,
                      std::shared_ptr<ArrayBuilder> builder,
                      typename ArrayConverter::OptionsType options)
      : ArrayConverter(type, builder, options),
        type_(checked_cast<const T&>(*type)),
        builder_(checked_cast<BuilderType*>(builder.get())) {}

  Status Reserve(int64_t additional_capacity) override {
    return this->builder_->Reserve(additional_capacity);
  }

  Status AppendNull() override { return this->builder_->AppendNull(); }

  Result<std::shared_ptr<Array>> Finish() override { return builder_->Finish(); };

 protected:
  const T& type_;
  BuilderType* builder_;
};

template <typename T, typename ArrayConverter>
class PrimitiveArrayConverter : public TypedArrayConverter<T, ArrayConverter> {
 public:
  using TypedArrayConverter<T, ArrayConverter>::TypedArrayConverter;
};

template <typename T, typename ArrayConverter>
class DictionaryArrayConverter
    : public TypedArrayConverter<DictionaryType, ArrayConverter, DictionaryBuilder<T>> {
 public:
  DictionaryArrayConverter(const std::shared_ptr<DataType>& type,
                           std::shared_ptr<ArrayBuilder> builder,
                           typename ArrayConverter::OptionsType options)
      : TypedArrayConverter<DictionaryType, ArrayConverter, DictionaryBuilder<T>>(
            type, builder, options),
        value_type_(checked_cast<const T&>(
            *checked_cast<const DictionaryType&>(*type).value_type())) {}

 protected:
  const T& value_type_;
};

template <typename T, typename ArrayConverter>
class ListArrayConverter : public TypedArrayConverter<T, ArrayConverter> {
 public:
  ListArrayConverter(const std::shared_ptr<DataType>& type,
                     std::shared_ptr<ArrayBuilder> builder,
                     std::shared_ptr<ArrayConverter> value_converter,
                     typename ArrayConverter::OptionsType options)
      : TypedArrayConverter<T, ArrayConverter>(type, builder, options),
        value_converter_(std::move(value_converter)) {}

 protected:
  std::shared_ptr<ArrayConverter> value_converter_;
};

template <typename T, typename ArrayConverter>
class StructArrayConverter : public TypedArrayConverter<T, ArrayConverter> {
 public:
  StructArrayConverter(const std::shared_ptr<DataType>& type,
                       std::shared_ptr<ArrayBuilder> builder,
                       std::vector<std::shared_ptr<ArrayConverter>> child_converters,
                       typename ArrayConverter::OptionsType options)
      : TypedArrayConverter<T, ArrayConverter>(type, builder, options),
        child_converters_(std::move(child_converters)) {}

 protected:
  std::vector<std::shared_ptr<ArrayConverter>> child_converters_;
};

#define DICTIONARY_CASE(TYPE_ENUM, TYPE_CLASS)                          \
  case Type::TYPE_ENUM:                                                 \
    out->reset(new DAC<TYPE_CLASS>(type, std::move(builder), options)); \
    break;

template <typename Options, typename AC, template <typename...> class PAC,
          template <typename...> class DAC, template <typename...> class LAC,
          template <typename...> class SAC>
struct ArrayConverterBuilder {
  using Self = ArrayConverterBuilder<Options, AC, PAC, DAC, LAC, SAC>;

  Status Visit(const NullType& t) {
    // TODO: merge with the primitive c_type variant below, requires a NullType ctor which
    // accepts a type instance
    using BuilderType = typename TypeTraits<NullType>::BuilderType;
    using ConverterType = PAC<NullType>;
    static_assert(std::is_same<typename ConverterType::ArrayConverterType, AC>::value,
                  "");

    auto builder = std::make_shared<BuilderType>(pool);
    out->reset(new ConverterType(type, std::move(builder), options));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<!is_nested_type<T>::value && !is_interval_type<T>::value &&
                  !is_dictionary_type<T>::value && !is_extension_type<T>::value,
              Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ConverterType = PAC<T>;
    static_assert(std::is_same<typename ConverterType::ArrayConverterType, AC>::value,
                  "");

    auto builder = std::make_shared<BuilderType>(type, pool);
    out->reset(new ConverterType(type, std::move(builder), options));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_list_like_type<T>::value && !std::is_same<T, MapType>::value, Status>
  Visit(const T& t) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using ConverterType = LAC<T>;
    static_assert(std::is_same<typename ConverterType::ArrayConverterType, AC>::value,
                  "");

    ARROW_ASSIGN_OR_RAISE(auto child_converter,
                          (Self::Make(t.value_type(), pool, options)));
    auto builder = std::make_shared<BuilderType>(pool, child_converter->builder(), type);
    out->reset(
        new ConverterType(type, std::move(builder), std::move(child_converter), options));
    return Status::OK();
  }

  Status Visit(const MapType& t) {
    using ConverterType = LAC<MapType>;
    static_assert(std::is_same<typename ConverterType::ArrayConverterType, AC>::value,
                  "");

    // TODO(kszucs): seems like builders not respect field nullability
    std::vector<std::shared_ptr<Field>> struct_fields{t.key_field(), t.item_field()};
    auto struct_type = std::make_shared<StructType>(struct_fields);
    ARROW_ASSIGN_OR_RAISE(auto struct_converter, Self::Make(struct_type, pool, options));

    auto struct_builder = struct_converter->builder();
    auto key_builder = struct_builder->child_builder(0);
    auto item_builder = struct_builder->child_builder(1);
    auto builder = std::make_shared<MapBuilder>(pool, key_builder, item_builder, type);

    out->reset(new ConverterType(type, std::move(builder), std::move(struct_converter),
                                 options));
    return Status::OK();
  }

  Status Visit(const DictionaryType& t) {
    std::unique_ptr<ArrayBuilder> builder;
    ARROW_RETURN_NOT_OK(MakeDictionaryBuilder(pool, type, nullptr, &builder));

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
    return Status::OK();
  }

  Status Visit(const StructType& t) {
    using ConverterType = SAC<StructType>;
    static_assert(std::is_same<typename ConverterType::ArrayConverterType, AC>::value,
                  "");

    std::shared_ptr<AC> child_converter;
    std::vector<std::shared_ptr<AC>> child_converters;
    std::vector<std::shared_ptr<ArrayBuilder>> child_builders;

    for (const auto& field : t.fields()) {
      ARROW_ASSIGN_OR_RAISE(child_converter, Self::Make(field->type(), pool, options));

      // TODO: use move
      child_converters.emplace_back(child_converter);
      child_builders.emplace_back(child_converter->builder());
    }

    auto builder = std::make_shared<StructBuilder>(type, pool, child_builders);
    out->reset(new ConverterType(type, std::move(builder), std::move(child_converters),
                                 options));
    return Status::OK();
  }

  Status Visit(const DataType& t) { return Status::NotImplemented(t.name()); }

  static Result<std::shared_ptr<AC>> Make(std::shared_ptr<DataType> type,
                                          MemoryPool* pool, Options options) {
    std::shared_ptr<AC> out;
    Self visitor = {type, pool, options, &out};
    ARROW_RETURN_NOT_OK(VisitTypeInline(*type, &visitor));
    ARROW_RETURN_NOT_OK(out->Init());
    return out;
  }

  const std::shared_ptr<DataType>& type;
  MemoryPool* pool;
  Options options;
  std::shared_ptr<AC>* out;
};

}  // namespace arrow
