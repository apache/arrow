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

#include "arrow/builder.h"

#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;

// ----------------------------------------------------------------------
// Helper functions

struct DictionaryBuilderCase {
  template <typename ValueType, typename Enable = typename ValueType::c_type>
  Status Visit(const ValueType&) {
    return CreateFor<ValueType>();
  }

  Status Visit(const NullType&) { return CreateFor<NullType>(); }
  Status Visit(const BinaryType&) { return CreateFor<BinaryType>(); }
  Status Visit(const StringType&) { return CreateFor<StringType>(); }
  Status Visit(const LargeBinaryType&) { return CreateFor<LargeBinaryType>(); }
  Status Visit(const LargeStringType&) { return CreateFor<LargeStringType>(); }
  Status Visit(const FixedSizeBinaryType&) { return CreateFor<FixedSizeBinaryType>(); }
  Status Visit(const Decimal128Type&) { return CreateFor<Decimal128Type>(); }
  Status Visit(const Decimal256Type&) { return CreateFor<Decimal256Type>(); }

  Status Visit(const DataType& value_type) { return NotImplemented(value_type); }
  Status Visit(const HalfFloatType& value_type) { return NotImplemented(value_type); }
  Status NotImplemented(const DataType& value_type) {
    return Status::NotImplemented(
        "MakeBuilder: cannot construct builder for dictionaries with value type ",
        value_type);
  }

  template <typename ValueType>
  Status CreateFor() {
    using AdaptiveBuilderType = DictionaryBuilder<ValueType>;
    if (dictionary != nullptr) {
      out->reset(new AdaptiveBuilderType(dictionary, pool));
    } else if (exact_index_type) {
      switch (index_type->id()) {
        case Type::UINT8:
          out->reset(new internal::DictionaryBuilderBase<UInt8Builder, ValueType>(
              value_type, pool));
          break;
        case Type::INT8:
          out->reset(new internal::DictionaryBuilderBase<Int8Builder, ValueType>(
              value_type, pool));
          break;
        case Type::UINT16:
          out->reset(new internal::DictionaryBuilderBase<UInt16Builder, ValueType>(
              value_type, pool));
          break;
        case Type::INT16:
          out->reset(new internal::DictionaryBuilderBase<Int16Builder, ValueType>(
              value_type, pool));
          break;
        case Type::UINT32:
          out->reset(new internal::DictionaryBuilderBase<UInt32Builder, ValueType>(
              value_type, pool));
          break;
        case Type::INT32:
          out->reset(new internal::DictionaryBuilderBase<Int32Builder, ValueType>(
              value_type, pool));
          break;
        case Type::UINT64:
          out->reset(new internal::DictionaryBuilderBase<UInt64Builder, ValueType>(
              value_type, pool));
          break;
        case Type::INT64:
          out->reset(new internal::DictionaryBuilderBase<Int64Builder, ValueType>(
              value_type, pool));
          break;
        default:
          return Status::TypeError("MakeBuilder: invalid index type ", *index_type);
      }
    } else {
      auto start_int_size = internal::GetByteWidth(*index_type);
      out->reset(new AdaptiveBuilderType(start_int_size, value_type, pool));
    }
    return Status::OK();
  }

  Status Make() { return VisitTypeInline(*value_type, this); }

  MemoryPool* pool;
  const std::shared_ptr<DataType>& index_type;
  const std::shared_ptr<DataType>& value_type;
  const std::shared_ptr<Array>& dictionary;
  bool exact_index_type;
  std::unique_ptr<ArrayBuilder>* out;
};

struct MakeBuilderImpl {
  template <typename T>
  enable_if_not_nested<T, Status> Visit(const T&) {
    out.reset(new typename TypeTraits<T>::BuilderType(type, pool));
    return Status::OK();
  }

  Status Visit(const DictionaryType& dict_type) {
    DictionaryBuilderCase visitor = {pool,
                                     dict_type.index_type(),
                                     dict_type.value_type(),
                                     /*dictionary=*/nullptr,
                                     exact_index_type,
                                     &out};
    return visitor.Make();
  }

  Status Visit(const ListType& list_type) {
    std::shared_ptr<DataType> value_type = list_type.value_type();
    ARROW_ASSIGN_OR_RAISE(auto value_builder, ChildBuilder(value_type));
    out.reset(new ListBuilder(pool, std::move(value_builder), type));
    return Status::OK();
  }

  Status Visit(const LargeListType& list_type) {
    std::shared_ptr<DataType> value_type = list_type.value_type();
    ARROW_ASSIGN_OR_RAISE(auto value_builder, ChildBuilder(value_type));
    out.reset(new LargeListBuilder(pool, std::move(value_builder), type));
    return Status::OK();
  }

  Status Visit(const MapType& map_type) {
    ARROW_ASSIGN_OR_RAISE(auto key_builder, ChildBuilder(map_type.key_type()));
    ARROW_ASSIGN_OR_RAISE(auto item_builder, ChildBuilder(map_type.item_type()));
    out.reset(
        new MapBuilder(pool, std::move(key_builder), std::move(item_builder), type));
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& list_type) {
    auto value_type = list_type.value_type();
    ARROW_ASSIGN_OR_RAISE(auto value_builder, ChildBuilder(value_type));
    out.reset(new FixedSizeListBuilder(pool, std::move(value_builder), type));
    return Status::OK();
  }

  Status Visit(const StructType& struct_type) {
    ARROW_ASSIGN_OR_RAISE(auto field_builders, FieldBuilders(*type, pool));
    out.reset(new StructBuilder(type, pool, std::move(field_builders)));
    return Status::OK();
  }

  Status Visit(const SparseUnionType&) {
    ARROW_ASSIGN_OR_RAISE(auto field_builders, FieldBuilders(*type, pool));
    out.reset(new SparseUnionBuilder(pool, std::move(field_builders), type));
    return Status::OK();
  }

  Status Visit(const DenseUnionType&) {
    ARROW_ASSIGN_OR_RAISE(auto field_builders, FieldBuilders(*type, pool));
    out.reset(new DenseUnionBuilder(pool, std::move(field_builders), type));
    return Status::OK();
  }

  Status Visit(const ExtensionType&) { return NotImplemented(); }
  Status Visit(const DataType&) { return NotImplemented(); }

  Status NotImplemented() {
    return Status::NotImplemented("MakeBuilder: cannot construct builder for type ",
                                  type->ToString());
  }

  Result<std::unique_ptr<ArrayBuilder>> ChildBuilder(
      const std::shared_ptr<DataType>& type) {
    MakeBuilderImpl impl{pool, type, exact_index_type, /*out=*/nullptr};
    RETURN_NOT_OK(VisitTypeInline(*type, &impl));
    return std::move(impl.out);
  }

  Result<std::vector<std::shared_ptr<ArrayBuilder>>> FieldBuilders(const DataType& type,
                                                                   MemoryPool* pool) {
    std::vector<std::shared_ptr<ArrayBuilder>> field_builders;
    for (const auto& field : type.fields()) {
      std::unique_ptr<ArrayBuilder> builder;
      MakeBuilderImpl impl{pool, field->type(), exact_index_type, /*out=*/nullptr};
      RETURN_NOT_OK(VisitTypeInline(*field->type(), &impl));
      field_builders.emplace_back(std::move(impl.out));
    }
    return field_builders;
  }

  MemoryPool* pool;
  const std::shared_ptr<DataType>& type;
  bool exact_index_type;
  std::unique_ptr<ArrayBuilder> out;
};

Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<ArrayBuilder>* out) {
  MakeBuilderImpl impl{pool, type, /*exact_index_type=*/false, /*out=*/nullptr};
  RETURN_NOT_OK(VisitTypeInline(*type, &impl));
  *out = std::move(impl.out);
  return Status::OK();
}

Status MakeBuilderExactIndex(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                             std::unique_ptr<ArrayBuilder>* out) {
  MakeBuilderImpl impl{pool, type, /*exact_index_type=*/true, /*out=*/nullptr};
  RETURN_NOT_OK(VisitTypeInline(*type, &impl));
  *out = std::move(impl.out);
  return Status::OK();
}

Status MakeDictionaryBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                             const std::shared_ptr<Array>& dictionary,
                             std::unique_ptr<ArrayBuilder>* out) {
  const auto& dict_type = static_cast<const DictionaryType&>(*type);
  DictionaryBuilderCase visitor = {
      pool,       dict_type.index_type(),     dict_type.value_type(),
      dictionary, /*exact_index_type=*/false, out};
  return visitor.Make();
}

}  // namespace arrow
