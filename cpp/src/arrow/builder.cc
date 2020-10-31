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
  Status Visit(const BinaryType&) { return Create<BinaryDictionaryBuilder>(); }
  Status Visit(const StringType&) { return Create<StringDictionaryBuilder>(); }
  Status Visit(const LargeBinaryType&) {
    return Create<DictionaryBuilder<LargeBinaryType>>();
  }
  Status Visit(const LargeStringType&) {
    return Create<DictionaryBuilder<LargeStringType>>();
  }
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
    return Create<DictionaryBuilder<ValueType>>();
  }

  template <typename BuilderType>
  Status Create() {
    BuilderType* builder;
    if (dictionary != nullptr) {
      builder = new BuilderType(dictionary, pool);
    } else {
      auto start_int_size = internal::GetByteWidth(*index_type);
      builder = new BuilderType(start_int_size, value_type, pool);
    }
    out->reset(builder);
    return Status::OK();
  }

  Status Make() { return VisitTypeInline(*value_type, this); }

  MemoryPool* pool;
  const std::shared_ptr<DataType>& index_type;
  const std::shared_ptr<DataType>& value_type;
  const std::shared_ptr<Array>& dictionary;
  std::unique_ptr<ArrayBuilder>* out;
};

#define BUILDER_CASE(TYPE_CLASS)                     \
  case TYPE_CLASS##Type::type_id:                    \
    out->reset(new TYPE_CLASS##Builder(type, pool)); \
    return Status::OK();

Result<std::vector<std::shared_ptr<ArrayBuilder>>> FieldBuilders(const DataType& type,
                                                                 MemoryPool* pool) {
  std::vector<std::shared_ptr<ArrayBuilder>> field_builders;

  for (const auto& field : type.fields()) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(pool, field->type(), &builder));
    field_builders.emplace_back(std::move(builder));
  }

  return field_builders;
}

Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<ArrayBuilder>* out) {
  switch (type->id()) {
    case Type::NA: {
      out->reset(new NullBuilder(pool));
      return Status::OK();
    }
      BUILDER_CASE(UInt8);
      BUILDER_CASE(Int8);
      BUILDER_CASE(UInt16);
      BUILDER_CASE(Int16);
      BUILDER_CASE(UInt32);
      BUILDER_CASE(Int32);
      BUILDER_CASE(UInt64);
      BUILDER_CASE(Int64);
      BUILDER_CASE(Date32);
      BUILDER_CASE(Date64);
      BUILDER_CASE(Duration);
      BUILDER_CASE(Time32);
      BUILDER_CASE(Time64);
      BUILDER_CASE(Timestamp);
      BUILDER_CASE(MonthInterval);
      BUILDER_CASE(DayTimeInterval);
      BUILDER_CASE(Boolean);
      BUILDER_CASE(HalfFloat);
      BUILDER_CASE(Float);
      BUILDER_CASE(Double);
      BUILDER_CASE(String);
      BUILDER_CASE(Binary);
      BUILDER_CASE(LargeString);
      BUILDER_CASE(LargeBinary);
      BUILDER_CASE(FixedSizeBinary);
      BUILDER_CASE(Decimal128);
      BUILDER_CASE(Decimal256);

    case Type::DICTIONARY: {
      const auto& dict_type = static_cast<const DictionaryType&>(*type);
      DictionaryBuilderCase visitor = {pool, dict_type.index_type(),
                                       dict_type.value_type(), nullptr, out};
      return visitor.Make();
    }

    case Type::LIST: {
      std::unique_ptr<ArrayBuilder> value_builder;
      std::shared_ptr<DataType> value_type =
          internal::checked_cast<const ListType&>(*type).value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new ListBuilder(pool, std::move(value_builder), type));
      return Status::OK();
    }

    case Type::LARGE_LIST: {
      std::unique_ptr<ArrayBuilder> value_builder;
      std::shared_ptr<DataType> value_type =
          internal::checked_cast<const LargeListType&>(*type).value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new LargeListBuilder(pool, std::move(value_builder), type));
      return Status::OK();
    }

    case Type::MAP: {
      const auto& map_type = internal::checked_cast<const MapType&>(*type);
      std::unique_ptr<ArrayBuilder> key_builder, item_builder;
      RETURN_NOT_OK(MakeBuilder(pool, map_type.key_type(), &key_builder));
      RETURN_NOT_OK(MakeBuilder(pool, map_type.item_type(), &item_builder));
      out->reset(
          new MapBuilder(pool, std::move(key_builder), std::move(item_builder), type));
      return Status::OK();
    }

    case Type::FIXED_SIZE_LIST: {
      const auto& list_type = internal::checked_cast<const FixedSizeListType&>(*type);
      std::unique_ptr<ArrayBuilder> value_builder;
      auto value_type = list_type.value_type();
      RETURN_NOT_OK(MakeBuilder(pool, value_type, &value_builder));
      out->reset(new FixedSizeListBuilder(pool, std::move(value_builder), type));
      return Status::OK();
    }

    case Type::STRUCT: {
      ARROW_ASSIGN_OR_RAISE(auto field_builders, FieldBuilders(*type, pool));
      out->reset(new StructBuilder(type, pool, std::move(field_builders)));
      return Status::OK();
    }

    case Type::SPARSE_UNION: {
      ARROW_ASSIGN_OR_RAISE(auto field_builders, FieldBuilders(*type, pool));
      out->reset(new SparseUnionBuilder(pool, std::move(field_builders), type));
      return Status::OK();
    }

    case Type::DENSE_UNION: {
      ARROW_ASSIGN_OR_RAISE(auto field_builders, FieldBuilders(*type, pool));
      out->reset(new DenseUnionBuilder(pool, std::move(field_builders), type));
      return Status::OK();
    }

    default:
      break;
  }
  return Status::NotImplemented("MakeBuilder: cannot construct builder for type ",
                                type->ToString());
}

Status MakeDictionaryBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                             const std::shared_ptr<Array>& dictionary,
                             std::unique_ptr<ArrayBuilder>* out) {
  const auto& dict_type = static_cast<const DictionaryType&>(*type);
  DictionaryBuilderCase visitor = {pool, dict_type.index_type(), dict_type.value_type(),
                                   dictionary, out};
  return visitor.Make();
}

}  // namespace arrow
