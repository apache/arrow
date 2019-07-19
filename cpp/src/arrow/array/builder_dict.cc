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

#include "arrow/array/builder_dict.h"

#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// DictionaryType unification

struct UnifyDictionaryValues {
  MemoryPool* pool_;
  std::shared_ptr<DataType> value_type_;
  const std::vector<const DictionaryType*>& types_;
  const std::vector<const Array*>& dictionaries_;
  std::shared_ptr<Array>* out_values_;
  std::vector<std::vector<int32_t>>* out_transpose_maps_;

  Status Visit(const DataType&, void* = nullptr) {
    // Default implementation for non-dictionary-supported datatypes
    return Status::NotImplemented("Unification of ", value_type_,
                                  " dictionaries is not implemented");
  }

  Status Visit(const DayTimeIntervalType&, void* = nullptr) {
    return Status::NotImplemented(
        "Unification of DayTime"
        " dictionaries is not implemented");
  }

  template <typename T>
  Status Visit(const T&,
               typename internal::DictionaryTraits<T>::MemoTableType* = nullptr) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    using DictTraits = typename internal::DictionaryTraits<T>;
    using MemoTableType = typename DictTraits::MemoTableType;

    MemoTableType memo_table;
    if (out_transpose_maps_ != nullptr) {
      out_transpose_maps_->clear();
      out_transpose_maps_->reserve(types_.size());
    }
    // Build up the unified dictionary values and the transpose maps
    for (size_t i = 0; i < types_.size(); ++i) {
      const ArrayType& values = checked_cast<const ArrayType&>(*dictionaries_[i]);
      if (out_transpose_maps_ != nullptr) {
        std::vector<int32_t> transpose_map;
        transpose_map.reserve(values.length());
        for (int64_t i = 0; i < values.length(); ++i) {
          int32_t dict_index = memo_table.GetOrInsert(values.GetView(i));
          transpose_map.push_back(dict_index);
        }
        out_transpose_maps_->push_back(std::move(transpose_map));
      } else {
        for (int64_t i = 0; i < values.length(); ++i) {
          memo_table.GetOrInsert(values.GetView(i));
        }
      }
    }
    // Build unified dictionary array
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(DictTraits::GetDictionaryArrayData(pool_, value_type_, memo_table,
                                                     0 /* start_offset */, &data));
    *out_values_ = MakeArray(data);
    return Status::OK();
  }
};

Status DictionaryType::Unify(MemoryPool* pool, const std::vector<const DataType*>& types,
                             const std::vector<const Array*>& dictionaries,
                             std::shared_ptr<DataType>* out_type,
                             std::shared_ptr<Array>* out_dictionary,
                             std::vector<std::vector<int32_t>>* out_transpose_maps) {
  if (types.size() == 0) {
    return Status::Invalid("need at least one input type");
  }

  if (types.size() != dictionaries.size()) {
    return Status::Invalid("expecting the same number of types and dictionaries");
  }

  std::vector<const DictionaryType*> dict_types;
  dict_types.reserve(types.size());
  for (const auto& type : types) {
    if (type->id() != Type::DICTIONARY) {
      return Status::TypeError("input types must be dictionary types");
    }
    dict_types.push_back(checked_cast<const DictionaryType*>(type));
  }

  // XXX Should we check the ordered flag?
  auto value_type = dict_types[0]->value_type();
  for (size_t i = 0; i < types.size(); ++i) {
    if (!(dictionaries[i]->type()->Equals(*value_type) &&
          dict_types[i]->value_type()->Equals(*value_type))) {
      return Status::TypeError("dictionary value types were not all consistent");
    }
    if (dictionaries[i]->null_count() != 0) {
      return Status::TypeError("input types have null values");
    }
  }

  std::shared_ptr<Array> values;
  {
    UnifyDictionaryValues visitor{pool,         value_type, dict_types,
                                  dictionaries, &values,    out_transpose_maps};
    RETURN_NOT_OK(VisitTypeInline(*value_type, &visitor));
  }

  // Build unified dictionary type with the right index type
  std::shared_ptr<DataType> index_type;
  if (values->length() <= std::numeric_limits<int8_t>::max()) {
    index_type = int8();
  } else if (values->length() <= std::numeric_limits<int16_t>::max()) {
    index_type = int16();
  } else if (values->length() <= std::numeric_limits<int32_t>::max()) {
    index_type = int32();
  } else {
    index_type = int64();
  }
  *out_type = arrow::dictionary(index_type, values->type());
  *out_dictionary = values;
  return Status::OK();
}

// ----------------------------------------------------------------------
// DictionaryBuilder

class internal::DictionaryMemoTable::DictionaryMemoTableImpl {
  struct MemoTableInitializer {
    std::shared_ptr<DataType> value_type_;
    std::unique_ptr<MemoTable>* memo_table_;

    Status Visit(const DataType&, void* = nullptr) {
      return Status::NotImplemented("Initialization of ", value_type_,
                                    " memo table is not implemented");
    }

    template <typename T>
    Status Visit(const T&,
                 typename internal::DictionaryTraits<T>::MemoTableType* = nullptr) {
      using MemoTable = typename internal::DictionaryTraits<T>::MemoTableType;
      memo_table_->reset(new MemoTable(0));
      return Status::OK();
    }
  };

  struct ArrayValuesInserter {
    DictionaryMemoTableImpl* impl_;

    template <typename T>
    Status Visit(const T& array) {
      return InsertValues(array.type(), array);
    }

   private:
    template <typename DataType, typename Array>
    Status InsertValues(const DataType& type, const Array&, void* = nullptr) {
      return Status::NotImplemented("Inserting array values of ", type,
                                    " is not implemented");
    }

    template <typename DataType, typename Array>
    Status InsertValues(
        const DataType&, const Array& array,
        typename internal::DictionaryTraits<DataType>::MemoTableType* = nullptr) {
      for (int64_t i = 0; i < array.length(); ++i) {
        ARROW_IGNORE_EXPR(impl_->GetOrInsert(array.GetView(i)));
      }
      return Status::OK();
    }
  };

  struct ArrayDataGetter {
    std::shared_ptr<DataType> value_type_;
    MemoTable* memo_table_;
    MemoryPool* pool_;
    int64_t start_offset_;
    std::shared_ptr<ArrayData>* out_;

    Status Visit(const DataType&, void* = nullptr) {
      return Status::NotImplemented("Getting array data of ", value_type_,
                                    " is not implemented");
    }

    template <typename T>
    Status Visit(const T&,
                 typename internal::DictionaryTraits<T>::MemoTableType* = nullptr) {
      using ConcreteMemoTable = typename internal::DictionaryTraits<T>::MemoTableType;
      auto memo_table = static_cast<ConcreteMemoTable*>(memo_table_);
      return internal::DictionaryTraits<T>::GetDictionaryArrayData(
          pool_, value_type_, *memo_table, start_offset_, out_);
    }
  };

 public:
  explicit DictionaryMemoTableImpl(const std::shared_ptr<DataType>& type)
      : type_(type), memo_table_(nullptr) {
    MemoTableInitializer visitor{type_, &memo_table_};
    ARROW_IGNORE_EXPR(VisitTypeInline(*type_, &visitor));
  }

  Status InsertValues(const std::shared_ptr<Array>& array) {
    ArrayValuesInserter visitor{this};
    return VisitArrayInline(*array, &visitor);
  }

  template <typename T>
  int32_t GetOrInsert(const T& value) {
    using ConcreteMemoTable = typename internal::DictionaryTraits<
        typename CTypeTraits<T>::ArrowType>::MemoTableType;
    return static_cast<ConcreteMemoTable*>(memo_table_.get())->GetOrInsert(value);
  }

  int32_t GetOrInsert(const util::string_view& value) {
    return static_cast<BinaryMemoTable*>(memo_table_.get())->GetOrInsert(value);
  }

  Status GetArrayData(MemoryPool* pool, int64_t start_offset,
                      std::shared_ptr<ArrayData>* out) {
    ArrayDataGetter visitor{type_, memo_table_.get(), pool, start_offset, out};
    return VisitTypeInline(*type_, &visitor);
  }

  int32_t size() const { return memo_table_->size(); }

 private:
  std::shared_ptr<DataType> type_;
  std::unique_ptr<MemoTable> memo_table_;
};

internal::DictionaryMemoTable::DictionaryMemoTable(const std::shared_ptr<DataType>& type)
    : impl_(new DictionaryMemoTableImpl(type)) {}

internal::DictionaryMemoTable::DictionaryMemoTable(
    const std::shared_ptr<Array>& dictionary)
    : impl_(new DictionaryMemoTableImpl(dictionary->type())) {
  ARROW_IGNORE_EXPR(impl_->InsertValues(dictionary));
}

internal::DictionaryMemoTable::~DictionaryMemoTable() = default;

int32_t internal::DictionaryMemoTable::GetOrInsert(const bool& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const int8_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const int16_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const int32_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const int64_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const uint8_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const uint16_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const uint32_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const uint64_t& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const float& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const double& value) {
  return impl_->GetOrInsert(value);
}

int32_t internal::DictionaryMemoTable::GetOrInsert(const util::string_view& value) {
  return impl_->GetOrInsert(value);
}

Status internal::DictionaryMemoTable::GetArrayData(MemoryPool* pool, int64_t start_offset,
                                                   std::shared_ptr<ArrayData>* out) {
  return impl_->GetArrayData(pool, start_offset, out);
}

int32_t internal::DictionaryMemoTable::size() const { return impl_->size(); }

}  // namespace arrow
