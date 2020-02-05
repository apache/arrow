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
#include "arrow/array/dict_internal.h"
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
// DictionaryBuilder

class internal::DictionaryMemoTable::DictionaryMemoTableImpl {
  struct MemoTableInitializer {
    std::shared_ptr<DataType> value_type_;
    std::unique_ptr<MemoTable>* memo_table_;

    template <typename T>
    enable_if_no_memoize<T, Status> Visit(const T&) {
      return Status::NotImplemented("Initialization of ", value_type_,
                                    " memo table is not implemented");
    }

    template <typename T>
    enable_if_memoize<T, Status> Visit(const T&) {
      using MemoTable = typename internal::DictionaryTraits<T>::MemoTableType;
      // TODO(fsaintjacques): Propagate memory pool
      memo_table_->reset(new MemoTable(default_memory_pool(), 0));
      return Status::OK();
    }
  };

  struct ArrayValuesInserter {
    DictionaryMemoTableImpl* impl_;
    const Array& values_;

    template <typename T>
    Status Visit(const T& type) {
      using ArrayType = typename TypeTraits<T>::ArrayType;
      return InsertValues(type, checked_cast<const ArrayType&>(values_));
    }

   private:
    template <typename DType, typename ArrayType>
    enable_if_no_memoize<DType, Status> InsertValues(const DType& type,
                                                     const ArrayType&) {
      return Status::NotImplemented("Inserting array values of ", type,
                                    " is not implemented");
    }

    template <typename DType, typename ArrayType>
    enable_if_memoize<DType, Status> InsertValues(const DType&, const ArrayType& array) {
      if (array.null_count() > 0) {
        return Status::Invalid("Cannot insert dictionary values containing nulls");
      }
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

    template <typename T>
    enable_if_no_memoize<T, Status> Visit(const T&) {
      return Status::NotImplemented("Getting array data of ", value_type_,
                                    " is not implemented");
    }

    template <typename T>
    enable_if_memoize<T, Status> Visit(const T&) {
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

  Status InsertValues(const Array& array) {
    if (!array.type()->Equals(*type_)) {
      return Status::Invalid("Array value type does not match memo type: ",
                             array.type()->ToString());
    }
    ArrayValuesInserter visitor{this, array};
    return VisitTypeInline(*array.type(), &visitor);
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
  ARROW_IGNORE_EXPR(impl_->InsertValues(*dictionary));
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

Status internal::DictionaryMemoTable::InsertValues(const Array& array) {
  return impl_->InsertValues(array);
}

int32_t internal::DictionaryMemoTable::size() const { return impl_->size(); }

}  // namespace arrow
