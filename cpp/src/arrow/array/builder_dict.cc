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
#include <utility>

#include "arrow/array/dict_internal.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

// ----------------------------------------------------------------------
// DictionaryBuilder

namespace internal {

class DictionaryMemoTable::DictionaryMemoTableImpl {
  // Type-dependent visitor for memo table initialization
  struct MemoTableInitializer {
    std::shared_ptr<DataType> value_type_;
    MemoryPool* pool_;
    std::unique_ptr<MemoTable>* memo_table_;

    template <typename T>
    enable_if_no_memoize<T, Status> Visit(const T&) {
      return Status::NotImplemented("Initialization of ", value_type_->ToString(),
                                    " memo table is not implemented");
    }

    template <typename T>
    enable_if_memoize<T, Status> Visit(const T&) {
      using MemoTable = typename DictionaryTraits<T>::MemoTableType;
      memo_table_->reset(new MemoTable(pool_, 0));
      return Status::OK();
    }
  };

  // Type-dependent visitor for memo table insertion
  struct ArrayValuesInserter {
    DictionaryMemoTableImpl* impl_;
    const Array& values_;

    template <typename T>
    Status Visit(const T& type) {
      using ArrayType = typename TypeTraits<T>::ArrayType;
      return InsertValues(type, checked_cast<const ArrayType&>(values_));
    }

   private:
    template <typename T, typename ArrayType>
    enable_if_no_memoize<T, Status> InsertValues(const T& type, const ArrayType&) {
      return Status::NotImplemented("Inserting array values of ", type,
                                    " is not implemented");
    }

    template <typename T, typename ArrayType>
    enable_if_memoize<T, Status> InsertValues(const T&, const ArrayType& array) {
      if (array.null_count() > 0) {
        return Status::Invalid("Cannot insert dictionary values containing nulls");
      }
      for (int64_t i = 0; i < array.length(); ++i) {
        int32_t unused_memo_index;
        RETURN_NOT_OK(impl_->GetOrInsert<T>(array.GetView(i), &unused_memo_index));
      }
      return Status::OK();
    }
  };

  // Type-dependent visitor for building ArrayData from memo table
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
      using ConcreteMemoTable = typename DictionaryTraits<T>::MemoTableType;
      auto memo_table = checked_cast<ConcreteMemoTable*>(memo_table_);
      return DictionaryTraits<T>::GetDictionaryArrayData(pool_, value_type_, *memo_table,
                                                         start_offset_, out_);
    }
  };

 public:
  DictionaryMemoTableImpl(MemoryPool* pool, std::shared_ptr<DataType> type)
      : pool_(pool), type_(std::move(type)), memo_table_(nullptr) {
    MemoTableInitializer visitor{type_, pool_, &memo_table_};
    ARROW_CHECK_OK(VisitTypeInline(*type_, &visitor));
  }

  Status InsertValues(const Array& array) {
    if (!array.type()->Equals(*type_)) {
      return Status::Invalid("Array value type does not match memo type: ",
                             array.type()->ToString());
    }
    ArrayValuesInserter visitor{this, array};
    return VisitTypeInline(*array.type(), &visitor);
  }

  template <typename PhysicalType,
            typename CType = typename DictionaryValue<PhysicalType>::type>
  Status GetOrInsert(CType value, int32_t* out) {
    using ConcreteMemoTable = typename DictionaryTraits<PhysicalType>::MemoTableType;
    return checked_cast<ConcreteMemoTable*>(memo_table_.get())->GetOrInsert(value, out);
  }

  Status GetArrayData(int64_t start_offset, std::shared_ptr<ArrayData>* out) {
    ArrayDataGetter visitor{type_, memo_table_.get(), pool_, start_offset, out};
    return VisitTypeInline(*type_, &visitor);
  }

  int32_t size() const { return memo_table_->size(); }

 private:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  std::unique_ptr<MemoTable> memo_table_;
};

DictionaryMemoTable::DictionaryMemoTable(MemoryPool* pool,
                                         const std::shared_ptr<DataType>& type)
    : impl_(new DictionaryMemoTableImpl(pool, type)) {}

DictionaryMemoTable::DictionaryMemoTable(MemoryPool* pool,
                                         const std::shared_ptr<Array>& dictionary)
    : impl_(new DictionaryMemoTableImpl(pool, dictionary->type())) {
  ARROW_CHECK_OK(impl_->InsertValues(*dictionary));
}

DictionaryMemoTable::~DictionaryMemoTable() = default;

#define GET_OR_INSERT(C_TYPE)                                                       \
  Status DictionaryMemoTable::GetOrInsert(                                          \
      const typename CTypeTraits<C_TYPE>::ArrowType*, C_TYPE value, int32_t* out) { \
    return impl_->GetOrInsert<typename CTypeTraits<C_TYPE>::ArrowType>(value, out); \
  }

GET_OR_INSERT(bool)
GET_OR_INSERT(int8_t)
GET_OR_INSERT(int16_t)
GET_OR_INSERT(int32_t)
GET_OR_INSERT(int64_t)
GET_OR_INSERT(uint8_t)
GET_OR_INSERT(uint16_t)
GET_OR_INSERT(uint32_t)
GET_OR_INSERT(uint64_t)
GET_OR_INSERT(float)
GET_OR_INSERT(double)

#undef GET_OR_INSERT

Status DictionaryMemoTable::GetOrInsert(const BinaryType*, util::string_view value,
                                        int32_t* out) {
  return impl_->GetOrInsert<BinaryType>(value, out);
}

Status DictionaryMemoTable::GetOrInsert(const LargeBinaryType*, util::string_view value,
                                        int32_t* out) {
  return impl_->GetOrInsert<LargeBinaryType>(value, out);
}

Status DictionaryMemoTable::GetArrayData(int64_t start_offset,
                                         std::shared_ptr<ArrayData>* out) {
  return impl_->GetArrayData(start_offset, out);
}

Status DictionaryMemoTable::InsertValues(const Array& array) {
  return impl_->InsertValues(array);
}

int32_t DictionaryMemoTable::size() const { return impl_->size(); }

}  // namespace internal
}  // namespace arrow
