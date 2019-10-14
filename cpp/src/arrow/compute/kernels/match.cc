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

#include "arrow/compute/kernels/match.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/dict_internal.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::DictionaryTraits;
using internal::HashTraits;

namespace compute {

class MatchKernelImpl : public UnaryKernel {
  virtual Status Compute(FunctionContext* ctx, const Datum& left, Datum* out) = 0;

 public:
  // \brief Check if value in both arrays or not and returns integer values/null
  Status Call(FunctionContext* ctx, const Datum& left, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, left.kind());
    RETURN_NOT_OK(Compute(ctx, left, out));
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return int32(); }

  virtual Status ConstructRight(FunctionContext* ctx, const Datum& right) = 0;
};

// ----------------------------------------------------------------------
// Using a visitor create a memo_table_ for the right array
// TODO: Implement for small lists

template <typename T, typename Scalar>
struct MatchMemoTableRight {
  Status VisitNull() {
    memo_table_->GetOrInsertNull();
    return Status::OK();
  }

  Status VisitValue(const Scalar& value) {
    memo_table_->GetOrInsert(value);
    return Status::OK();
  }

  Status Reset(MemoryPool* pool) {
    memo_table_.reset(new MemoTable(pool, 0));
    return Status::OK();
  }

  Status Append(FunctionContext* ctx, const Datum& right) {
    const ArrayData& right_data = *right.array();
    return ArrayDataVisitor<T>::Visit(right_data, this);
  }

  using MemoTable = typename HashTraits<T>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;
};

// ----------------------------------------------------------------------

template <typename Type, typename Scalar>
class MatchKernel : public MatchKernelImpl {
 public:
  MatchKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type), pool_(pool) {}

  // \brief if left array has a null and right array has null,
  // return the index, else null
  Status VisitNull() {
    if (memo_table_->GetNull() != -1) {
      indices_builder_.UnsafeAppend(memo_table_->GetNull());
    } else {
      indices_builder_.UnsafeAppendNull();
    }
    return Status::OK();
  }

  // \brief Iterate over the left array using another visitor.
  // In VisitValue, use the memo_table_ (for right array) and check if value
  // in left array is in the memo_table_. Return the index if condition satisfied,
  // else null.
  Status VisitValue(const Scalar& value) {
    if (memo_table_->Get(value) != -1) {
      indices_builder_.UnsafeAppend(memo_table_->Get(value));
    } else {
      indices_builder_.UnsafeAppendNull();
    }
    return Status::OK();
  }

  Status Compute(FunctionContext* ctx, const Datum& left, Datum* out) override {
    const ArrayData& left_data = *left.array();

    indices_builder_.Reset();
    indices_builder_.Reserve(left_data.length);

    RETURN_NOT_OK(ArrayDataVisitor<Type>::Visit(left_data, this));

    RETURN_NOT_OK(indices_builder_.FinishInternal(&output));
    out->value = std::move(output);
    return Status::OK();
  }

  Status ConstructRight(FunctionContext* ctx, const Datum& right) override {
    MatchMemoTableRight<Type, Scalar> func;
    RETURN_NOT_OK(func.Reset(pool_));

    if (right.kind() == Datum::ARRAY) {
      RETURN_NOT_OK(func.Append(ctx, right));
    } else if (right.kind() == Datum::CHUNKED_ARRAY) {
      const ChunkedArray& right_array = *right.chunked_array();
      for (int i = 0; i < right_array.num_chunks(); i++) {
        RETURN_NOT_OK(func.Append(ctx, right_array.chunk(i)));
      }
    } else {
      return Status::Invalid("Input Datum was not array-like");
    }

    memo_table_ = std::move(func.memo_table_);
    return Status::OK();
  }

 protected:
  using MemoTable = typename HashTraits<Type>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;

 private:
  std::shared_ptr<ArrayData> output;
  Int32Builder indices_builder_;
};

// ----------------------------------------------------------------------
// (NullType has a separate implementation)

class NullMatchKernel : public MatchKernelImpl {
 public:
  NullMatchKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  // \brief When array is NullType, based on the null count for the arrays,
  // return index, else nulls
  Status Compute(FunctionContext* ctx, const Datum& left, Datum* out) override {
    const ArrayData& left_data = *left.array();
    left_null_count = left_data.GetNullCount();
    indices_builder_.Reset();
    indices_builder_.Reserve(left_data.length);

    if (left_null_count != 0 && right_null_count == 0) {
      for (int64_t i = 0; i < left_data.length; ++i) {
        indices_builder_.UnsafeAppendNull();
      }
    } else {
      for (int64_t i = 0; i < left_data.length; ++i) {
        indices_builder_.UnsafeAppend(0);
      }
    }
    RETURN_NOT_OK(indices_builder_.FinishInternal(&output));
    out->value = std::move(output);
    return Status::OK();
  }

  Status ConstructRight(FunctionContext* ctx, const Datum& right) override {
    if (right.kind() == Datum::ARRAY) {
      const ArrayData& right_data = *right.array();
      right_null_count = right_data.GetNullCount();
    } else if (right.kind() == Datum::CHUNKED_ARRAY) {
      const ChunkedArray& right_array = *right.chunked_array();
      for (int i = 0; i < right_array.num_chunks(); i++) {
        right_null_count += right_array.chunk(i)->null_count();
      }
    } else {
      return Status::Invalid("Input Datum was not array-like");
    }
    return Status::OK();
  }

 private:
  int64_t left_null_count{};
  int64_t right_null_count{};
  std::shared_ptr<ArrayData> output;
  Int32Builder indices_builder_;
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

template <typename Type, typename Enable = void>
struct MatchKernelTraits {};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_null<Type>> {
  using MatchKernelImpl = NullMatchKernel;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_has_c_type<Type>> {
  using MatchKernelImpl = MatchKernel<Type, typename Type::c_type>;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_boolean<Type>> {
  using MatchKernelImpl = MatchKernel<Type, bool>;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_binary<Type>> {
  using MatchKernelImpl = MatchKernel<Type, util::string_view>;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_fixed_size_binary<Type>> {
  using MatchKernelImpl = MatchKernel<Type, util::string_view>;
};

Status GetMatchKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                      const Datum& right, std::unique_ptr<MatchKernelImpl>* out) {
  std::unique_ptr<MatchKernelImpl> kernel;

#define MATCH_CASE(InType)                                                \
  case InType::type_id:                                                   \
    kernel.reset(new typename MatchKernelTraits<InType>::MatchKernelImpl( \
        type, ctx->memory_pool()));                                       \
    break

  switch (type->id()) {
    MATCH_CASE(NullType);
    MATCH_CASE(BooleanType);
    MATCH_CASE(UInt8Type);
    MATCH_CASE(Int8Type);
    MATCH_CASE(UInt16Type);
    MATCH_CASE(Int16Type);
    MATCH_CASE(UInt32Type);
    MATCH_CASE(Int32Type);
    MATCH_CASE(UInt64Type);
    MATCH_CASE(Int64Type);
    MATCH_CASE(FloatType);
    MATCH_CASE(DoubleType);
    MATCH_CASE(Date32Type);
    MATCH_CASE(Date64Type);
    MATCH_CASE(Time32Type);
    MATCH_CASE(Time64Type);
    MATCH_CASE(TimestampType);
    MATCH_CASE(BinaryType);
    MATCH_CASE(StringType);
    MATCH_CASE(FixedSizeBinaryType);
    MATCH_CASE(Decimal128Type);
    default:
      break;
  }
#undef MATCH_CASE

  if (!kernel) {
    return Status::NotImplemented("Match is not implemented for ", type->ToString());
  }
  RETURN_NOT_OK(kernel->ConstructRight(ctx, right));
  *out = std::move(kernel);
  return Status::OK();
}

Status Match(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  DCHECK(left.type()->Equals(right.type()));
  std::vector<Datum> outputs;
  std::unique_ptr<MatchKernelImpl> lkernel;

  RETURN_NOT_OK(GetMatchKernel(ctx, left.type(), right, &lkernel));
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, lkernel.get(), left, &outputs));

  *out = detail::WrapDatumsLike(left, outputs);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
