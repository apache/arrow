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

#include "arrow/compute/kernels/isin.h"

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
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/memory_pool.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
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

class IsInKernelImpl : public UnaryKernel {
  virtual Status Compute(FunctionContext* ctx, const ArrayData& right) = 0;

 public:
  // \brief Check if value in both arrays or not and returns boolean values
  Status Call(FunctionContext* ctx, const Datum& right, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, right.kind());
    const ArrayData& right_data = *right.array();
    RETURN_NOT_OK(Compute(ctx, right_data));
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  virtual Status CompareArray(const Datum& left, Datum* out) = 0;
};

// ----------------------------------------------------------------------
// Using a visitor create a memo_table_ for the right array
// TODO: Implement for small lists

template <typename Type, typename Scalar>
struct MemoTableRight {
  Status VisitNull() { return Status::OK(); }

  Status VisitValue(const Scalar& value) {
    memo_table_->GetOrInsert(value);
    return Status::OK();
  }

  Status Append(const ArrayData& right) {
    memo_table_.reset(new MemoTable(0));
    return ArrayDataVisitor<Type>::Visit(right, this);
  }

  using MemoTable = typename HashTraits<Type>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;
};

// ----------------------------------------------------------------------

template <typename T, typename Scalar>
class IsInKernel : public IsInKernelImpl {
 public:
  IsInKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  // \brief if null_count in right is not 0, then append true to the
  // BooleanBuilder when left array has a null, else append null.
  Status VisitNull() {
    if (right_null_count != 0) {
      bool_builder_.UnsafeAppend(true);
    } else {
      bool_builder_.UnsafeAppendNull();
    }
    return Status::OK();
  }

  // \brief Iterate over the left array using another visitor.
  // In VisitValue, use the memo_table_ (for right array) and check if value
  // in left array is in the memo_table_. Append the true to the
  // BooleanBuilder if condition satisfied, else false.
  Status VisitValue(const Scalar& value) {
    bool_builder_.UnsafeAppend(memo_table_->Get(value) != -1);
    return Status::OK();
  }

  Status Compute(FunctionContext* ctx, const ArrayData& right) override {
    MemoTableRight<T, Scalar> func;
    func.Append(right);
    memo_table_ = std::move(func.memo_table_);

    right_null_count = right.GetNullCount();
    return Status::OK();
  }

  Status CompareArray(const Datum& left, Datum* out) override {
    const ArrayData& left_data = *left.array();
    bool_builder_.Reset();
    bool_builder_.Reserve(left_data.length);
    ArrayDataVisitor<T>::Visit(left_data, this);
    RETURN_NOT_OK(bool_builder_.FinishInternal(&output));
    out->value = std::move(output);
    return Status::OK();
  }

 protected:
  using MemoTable = typename HashTraits<T>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;

 private:
  // \brief Additional member "right_null_count" is used to check if
  // null count in right is not 0
  int64_t right_null_count;
  BooleanBuilder bool_builder_;
  std::shared_ptr<ArrayData> output;
};

// ----------------------------------------------------------------------
// (NullType has a separate implementation)

// \brief When array is NullType, based on the null_count for the arrays,
// append true/null to the BooleanBuilder

class NullIsInKernel : public IsInKernelImpl {
 public:
  NullIsInKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  Status Compute(FunctionContext* ctx, const ArrayData& right) override {
    right_null_count = right.GetNullCount();
    return Status::OK();
  }

  Status CompareArray(const Datum& left, Datum* out) override {
    const ArrayData& left_data = *left.array();
    bool_builder_.Reset();
    bool_builder_.Reserve(left_data.length);
    left_null_count = left_data.GetNullCount();

    if (left_null_count != 0 && right_null_count != 0) {
      for (int64_t i = 0; i < left_data.length; ++i) {
        bool_builder_.UnsafeAppend(true);
      }
    } else if (left_null_count != 0 && right_null_count == 0) {
      for (int64_t i = 0; i < left_data.length; ++i) {
        bool_builder_.UnsafeAppendNull();
      }
    }

    RETURN_NOT_OK(bool_builder_.FinishInternal(&output));
    out->value = std::move(output);
    return Status::OK();
  }

 private:
  int64_t left_null_count;
  int64_t right_null_count;
  BooleanBuilder bool_builder_;
  std::shared_ptr<ArrayData> output;
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

template <typename Type, typename Enable = void>
struct IsInKernelTraits {};

template <typename Type>
struct IsInKernelTraits<Type, enable_if_null<Type>> {
  using IsInKernelImpl = NullIsInKernel;
};

template <typename Type>
struct IsInKernelTraits<Type, enable_if_has_c_type<Type>> {
  using IsInKernelImpl = IsInKernel<Type, typename Type::c_type>;
};

template <typename Type>
struct IsInKernelTraits<Type, enable_if_boolean<Type>> {
  using IsInKernelImpl = IsInKernel<Type, bool>;
};

template <typename Type>
struct IsInKernelTraits<Type, enable_if_binary<Type>> {
  using IsInKernelImpl = IsInKernel<Type, util::string_view>;
};

template <typename Type>
struct IsInKernelTraits<Type, enable_if_fixed_size_binary<Type>> {
  using IsInKernelImpl = IsInKernel<Type, util::string_view>;
};

Status GetIsInKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                     std::unique_ptr<IsInKernelImpl>* out) {
  std::unique_ptr<IsInKernelImpl> kernel;

#define ISIN_CASE(InType)                                               \
  case InType::type_id:                                                 \
    kernel.reset(new typename IsInKernelTraits<InType>::IsInKernelImpl( \
        type, ctx->memory_pool()));                                     \
    break

  switch (type->id()) {
    ISIN_CASE(NullType);
    ISIN_CASE(BooleanType);
    ISIN_CASE(UInt8Type);
    ISIN_CASE(Int8Type);
    ISIN_CASE(UInt16Type);
    ISIN_CASE(Int16Type);
    ISIN_CASE(UInt32Type);
    ISIN_CASE(Int32Type);
    ISIN_CASE(UInt64Type);
    ISIN_CASE(Int64Type);
    ISIN_CASE(FloatType);
    ISIN_CASE(DoubleType);
    ISIN_CASE(Date32Type);
    ISIN_CASE(Date64Type);
    ISIN_CASE(Time32Type);
    ISIN_CASE(Time64Type);
    ISIN_CASE(TimestampType);
    ISIN_CASE(BinaryType);
    ISIN_CASE(StringType);
    ISIN_CASE(FixedSizeBinaryType);
    ISIN_CASE(Decimal128Type);
    default:
      break;
  }
#undef ISIN_CASE

  if (!kernel) {
    return Status::NotImplemented("isin", " not implemented for ", type->ToString());
  }

  *out = std::move(kernel);
  return Status::OK();
}

Status IsIn(FunctionContext* context, const Datum& left, const Datum& right, Datum* out) {
  DCHECK(left.type()->Equals(right.type()));
  std::unique_ptr<IsInKernelImpl> kernel;
  std::vector<Datum> unused_output;
  RETURN_NOT_OK(GetIsInKernel(context, right.type(), &kernel));

  RETURN_NOT_OK(
      detail::InvokeUnaryArrayKernel(context, kernel.get(), right, &unused_output));

  Datum output;
  std::vector<std::shared_ptr<Array>> arrays;

  if (left.kind() == Datum::CHUNKED_ARRAY) {
    const ChunkedArray& left_array = *left.chunked_array();
    for (int i = 0; i < left_array.num_chunks(); i++) {
      output.value = ArrayData::Make(kernel->out_type(), left_array.chunk(i)->length());
      RETURN_NOT_OK(kernel->CompareArray(left_array.chunk(i), &output));
      arrays.emplace_back(MakeArray(output.array()));
    }
  } else {
    RETURN_NOT_OK(kernel->CompareArray(left, &output));
    arrays.emplace_back(MakeArray(output.array()));
  }

  *out = detail::WrapArraysLike(left, arrays);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
