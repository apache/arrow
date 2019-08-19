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

class IsInKernelImpl : public UnaryKernel {
  virtual Status Compute(FunctionContext* ctx, const Datum& left, Datum* out) = 0;

 public:
  // \brief Check if value in both arrays or not and returns boolean values/null
  Status Call(FunctionContext* ctx, const Datum& left, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, left.kind());
    RETURN_NOT_OK(Compute(ctx, left, out));
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return boolean(); }

  virtual Status ConstructRight(FunctionContext* ctx, const Datum& right) = 0;
};

// ----------------------------------------------------------------------
// Using a visitor create a memo_table_ for the right array
// TODO: Implement for small lists

template <typename T, typename Scalar>
struct MemoTableRight {
  Status VisitNull() { return Status::OK(); }

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
    right_null_count += right_data.GetNullCount();
    return ArrayDataVisitor<T>::Visit(right_data, this);
  }

  using MemoTable = typename HashTraits<T>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;
  int64_t right_null_count{};
};

// ----------------------------------------------------------------------

template <typename Type, typename Scalar>
class IsInKernel : public IsInKernelImpl {
 public:
  IsInKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : type_(type), pool_(pool) {}

  // \brief if left array has a null return true
  Status VisitNull() {
    writer->Set();
    writer->Next();
    return Status::OK();
  }

  // \brief Iterate over the left array using another visitor.
  // In VisitValue, use the memo_table_ (for right array) and check if value
  // in left array is in the memo_table_. Return true if condition satisfied,
  // else false.
  Status VisitValue(const Scalar& value) {
    if (memo_table_->Get(value) != -1) {
      writer->Set();
    } else {
      writer->Clear();
    }
    writer->Next();
    return Status::OK();
  }

  Status Compute(FunctionContext* ctx, const Datum& left, Datum* out) override {
    const ArrayData& left_data = *left.array();

    output = out->array();
    output->type = boolean();

    writer = std::make_shared<internal::FirstTimeBitmapWriter>(
        output.get()->buffers[1]->mutable_data(), output.get()->offset, left_data.length);

    RETURN_NOT_OK(ArrayDataVisitor<Type>::Visit(left_data, this));
    writer->Finish();

    // if right null count is zero and left null count is not zero, propagate nulls
    if (right_null_count_ == 0 && left_data.GetNullCount() != 0) {
      RETURN_NOT_OK(detail::PropagateNulls(ctx, left_data, output.get()));
    }
    return Status::OK();
  }

  Status ConstructRight(FunctionContext* ctx, const Datum& right) override {
    MemoTableRight<Type, Scalar> func;
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
    right_null_count_ = func.right_null_count;
    return Status::OK();
  }

 protected:
  using MemoTable = typename HashTraits<Type>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;

 private:
  // \brief Additional member "right_null_count" is used to check if
  // null count in right is not 0
  int64_t right_null_count_{};
  std::shared_ptr<internal::FirstTimeBitmapWriter> writer;
  std::shared_ptr<ArrayData> output;
};

// ----------------------------------------------------------------------
// (NullType has a separate implementation)

class NullIsInKernel : public IsInKernelImpl {
 public:
  NullIsInKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  // \brief When array is NullType, based on the null count for the arrays,
  // return true, else propagate to all nulls
  Status Compute(FunctionContext* ctx, const Datum& left, Datum* out) override {
    const ArrayData& left_data = *left.array();
    left_null_count = left_data.GetNullCount();

    output = out->array();
    output->type = boolean();

    writer = std::make_shared<internal::FirstTimeBitmapWriter>(
        output.get()->buffers[1]->mutable_data(), output.get()->offset, left_data.length);

    if (left_null_count != 0 && right_null_count == 0) {
      RETURN_NOT_OK(detail::PropagateNulls(ctx, left_data, output.get()));
    } else {
      for (int64_t i = 0; i < left_data.length; ++i) {
        writer->Set();
        writer->Next();
      }
      writer->Finish();
    }
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
  std::shared_ptr<internal::FirstTimeBitmapWriter> writer;
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
                     const Datum& right, std::unique_ptr<IsInKernelImpl>* out) {
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
    return Status::NotImplemented("IsIn is not implemented for ", type->ToString());
  }
  RETURN_NOT_OK(kernel->ConstructRight(ctx, right));
  *out = std::move(kernel);
  return Status::OK();
}

Status IsIn(FunctionContext* ctx, const Datum& left, const Datum& right, Datum* out) {
  DCHECK(left.type()->Equals(right.type()));
  std::vector<Datum> outputs;
  std::unique_ptr<IsInKernelImpl> lkernel;

  RETURN_NOT_OK(GetIsInKernel(ctx, left.type(), right, &lkernel));
  detail::PrimitiveAllocatingUnaryKernel kernel(lkernel.get());
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, &kernel, left, &outputs));

  *out = detail::WrapDatumsLike(left, outputs);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
