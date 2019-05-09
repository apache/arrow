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

using arrow::internal::BitmapReader;
using internal::checked_cast;
using internal::DictionaryTraits;
using internal::HashTraits;
namespace compute {

#define CHECK_IMPLEMENTED(KERNEL, FUNCNAME, TYPE)                                       \
  if (!KERNEL) {                                                                        \
    return Status::NotImplemented(FUNCNAME, " not implemented for ", type->ToString()); \
  }

class HashBinaryKernelImpl : public BinaryKernel {
  virtual Status Compute(FunctionContext* ctx, const ArrayData& left,
                         const ArrayData& right, std::shared_ptr<ArrayData>& out) = 0;

 public:
  Status Call(FunctionContext* ctx, const Datum& left, const Datum& right,
              Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, right.kind());
    DCHECK_EQ(Datum::ARRAY, left.kind());

    const ArrayData& left_data = *left.array();
    const ArrayData& right_data = *right.array();

    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(Compute(ctx, left_data, right_data, result));

    out->value = std::move(result);
    return Status::OK();
  }
  std::shared_ptr<DataType> out_type() const override { return boolean(); }
};

template <typename Type, typename Scalar>
struct MemoTableRight {
 public:
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

template <typename T, typename Scalar>
class IsInKernel : public HashBinaryKernelImpl {
 public:
  IsInKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  Status VisitNull() {
    if (right_null_count != 0) {
      bool_builder_.UnsafeAppend(true);
    } else {
      bool_builder_.UnsafeAppend(false);
    }
    return Status::OK();
  }

  Status VisitValue(const Scalar& value) {
    if (memo_table_->Get(value) != -1) {
      bool_builder_.UnsafeAppend(true);
    } else {
      bool_builder_.UnsafeAppend(false);
    }
    return Status::OK();
  }

  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 std::shared_ptr<ArrayData>& out) override {
    bool_builder_.Reset();
    bool_builder_.Reserve(left.length);

    MemoTableRight<T, Scalar> func;
    func.Append(right);
    memo_table_ = std::move(func.memo_table_);

    right_null_count = right.null_count;
    ArrayDataVisitor<T>::Visit(left, this);

    RETURN_NOT_OK(bool_builder_.FinishInternal(&out));
    return Status::OK();
  }

 protected:
  using MemoTable = typename HashTraits<T>::MemoTableType;
  std::unique_ptr<MemoTable> memo_table_;
  int64_t right_null_count;

 private:
  BooleanBuilder bool_builder_;
};

class NullIsInKernel : public HashBinaryKernelImpl {
 public:
  NullIsInKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  Status Compute(FunctionContext* ctx, const ArrayData& left, const ArrayData& right,
                 std::shared_ptr<ArrayData>& out) override {
    bool_builder_.Reset();
    bool_builder_.Reserve(left.length);

    if (left.null_count != 0 && right.null_count != 0) {
      for (int64_t i = 0; i < left.length; ++i) {
        bool_builder_.UnsafeAppend(true);
      }
    } else if (left.null_count != 0 && right.null_count == 0) {
      for (int64_t i = 0; i < left.length; ++i) {
        bool_builder_.UnsafeAppend(false);
      }
    }
    RETURN_NOT_OK(bool_builder_.FinishInternal(&out));
    return Status::OK();
  }

 private:
  BooleanBuilder bool_builder_;
};

template <typename Type, typename Enable = void>
struct HashBinaryKernelTraits {};

template <typename Type>
struct HashBinaryKernelTraits<Type, enable_if_null<Type>> {
  using HashBinaryKernelImpl = NullIsInKernel;
};

template <typename Type>
struct HashBinaryKernelTraits<Type, enable_if_has_c_type<Type>> {
  using HashBinaryKernelImpl = IsInKernel<Type, typename Type::c_type>;
};

template <typename Type>
struct HashBinaryKernelTraits<Type, enable_if_boolean<Type>> {
  using HashBinaryKernelImpl = IsInKernel<Type, bool>;
};

template <typename Type>
struct HashBinaryKernelTraits<Type, enable_if_binary<Type>> {
  using HashBinaryKernelImpl = IsInKernel<Type, util::string_view>;
};

template <typename Type>
struct HashBinaryKernelTraits<Type, enable_if_fixed_size_binary<Type>> {
  using HashBinaryKernelImpl = IsInKernel<Type, util::string_view>;
};

Status GetIsInKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                     std::unique_ptr<HashBinaryKernelImpl>* out) {
  std::unique_ptr<HashBinaryKernelImpl> kernel;

#define ISIN_CASE(InType)                                                           \
  case InType::type_id:                                                             \
    kernel.reset(new typename HashBinaryKernelTraits<InType>::HashBinaryKernelImpl( \
        type, ctx->memory_pool()));                                                 \
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

  CHECK_IMPLEMENTED(kernel, "isin", type);
  *out = std::move(kernel);
  return Status::OK();
}

Status IsIn(FunctionContext* context, const Datum& left, const Datum& right, Datum* out) {
  DCHECK(left.type()->Equals(right.type()));
  std::unique_ptr<HashBinaryKernelImpl> isin_kernel;
  RETURN_NOT_OK(GetIsInKernel(context, right.type(), &isin_kernel));

  detail::PrimitiveAllocatingBinaryKernel kernel(isin_kernel.get());
  out->value = ArrayData::Make(isin_kernel->out_type(), (left.make_array())->length());
  kernel.Call(context, left, right, out);

  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
