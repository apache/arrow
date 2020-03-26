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
 public:
  std::shared_ptr<DataType> out_type() const override { return int32(); }

  virtual Status Init(const Datum& needles) = 0;
};

template <typename Type, typename Scalar>
class MatchKernel : public MatchKernelImpl {
 public:
  MatchKernel(std::shared_ptr<DataType> type, MemoryPool* pool)
      : type_(std::move(type)), pool_(pool) {}

  Status Call(FunctionContext* ctx, const Datum& haystack, Datum* out) override {
    if (!haystack.is_arraylike()) {
      return Status::Invalid("Haystack input to match kernel was not array-like");
    }

    Int32Builder indices_builder;
    RETURN_NOT_OK(indices_builder.Reserve(haystack.length()));

    auto lookup_value = [&](util::optional<Scalar> v) {
      if (v.has_value()) {
        // check if value in haystack array is in the needles_table_
        if (needles_table_->Get(*v) != -1) {
          // matching needle; output index from needles_table_
          indices_builder.UnsafeAppend(needles_table_->Get(*v));
        } else {
          // no matching needle; output null
          indices_builder.UnsafeAppendNull();
        }
      } else {
        if (needles_table_->GetNull() != -1) {
          // needles include null; output index from needles_table_
          indices_builder.UnsafeAppend(needles_table_->GetNull());
        } else {
          // needles do not include null; output null
          indices_builder.UnsafeAppendNull();
        }
      }
    };

    if (haystack.kind() == Datum::ARRAY) {
      VisitArrayDataInline<Type>(*haystack.array(), lookup_value);
    }

    if (haystack.kind() == Datum::CHUNKED_ARRAY) {
      for (const auto& chunk : haystack.chunked_array()->chunks()) {
        VisitArrayDataInline<Type>(*chunk->data(), lookup_value);
      }
    }

    std::shared_ptr<ArrayData> out_data;
    RETURN_NOT_OK(indices_builder.FinishInternal(&out_data));
    out->value = std::move(out_data);
    return Status::OK();
  }

  Status Init(const Datum& needles) override {
    if (!needles.is_arraylike()) {
      return Status::Invalid("Needles input to match kernel was not array-like");
    }

    needles_table_.reset(new MemoTable(pool_, 0));

    auto insert_value = [&](util::optional<Scalar> v) {
      if (v.has_value()) {
        int32_t unused_memo_index;
        return needles_table_->GetOrInsert(*v, &unused_memo_index);
      }
      needles_table_->GetOrInsertNull();
      return Status::OK();
    };

    if (needles.kind() == Datum::ARRAY) {
      return VisitArrayDataInline<Type>(*needles.array(), insert_value);
    }

    for (const auto& chunk : needles.chunked_array()->chunks()) {
      RETURN_NOT_OK(VisitArrayDataInline<Type>(*chunk->data(), insert_value));
    }
    return Status::OK();
  }

 protected:
  using MemoTable = typename HashTraits<Type>::MemoTableType;
  std::unique_ptr<MemoTable> needles_table_;
  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;
};

// ----------------------------------------------------------------------
// (NullType has a separate implementation)

class NullMatchKernel : public MatchKernelImpl {
 public:
  NullMatchKernel(const std::shared_ptr<DataType>& type, MemoryPool* pool) {}

  Status Call(FunctionContext* ctx, const Datum& haystack, Datum* out) override {
    if (!haystack.is_arraylike()) {
      return Status::Invalid("Haystack input to match kernel was not array-like");
    }

    Int32Builder indices_builder;
    if (haystack.length() != 0) {
      if (needles_null_count_ == 0) {
        RETURN_NOT_OK(indices_builder.AppendNulls(haystack.length()));
      } else {
        RETURN_NOT_OK(indices_builder.Reserve(haystack.length()));

        for (int64_t i = 0; i < haystack.length(); ++i) {
          indices_builder.UnsafeAppend(0);
        }
      }
    }

    std::shared_ptr<ArrayData> out_data;
    RETURN_NOT_OK(indices_builder.FinishInternal(&out_data));
    out->value = std::move(out_data);
    return Status::OK();
  }

  Status Init(const Datum& needles) override {
    if (!needles.is_arraylike()) {
      return Status::Invalid("Needles input to match kernel was not array-like");
    }

    needles_null_count_ = needles.length();
    return Status::OK();
  }

 private:
  int64_t needles_null_count_{};
};

// ----------------------------------------------------------------------
// Kernel wrapper for generic hash table kernels

template <typename Type, typename Enable = void>
struct MatchKernelTraits;

template <>
struct MatchKernelTraits<NullType> {
  using MatchKernelImpl = NullMatchKernel;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_has_c_type<Type>> {
  using MatchKernelImpl = MatchKernel<Type, typename Type::c_type>;
};

template <>
struct MatchKernelTraits<BooleanType> {
  using MatchKernelImpl = MatchKernel<BooleanType, bool>;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_base_binary<Type>> {
  using MatchKernelImpl = MatchKernel<Type, util::string_view>;
};

template <typename Type>
struct MatchKernelTraits<Type, enable_if_fixed_size_binary<Type>> {
  using MatchKernelImpl = MatchKernel<Type, util::string_view>;
};

Status GetMatchKernel(FunctionContext* ctx, const std::shared_ptr<DataType>& type,
                      const Datum& needles, std::unique_ptr<MatchKernelImpl>* out) {
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
  RETURN_NOT_OK(kernel->Init(needles));
  *out = std::move(kernel);
  return Status::OK();
}

Status Match(FunctionContext* ctx, const Datum& haystack, const Datum& needles,
             Datum* out) {
  DCHECK(haystack.type()->Equals(needles.type()));
  std::vector<Datum> outputs;
  std::unique_ptr<MatchKernelImpl> kernel;

  RETURN_NOT_OK(GetMatchKernel(ctx, haystack.type(), needles, &kernel));
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, kernel.get(), haystack, &outputs));

  *out = detail::WrapDatumsLike(haystack, kernel->out_type(), outputs);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
