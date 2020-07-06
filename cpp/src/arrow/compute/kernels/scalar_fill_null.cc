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

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_writer.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitBlockCounter;

namespace compute {
namespace internal {

namespace {

template <typename OutType, typename InType, typename Enable = void>
struct FillNullFunctor {};

template <typename OutType, typename InType>
struct FillNullFunctor<OutType, InType, enable_if_t<is_number_type<InType>::value>> {
  using value_type = typename TypeTraits<InType>::CType;
  using BuilderType = typename TypeTraits<OutType>::BuilderType;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const Datum& in_arr = batch[0];
    const Datum& fill_value = batch[1];

    if (!in_arr.is_arraylike()) {
      ctx->SetStatus(Status::Invalid("Values must be Array or ChunkedArray"));
    }
    if (!fill_value.is_scalar()) {
      ctx->SetStatus(Status::Invalid("fill value must be a scalar"));
    }

    ctx->SetStatus(Fill(ctx, *in_arr.array(), *fill_value.scalar(), out));
  }

  static Status Fill(KernelContext* ctx, const ArrayData& data, const Scalar& fill_value,
                     Datum* out) {
    value_type value = UnboxScalar<InType>::Unbox(fill_value);
    ArrayData* output = out->mutable_array();

    if (data.null_count != 0 && fill_value.is_valid) {
      BuilderType builder(data.type, ctx->memory_pool());
      RETURN_NOT_OK(builder.Reserve(data.length));

      RETURN_NOT_OK(VisitArrayDataInline<InType>(
          data, [&](value_type v) { return builder.Append(v); },
          [&]() { return builder.Append(value); }));

      std::shared_ptr<Array> output_array;
      RETURN_NOT_OK(builder.Finish(&output_array));
      *output = std::move(*output_array->data());

    } else {
      *output = data;
    }
    return Status::OK();
  }
};

template <typename OutType, typename InType>
struct FillNullFunctor<OutType, InType, enable_if_t<is_boolean_type<InType>::value>> {
  using value_type = typename TypeTraits<InType>::CType;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const Datum& in_arr = batch[0];
    const Datum& fill_value = batch[1];

    if (!in_arr.is_arraylike()) {
      ctx->SetStatus(Status::Invalid("Values must be Array or ChunkedArray"));
    }
    if (!fill_value.is_scalar()) {
      ctx->SetStatus(Status::Invalid("fill value must be a scalar"));
    }

    ctx->SetStatus(Fill(ctx, *in_arr.array(), *fill_value.scalar(), out));
  }

  static Status Fill(KernelContext* ctx, const ArrayData& data, const Scalar& fill_value,
                     Datum* out) {
    value_type value = UnboxScalar<InType>::Unbox(fill_value);
    ArrayData* output = out->mutable_array();

    if (data.null_count != 0 && fill_value.is_valid) {
      int64_t position = 0;
      const uint8_t* bitmap = data.buffers[1]->data();
      const uint8_t* bitmap_validity = output->buffers[0]->data();
      auto length = data.length;
      auto offset = data.offset;

      BooleanBuilder builder(data.type, ctx->memory_pool());
      RETURN_NOT_OK(builder.Reserve(length));
      BitBlockCounter bit_counter(bitmap_validity, offset, length);
      while (position < length) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.AllSet()) {
          for (int64_t i = 0; i < block.length; ++i, ++position) {
            if (BitUtil::GetBit(bitmap, offset + position)) {
              RETURN_NOT_OK(builder.Append(true));
            } else {
              RETURN_NOT_OK(builder.Append(false));
            }
          }
        } else if (block.NoneSet()) {
          for (int64_t i = 0; i < block.length; ++i, ++position) {
            RETURN_NOT_OK(builder.Append(value));
          }
        } else {
          for (int64_t i = 0; i < block.length; ++i, ++position) {
            if (BitUtil::GetBit(bitmap_validity, offset + position)) {
              if (BitUtil::GetBit(bitmap, offset + position)) {
                RETURN_NOT_OK(builder.Append(true));
              } else {
                RETURN_NOT_OK(builder.Append(false));
              }
            } else {
              RETURN_NOT_OK(builder.Append(value));
            }
          }
        }
      }
      std::shared_ptr<Array> output_array;
      RETURN_NOT_OK(builder.Finish(&output_array));
      *output = std::move(*output_array->data());
    } else {
      *output = data;
    }
    return Status::OK();
  }
};

template <typename OutType, typename InType>
struct FillNullFunctor<OutType, InType, enable_if_t<is_null_type<InType>::value>> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const Datum& in_arr = batch[0];
    const Datum& fill_value = batch[1];

    if (!in_arr.is_arraylike()) {
      ctx->SetStatus(Status::Invalid("Values must be Array or ChunkedArray"));
    }
    if (!fill_value.is_scalar()) {
      ctx->SetStatus(Status::Invalid("fill value must be a scalar"));
    }

    ctx->SetStatus(Fill(ctx, *in_arr.array(), *fill_value.scalar(), out));
  }

  static Status Fill(KernelContext* ctx, const ArrayData& data, const Scalar& fill_value,
                     Datum* out) {
    ArrayData* output = out->mutable_array();
    *output = data;
    return Status::OK();
  }
};

template <template <typename...> class Generator>
ArrayKernelExec GeneratePhysicalIntegerSameType(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return Generator<Int8Type, Int8Type>::Exec;
    case Type::INT16:
      return Generator<Int16Type, Int16Type>::Exec;
    case Type::INT32:
    case Type::DATE32:
    case Type::TIME32:
      return Generator<Int32Type, Int32Type>::Exec;
    case Type::INT64:
    case Type::DATE64:
    case Type::TIMESTAMP:
    case Type::TIME64:
    case Type::DURATION:
      return Generator<Int64Type, Int64Type>::Exec;
    case Type::UINT8:
      return Generator<UInt8Type, UInt8Type>::Exec;
    case Type::UINT16:
      return Generator<UInt16Type, UInt16Type>::Exec;
    case Type::UINT32:
      return Generator<UInt32Type, UInt32Type>::Exec;
    case Type::UINT64:
      return Generator<UInt64Type, UInt64Type>::Exec;
    case Type::BOOL:
      return Generator<BooleanType, BooleanType>::Exec;
    case Type::DOUBLE:
      return Generator<DoubleType, DoubleType>::Exec;
    case Type::FLOAT:
      return Generator<FloatType, FloatType>::Exec;
    case Type::NA:
      return Generator<NullType, NullType>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

void AddBasicFillNullKernels(ScalarKernel kernel, ScalarFunction* func) {
  auto AddKernels = [&](const std::vector<std::shared_ptr<DataType>>& types) {
    for (const std::shared_ptr<DataType>& ty : types) {
      auto exec = GeneratePhysicalIntegerSameType<FillNullFunctor>(*ty);
      DCHECK_OK(func->AddKernel({InputType::Array(ty), InputType::Scalar(ty)}, ty,
                                std::move(exec)));
    }
  };

  AddKernels(NumericTypes());
  AddKernels(TemporalTypes());
  AddKernels({boolean(), null()});
}

}  // namespace

void RegisterScalarFillNull(FunctionRegistry* registry) {
  {
    ScalarKernel fill_null_base;
    fill_null_base.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    fill_null_base.mem_allocation = MemAllocation::NO_PREALLOCATE;
    auto fill_null = std::make_shared<ScalarFunction>("fill_null", Arity::Binary());
    AddBasicFillNullKernels(fill_null_base, fill_null.get());
    DCHECK_OK(registry->AddFunction(fill_null));
  }
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
