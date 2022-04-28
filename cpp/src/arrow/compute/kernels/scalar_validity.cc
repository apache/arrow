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

#include <cmath>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::CopyBitmap;
using internal::InvertBitmap;

namespace compute {
namespace internal {
namespace {

struct IsValidOperator {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    checked_cast<BooleanScalar*>(out)->value = in.is_valid;
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
    if (arr.type->id() == Type::NA) {
      // Input is all nulls => output is entirely false.
      ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                            ctx->AllocateBitmap(out->length + out->offset));
      bit_util::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length,
                          false);
      return Status::OK();
    }

    DCHECK_EQ(out->offset, 0);
    DCHECK_LE(out->length, arr.length);
    if (arr.MayHaveNulls()) {
      // Input has nulls => output is the null (validity) bitmap.
      // To avoid copying the null bitmap, slice from the starting byte offset
      // and set the offset to the remaining bit offset.
      out->offset = arr.offset % 8;
      out->buffers[1] =
          arr.offset == 0
              ? arr.buffers[0]
              : SliceBuffer(arr.buffers[0], arr.offset / 8,
                            bit_util::BytesForBits(out->length + out->offset));
      return Status::OK();
    }

    // Input has no nulls => output is entirely true.
    ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                          ctx->AllocateBitmap(out->length + out->offset));
    bit_util::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length, true);
    return Status::OK();
  }
};

struct IsFiniteOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    return std::isfinite(value);
  }
};

struct IsInfOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    return std::isinf(value);
  }
};

using NanOptionsState = OptionsWrapper<NullOptions>;

struct IsNullOperator {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& options = NanOptionsState::Get(ctx);
    bool* out_value = &checked_cast<BooleanScalar*>(out)->value;

    if (in.is_valid) {
      if (options.nan_is_null && is_floating(in.type->id())) {
        switch (in.type->id()) {
          case Type::FLOAT:
            *out_value = std::isnan(internal::UnboxScalar<FloatType>::Unbox(in));
            break;
          case Type::DOUBLE:
            *out_value = std::isnan(internal::UnboxScalar<DoubleType>::Unbox(in));
            break;
          default:
            return Status::NotImplemented("NaN detection not implemented for type ",
                                          in.type->ToString());
        }
      } else {
        *out_value = false;
      }
    } else {
      *out_value = true;
    }

    return Status::OK();
  }

  template <typename T>
  static void SetNanBits(const ArrayData& arr, uint8_t* out_bitmap, int64_t out_offset) {
    const T* data = arr.GetValues<T>(1);
    for (int64_t i = 0; i < arr.length; ++i) {
      if (std::isnan(data[i])) {
        bit_util::SetBit(out_bitmap, i + out_offset);
      }
    }
  }

  static Status Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
    const auto& options = NanOptionsState::Get(ctx);

    uint8_t* out_bitmap = out->buffers[1]->mutable_data();
    if (arr.GetNullCount() > 0) {
      // Input has nulls => output is the inverted null (validity) bitmap.
      InvertBitmap(arr.buffers[0]->data(), arr.offset, arr.length, out_bitmap,
                   out->offset);
    } else {
      // Input has no nulls => output is entirely false.
      bit_util::SetBitsTo(out_bitmap, out->offset, out->length, false);
    }

    if (is_floating(arr.type->id()) && options.nan_is_null) {
      switch (arr.type->id()) {
        case Type::FLOAT:
          SetNanBits<float>(arr, out_bitmap, out->offset);
          break;
        case Type::DOUBLE:
          SetNanBits<double>(arr, out_bitmap, out->offset);
          break;
        default:
          return Status::NotImplemented("NaN detection not implemented for type ",
                                        arr.type->ToString());
      }
    }
    return Status::OK();
  }
};

struct TrueUnlessNullOperator {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    checked_cast<BooleanScalar*>(out)->is_valid = in.is_valid;
    checked_cast<BooleanScalar*>(out)->value = true;
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
    // NullHandling::INTERSECTION with a single input means the execution engine
    // has already reused or allocated a null_bitmap which can be reused as the values
    // buffer.
    if (out->buffers[0]) {
      out->buffers[1] = out->buffers[0];
    } else {
      // But for all-valid inputs, the engine will skip allocating a
      // buffer; we have to allocate one ourselves
      ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->AllocateBitmap(arr.length));
      std::memset(out->buffers[1]->mutable_data(), 0xFF, out->buffers[1]->size());
    }
    return Status::OK();
  }
};

struct IsNanOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    return std::isnan(value);
  }
};

void MakeFunction(std::string name, const FunctionDoc* doc,
                  std::vector<InputType> in_types, OutputType out_type,
                  ArrayKernelExec exec, FunctionRegistry* registry,
                  MemAllocation::type mem_allocation, NullHandling::type null_handling,
                  bool can_write_into_slices,
                  const FunctionOptions* default_options = NULLPTR,
                  KernelInit init = NULLPTR) {
  Arity arity{static_cast<int>(in_types.size())};
  auto func = std::make_shared<ScalarFunction>(name, arity, doc, default_options);

  ScalarKernel kernel(std::move(in_types), out_type, exec, init);
  kernel.null_handling = null_handling;
  kernel.can_write_into_slices = can_write_into_slices;
  kernel.mem_allocation = mem_allocation;

  DCHECK_OK(func->AddKernel(std::move(kernel)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <typename InType, typename Op>
void AddFloatValidityKernel(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  DCHECK_OK(func->AddKernel({ty}, boolean(),
                            applicator::ScalarUnary<BooleanType, InType, Op>::Exec));
}

template <bool kConstant>
Status ConstBoolExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (batch.values[0].is_scalar()) {
    checked_cast<BooleanScalar*>(out->scalar().get())->value = kConstant;
    return Status::OK();
  }
  ArrayData* array = out->mutable_array();
  bit_util::SetBitsTo(array->buffers[1]->mutable_data(), array->offset, array->length,
                      kConstant);
  return Status::OK();
}

std::shared_ptr<ScalarFunction> MakeIsFiniteFunction(std::string name,
                                                     const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddFloatValidityKernel<FloatType, IsFiniteOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsFiniteOperator>(float64(), func.get());

  for (const auto& ty : IntTypes()) {
    DCHECK_OK(func->AddKernel({InputType(ty->id())}, boolean(), ConstBoolExec<true>));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::NA)}, boolean(), ConstBoolExec<true>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL128)}, boolean(), ConstBoolExec<true>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL256)}, boolean(), ConstBoolExec<true>));

  return func;
}

std::shared_ptr<ScalarFunction> MakeIsInfFunction(std::string name,
                                                  const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddFloatValidityKernel<FloatType, IsInfOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsInfOperator>(float64(), func.get());

  for (const auto& ty : IntTypes()) {
    DCHECK_OK(func->AddKernel({InputType(ty->id())}, boolean(), ConstBoolExec<false>));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::NA)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL128)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL256)}, boolean(), ConstBoolExec<false>));

  return func;
}

std::shared_ptr<ScalarFunction> MakeIsNanFunction(std::string name,
                                                  const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddFloatValidityKernel<FloatType, IsNanOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsNanOperator>(float64(), func.get());

  for (const auto& ty : IntTypes()) {
    DCHECK_OK(func->AddKernel({InputType(ty->id())}, boolean(), ConstBoolExec<false>));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::NA)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL128)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL256)}, boolean(), ConstBoolExec<false>));

  return func;
}

Status IsValidExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return applicator::SimpleUnary<IsValidOperator>(ctx, batch, out);
}

Status IsNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const Datum& arg0 = batch[0];
  if (arg0.type()->id() == Type::NA) {
    if (arg0.kind() == Datum::SCALAR) {
      out->value = std::make_shared<BooleanScalar>(true);
    } else {
      // Data is preallocated
      ArrayData* out_arr = out->mutable_array();
      bit_util::SetBitsTo(out_arr->buffers[1]->mutable_data(), out_arr->offset,
                          out_arr->length, true);
    }
    return Status::OK();
  } else {
    return applicator::SimpleUnary<IsNullOperator>(ctx, batch, out);
  }
}

Status TrueUnlessNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return applicator::SimpleUnary<TrueUnlessNullOperator>(ctx, batch, out);
}

const FunctionDoc is_valid_doc(
    "Return true if non-null",
    ("For each input value, emit true iff the value is valid (i.e. non-null)."),
    {"values"});

const FunctionDoc is_finite_doc(
    "Return true if value is finite",
    ("For each input value, emit true iff the value is finite\n"
     "(i.e. neither NaN, inf, nor -inf)."),
    {"values"});

const FunctionDoc is_inf_doc(
    "Return true if infinity",
    ("For each input value, emit true iff the value is infinite (inf or -inf)."),
    {"values"});

const FunctionDoc is_null_doc(
    "Return true if null (and optionally NaN)",
    ("For each input value, emit true iff the value is null.\n"
     "True may also be emitted for NaN values by setting the `nan_is_null` flag."),
    {"values"}, "NullOptions");

const FunctionDoc true_unless_null_doc("Return true if non-null, else return null",
                                       ("For each input value, emit true iff the value\n"
                                        "is valid (non-null), otherwise emit null."),
                                       {"values"});

const FunctionDoc is_nan_doc("Return true if NaN",
                             ("For each input value, emit true iff the value is NaN."),
                             {"values"});

}  // namespace

void RegisterScalarValidity(FunctionRegistry* registry) {
  static auto kNullOptions = NullOptions::Defaults();
  MakeFunction("is_valid", &is_valid_doc, {ValueDescr::ANY}, boolean(), IsValidExec,
               registry, MemAllocation::NO_PREALLOCATE, NullHandling::OUTPUT_NOT_NULL,
               /*can_write_into_slices=*/false);

  MakeFunction("is_null", &is_null_doc, {ValueDescr::ANY}, boolean(), IsNullExec,
               registry, MemAllocation::PREALLOCATE, NullHandling::OUTPUT_NOT_NULL,
               /*can_write_into_slices=*/true, &kNullOptions, NanOptionsState::Init);

  MakeFunction("true_unless_null", &true_unless_null_doc, {ValueDescr::ANY}, boolean(),
               TrueUnlessNullExec, registry, MemAllocation::NO_PREALLOCATE,
               NullHandling::INTERSECTION,
               /*can_write_into_slices=*/false);

  DCHECK_OK(registry->AddFunction(MakeIsFiniteFunction("is_finite", &is_finite_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsInfFunction("is_inf", &is_inf_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsNanFunction("is_nan", &is_nan_doc)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
