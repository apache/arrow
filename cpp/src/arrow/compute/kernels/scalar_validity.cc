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
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/dict_util_internal.h"
#include "arrow/util/float16.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/union_util.h"

namespace arrow {

using internal::CopyBitmap;
using internal::InvertBitmap;
using util::Float16;

namespace compute {
namespace internal {
namespace {

using NanOptionsState = OptionsWrapper<NullOptions>;

struct IsFiniteOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    if constexpr (std::is_same_v<InType, Float16>) {
      return value.is_finite();
    } else {
      return std::isfinite(value);
    }
  }
};

struct IsInfOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    if constexpr (std::is_same_v<InType, Float16>) {
      return value.is_infinity();
    } else {
      return std::isinf(value);
    }
  }
};

template <typename T>
static void SetNanBits(const ArraySpan& arr, uint8_t* out_bitmap, int64_t out_offset) {
  const T* data = arr.GetValues<T>(1);
  for (int64_t i = 0; i < arr.length; ++i) {
    bool is_nan(false);
    if constexpr (std::is_same_v<T, uint16_t>) {
      is_nan = Float16::FromBits(data[i]).is_nan();
    } else {
      is_nan = std::isnan(data[i]);
    }

    if (is_nan) {
      bit_util::SetBit(out_bitmap, i + out_offset);
    }
  }
}

static Status SetLogicalNullBits(KernelContext* ctx, const ArraySpan& span,
                                 uint8_t* out_bitmap, int64_t out_offset,
                                 bool set_on_null) {
  const Type::type t = span.type->id();
  if (t == Type::NA) {
    // Input is all nulls, so all output bits are the same.
    bit_util::SetBitsTo(out_bitmap, out_offset, span.length, set_on_null);
  } else if (t == Type::SPARSE_UNION) {
    union_util::SetLogicalNullBitsSparse(span, out_bitmap, out_offset, set_on_null);
  } else if (t == Type::DENSE_UNION) {
    union_util::SetLogicalNullBitsDense(span, out_bitmap, out_offset, set_on_null);
  } else if (t == Type::RUN_END_ENCODED) {
    ree_util::SetLogicalNullBits(span, out_bitmap, out_offset, set_on_null);
  } else if (t == Type::DICTIONARY) {
    dict_util::SetLogicalNullBits(span, out_bitmap, out_offset, set_on_null);
  } else {
    // Input is a type for which logical and physical nulls are the same, so we can
    // use GetNullCount() and the validity bitmap
    if (span.GetNullCount() > 0) {
      // Input has nulls. The output is either the validity bitmap or the inverse of the
      // validity bitmap.
      if (set_on_null) {
        InvertBitmap(span.buffers[0].data, span.offset, span.length, out_bitmap,
                     out_offset);
      } else {
        CopyBitmap(span.buffers[0].data, span.offset, span.length, out_bitmap,
                   out_offset);
      }
    } else {
      // Input has no nulls, so all output bits are the same.
      bit_util::SetBitsTo(out_bitmap, out_offset, span.length, !set_on_null);
    }

    // If nan_is_null, we must also check for nans.
    if (is_floating(t) && NanOptionsState::Get(ctx).nan_is_null) {
      switch (t) {
        case Type::FLOAT:
          SetNanBits<float>(span, out_bitmap, out_offset);
          break;
        case Type::DOUBLE:
          SetNanBits<double>(span, out_bitmap, out_offset);
          break;
        case Type::HALF_FLOAT:
          SetNanBits<uint16_t>(span, out_bitmap, out_offset);
          break;
        default:
          return Status::NotImplemented("NaN detection not implemented for type ",
                                        span.type->ToString());
      }
    }
  }
  return Status::OK();
}

Status IsValidExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* out_span = out->array_span_mutable();
  return SetLogicalNullBits(ctx, batch[0].array, out_span->buffers[1].data,
                            out_span->offset, false);
}

Status IsNullExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* out_span = out->array_span_mutable();
  return SetLogicalNullBits(ctx, batch[0].array, out_span->buffers[1].data,
                            out_span->offset, true);
}

struct IsNanOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    if constexpr (std::is_same_v<InType, Float16>) {
      return value.is_nan();
    } else {
      return std::isnan(value);
    }
  }
};

void MakeFunction(std::string name, FunctionDoc doc, std::vector<InputType> in_types,
                  OutputType out_type, ArrayKernelExec exec, FunctionRegistry* registry,
                  NullHandling::type null_handling, bool can_write_into_slices,
                  const FunctionOptions* default_options = NULLPTR,
                  KernelInit init = NULLPTR) {
  Arity arity{static_cast<int>(in_types.size())};
  auto func =
      std::make_shared<ScalarFunction>(name, arity, std::move(doc), default_options);

  ScalarKernel kernel(std::move(in_types), out_type, exec, init);
  kernel.null_handling = null_handling;
  kernel.can_write_into_slices = can_write_into_slices;

  DCHECK_OK(func->AddKernel(std::move(kernel)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <typename InType, typename Op>
void AddFloatValidityKernel(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  DCHECK_OK(func->AddKernel({ty}, boolean(),
                            applicator::ScalarUnary<BooleanType, InType, Op>::Exec));
}

template <bool kConstant>
Status ConstBoolExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* array = out->array_span_mutable();
  bit_util::SetBitsTo(array->buffers[1].data, array->offset, array->length, kConstant);
  return Status::OK();
}

std::shared_ptr<ScalarFunction> MakeIsFiniteFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc));

  AddFloatValidityKernel<FloatType, IsFiniteOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsFiniteOperator>(float64(), func.get());
  AddFloatValidityKernel<HalfFloatType, IsFiniteOperator>(float16(), func.get());

  for (const auto& ty : IntTypes()) {
    DCHECK_OK(func->AddKernel({InputType(ty->id())}, boolean(), ConstBoolExec<true>));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::NA)}, boolean(), ConstBoolExec<true>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL128)}, boolean(), ConstBoolExec<true>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL256)}, boolean(), ConstBoolExec<true>));
  DCHECK_OK(func->AddKernel({InputType(Type::DURATION)}, boolean(), ConstBoolExec<true>));

  return func;
}

std::shared_ptr<ScalarFunction> MakeIsInfFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc));

  AddFloatValidityKernel<FloatType, IsInfOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsInfOperator>(float64(), func.get());
  AddFloatValidityKernel<HalfFloatType, IsInfOperator>(float16(), func.get());

  for (const auto& ty : IntTypes()) {
    DCHECK_OK(func->AddKernel({InputType(ty->id())}, boolean(), ConstBoolExec<false>));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::NA)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL128)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL256)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DURATION)}, boolean(), ConstBoolExec<false>));
  return func;
}

std::shared_ptr<ScalarFunction> MakeIsNanFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc));

  AddFloatValidityKernel<FloatType, IsNanOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsNanOperator>(float64(), func.get());
  AddFloatValidityKernel<HalfFloatType, IsNanOperator>(float16(), func.get());

  for (const auto& ty : IntTypes()) {
    DCHECK_OK(func->AddKernel({InputType(ty->id())}, boolean(), ConstBoolExec<false>));
  }
  DCHECK_OK(func->AddKernel({InputType(Type::NA)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL128)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DECIMAL256)}, boolean(), ConstBoolExec<false>));
  DCHECK_OK(
      func->AddKernel({InputType(Type::DURATION)}, boolean(), ConstBoolExec<false>));

  return func;
}

Status TrueUnlessNullExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // Set all bits in the output's value bitmap to true
  ArraySpan* out_span = out->array_span_mutable();
  bit_util::SetBitsTo(out_span->buffers[1].data, out_span->offset, out_span->length,
                      true);

  // Set the output's validity bitmap based on the nullity of the input array
  // NOTE: alternatively, we could switch this kernel's null handling back to
  // NullHandling::INTERSECTION and change the validity checks in exec.cc so that
  // they correctly handle logical nulls, but that would invove significant changes
  // in exec.cc which might have more side effects
  return SetLogicalNullBits(ctx, batch[0].array, out_span->buffers[0].data,
                            out_span->offset, false);
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
  MakeFunction("is_valid", is_valid_doc, {InputType::Any()}, boolean(), IsValidExec,
               registry, NullHandling::OUTPUT_NOT_NULL,
               /*can_write_into_slices=*/false);

  MakeFunction("is_null", is_null_doc, {InputType::Any()}, boolean(), IsNullExec,
               registry, NullHandling::OUTPUT_NOT_NULL,
               /*can_write_into_slices=*/true, &kNullOptions, NanOptionsState::Init);

  MakeFunction("true_unless_null", true_unless_null_doc, {InputType::Any()}, boolean(),
               TrueUnlessNullExec, registry, NullHandling::COMPUTED_PREALLOCATE,
               /*can_write_into_slices=*/false);

  DCHECK_OK(registry->AddFunction(MakeIsFiniteFunction("is_finite", is_finite_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsInfFunction("is_inf", is_inf_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsNanFunction("is_nan", is_nan_doc)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
