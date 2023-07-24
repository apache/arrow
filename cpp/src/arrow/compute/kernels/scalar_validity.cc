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

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::CopyBitmap;
using internal::InvertBitmap;

namespace compute {
namespace internal {
namespace {

Status IsValidExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& arr = batch[0].array;
  ArraySpan* out_span = out->array_span_mutable();
  if (arr.type->id() == Type::NA) {
    // Input is all nulls => output is entirely false.
    bit_util::SetBitsTo(out_span->buffers[1].data, out_span->offset, out_span->length,
                        false);
    return Status::OK();
  }

  DCHECK_EQ(out_span->offset, 0);
  DCHECK_LE(out_span->length, arr.length);
  if (arr.MayHaveNulls()) {
    // We could do a zero-copy optimization, but it isn't worth the added complexity
    ::arrow::internal::CopyBitmap(arr.buffers[0].data, arr.offset, arr.length,
                                  out_span->buffers[1].data, out_span->offset);
  } else {
    // Input has no nulls => output is entirely true.
    bit_util::SetBitsTo(out_span->buffers[1].data, out_span->offset, out_span->length,
                        true);
  }
  return Status::OK();
}

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

template <typename T>
static void SetNanBits(const ArraySpan& arr, uint8_t* out_bitmap, int64_t out_offset) {
  const T* data = arr.GetValues<T>(1);
  for (int64_t i = 0; i < arr.length; ++i) {
    if (std::isnan(data[i])) {
      bit_util::SetBit(out_bitmap, i + out_offset);
    }
  }
}

Status IsNullExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& arr = batch[0].array;
  ArraySpan* out_span = out->array_span_mutable();
  if (arr.type->id() == Type::NA) {
    bit_util::SetBitsTo(out_span->buffers[1].data, out_span->offset, out_span->length,
                        true);
    return Status::OK();
  }

  const auto& options = NanOptionsState::Get(ctx);
  uint8_t* out_bitmap = out_span->buffers[1].data;
  if (arr.GetNullCount() > 0) {
    // Input has nulls => output is the inverted null (validity) bitmap.
    InvertBitmap(arr.buffers[0].data, arr.offset, arr.length, out_bitmap,
                 out_span->offset);
  } else {
    // Input has no nulls => output is entirely false.
    bit_util::SetBitsTo(out_bitmap, out_span->offset, out_span->length, false);
  }

  if (is_floating(arr.type->id()) && options.nan_is_null) {
    switch (arr.type->id()) {
      case Type::FLOAT:
        SetNanBits<float>(arr, out_bitmap, out_span->offset);
        break;
      case Type::DOUBLE:
        SetNanBits<double>(arr, out_bitmap, out_span->offset);
        break;
      default:
        return Status::NotImplemented("NaN detection not implemented for type ",
                                      arr.type->ToString());
    }
  }
  return Status::OK();
}

struct IsNanOperator {
  template <typename OutType, typename InType>
  static constexpr OutType Call(KernelContext*, const InType& value, Status*) {
    return std::isnan(value);
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

std::shared_ptr<ScalarFunction> MakeIsInfFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc));

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

std::shared_ptr<ScalarFunction> MakeIsNanFunction(std::string name, FunctionDoc doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc));

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

Status TrueUnlessNullExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ArraySpan* out_span = out->array_span_mutable();
  if (out_span->buffers[0].data) {
    // If there is a validity bitmap computed above the kernel
    // invocation, we copy it to the output buffers
    ::arrow::internal::CopyBitmap(out_span->buffers[0].data, out_span->offset,
                                  out_span->length, out_span->buffers[1].data,
                                  out_span->offset);
  } else {
    // But for all-valid inputs, the engine will skip allocating a
    // validity bitmap, so we set everything to true
    bit_util::SetBitsTo(out_span->buffers[1].data, out_span->offset, out_span->length,
                        true);
  }
  return Status::OK();
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
               TrueUnlessNullExec, registry, NullHandling::INTERSECTION,
               /*can_write_into_slices=*/false);

  DCHECK_OK(registry->AddFunction(MakeIsFiniteFunction("is_finite", is_finite_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsInfFunction("is_inf", is_inf_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsNanFunction("is_nan", is_nan_doc)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
