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
    DCHECK_EQ(out->offset, 0);
    DCHECK_LE(out->length, arr.length);
    if (arr.MayHaveNulls()) {
      // Input has nulls => output is the null (validity) bitmap.
      // To avoid copying the null bitmap, slice from the starting byte offset
      // and set the offset to the remaining bit offset.
      out->offset = arr.offset % 8;
      out->buffers[1] =
          arr.offset == 0 ? arr.buffers[0]
                          : SliceBuffer(arr.buffers[0], arr.offset / 8,
                                        BitUtil::BytesForBits(out->length + out->offset));
      return Status::OK();
    }

    // Input has no nulls => output is entirely true.
    ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                          ctx->AllocateBitmap(out->length + out->offset));
    BitUtil::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length, true);
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

struct IsNullOperator {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    checked_cast<BooleanScalar*>(out)->value = !in.is_valid;
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
    if (arr.MayHaveNulls()) {
      // Input has nulls => output is the inverted null (validity) bitmap.
      InvertBitmap(arr.buffers[0]->data(), arr.offset, arr.length,
                   out->buffers[1]->mutable_data(), out->offset);
    } else {
      // Input has no nulls => output is entirely false.
      BitUtil::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length,
                         false);
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
                  MemAllocation::type mem_allocation, bool can_write_into_slices) {
  Arity arity{static_cast<int>(in_types.size())};
  auto func = std::make_shared<ScalarFunction>(name, arity, doc);

  ScalarKernel kernel(std::move(in_types), out_type, exec);
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
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

std::shared_ptr<ScalarFunction> MakeIsFiniteFunction(std::string name,
                                                     const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddFloatValidityKernel<FloatType, IsFiniteOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsFiniteOperator>(float64(), func.get());

  return func;
}

std::shared_ptr<ScalarFunction> MakeIsInfFunction(std::string name,
                                                  const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddFloatValidityKernel<FloatType, IsInfOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsInfOperator>(float64(), func.get());

  return func;
}

std::shared_ptr<ScalarFunction> MakeIsNanFunction(std::string name,
                                                  const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddFloatValidityKernel<FloatType, IsNanOperator>(float32(), func.get());
  AddFloatValidityKernel<DoubleType, IsNanOperator>(float64(), func.get());

  return func;
}

Status IsValidExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const Datum& arg0 = batch[0];
  if (arg0.type()->id() == Type::NA) {
    auto false_value = std::make_shared<BooleanScalar>(false);
    if (arg0.kind() == Datum::SCALAR) {
      out->value = false_value;
    } else {
      std::shared_ptr<Array> false_values;
      RETURN_NOT_OK(MakeArrayFromScalar(*false_value, out->length(), ctx->memory_pool())
                        .Value(&false_values));
      out->value = false_values->data();
    }
    return Status::OK();
  } else {
    return applicator::SimpleUnary<IsValidOperator>(ctx, batch, out);
  }
}

Status IsNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const Datum& arg0 = batch[0];
  if (arg0.type()->id() == Type::NA) {
    if (arg0.kind() == Datum::SCALAR) {
      out->value = std::make_shared<BooleanScalar>(true);
    } else {
      // Data is preallocated
      ArrayData* out_arr = out->mutable_array();
      BitUtil::SetBitsTo(out_arr->buffers[1]->mutable_data(), out_arr->offset,
                         out_arr->length, true);
    }
    return Status::OK();
  } else {
    return applicator::SimpleUnary<IsNullOperator>(ctx, batch, out);
  }
}

const FunctionDoc is_valid_doc(
    "Return true if non-null",
    ("For each input value, emit true iff the value is valid (non-null)."), {"values"});

const FunctionDoc is_finite_doc(
    "Return true if value is finite",
    ("For each input value, emit true iff the value is finite (not NaN, inf, or -inf)."),
    {"values"});

const FunctionDoc is_inf_doc(
    "Return true if infinity",
    ("For each input value, emit true iff the value is infinite (inf or -inf)."),
    {"values"});

const FunctionDoc is_null_doc("Return true if null",
                              ("For each input value, emit true iff the value is null."),
                              {"values"});

const FunctionDoc is_nan_doc("Return true if NaN",
                             ("For each input value, emit true iff the value is NaN."),
                             {"values"});

}  // namespace

void RegisterScalarValidity(FunctionRegistry* registry) {
  MakeFunction("is_valid", &is_valid_doc, {ValueDescr::ANY}, boolean(), IsValidExec,
               registry, MemAllocation::NO_PREALLOCATE, /*can_write_into_slices=*/false);

  MakeFunction("is_null", &is_null_doc, {ValueDescr::ANY}, boolean(), IsNullExec,
               registry, MemAllocation::PREALLOCATE,
               /*can_write_into_slices=*/true);

  DCHECK_OK(registry->AddFunction(MakeIsFiniteFunction("is_finite", &is_finite_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsInfFunction("is_inf", &is_inf_doc)));
  DCHECK_OK(registry->AddFunction(MakeIsNanFunction("is_nan", &is_nan_doc)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
