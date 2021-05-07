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
    if (arr.type->id() == Type::NA) {
      // Input is all nulls => output is entirely false.
      ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                            ctx->AllocateBitmap(out->length + out->offset));
      BitUtil::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length,
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

struct IsNullOperator {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    checked_cast<BooleanScalar*>(out)->value = !in.is_valid;
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
    if (arr.type->id() == Type::NA) {
      // Input is all nulls => output is entirely true.
      ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                            ctx->AllocateBitmap(out->length + out->offset));
      BitUtil::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length, true);
      return Status::OK();
    }

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
    out->buffers[1] = out->buffers[0];
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
                  MemAllocation::type mem_allocation, bool can_write_into_slices,
                  NullHandling::type null_handling = NullHandling::OUTPUT_NOT_NULL) {
  Arity arity{static_cast<int>(in_types.size())};
  auto func = std::make_shared<ScalarFunction>(name, arity, doc);

  ScalarKernel kernel(std::move(in_types), out_type, exec);
  kernel.null_handling = null_handling;
  kernel.can_write_into_slices = can_write_into_slices;
  kernel.mem_allocation = mem_allocation;

  DCHECK_OK(func->AddKernel(std::move(kernel)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

template <typename InType>
void AddIsNanKernel(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  DCHECK_OK(
      func->AddKernel({ty}, boolean(),
                      applicator::ScalarUnary<BooleanType, InType, IsNanOperator>::Exec));
}

std::shared_ptr<ScalarFunction> MakeIsNanFunction(std::string name,
                                                  const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  AddIsNanKernel<FloatType>(float32(), func.get());
  AddIsNanKernel<DoubleType>(float64(), func.get());

  return func;
}

Status IsValidExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return applicator::SimpleUnary<IsValidOperator>(ctx, batch, out);
}

Status IsNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return applicator::SimpleUnary<IsNullOperator>(ctx, batch, out);
}

Status TrueUnlessNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  return applicator::SimpleUnary<TrueUnlessNullOperator>(ctx, batch, out);
}

const FunctionDoc is_valid_doc(
    "Return true if non-null",
    ("For each input value, emit true iff the value is valid (non-null)."), {"values"});

const FunctionDoc is_null_doc("Return true if null",
                              ("For each input value, emit true iff the value is null."),
                              {"values"});

const FunctionDoc true_unless_null_doc("Return true if non-null, else return null",
                                       ("For each input value, emit true iff the value "
                                        "is valid (non-null), otherwise emit null."),
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

  MakeFunction("true_unless_null", &true_unless_null_doc, {ValueDescr::ANY}, boolean(),
               TrueUnlessNullExec, registry, MemAllocation::NO_PREALLOCATE,
               /*can_write_into_slices=*/false,
               /*null_handling=*/NullHandling::INTERSECTION);

  DCHECK_OK(registry->AddFunction(MakeIsNanFunction("is_nan", &is_nan_doc)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
