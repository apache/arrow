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
  static void Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    checked_cast<BooleanScalar*>(out)->value = in.is_valid;
  }

  static void Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
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
      return;
    }

    // Input has no nulls => output is entirely true.
    KERNEL_ASSIGN_OR_RAISE(out->buffers[1], ctx,
                           ctx->AllocateBitmap(out->length + out->offset));
    BitUtil::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length, true);
  }
};

struct IsNullOperator {
  static void Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    checked_cast<BooleanScalar*>(out)->value = !in.is_valid;
  }

  static void Call(KernelContext* ctx, const ArrayData& arr, ArrayData* out) {
    if (arr.MayHaveNulls()) {
      // Input has nulls => output is the inverted null (validity) bitmap.
      InvertBitmap(arr.buffers[0]->data(), arr.offset, arr.length,
                   out->buffers[1]->mutable_data(), out->offset);
      return;
    }

    // Input has no nulls => output is entirely false.
    BitUtil::SetBitsTo(out->buffers[1]->mutable_data(), out->offset, out->length, false);
  }
};

void MakeFunction(std::string name, std::vector<InputType> in_types, OutputType out_type,
                  ArrayKernelExec exec, FunctionRegistry* registry,
                  MemAllocation::type mem_allocation, bool can_write_into_slices) {
  Arity arity{static_cast<int>(in_types.size())};
  auto func = std::make_shared<ScalarFunction>(name, arity);

  ScalarKernel kernel(std::move(in_types), out_type, exec);
  kernel.null_handling = NullHandling::OUTPUT_NOT_NULL;
  kernel.can_write_into_slices = can_write_into_slices;
  kernel.mem_allocation = mem_allocation;

  DCHECK_OK(func->AddKernel(std::move(kernel)));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void IsValidExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const Datum& arg0 = batch[0];
  if (arg0.type()->id() == Type::NA) {
    auto false_value = std::make_shared<BooleanScalar>(false);
    if (arg0.kind() == Datum::SCALAR) {
      out->value = false_value;
    } else {
      std::shared_ptr<Array> false_values;
      KERNEL_RETURN_IF_ERROR(
          ctx, MakeArrayFromScalar(*false_value, out->length(), ctx->memory_pool())
                   .Value(&false_values));
      out->value = false_values->data();
    }
  } else {
    applicator::SimpleUnary<IsValidOperator>(ctx, batch, out);
  }
}

void IsNullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
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
  } else {
    applicator::SimpleUnary<IsNullOperator>(ctx, batch, out);
  }
}

}  // namespace

void RegisterScalarValidity(FunctionRegistry* registry) {
  MakeFunction("is_valid", {ValueDescr::ANY}, boolean(), IsValidExec, registry,
               MemAllocation::NO_PREALLOCATE, /*can_write_into_slices=*/false);

  MakeFunction("is_null", {ValueDescr::ANY}, boolean(), IsNullExec, registry,
               MemAllocation::PREALLOCATE,
               /*can_write_into_slices=*/true);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
