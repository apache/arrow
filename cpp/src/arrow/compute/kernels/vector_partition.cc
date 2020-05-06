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

#include <algorithm>
#include <numeric>
#include <utility>

#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {

namespace {

// We need to preserve the options
struct PartitionIndicesState : public KernelState {
  explicit PartitionIndicesState(int64_t pivot) : pivot(pivot) {}
  int64_t pivot;
};

template <typename OutType, typename InType>
struct PartitionIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayType arr(batch[0].array());

    int64_t pivot = checked_cast<const PartitionIndicesState&>(*ctx->state()).pivot;
    if (pivot > arr.length()) {
      ctx->SetStatus(Status::IndexError("NthToIndices index out of bound"));
      return;
    }
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);
    if (pivot == arr.length()) {
      return;
    }
    uint64_t* nulls_begin = out_end;
    if (arr.null_count()) {
      nulls_begin = std::stable_partition(
          out_begin, out_end, [&arr](uint64_t ind) { return !arr.IsNull(ind); });
    }
    auto nth_begin = out_begin + pivot;
    if (nth_begin < nulls_begin) {
      std::nth_element(out_begin, nth_begin, nulls_begin,
                       [&arr](uint64_t left, uint64_t right) {
                         return arr.GetView(left) < arr.GetView(right);
                       });
    }
  }
};

}  // namespace

namespace internal {

// Partition kernel implemented for
//
// * Number types
// * Base binary types

std::unique_ptr<KernelState> InitPartitionIndices(KernelContext*, const Kernel&,
                                                  const FunctionOptions* options) {
  int64_t pivot = static_cast<const PartitionOptions*>(options)->pivot;
  return std::unique_ptr<KernelState>(new PartitionIndicesState(pivot));
}

void RegisterVectorPartitionFunctions(FunctionRegistry* registry) {
  auto func = std::make_shared<VectorFunction>("partition_indices", /*arity=*/1);
  VectorKernel base;
  base.init = InitPartitionIndices;

  // The kernel outputs into preallocated memory and is never null
  base.mem_allocation = MemAllocation::PREALLOCATE;
  base.null_handling = NullHandling::OUTPUT_NOT_NULL;

  for (const auto& ty : codegen::NumericTypes()) {
    base.signature = KernelSignature::Make({ty}, uint64());
    base.exec = codegen::NumericSetReturn<PartitionIndices, UInt64Type>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : codegen::BaseBinaryTypes()) {
    base.signature = KernelSignature::Make({ty}, uint64());
    base.exec = codegen::BaseBinarySetReturn<PartitionIndices, UInt64Type>(*ty);
    DCHECK_OK(func->AddKernel(base));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
