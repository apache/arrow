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
#include <cmath>
#include <iterator>
#include <limits>
#include <numeric>
#include <type_traits>
#include <utility>

#include "arrow/array/data.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

namespace {

struct VectorMisbehave {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    auto out_arr = out->mutable_array();
    ARROW_ASSIGN_OR_RAISE(out_arr->buffers[0], ctx->Allocate(64));
    //return Status::NotImplemented("This kernel only exists for testing purposes");
    return Status::OK();
  }
};

const FunctionDoc misbehave_doc{"Test kernel that does nothing but allocate memory "
                                "while it shouldn't",
                                ("This Kernel only exists for testing purposes.\n"
                                 "It allocates memory while it promised not to \n"
                                 "(because of MemAllocation::PREALLOCATE)."),
                                {}};

}  // namespace

void RegisterVectorMisbehave(FunctionRegistry* registry) {
  // The kernel outputs into preallocated memory and is never null
  VectorKernel base;
  base.mem_allocation = MemAllocation::PREALLOCATE;
  base.null_handling = NullHandling::COMPUTED_PREALLOCATE;

  auto func = std::make_shared<VectorFunction>(
      "vector_misbehave", Arity::Nullary(), &misbehave_doc);
  // TODO: Does this need to be defined? Not for scalar kernels, it seems
  //  base.init = PartitionNthToIndicesState::Init;
  base.signature = KernelSignature::Make({}, uint64());
  base.exec = VectorMisbehave::Exec;
  DCHECK_OK(func->AddKernel(base));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
