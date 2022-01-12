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

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

namespace compute {
namespace internal {

namespace {

template <typename AllocateMem>
struct ScalarMisbehaveExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // allocate a buffer even though we've promised not to
    ARROW_ASSIGN_OR_RAISE(auto buffer, ctx->Allocate(64));
    BufferVector buffers{nullptr, std::move(buffer)};
    auto array = std::make_shared<ArrayData>(int64(), 8, std::move(buffers),
    0);
    *out = array;
    // return Status::NotImplemented("This kernel only exists for testing purposes");
    // The function should return OK, otherwise the buffer check is not performed
    return Status::OK();
  }
};

void AddScalarMisbehaveKernels(const std::shared_ptr<ScalarFunction>& scalar_function) {
//  ScalarKernel kernel({InputType(Type::FIXED_SIZE_BINARY)},
//                      OutputType(ValueDescr(fixed_size_binary(2)))),
//                      ScalarMisbehaveExec</*AllocateMem=*/std::false_type>::Exec);
//  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
//  kernel.mem_allocation = MemAllocation::PREALLOCATE; //is the default
//  kernel.can_write_into_slices = true;

  DCHECK_OK(scalar_function->AddKernel({InputType(Type::FIXED_SIZE_BINARY)},
                                       OutputType(ValueDescr(fixed_size_binary(2))),
            ScalarMisbehaveExec</*AllocateMem=*/std::false_type>::Exec));
}

const FunctionDoc misbehave_doc{
    "Test kernel that does nothing but allocate memory "
    "while it shouldn't",
    "This Kernel only exists for testing purposes.\n"
    "It allocates memory while it promised not to \n"
    "(because of MemAllocation::PREALLOCATE).",
    {}};
}  // namespace

}  // namespace internal
using arrow::compute::internal::AddScalarMisbehaveKernels;
std::shared_ptr<const ScalarFunction> CreateScalarMisbehaveFunction() {
  auto func = std::make_shared<ScalarFunction>("scalar_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  AddScalarMisbehaveKernels(func);
  return func;
}
}  // namespace compute
}  // namespace arrow
