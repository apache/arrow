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

#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/array/builder_union.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"

namespace arrow {

namespace compute {
namespace internal {

namespace {

template <typename AllocateMem>
struct ScalarMisbehaveExec {
    static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
      // allocate a buffer even though we've promised not to
      ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[0],
                            ctx->Allocate(64));
      //return Status::NotImplemented("This kernel only exists for testing purposes");
      // The function should return OK, otherwise the buffer check is not performed
      return Status::OK();
    }
};

struct ScalarMisbehaveFunction : ScalarFunction {
    using ScalarFunction::ScalarFunction;
};

void AddScalarMisbehaveKernels(const std::shared_ptr<ScalarFunction>& scalar_function) {
    ScalarKernel kernel({}, null(),
                        ScalarMisbehaveExec</*AllocateMem=*/std::false_type>::Exec);
    kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::PREALLOCATE;
    kernel.can_write_into_slices = true;

    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
} // namespace

const FunctionDoc misbehave_doc{"Test kernel that does nothing but allocate memory "
                                "while it shouldn't",
                                ("This Kernel only exists for testing purposes.\n"
                                 "It allocates memory while it promised not to \n"
                                 "(because of MemAllocation::PREALLOCATE)."),
                                {}};

void RegisterScalarMisbehave(FunctionRegistry* registry) {
auto func =
        std::make_shared<ScalarMisbehaveFunction>("misbehave", Arity::Nullary(),
                                                  &misbehave_doc);

  AddScalarMisbehaveKernels(func);
DCHECK_OK(registry->AddFunction(std::move(func)));
//TODO: We want to prevent people from actually using this kernel.
// Can we add the function only for testing?
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
