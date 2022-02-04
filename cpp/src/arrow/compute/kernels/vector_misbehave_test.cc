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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

struct VectorReAllocValidBufExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // allocate new buffers even though we've promised not to
    ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[0], ctx->AllocateBitmap(8));
    return Status::OK();
  }
};

struct VectorReAllocDataBufExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // allocate new buffers even though we've promised not to
    ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[1], ctx->Allocate(64));
    return Status::OK();
  }
};

void AddVectorMisbehaveKernel(const std::shared_ptr<VectorFunction>& Vector_function,
                              Status (*kernel_exec)(KernelContext*, const ExecBatch&,
                                                    Datum*)) {
  VectorKernel kernel({int32()}, int32(), kernel_exec);
  kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::PREALLOCATE;
  kernel.can_write_into_slices = true;
  kernel.can_execute_chunkwise = false;
  kernel.output_chunked = false;

  DCHECK_OK(Vector_function->AddKernel(std::move(kernel)));
}

const FunctionDoc misbehave_doc{
    "Test kernel that does nothing but allocate memory "
    "while it shouldn't",
    "This Kernel only exists for testing purposes.\n"
    "It allocates memory while it promised not to \n"
    "(because of MemAllocation::PREALLOCATE).",
    {}};

TEST(Misbehave, ReallocValidBufferVectorKernel) {
  ExecContext ctx;
  auto func = std::make_shared<VectorFunction>("vector_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  AddVectorMisbehaveKernel(func, VectorReAllocValidBufExec::Exec);
  Datum datum(ChunkedArray(ArrayVector{}, int32()));
  const std::vector<Datum>& args = {datum};
  const FunctionOptions* options = nullptr;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: "
                         "Pre-allocated validity buffer was modified "
                         "in function kernel"),
      func->Execute(args, options, &ctx));
}

TEST(Misbehave, ReallocDataBufferVectorKernel) {
  ExecContext ctx;
  auto func = std::make_shared<VectorFunction>("vector_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  AddVectorMisbehaveKernel(func, VectorReAllocDataBufExec::Exec);
  Datum datum(ChunkedArray(ArrayVector{}, int32()));
  const std::vector<Datum>& args = {datum};
  const FunctionOptions* options = nullptr;
  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid,
                                  testing::HasSubstr("Invalid: "
                                                     "Unauthorized memory allocations "
                                                     "in function kernel"),
                                  func->Execute(args, options, &ctx));
}
}  // namespace compute
}  // namespace arrow
