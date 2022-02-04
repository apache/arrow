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
#include "arrow/compute/api_scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

struct ScalarReAllocValidBufExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // allocate a validity buffer even though we've promised not to
    ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[0], ctx->AllocateBitmap(8));
    return Status::OK();
  }
};

struct ScalarReAllocDataBufExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // allocate a validity buffer even though we've promised not to
    ARROW_ASSIGN_OR_RAISE(out->mutable_array()->buffers[1], ctx->Allocate(64));
    return Status::OK();
  }
};

const FunctionDoc misbehave_doc{
    "Test kernel that does nothing but allocate memory "
    "while it shouldn't",
    "This Kernel only exists for testing purposes.\n"
    "It allocates memory while it promised not to \n"
    "(because of MemAllocation::PREALLOCATE).",
    {}};

TEST(Misbehave, ReallocValidBufferScalarKernel) {
  ExecContext ctx;
  auto func = std::make_shared<ScalarFunction>("scalar_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)},
                            OutputType(ValueDescr(fixed_size_binary(2))),
                            ScalarReAllocValidBufExec::Exec));
  Datum datum(ArrayFromJSON(fixed_size_binary(6), R"(["123456"])"));
  const std::vector<Datum>& args = {datum};
  const FunctionOptions* options = nullptr;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid,
      testing::HasSubstr("Invalid: "
                         "Pre-allocated validity buffer was modified "
                         "in function kernel"),
      func->Execute(args, options, &ctx));
}

TEST(Misbehave, ReallocDataBufferScalarKernel) {
  ExecContext ctx;
  auto func = std::make_shared<ScalarFunction>("scalar_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  DCHECK_OK(func->AddKernel({InputType(Type::FIXED_SIZE_BINARY)},
                            OutputType(ValueDescr(fixed_size_binary(2))),
                            ScalarReAllocDataBufExec::Exec));
  Datum datum(ArrayFromJSON(fixed_size_binary(6), R"(["123456"])"));
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
