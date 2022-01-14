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

struct ScalarMisbehaveExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // allocate a buffer even though we've promised not to
    ARROW_ASSIGN_OR_RAISE(auto buffer, ctx->Allocate(64));
    ARROW_ASSIGN_OR_RAISE(auto nullbitmap, ctx->AllocateBitmap(8));
    BufferVector buffers{nullbitmap, buffer};
    auto array = std::make_shared<ArrayData>(int64(), 8, buffers);
    *out = Datum(array);
    return Status::OK();
  }
};

void AddScalarMisbehaveKernels(const std::shared_ptr<ScalarFunction>& scalar_function) {
  DCHECK_OK(scalar_function->AddKernel({InputType(Type::FIXED_SIZE_BINARY)},
                                       OutputType(ValueDescr(fixed_size_binary(2))),
                                       ScalarMisbehaveExec::Exec));
}

const FunctionDoc misbehave_doc{
    "Test kernel that does nothing but allocate memory "
    "while it shouldn't",
    "This Kernel only exists for testing purposes.\n"
    "It allocates memory while it promised not to \n"
    "(because of MemAllocation::PREALLOCATE).",
    {}};

std::shared_ptr<const ScalarFunction> CreateScalarMisbehaveFunction() {
  auto func = std::make_shared<ScalarFunction>("scalar_misbehave", Arity::Unary(),
                                               &misbehave_doc);
  AddScalarMisbehaveKernels(func);
  return func;
}

TEST(Misbehave, MisbehavingScalarKernel) {
  ExecContext ctx;
  auto func = CreateScalarMisbehaveFunction();
  Datum datum(ArrayFromJSON(fixed_size_binary(6), R"(["123456"])"));
  const std::vector<Datum>& args = {datum};
  const FunctionOptions* options = nullptr;
  EXPECT_RAISES_WITH_MESSAGE_THAT(ExecutionError,
                                  testing::HasSubstr("ExecutionError in Gandiva: "
                                                     "Unauthorized memory allocations "
                                                     "in function kernel"),
                                  func->Execute(args, options, &ctx));
}
}  // namespace compute
}  // namespace arrow
