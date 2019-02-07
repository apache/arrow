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

#include <utility>

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/aggregate.h"

namespace arrow {
namespace compute {

// Helper class that properly invokes destructor when state goes out of scope.
class ManagedAggregateState {
 public:
  ManagedAggregateState(std::shared_ptr<AggregateFunction>& desc,
                        std::shared_ptr<Buffer>&& buffer)
      : desc_(desc), state_(buffer) {
    desc_->New(state_->mutable_data());
  }

  ~ManagedAggregateState() { desc_->Delete(state_->mutable_data()); }

  void* mutable_data() { return state_->mutable_data(); }

  static std::shared_ptr<ManagedAggregateState> Make(
      std::shared_ptr<AggregateFunction>& desc, MemoryPool* pool) {
    std::shared_ptr<Buffer> buf;
    if (!AllocateBuffer(pool, desc->Size(), &buf).ok()) return nullptr;

    return std::make_shared<ManagedAggregateState>(desc, std::move(buf));
  }

 private:
  std::shared_ptr<AggregateFunction> desc_;
  std::shared_ptr<Buffer> state_;
};

Status AggregateUnaryKernel::Call(FunctionContext* ctx, const Datum& input, Datum* out) {
  if (!input.is_array()) return Status::Invalid("AggregateKernel expects Array datum");

  auto state = ManagedAggregateState::Make(aggregate_function_, ctx->memory_pool());
  if (!state) return Status::OutOfMemory("AggregateState allocation failed");

  auto array = input.make_array();
  RETURN_NOT_OK(aggregate_function_->Consume(*array, state->mutable_data()));
  RETURN_NOT_OK(aggregate_function_->Finalize(state->mutable_data(), out));

  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
