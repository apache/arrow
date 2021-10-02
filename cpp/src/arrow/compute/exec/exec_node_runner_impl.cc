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
#include "arrow/compute/exec/exec_node_runner_impl.h"

#include <functional>
#include <memory>
#include <vector>

#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/util.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/async_util.h"
#include "arrow/util/cancel.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace compute {

class SimpleSyncRunnerImpl : public ExecNodeRunnerImpl {
 public:
  explicit SimpleSyncRunnerImpl(ExecContext* ctx) : ctx_(ctx) {}

  Status SubmitTask(std::function<Status()> task) override {
    if (finished_.is_finished()) {
      return Status::OK();
    }
    auto status = task();
    if (!status.ok()) {
      return status;
    }
    if (input_counter_.Increment()) {
      this->MarkFinished();
    }
    return Status::OK();
  }

  void MarkFinished(Status status = Status::OK()) override {
    this->finished_.MarkFinished(status);
  }

  void InputFinished(int total_batches) override {
    if (input_counter_.SetTotal(total_batches)) {
      this->MarkFinished();
    }
  }

  void StopProducing() override {
    if (input_counter_.Cancel()) {
      this->MarkFinished();
    }
  }

 protected:
  // Executor context
  ExecContext* ctx_;

  // Counter for the number of batches received
  AtomicCounter input_counter_;
};

class SimpleParallelRunner : public SimpleSyncRunnerImpl {
 public:
  explicit SimpleParallelRunner(ExecContext* ctx) : SimpleSyncRunnerImpl(ctx) {
    executor_ = ctx->executor();
  }

  Status SubmitTask(std::function<Status()> task) override {
    if (finished_.is_finished()) {
      return Status::OK();
    }
    auto status = task_group_.AddTask([this, task]() -> Result<Future<>> {
      return this->executor_->Submit(this->stop_source_.token(),
                                     [task]() { return task(); });
    });
    if (!status.ok()) {
      return status;
    }
    if (input_counter_.Increment()) {
      this->MarkFinished();
    }
    return Status::OK();
  }

  void MarkFinished(Status status = Status::OK()) override {
    if (!status.ok()) {
      this->StopProducing();
      if (input_counter_.Cancel()) {
        this->MarkFinished();
      }
      return;
    }
    task_group_.End().AddCallback([this](const Status& status) {
      if (!status.ok()) {
        this->finished_.MarkFinished();
      } else if (!this->finished_.is_finished()) {
        this->finished_.MarkFinished(status);
      }
    });
  }

  void StopProducing() override {
    this->stop_source_.RequestStop();
    if (input_counter_.Cancel()) {
      this->MarkFinished();
    }
  }

 protected:
  // The task group for the corresponding batches
  util::AsyncTaskGroup task_group_;

  // Executor
  ::arrow::internal::Executor* executor_;

  // Variable used to cancel remaining tasks in the executor
  StopSource stop_source_;
};

Result<std::unique_ptr<ExecNodeRunnerImpl>> ExecNodeRunnerImpl::MakeSimpleSyncRunner(
    ExecContext* ctx) {
  std::unique_ptr<ExecNodeRunnerImpl> impl{new SimpleSyncRunnerImpl(ctx)};
  return std::move(impl);
}

Result<std::unique_ptr<ExecNodeRunnerImpl>> ExecNodeRunnerImpl::MakeSimpleParallelRunner(
    ExecContext* ctx) {
  std::unique_ptr<ExecNodeRunnerImpl> impl{new SimpleParallelRunner(ctx)};
  return std::move(impl);
}

}  // namespace compute
}  // namespace arrow
