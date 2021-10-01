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

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "arrow/compute/exec/util.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/future.h"

namespace arrow {
namespace compute {

class ExecNodeRunnerImpl {
 public:
  bool Cancel() { return input_counter_.Cancel(); }

  Future<> finished() { return finished_; }

  virtual ~ExecNodeRunnerImpl() = default;

  virtual Status SubmitTask(std::function<Status()> task) = 0;

  virtual void MarkFinished(Status s = Status::OK()) = 0;

  virtual void InputFinished(int total_batches) = 0;

  virtual void StopProducing() = 0;

  static Result<std::unique_ptr<ExecNodeRunnerImpl>> MakeSimpleSyncRunner(
      ExecContext* ctx);

  static Result<std::unique_ptr<ExecNodeRunnerImpl>> MakeSimpleParallelRunner(
      ExecContext* ctx);

 protected:
  // Counter for the number of batches received
  AtomicCounter input_counter_;
  // Future to sync finished
  Future<> finished_ = Future<>::Make();
};

}  // namespace compute
}  // namespace arrow
