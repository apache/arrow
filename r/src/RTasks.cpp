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

#include "./r_task_group.h"

namespace arrow {
namespace r {

RTasks::RTasks(bool use_threads)
    : use_threads_(use_threads),
      stop_source_(),
      parallel_tasks_(use_threads
                          ? arrow::internal::TaskGroup::MakeThreaded(
                                arrow::internal::GetCpuThreadPool(), stop_source_.token())
                          : nullptr) {}

Status RTasks::Finish() {
  Status status = Status::OK();

  // run the delayed tasks now
  for (auto& task : delayed_serial_tasks_) {
    status &= std::move(task)();
    if (!status.ok()) {
      stop_source_.RequestStop();
      break;
    }
  }

  // then wait for the parallel tasks to finish
  if (use_threads_) {
    status &= parallel_tasks_->Finish();
  }

  return status;
}

void RTasks::Append(bool parallel, RTasks::Task&& task) {
  if (parallel && use_threads_) {
    parallel_tasks_->Append(std::move(task));
  } else {
    delayed_serial_tasks_.push_back(std::move(task));
  }
}

void RTasks::Reset() {
  delayed_serial_tasks_.clear();

  stop_source_.Reset();
  if (use_threads_) {
    parallel_tasks_ = arrow::internal::TaskGroup::MakeThreaded(
        arrow::internal::GetCpuThreadPool(), stop_source_.token());
  }
}

}  // namespace r
}  // namespace arrow
