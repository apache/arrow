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

#include "arrow/acero/query_context.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/io_util.h"

namespace arrow {
using arrow::internal::CpuInfo;
namespace acero {

namespace {
io::IOContext GetIoContext(const QueryOptions& opts, const ExecContext& exec_context) {
  if (opts.custom_io_executor == nullptr) {
    return io::IOContext(exec_context.memory_pool());
  }
  return io::IOContext(exec_context.memory_pool(), opts.custom_io_executor);
}
}  // namespace

QueryContext::QueryContext(QueryOptions opts, ExecContext exec_context)
    : options_(std::move(opts)),
      exec_context_(exec_context),
      io_context_(GetIoContext(options_, exec_context_)) {}

const CpuInfo* QueryContext::cpu_info() const { return CpuInfo::GetInstance(); }
int64_t QueryContext::hardware_flags() const { return cpu_info()->hardware_flags(); }

Status QueryContext::Init(size_t max_num_threads, util::AsyncTaskScheduler* scheduler) {
  tld_.resize(max_num_threads);
  async_scheduler_ = scheduler;
  return Status::OK();
}

size_t QueryContext::GetThreadIndex() { return thread_indexer_(); }

size_t QueryContext::max_concurrency() const { return thread_indexer_.Capacity(); }

Result<util::TempVectorStack*> QueryContext::GetTempStack(size_t thread_index) {
  if (!tld_[thread_index].is_init) {
    RETURN_NOT_OK(tld_[thread_index].stack.Init(
        memory_pool(), 8 * util::MiniBatch::kMiniBatchLength * sizeof(uint64_t)));
    tld_[thread_index].is_init = true;
  }
  return &tld_[thread_index].stack;
}

Result<Future<>> QueryContext::BeginExternalTask(std::string_view name) {
  Future<> completion_future = Future<>::Make();
  if (async_scheduler_->AddSimpleTask([completion_future] { return completion_future; },
                                      name)) {
    return completion_future;
  }
  return Future<>{};
}

void QueryContext::ScheduleTask(std::function<Status()> fn, std::string_view name) {
  ::arrow::internal::Executor* exec = executor();
  // Adds a task which submits fn to the executor and tracks its progress.  If we're
  // already stopping then the task is ignored and fn is not executed.
  async_scheduler_->AddSimpleTask(
      [exec, fn = std::move(fn)]() mutable { return exec->Submit(std::move(fn)); }, name);
}

void QueryContext::ScheduleTask(std::function<Status(size_t)> fn, std::string_view name) {
  std::function<Status()> indexed_fn = [this, fn]() {
    size_t thread_index = GetThreadIndex();
    return fn(thread_index);
  };
  ScheduleTask(std::move(indexed_fn), name);
}

void QueryContext::ScheduleIOTask(std::function<Status()> fn, std::string_view name) {
  async_scheduler_->AddSimpleTask(
      [this, fn]() { return io_context_.executor()->Submit(std::move(fn)); }, name);
}

int QueryContext::RegisterTaskGroup(std::function<Status(size_t, int64_t)> task,
                                    std::function<Status(size_t)> on_finished) {
  return task_scheduler_->RegisterTaskGroup(std::move(task), std::move(on_finished));
}

Status QueryContext::StartTaskGroup(int task_group_id, int64_t num_tasks) {
  return task_scheduler_->StartTaskGroup(GetThreadIndex(), task_group_id, num_tasks);
}
}  // namespace acero
}  // namespace arrow
