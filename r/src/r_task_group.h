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

#if defined(ARROW_R_WITH_ARROW)

#include <arrow/util/parallel.h>
#include <arrow/util/task_group.h>

namespace arrow {
namespace r {

class RTasks {
 public:
  using Task = internal::FnOnce<Status()>;

  explicit RTasks(bool use_threads);

  // This Finish() method must never be called from a thread pool thread
  // as this would deadlock.
  //
  // Usage is to :
  // - create an RTasks instance on the main thread
  // - add some tasks with .Append()
  // - and then call .Finish() so that the parallel tasks are finished
  Status Finish();
  void Append(bool parallel, Task&& task);

  void Reset();

  bool use_threads_;
  StopSource stop_source_;
  std::shared_ptr<arrow::internal::TaskGroup> parallel_tasks_;
  std::vector<Task> delayed_serial_tasks_;
};

}  // namespace r
}  // namespace arrow

#endif
