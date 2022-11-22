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

#include <cstdint>
#include <memory>
#include <random>
#include <utility>
#include <vector>
#include "arrow/config.h"
#include "arrow/status.h"

#ifdef ARROW_WITH_QPL
#include "qpl/qpl.h"
#include "qpl/qpl.hpp"

namespace arrow {
namespace util {
namespace internal {

/// QplJobHWPool is resource pool to provide the job objects, which is
/// used for storing context information during.
/// Memory for QPL job will be allocated when the QPLJobHWPool instance is created
///
//  QPL job can offload RLE-decoding/Filter/(De)compression works to hardware accelerator.
class QplJobHWPool {
 public:
  static QplJobHWPool& GetInstance();

  /// Acquire QPL job
  ///
  /// @param job_id QPL job id, used when release QPL job
  /// \return Pointer to the QPL job. If acquire job failed, return nullptr.
  qpl_job* AcquireJob(uint32_t& job_id);

  /// \brief Release QPL job by the job_id.
  void ReleaseJob(uint32_t job_id);

  /// \brief Return if the QPL job is allocated sucessfully.
  const bool& job_ready() { return iaa_job_ready; }

 private:
  QplJobHWPool();
  ~QplJobHWPool();
  bool tryLockJob(uint32_t index);
  void unLockJob(uint32_t index);
  arrow::Status AllocateQPLJob();

  /// Max jobs in QPL_JOB_POOL
  static constexpr auto MAX_JOB_NUMBER = 512;
  /// Entire buffer for storing all job objects
  static std::unique_ptr<uint8_t[]> hw_jobs_buffer;
  /// Job pool for storing all job object pointers
  static std::array<qpl_job*, MAX_JOB_NUMBER> hw_job_ptr_pool;
  /// Locks for accessing each job object pointers
  static std::array<std::atomic_bool, MAX_JOB_NUMBER> job_ptr_locks;
  static bool iaa_job_ready;
  std::mt19937 random_engine;
  std::uniform_int_distribution<int> distribution;
};
}  //  namespace internal
}  //  namespace util
}  //  namespace arrow
#endif
