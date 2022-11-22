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

#include "arrow/util/qpl_job_pool.h"
#include "arrow/status.h"

#ifdef ARROW_WITH_QPL

namespace arrow {
namespace util {
namespace internal {

std::array<qpl_job*, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::hw_job_ptr_pool;
std::array<std::atomic_bool, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::job_ptr_locks;
bool QplJobHWPool::iaa_job_ready = false;
std::unique_ptr<uint8_t[]> QplJobHWPool::hw_jobs_buffer;

QplJobHWPool& QplJobHWPool::GetInstance() {
  static QplJobHWPool pool;
  return pool;
}

QplJobHWPool::QplJobHWPool()
    : random_engine(std::random_device()()), distribution(0, MAX_JOB_NUMBER - 1) {
  (void)AllocateQPLJob();
}

QplJobHWPool::~QplJobHWPool() {
  for (uint32_t i = 0; i < MAX_JOB_NUMBER; ++i) {
    if (hw_job_ptr_pool[i]) {
      qpl_fini_job(hw_job_ptr_pool[i]);
      hw_job_ptr_pool[i] = nullptr;
    }
  }
  iaa_job_ready = false;
}

arrow::Status QplJobHWPool::AllocateQPLJob() {
  uint32_t job_size = 0;

  /// Get size required for saving a single qpl job object
  qpl_get_job_size(qpl_path_hardware, &job_size);
  /// Allocate entire buffer for storing all job objects
  hw_jobs_buffer = std::make_unique<uint8_t[]>(job_size * MAX_JOB_NUMBER);
  /// Initialize pool for storing all job object pointers
  /// Reallocate buffer by shifting address offset for each job object.
  for (uint32_t index = 0; index < MAX_JOB_NUMBER; ++index) {
    qpl_job* qpl_job_ptr =
        reinterpret_cast<qpl_job*>(hw_jobs_buffer.get() + index * job_size);
    if (qpl_init_job(qpl_path_hardware, qpl_job_ptr) != QPL_STS_OK) {
      iaa_job_ready = false;
      return arrow::Status::Invalid(
          "Initialization of hardware IAA failed."
          " Please check if Intel In-Memory Analytics Accelerator (IAA) "
          "is properly set up!");
    }
    hw_job_ptr_pool[index] = qpl_job_ptr;
    job_ptr_locks[index].store(false);
  }

  iaa_job_ready = true;
  return arrow::Status::OK();
}

qpl_job* QplJobHWPool::AcquireJob(uint32_t& job_id) {
  if (!job_ready()) {
    return nullptr;
  }
  uint32_t retry = 0;
  auto index = distribution(random_engine);
  while (!tryLockJob(index)) {
    index = distribution(random_engine);
    retry++;
    if (retry > MAX_JOB_NUMBER) {
      return nullptr;
    }
  }
  job_id = MAX_JOB_NUMBER - index;
  if (index >= MAX_JOB_NUMBER) {
    return nullptr;
  }
  return hw_job_ptr_pool[index];
}

void QplJobHWPool::ReleaseJob(uint32_t job_id) {
  if (job_ready()) {
    job_ptr_locks[MAX_JOB_NUMBER - job_id].store(false);
  }
}

bool QplJobHWPool::tryLockJob(uint32_t index) {
  bool expected = false;
  if (index >= MAX_JOB_NUMBER) {
    return false;
  }
  return job_ptr_locks[index].compare_exchange_strong(expected, true);
}

void QplJobHWPool::unLockJob(uint32_t index) {
  if (index >= MAX_JOB_NUMBER) {
    return;
  }
  job_ptr_locks[index].store(false);
}
}  // namespace internal
}  // namespace util
}  // namespace arrow
#endif
