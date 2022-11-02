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
#include <utility>
#include <vector>
#include <iostream>
#include <random>

#ifdef ENABLE_QPL_ANALYSIS
#include <qpl/qpl.hpp>
#include <qpl/qpl.h>


namespace parquet {

/// QplJobHWPool is resource pool to provide the job objects.
/// Job object is used for storing context information during
//  offloading compression job to HW Accelerator.
class QplJobHWPool {
 public:
    QplJobHWPool();
    ~QplJobHWPool();

    static QplJobHWPool & instance();

    qpl_job * acquireJob(uint32_t & job_id);
    static void releaseJob(uint32_t job_id);
    static const bool & isJobPoolReady() { return job_pool_ready; }

 private:
    static bool tryLockJob(uint32_t index);
    static void unLockJob(uint32_t index);

    /// Maximum jobs running in parallel supported by IAA hardware
    static constexpr auto MAX_JOB_NUMBER = 512;
    /// Entire buffer for storing all job objects
    static std::unique_ptr<uint8_t[]> hw_jobs_buffer;
    /// Job pool for storing all job object pointers
    static std::array<qpl_job *, MAX_JOB_NUMBER> hw_job_ptr_pool;
    /// Locks for accessing each job object pointers
    static std::array<std::atomic_bool, MAX_JOB_NUMBER> job_ptr_locks;
    static bool job_pool_ready;
    std::mt19937 random_engine;
    std::uniform_int_distribution<int> distribution;
};
} // namespace parquet

#endif
