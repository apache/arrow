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

#include <exception>
#include "parquet/exception.h"

#ifdef ENABLE_QPL_ANALYSIS
#include "parquet/qpl_job_pool.h"

namespace parquet {

std::array<qpl_job *, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::hw_job_ptr_pool;
std::array<std::atomic_bool, QplJobHWPool::MAX_JOB_NUMBER> QplJobHWPool::job_ptr_locks;
bool QplJobHWPool::job_pool_ready = false;
std::unique_ptr<uint8_t[]> QplJobHWPool::hw_jobs_buffer;

QplJobHWPool & QplJobHWPool::instance() {
    static QplJobHWPool pool;
    return pool;
}

QplJobHWPool::QplJobHWPool()
    : random_engine(std::random_device()())
    , distribution(0, MAX_JOB_NUMBER - 1) {
    uint32_t job_size = 0;

    /// Get size required for saving a single qpl job object
    qpl_get_job_size(qpl_path_hardware, &job_size);
    /// Allocate entire buffer for storing all job objects
    hw_jobs_buffer = std::make_unique<uint8_t[]>(job_size * MAX_JOB_NUMBER);
    /// Initialize pool for storing all job object pointers
    /// Reallocate buffer by shifting address offset for each job object.
    for (uint32_t index = 0; index < MAX_JOB_NUMBER; ++index) {
        qpl_job * qpl_job_ptr = reinterpret_cast<qpl_job *>(hw_jobs_buffer.get() + index * job_size);
        if (qpl_init_job(qpl_path_hardware, qpl_job_ptr) != QPL_STS_OK) {
            job_pool_ready = false;
            throw ParquetException("Initialization of QPL hardware failed." +
                "Please check if IAA is properly set up");
            return;
        }
        hw_job_ptr_pool[index] = qpl_job_ptr;
        unLockJob(index);
    }

    job_pool_ready = true;
}

QplJobHWPool::~QplJobHWPool() {
    for (uint32_t i = 0; i < MAX_JOB_NUMBER; ++i) {
        if (hw_job_ptr_pool[i]) {
            qpl_fini_job(hw_job_ptr_pool[i]);
            hw_job_ptr_pool[i] = nullptr;
        }
    }
    job_pool_ready = false;
}



qpl_job * QplJobHWPool::acquireJob(uint32_t & job_id) {
    if (isJobPoolReady()) {
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
        assert(index < MAX_JOB_NUMBER);
        return hw_job_ptr_pool[index];
    } else {
        return nullptr;
    }
}

void QplJobHWPool::releaseJob(uint32_t job_id) {
    if (isJobPoolReady())
        unLockJob(MAX_JOB_NUMBER - job_id);
}

bool QplJobHWPool::tryLockJob(uint32_t index) {
    bool expected = false;
    assert(index < MAX_JOB_NUMBER);
    return job_ptr_locks[index].compare_exchange_strong(expected, true);
}

void QplJobHWPool::unLockJob(uint32_t index) {
    assert(index < MAX_JOB_NUMBER);
    job_ptr_locks[index].store(false);
}
} // end namespace parquet

#endif
