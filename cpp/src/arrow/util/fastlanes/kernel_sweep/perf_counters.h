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

// Self-process hardware counters (instructions, cycles) via perf_event_open.
// Requires /proc/sys/kernel/perf_event_paranoid <= 2 for same-uid,
// self-process monitoring (no CAP_PERFMON needed at that level).
#pragma once

#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <cstring>

inline long perf_event_open(struct perf_event_attr* attr, pid_t pid, int cpu, int group_fd,
                             unsigned long flags) {
  return syscall(__NR_perf_event_open, attr, pid, cpu, group_fd, flags);
}

class PerfCounters {
 public:
  PerfCounters() {
    fd_instructions_ = open_counter(PERF_COUNT_HW_INSTRUCTIONS, -1);
    fd_cycles_ = open_counter(PERF_COUNT_HW_CPU_CYCLES, fd_instructions_);
    ok_ = fd_instructions_ >= 0 && fd_cycles_ >= 0;
    if (!ok_) {
      std::fprintf(stderr,
                    "perf_event_open failed (errno=%d): counters unavailable, "
                    "check /proc/sys/kernel/perf_event_paranoid\n",
                    errno);
    }
  }

  ~PerfCounters() {
    if (fd_instructions_ >= 0) close(fd_instructions_);
    if (fd_cycles_ >= 0) close(fd_cycles_);
  }

  bool ok() const { return ok_; }

  void start() {
    ioctl(fd_cycles_, PERF_EVENT_IOC_RESET, PERF_IOC_FLAG_GROUP);
    ioctl(fd_cycles_, PERF_EVENT_IOC_ENABLE, PERF_IOC_FLAG_GROUP);
  }

  void stop(uint64_t* instructions, uint64_t* cycles) {
    ioctl(fd_cycles_, PERF_EVENT_IOC_DISABLE, PERF_IOC_FLAG_GROUP);
    read(fd_instructions_, instructions, sizeof(uint64_t));
    read(fd_cycles_, cycles, sizeof(uint64_t));
  }

 private:
  static int open_counter(uint64_t config, int group_fd) {
    struct perf_event_attr attr;
    std::memset(&attr, 0, sizeof(attr));
    attr.type = PERF_TYPE_HARDWARE;
    attr.size = sizeof(attr);
    attr.config = config;
    attr.disabled = 1;
    attr.exclude_kernel = 1;
    attr.exclude_hv = 1;
    // exclude_idle is left unset (0): this container's virtualized PMU
    // returns EOPNOTSUPP for perf_event_open when it is set to 1, even
    // though plain hardware counting works fine otherwise.
    return static_cast<int>(perf_event_open(&attr, 0 /*self*/, -1 /*any cpu*/, group_fd, 0));
  }

  int fd_instructions_ = -1;
  int fd_cycles_ = -1;
  bool ok_ = false;
};
