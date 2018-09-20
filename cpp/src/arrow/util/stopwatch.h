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

#include <stdio.h>
#ifndef _MSC_VER
#include <sys/time.h>
#endif

#include <ctime>
#include <iostream>

namespace arrow {

uint64_t CurrentTime() {
  timespec time;
  clock_gettime(CLOCK_MONOTONIC, &time);
  return 1000000000L * time.tv_sec + time.tv_nsec;
}

class StopWatch {
 public:
  StopWatch() {}

  void Start() { start_ = CurrentTime(); }

  // Returns time in nanoseconds.
  uint64_t Stop() { return CurrentTime() - start_; }

 private:
  uint64_t start_;
};

}  // namespace arrow
