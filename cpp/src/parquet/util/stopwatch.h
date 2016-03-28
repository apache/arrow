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

#ifndef PARQUET_UTIL_STOPWATCH_H
#define PARQUET_UTIL_STOPWATCH_H

#include <stdio.h>
#include <sys/time.h>

#include <iostream>
#include <ctime>

namespace parquet {

class StopWatch {
 public:
  StopWatch() {
  }

  void Start() {
    gettimeofday(&start_time, 0);
  }

  // Returns time in nanoseconds.
  uint64_t Stop() {
    struct timeval t_time;
    gettimeofday(&t_time, 0);

    return (1000L * 1000L * 1000L * (t_time.tv_sec - start_time.tv_sec)
                   + (t_time.tv_usec - start_time.tv_usec));
  }

 private:
  struct timeval  start_time;
};

} // namespace parquet

#endif
