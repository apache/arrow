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

#include <thread>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace util {
namespace tracing {

#ifdef ARROW_WITH_OPENTELEMETRY

TEST(Tracing, Attach) {
  ASSERT_OK(::arrow::internal::GetCpuThreadPool()->Spawn([] {
    // This thread will outlive the main test thread.
    Span span;
    START_SPAN(span, "Test");
    SleepFor(0.1);
  }));
}

#endif

}  // namespace tracing
}  // namespace util
}  // namespace arrow
