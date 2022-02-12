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
#include "arrow/util/tracing_internal.h"

namespace arrow {
namespace util {
namespace tracing {

#ifdef ARROW_WITH_OPENTELEMETRY

TEST(Tracing, Attach) {
  std::thread task([] {
    // If this next line is commented out then the test will emit tsan
    // errors because, when the main thread exits, the OT infrastructure
    // will be torn down.  By grabbing a handle we tie the lifetime of OT
    // to the thread so that OT will not shutdown until after the thread.
    auto handle = ::arrow::internal::tracing::Attach();
    Span span;
    START_SPAN(span, "Test");
    SleepFor(0.1);
  });
  // We don't detach our threads in Arrow but we don't control their
  // lifetime since they are tied to static thread pools.  So detaching
  // here allows us to simulate that.
  task.detach();
}

#endif

}  // namespace tracing
}  // namespace util
}  // namespace arrow