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

#include <chrono>
#include <cstdint>
#include <iostream>

#include <gtest/gtest.h>

#include "arrow/util/logging.h"

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging_test.cc.

namespace arrow {
namespace util {

int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

// This is not really test.
// This file just print some information using the logging macro.

void PrintLog() {
  ARROW_LOG(DEBUG) << "This is the"
                   << " DEBUG"
                   << " message";
  ARROW_LOG(INFO) << "This is the"
                  << " INFO message";
  ARROW_LOG(WARNING) << "This is the"
                     << " WARNING message";
  ARROW_LOG(ERROR) << "This is the"
                   << " ERROR message";
  ARROW_CHECK(true) << "This is a ARROW_CHECK"
                    << " message but it won't show up";
  // The following 2 lines should not run since it will cause program failure.
  // ARROW_LOG(FATAL) << "This is the FATAL message";
  // ARROW_CHECK(false) << "This is a ARROW_CHECK message but it won't show up";
}

TEST(PrintLogTest, LogTestWithoutInit) {
  // Without ArrowLog::StartArrowLog, this should also work.
  PrintLog();
}

TEST(PrintLogTest, LogTestWithInit) {
  // Test empty app name.
  ArrowLog::StartArrowLog("", ArrowLogLevel::ARROW_DEBUG);
  PrintLog();
  ArrowLog::ShutDownArrowLog();
}

}  // namespace util

TEST(DcheckMacros, DoNotEvaluateReleaseMode) {
#ifdef NDEBUG
  int i = 0;
  auto f1 = [&]() {
    ++i;
    return true;
  };
  DCHECK(f1());
  ASSERT_EQ(0, i);
  auto f2 = [&]() {
    ++i;
    return i;
  };
  DCHECK_EQ(f2(), 0);
  DCHECK_NE(f2(), 0);
  DCHECK_LT(f2(), 0);
  DCHECK_LE(f2(), 0);
  DCHECK_GE(f2(), 0);
  DCHECK_GT(f2(), 0);
  ASSERT_EQ(0, i);
  ARROW_UNUSED(f1);
  ARROW_UNUSED(f2);
#endif
}

}  // namespace arrow

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
