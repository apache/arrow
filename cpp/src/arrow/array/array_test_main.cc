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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>  // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <iostream>

#include <gflags/gflags.h>
#include <gtest/gtest.h>


class TestingTimeUsagePrinter : public testing::EmptyTestEventListener {
 public:
  // Called before a test starts.
  void OnTestSuiteStart(const testing::TestSuite& test_suite) override {
    current_test_started_ = std::chrono::system_clock::now();
  }

  // Called after a test ends.
  void OnTestSuiteEnd(const testing::TestSuite& test_suite) override {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - current_test_started_);
    TestResult result{};
    result.name = test_suite.name();
    result.time_used = ms.count();
    testing_time_ms.push_back(std::move(result));
  }

  struct TestResult {
    std::string name;
    int64_t time_used;
  };

  void PrintTop20Tests() {
    std::sort(testing_time_ms.begin(), testing_time_ms.end(), [](const TestResult& a, const TestResult& b) {
      return a.time_used > b.time_used;
    });
    std::cout << "Top 20 slowest tests:" << std::endl;
    for (int i = 0; i < std::min(20, static_cast<int>(testing_time_ms.size())); ++i) {
      std::cout << "Test: " << testing_time_ms[i].name << " " << testing_time_ms[i].time_used << "(ms)" << std::endl;
    }
  }

  std::vector<TestResult> testing_time_ms;
  std::chrono::time_point<std::chrono::system_clock> current_test_started_{};
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Gets hold of the event listener list.
  testing::TestEventListeners& listeners =
      testing::UnitTest::GetInstance()->listeners();
  auto event_printer = new TestingTimeUsagePrinter();
  // Adds a listener to the end.  GoogleTest takes the ownership.
  listeners.Append(event_printer);

  int ret = RUN_ALL_TESTS();

  event_printer->PrintTop20Tests();

  return ret;
}
