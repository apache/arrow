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

#include <functional>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging_v2.h"

namespace arrow {
namespace util {

namespace {

class MockLogger : public Logger {
 public:
  explicit MockLogger(ArrowLogLevel threshold = ArrowLogLevel::ARROW_TRACE)
      : threshold(threshold) {}
  void Log(const LogDetails& details) override {
    if (callback) {
      callback(details);
    }
  }
  ArrowLogLevel severity_threshold() const override { return threshold; }
  bool is_enabled() const override { return enabled; }

  ArrowLogLevel threshold;
  bool enabled = true;
  std::function<void(const LogDetails&)> callback;
};

struct LoggingTracer {
  mutable bool was_evaluated = false;
  friend std::ostream& operator<<(std::ostream& os, const LoggingTracer& tracer) {
    tracer.was_evaluated = true;
    return os;
  }
};

}  // namespace

TEST(LoggingV2Test, Basics) {
  // Basic tests using the default logger
  ARROW_LOG_V2(ERROR);
  ARROW_LOG_V2(ERROR, "foo") << "bar";
  auto to_int = [](ArrowLogLevel lvl) { return static_cast<int>(lvl); };
  ARROW_LOG_V2(TRACE, "sev: ", to_int(ArrowLogLevel::ARROW_TRACE), ":TRACE");
  ARROW_LOG_V2(DEBUG, "sev: ", to_int(ArrowLogLevel::ARROW_DEBUG), ":DEBUG");
  ARROW_LOG_V2(INFO, "sev: ", to_int(ArrowLogLevel::ARROW_INFO), ":INFO");
  ARROW_LOG_V2(WARNING, "sev: ", to_int(ArrowLogLevel::ARROW_WARNING), ":WARNING");
  ARROW_LOG_V2(ERROR, "sev: ", to_int(ArrowLogLevel::ARROW_ERROR), ":ERROR");

  {
    auto logger = std::make_shared<MockLogger>(ArrowLogLevel::ARROW_WARNING);
    LoggingTracer tracers[5]{};
    ARROW_LOG_WITH(logger, TRACE, "foo", tracers[0], "bar");
    ARROW_LOG_WITH(logger, DEBUG, "foo", tracers[1], "bar");
    ARROW_LOG_WITH(logger, INFO, "foo", tracers[2], "bar");
    ARROW_LOG_WITH(logger, WARNING, "foo", tracers[3], "bar");
    ARROW_LOG_WITH(logger, ERROR, "foo", tracers[4], "bar");

    // If the message severity doesn't meet the logger's minimum severity, the LogMessage
    // stream shouldn't be appended to
    ASSERT_FALSE(tracers[0].was_evaluated);
    ASSERT_FALSE(tracers[1].was_evaluated);
    ASSERT_FALSE(tracers[2].was_evaluated);
    ASSERT_TRUE(tracers[3].was_evaluated);
    ASSERT_TRUE(tracers[4].was_evaluated);
  }

  {
    auto logger = std::make_shared<MockLogger>(ArrowLogLevel::ARROW_WARNING);
    logger->enabled = false;
    LoggingTracer tracer;
    // If the underlying logger is disabled, the LogMessage stream shouldn't be appended
    // to (regardless of severity)
    ARROW_LOG_WITH(logger, WARNING, tracer);
    ARROW_LOG_WITH(logger, ERROR, tracer);
    ASSERT_FALSE(tracer.was_evaluated);
  }
}

TEST(LoggingV2Test, EmittedMessages) {
  std::vector<std::string> messages;
  auto callback = [&messages](const LogDetails& details) {
    messages.push_back(std::string(details.message));
  };

  auto logger = std::make_shared<MockLogger>(ArrowLogLevel::ARROW_TRACE);
  logger->callback = callback;
  for (int i = 0; i < 3; ++i) {
    ARROW_LOG_WITH(logger, TRACE, "i=", i);
  }

  ASSERT_EQ(messages.size(), 3);
  EXPECT_EQ(messages[0], "i=0");
  EXPECT_EQ(messages[1], "i=1");
  EXPECT_EQ(messages[2], "i=2");
}

TEST(LoggingV2Test, Registry) {
  std::string name = "test-logger";
  std::shared_ptr<Logger> logger = std::make_shared<MockLogger>();

  ASSERT_OK(LoggerRegistry::RegisterLogger(name, logger));
  ASSERT_EQ(logger, LoggerRegistry::GetLogger(name));

  // Error if the logger is already registered
  ASSERT_RAISES(Invalid, LoggerRegistry::RegisterLogger(name, logger));

  LoggerRegistry::UnregisterLogger(name);
  ASSERT_OK(LoggerRegistry::RegisterLogger(name, logger));
  ASSERT_EQ(logger, LoggerRegistry::GetLogger(name));

  LoggerRegistry::UnregisterLogger(name);
}

}  // namespace util
}  // namespace arrow
