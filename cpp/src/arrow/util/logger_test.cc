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
#include "arrow/util/logger.h"

// Emit log via the default logger
#define DO_LOG(LEVEL, ...) ARROW_LOGGER_CALL("", LEVEL, __VA_ARGS__)

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

struct OstreamableTracer {
  mutable bool was_evaluated = false;
  friend std::ostream& operator<<(std::ostream& os, const OstreamableTracer& tracer) {
    tracer.was_evaluated = true;
    return os;
  }
};

}  // namespace

TEST(LoggerTest, Basics) {
  // Basic tests using the default logger
  DO_LOG(ERROR);
  DO_LOG(ERROR, "foo");
  auto to_int = [](ArrowLogLevel lvl) { return static_cast<int>(lvl); };
  DO_LOG(TRACE, "sev: ", to_int(ArrowLogLevel::ARROW_TRACE), ":TRACE");
  DO_LOG(DEBUG, "sev: ", to_int(ArrowLogLevel::ARROW_DEBUG), ":DEBUG");
  DO_LOG(INFO, "sev: ", to_int(ArrowLogLevel::ARROW_INFO), ":INFO");
  DO_LOG(WARNING, "sev: ", to_int(ArrowLogLevel::ARROW_WARNING), ":WARNING");
  DO_LOG(ERROR, "sev: ", to_int(ArrowLogLevel::ARROW_ERROR), ":ERROR");

  {
    auto logger = std::make_shared<MockLogger>(ArrowLogLevel::ARROW_WARNING);
    OstreamableTracer tracers[5]{};
    ARROW_LOGGER_CALL(logger, TRACE, "foo", tracers[0], "bar");
    ARROW_LOGGER_CALL(logger, DEBUG, "foo", tracers[1], "bar");
    ARROW_LOGGER_CALL(logger, INFO, "foo", tracers[2], "bar");
    ARROW_LOGGER_CALL(logger, WARNING, "foo", tracers[3], "bar");
    ARROW_LOGGER_CALL(logger, ERROR, "foo", tracers[4], "bar");

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
    OstreamableTracer tracer;
    // If the underlying logger is disabled, the LogMessage stream shouldn't be appended
    // to (regardless of severity)
    ARROW_LOGGER_CALL(logger, WARNING, tracer);
    ARROW_LOGGER_CALL(logger, ERROR, tracer);
    ASSERT_FALSE(tracer.was_evaluated);
  }
}

TEST(LoggerTest, EmittedMessages) {
  std::vector<std::string> messages;
  auto callback = [&messages](const LogDetails& details) {
    messages.push_back(std::string(details.message));
  };

  auto logger = std::make_shared<MockLogger>(ArrowLogLevel::ARROW_TRACE);
  logger->callback = callback;
  for (int i = 0; i < 3; ++i) {
    ARROW_LOGGER_CALL(logger, TRACE, "i=", i);
  }

  ASSERT_EQ(messages.size(), 3);
  EXPECT_EQ(messages[0], "i=0");
  EXPECT_EQ(messages[1], "i=1");
  EXPECT_EQ(messages[2], "i=2");
}

TEST(LoggerTest, Registry) {
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

#undef DO_LOG

}  // namespace util
}  // namespace arrow
