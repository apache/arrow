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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/telemetry/logging.h"
#include "arrow/telemetry/util_internal.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/logging.h"
#include "arrow/util/tracing_internal.h"

#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>

namespace arrow {
namespace telemetry {

class OtelEnvironment : public ::testing::Environment {
 public:
  static constexpr std::string_view kLoggerName = "arrow-telemetry-test";

  void SetUp() override {
    std::vector<std::unique_ptr<otel::sdk::trace::SpanProcessor>> processors;
    auto tracer_provider = otel::nostd::shared_ptr<otel::sdk::trace::TracerProvider>(
        new otel::sdk::trace::TracerProvider(std::move(processors)));
    otel::trace::Provider::SetTracerProvider(std::move(tracer_provider));

    otel::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
        otel::nostd::shared_ptr<otel::context::propagation::TextMapPropagator>(
            new otel::trace::propagation::HttpTraceContext()));

    ASSERT_OK(internal::InitializeOtelLoggerProvider());
    auto logging_options = LoggingOptions::Defaults();
    logging_options.severity_threshold = LogLevel::ARROW_TRACE;
    logging_options.flush_severity = LogLevel::ARROW_TRACE;
    ASSERT_OK_AND_ASSIGN(
        auto logger,
        OtelLoggerProvider::MakeLogger(
            kLoggerName, logging_options,
            AttributeList{Attribute{"fooInt", 42}, Attribute{"barStr", "fourty two"}}));
    ASSERT_OK(util::LoggerRegistry::RegisterLogger(logger->name(), logger));
  }

  void TearDown() override { EXPECT_TRUE(internal::ShutdownOtelLoggerProvider()); }
};

static ::testing::Environment* kOtelEnvironment =
    ::testing::AddGlobalTestEnvironment(new OtelEnvironment);

template <typename... Args>
void Log(Args&&... args) {
  auto logger = std::dynamic_pointer_cast<telemetry::Logger>(
      util::LoggerRegistry::GetLogger(OtelEnvironment::kLoggerName));
  ASSERT_NE(logger, nullptr);
  logger->Log(std::forward<Args>(args)...);
}

class TestLogging : public ::testing::Test {
 public:
  void SetUp() override {
    tracer_ = arrow::internal::tracing::GetTracer();
    span_ = tracer_->StartSpan("test-logging");
  }

  otel::trace::Scope MakeScope() { return tracer_->WithActiveSpan(span_); }

 protected:
  otel::trace::Tracer* tracer_;
  otel::nostd::shared_ptr<otel::trace::Span> span_;
};

TEST_F(TestLogging, Basics) {
  auto scope = MakeScope();
  Log(LogLevel::ARROW_ERROR, "foo bar", EventId(13, "Event13"));
  Log(LogLevel::ARROW_WARNING, "baz bal",
      AttributeList{Attribute{"intAttr", 24}, Attribute{"boolAttr", true},
                    Attribute{"strAttr", std::string("ab") + "c"}});
}

}  // namespace telemetry
}  // namespace arrow
