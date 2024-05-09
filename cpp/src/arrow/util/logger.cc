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

#include <iostream>
#include <mutex>
#include <sstream>
#include <unordered_map>

#include "arrow/util/logger.h"

namespace arrow {
namespace util {

namespace {

class NoopLogger : public Logger {
 public:
  void Log(const LogDetails&) override {}
  bool is_enabled() const override { return false; }
};

class SimpleLogger : public Logger {
 public:
  SimpleLogger(ArrowLogLevel severity_threshold, std::ostream* sink)
      : severity_threshold_(severity_threshold), sink_(sink) {}

  void Log(const LogDetails& details) override {
    *sink_ << details.source_location.file << ":" << details.source_location.line << ": "
           << std::string(details.message) << std::endl;
  }

  bool Flush(std::chrono::microseconds) override {
    sink_->flush();
    return true;
  }

  bool is_enabled() const override { return true; }

  ArrowLogLevel severity_threshold() const override { return severity_threshold_; }

 private:
  ArrowLogLevel severity_threshold_;
  std::ostream* sink_;
};

using LoggerMap = std::unordered_map<std::string, std::shared_ptr<Logger>>;

struct RegistryImpl {
  LoggerMap loggers;
  std::shared_ptr<Logger> default_logger = MakeOStreamLogger(ArrowLogLevel::ARROW_INFO);
  std::mutex mtx;
};

RegistryImpl* GetRegistry() {
  static auto instance = std::make_unique<RegistryImpl>();
  return instance.get();
}

}  // namespace

std::shared_ptr<Logger> MakeOStreamLogger(ArrowLogLevel severity_threshold,
                                          std::ostream& sink) {
  return std::make_shared<SimpleLogger>(severity_threshold, &sink);
}
std::shared_ptr<Logger> MakeOStreamLogger(ArrowLogLevel severity_threshold) {
  return MakeOStreamLogger(severity_threshold, std::cerr);
}

Status LoggerRegistry::RegisterLogger(std::string_view name,
                                      std::shared_ptr<Logger> logger) {
  DCHECK_NE(logger, nullptr);
  auto registry = GetRegistry();

  std::lock_guard<std::mutex> lk(registry->mtx);
  auto ret = registry->loggers.emplace(name, std::move(logger));
  ARROW_RETURN_IF(!ret.second, Status::Invalid("Logger with name \"", std::string(name),
                                               "\" is already registered"));
  return Status::OK();
}

void LoggerRegistry::UnregisterLogger(std::string_view name) {
  auto registry = GetRegistry();
  std::lock_guard<std::mutex> lk(registry->mtx);
  registry->loggers.erase(std::string(name));
}

std::shared_ptr<Logger> LoggerRegistry::GetLogger(std::string_view name) {
  if (name.empty()) {
    return GetDefaultLogger();
  }

  auto registry = GetRegistry();
  {
    std::lock_guard<std::mutex> lk(registry->mtx);
    const auto& loggers = registry->loggers;
    if (auto it = loggers.find(std::string(name)); it != loggers.end()) {
      return it->second;
    }
  }
  return std::make_shared<NoopLogger>();
}

std::shared_ptr<Logger> LoggerRegistry::GetDefaultLogger() {
  auto registry = GetRegistry();
  std::lock_guard<std::mutex> lk(registry->mtx);
  return registry->default_logger;
}

void LoggerRegistry::SetDefaultLogger(std::shared_ptr<Logger> logger) {
  DCHECK_NE(logger, nullptr);
  auto registry = GetRegistry();
  std::lock_guard<std::mutex> lk(registry->mtx);
  registry->default_logger = std::move(logger);
}

class LogMessage::Impl {
 public:
  Impl(ArrowLogLevel severity, SourceLocation source_location) {
    details.severity = severity;
    details.source_location = source_location;
  }
  Impl(ArrowLogLevel severity, std::shared_ptr<Logger> logger,
       SourceLocation source_location)
      : Impl(severity, source_location) {
    logger_ = std::move(logger);
  }
  Impl(ArrowLogLevel severity, std::string logger_name, SourceLocation source_location)
      : Impl(severity, source_location) {
    this->logger_name = std::move(logger_name);
  }

  ~Impl() {
    auto* logger = GetResolvedLogger();

    if (logger && logger->is_enabled() &&
        details.severity >= logger->severity_threshold()) {
      auto str = stream.str();
      details.message = str;
      logger->Log(details);
    }

    // It's debatable whether this should be the underlying logger's responsibility
    if (details.severity == ArrowLogLevel::ARROW_FATAL) {
      if (logger) {
        logger->Flush();
      }
      std::abort();
    }
  }

  Logger* GetResolvedLogger() {
    if (!logger_) {
      logger_ = LoggerRegistry::GetLogger(logger_name);
    }
    return logger_.get();
  }

  LogDetails details{};
  std::string logger_name;
  std::stringstream stream;

 private:
  std::shared_ptr<Logger> logger_;
};

LogMessage::LogMessage(ArrowLogLevel severity, std::shared_ptr<Logger> logger,
                       SourceLocation source_location)
    : impl_(std::make_shared<Impl>(severity, std::move(logger), source_location)) {}

LogMessage::LogMessage(ArrowLogLevel severity, std::string_view logger_name,
                       SourceLocation source_location)
    : impl_(std::make_shared<Impl>(severity, std::string(logger_name), source_location)) {
}

std::ostream& LogMessage::Stream() { return impl_->stream; }

bool LogMessage::CheckIsEnabled() {
  auto* logger = impl_->GetResolvedLogger();
  return (logger && logger->is_enabled() &&
          impl_->details.severity >= logger->severity_threshold());
}

}  // namespace util
}  // namespace arrow
