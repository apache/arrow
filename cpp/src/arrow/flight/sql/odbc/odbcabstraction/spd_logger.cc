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

#include "odbcabstraction/spd_logger.h"

#include "odbcabstraction/logger.h"

#include <spdlog/async.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include <cstdint>
#include <csignal>

namespace driver {
namespace odbcabstraction {

const std::string SPDLogger::LOG_LEVEL = "LogLevel";
const std::string SPDLogger::LOG_PATH= "LogPath";
const std::string SPDLogger::MAXIMUM_FILE_SIZE= "MaximumFileSize";
const std::string SPDLogger::FILE_QUANTITY= "FileQuantity";
const std::string SPDLogger::LOG_ENABLED= "LogEnabled";

namespace {
std::function<void(int)> shutdown_handler;
void signal_handler(int signal) {
  shutdown_handler(signal);
}

typedef void (*Handler)(int signum);

Handler old_sigint_handler = SIG_IGN;
Handler old_sigsegv_handler = SIG_IGN;
Handler old_sigabrt_handler = SIG_IGN;
#ifdef SIGKILL
Handler old_sigkill_handler = SIG_IGN;
#endif

Handler GetHandlerFromSignal(int signum) {
  switch (signum) {
    case(SIGINT):
      return old_sigint_handler;
    case(SIGSEGV):
      return old_sigsegv_handler;
    case(SIGABRT):
      return old_sigabrt_handler;
#ifdef SIGKILL
    case(SIGKILL):
      return old_sigkill_handler;
#endif
  }
}

void SetSignalHandler(int signum) {
  Handler old = signal(signum, SIG_IGN);
  if (old != SIG_IGN) {
    auto old_handler = GetHandlerFromSignal(signum);
    old_handler = old;
  }
  signal(signum, signal_handler);
}

void ResetSignalHandler(int signum) {
  Handler actual_handler = signal(signum, SIG_IGN);
  if (actual_handler == signal_handler) {
    signal(signum, GetHandlerFromSignal(signum));
  }
}


inline spdlog::level::level_enum ToSpdLogLevel(LogLevel level) {
  switch (level) {
  case LogLevel_TRACE:
    return spdlog::level::trace;
  case LogLevel_DEBUG:
    return spdlog::level::debug;
  case LogLevel_INFO:
    return spdlog::level::info;
  case LogLevel_WARN:
    return spdlog::level::warn;
  case LogLevel_ERROR:
    return spdlog::level::err;
  default:
    return spdlog::level::off;
  }
}
} // namespace

void SPDLogger::init(int64_t fileQuantity, int64_t maxFileSize,
                     const std::string &fileNamePrefix, LogLevel level) {
  logger_ = spdlog::rotating_logger_mt<spdlog::async_factory>(
      "ODBC Logger", fileNamePrefix, maxFileSize, fileQuantity);

  logger_->set_level(ToSpdLogLevel(level));

  if (level != LogLevel::LogLevel_OFF) {
    SetSignalHandler(SIGINT);
    SetSignalHandler(SIGSEGV);
    SetSignalHandler(SIGABRT);
#ifdef SIGKILL
    SetSignalHandler(SIGKILL);
#endif
    shutdown_handler = [&](int signal) {
      logger_->flush();
      spdlog::shutdown();
      auto handler = GetHandlerFromSignal(signal);
      handler(signal);
    };
  }
}

void SPDLogger::log(LogLevel level, const std::function<std::string(void)> &build_message) {
  auto level_set = logger_->level();
  spdlog::level::level_enum spdlog_level = ToSpdLogLevel(level);
  if (level_set == spdlog::level::off || level_set > spdlog_level) {
    return;
  }

  const std::string &message = build_message();
  logger_->log(spdlog_level, message);
}

SPDLogger::~SPDLogger() {
  ResetSignalHandler(SIGINT);
  ResetSignalHandler(SIGSEGV);
  ResetSignalHandler(SIGABRT);
#ifdef SIGKILL
  ResetSignalHandler(SIGKILL);
#endif
}

} // namespace odbcabstraction
} // namespace driver
