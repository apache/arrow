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

#include "arrow/util/logging.h"

#ifdef ARROW_WITH_BACKTRACE
#include <execinfo.h>
#endif
#include <cstdlib>
#include <iostream>

#ifdef ARROW_USE_GLOG
#include <signal.h>
#include <vector>
#include "glog/logging.h"
#endif

namespace arrow {
namespace util {

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.cc.

// This is the default implementation of arrow log,
// which is independent of any libs.
class CerrLog {
 public:
  explicit CerrLog(ArrowLogLevel severity) : severity_(severity), has_logged_(false) {}

  virtual ~CerrLog() {
    if (has_logged_) {
      std::cerr << std::endl;
    }
    if (severity_ == ArrowLogLevel::ARROW_FATAL) {
      PrintBackTrace();
      std::abort();
    }
  }

  std::ostream& Stream() {
    has_logged_ = true;
    return std::cerr;
  }

  template <class T>
  CerrLog& operator<<(const T& t) {
    if (severity_ != ArrowLogLevel::ARROW_DEBUG) {
      has_logged_ = true;
      std::cerr << t;
    }
    return *this;
  }

 protected:
  const ArrowLogLevel severity_;
  bool has_logged_;

  void PrintBackTrace() {
#ifdef ARROW_WITH_BACKTRACE
    void* buffer[255];
    const int calls = backtrace(buffer, static_cast<int>(sizeof(buffer) / sizeof(void*)));
    backtrace_symbols_fd(buffer, calls, 1);
#endif
  }
};

#ifdef ARROW_USE_GLOG
typedef google::LogMessage LoggingProvider;
#else
typedef CerrLog LoggingProvider;
#endif

ArrowLogLevel ArrowLog::severity_threshold_ = ArrowLogLevel::ARROW_INFO;
// Keep the log directory.
static std::unique_ptr<std::string> log_dir_;

#ifdef ARROW_USE_GLOG

// Glog's severity map.
static int GetMappedSeverity(ArrowLogLevel severity) {
  switch (severity) {
    case ArrowLogLevel::ARROW_DEBUG:
      return google::GLOG_INFO;
    case ArrowLogLevel::ARROW_INFO:
      return google::GLOG_INFO;
    case ArrowLogLevel::ARROW_WARNING:
      return google::GLOG_WARNING;
    case ArrowLogLevel::ARROW_ERROR:
      return google::GLOG_ERROR;
    case ArrowLogLevel::ARROW_FATAL:
      return google::GLOG_FATAL;
    default:
      ARROW_LOG(FATAL) << "Unsupported logging level: " << static_cast<int>(severity);
      // This return won't be hit but compiler needs it.
      return google::GLOG_FATAL;
  }
}

#endif

void ArrowLog::StartArrowLog(const std::string& app_name,
                             ArrowLogLevel severity_threshold,
                             const std::string& log_dir) {
  severity_threshold_ = severity_threshold;
  // In InitGoogleLogging, it simply keeps the pointer.
  // We need to make sure the app name passed to InitGoogleLogging exist.
  // We should avoid using static string is a dynamic lib.
  static std::unique_ptr<std::string> app_name_;
  app_name_.reset(new std::string(app_name));
  log_dir_.reset(new std::string(log_dir));
#ifdef ARROW_USE_GLOG
  int mapped_severity_threshold = GetMappedSeverity(severity_threshold_);
  google::SetStderrLogging(mapped_severity_threshold);
  // Enble log file if log_dir is not empty.
  if (!log_dir.empty()) {
    auto dir_ends_with_slash = log_dir;
    if (log_dir[log_dir.length() - 1] != '/') {
      dir_ends_with_slash += "/";
    }
    auto app_name_without_path = app_name;
    if (app_name.empty()) {
      app_name_without_path = "DefaultApp";
    } else {
      // Find the app name without the path.
      size_t pos = app_name.rfind('/');
      if (pos != app_name.npos && pos + 1 < app_name.length()) {
        app_name_without_path = app_name.substr(pos + 1);
      }
    }
    // If InitGoogleLogging is called but SetLogDestination is not called,
    // the log will be output to /tmp besides stderr. If log_dir is not
    // provided, we'd better not call InitGoogleLogging.
    google::InitGoogleLogging(app_name_->c_str());
    google::SetLogFilenameExtension(app_name_without_path.c_str());
    for (int i = static_cast<int>(severity_threshold_);
         i <= static_cast<int>(ArrowLogLevel::ARROW_FATAL); ++i) {
      int level = GetMappedSeverity(static_cast<ArrowLogLevel>(i));
      google::SetLogDestination(level, dir_ends_with_slash.c_str());
    }
  }
#endif
}

void ArrowLog::UninstallSignalAction() {
#ifdef ARROW_USE_GLOG
  ARROW_LOG(DEBUG) << "Uninstall signal handlers.";
  // This signal list comes from glog's signalhandler.cc.
  // https://github.com/google/glog/blob/master/src/signalhandler.cc#L58-L70
  std::vector<int> installed_signals({SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGTERM});
#ifdef WIN32
  for (int signal_num : installed_signals) {
    ARROW_CHECK(signal(signal_num, SIG_DFL) != SIG_ERR);
  }
#else
  struct sigaction sig_action;
  memset(&sig_action, 0, sizeof(sig_action));
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_handler = SIG_DFL;
  for (int signal_num : installed_signals) {
    ARROW_CHECK(sigaction(signal_num, &sig_action, NULL) == 0);
  }
#endif
#endif
}

void ArrowLog::ShutDownArrowLog() {
#ifdef ARROW_USE_GLOG
  if (!log_dir_->empty()) {
    google::ShutdownGoogleLogging();
  }
#endif
}

void ArrowLog::InstallFailureSignalHandler() {
#ifdef ARROW_USE_GLOG
  google::InstallFailureSignalHandler();
#endif
}

bool ArrowLog::IsLevelEnabled(ArrowLogLevel log_level) {
  return log_level >= severity_threshold_;
}

ArrowLog::ArrowLog(const char* file_name, int line_number, ArrowLogLevel severity)
    // glog does not have DEBUG level, we can handle it using is_enabled_.
    : logging_provider_(nullptr), is_enabled_(severity >= severity_threshold_) {
#ifdef ARROW_USE_GLOG
  if (is_enabled_) {
    logging_provider_ =
        new google::LogMessage(file_name, line_number, GetMappedSeverity(severity));
  }
#else
  auto logging_provider = new CerrLog(severity);
  *logging_provider << file_name << ":" << line_number << ": ";
  logging_provider_ = logging_provider;
#endif
}

std::ostream& ArrowLog::Stream() {
  auto logging_provider = reinterpret_cast<LoggingProvider*>(logging_provider_);
#ifdef ARROW_USE_GLOG
  // Before calling this function, user should check IsEnabled.
  // When IsEnabled == false, logging_provider_ will be empty.
  return logging_provider->stream();
#else
  return logging_provider->Stream();
#endif
}

bool ArrowLog::IsEnabled() const { return is_enabled_; }

ArrowLog::~ArrowLog() {
  if (logging_provider_ != nullptr) {
    delete reinterpret_cast<LoggingProvider*>(logging_provider_);
    logging_provider_ = nullptr;
  }
}

}  // namespace util
}  // namespace arrow
