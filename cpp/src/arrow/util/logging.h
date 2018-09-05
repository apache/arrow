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

#ifndef ARROW_UTIL_LOGGING_H
#define ARROW_UTIL_LOGGING_H

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include "arrow/util/macros.h"

// Forward declaration for the log provider.
#ifdef ARROW_USE_GLOG
namespace google {
class LogMessage;
}  // namespace google
typedef google::LogMessage LoggingProvider;
#else
namespace arrow {
class CerrLog;
}  // namespace arrow
typedef arrow::CerrLog LoggingProvider;
#endif

namespace arrow {

// Stubbed versions of macros defined in glog/logging.h, intended for
// environments where glog headers aren't available.
//
// Add more as needed.

// Log levels. LOG ignores them, so their values are arbitrary.

#define ARROW_DEBUG (-1)
#define ARROW_INFO 0
#define ARROW_WARNING 1
#define ARROW_ERROR 2
#define ARROW_FATAL 3

#define ARROW_LOG_INTERNAL(level) ::arrow::ArrowLog(__FILE__, __LINE__, level)
#define ARROW_LOG(level) ARROW_LOG_INTERNAL(ARROW_##level)
#define ARROW_IGNORE_EXPR(expr) ((void)(expr))

#define ARROW_CHECK(condition)                                                          \
  (condition) ? ARROW_IGNORE_EXPR(0)                                                    \
              : ::arrow::Voidify() & ::arrow::ArrowLog(__FILE__, __LINE__, ARROW_FATAL) \
                                         << " Check failed: " #condition " "

// If 'to_call' returns a bad status, CHECK immediately with a logged message
// of 'msg' followed by the status.
#define ARROW_CHECK_OK_PREPEND(to_call, msg)                \
  do {                                                      \
    ::arrow::Status _s = (to_call);                         \
    ARROW_CHECK(_s.ok()) << (msg) << ": " << _s.ToString(); \
  } while (false)

// If the status is bad, CHECK immediately, appending the status to the
// logged message.
#define ARROW_CHECK_OK(s) ARROW_CHECK_OK_PREPEND(s, "Bad status")

#ifdef NDEBUG
#define ARROW_DFATAL ARROW_WARNING

#define DCHECK(condition)       \
  ARROW_IGNORE_EXPR(condition); \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_OK(status)    \
  ARROW_IGNORE_EXPR(status); \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_EQ(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_NE(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_LE(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_LT(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_GE(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::ArrowLogBase()
#define DCHECK_GT(val1, val2) \
  ARROW_IGNORE_EXPR(val1);    \
  while (false) ::arrow::ArrowLogBase()

#else
#define ARROW_DFATAL ARROW_FATAL

#define DCHECK(condition) ARROW_CHECK(condition)
#define DCHECK_OK(status) (ARROW_CHECK((status).ok()) << (status).message())
#define DCHECK_EQ(val1, val2) ARROW_CHECK((val1) == (val2))
#define DCHECK_NE(val1, val2) ARROW_CHECK((val1) != (val2))
#define DCHECK_LE(val1, val2) ARROW_CHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) ARROW_CHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) ARROW_CHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) ARROW_CHECK((val1) > (val2))

#endif  // NDEBUG

// To make the logging lib plugable with other logging libs and make
// the implementation unawared by the user, ArrowLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a null log which does not output anything.
class ArrowLogBase {
 public:
  virtual ~ArrowLogBase() {}

  virtual bool IsEnabled() const { return false; }

  template <typename T>
  ArrowLogBase& operator<<(const T& t) {
    if (IsEnabled()) {
      Stream() << t;
    } else {
      ARROW_IGNORE_EXPR(t);
    }
    return *this;
  }

 protected:
  virtual std::ostream& Stream() { return std::cerr; }
};

class ArrowLog : public ArrowLogBase {
 public:
  ArrowLog(const char* file_name, int line_number, int severity);

  virtual ~ArrowLog();

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  virtual bool IsEnabled() const;

  // The init function of arrow log for a program which should be called only once.
  // If logDir is empty, the log won't output to file.
  static void StartArrowLog(const std::string& appName,
                            int severity_threshold = ARROW_ERROR,
                            const std::string& logDir = "");

  // The shutdown function of arrow log which should be used with StartArrowLog as a pair.
  static void ShutDownArrowLog();

  // Install the failure signal handler to output call stack when crash.
  // If glog is not installed, this function won't do anything.
  static void InstallFailureSignalHandler();

 private:
  std::unique_ptr<LoggingProvider> logging_provider_;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;
  static int severity_threshold_;
  // In InitGoogleLogging, it simply keeps the pointer.
  // We need to make sure the app name passed to InitGoogleLogging exist.
  static std::unique_ptr<char, std::default_delete<char[]>> app_name_;

 protected:
  virtual std::ostream& Stream();
};

// This class make ARROW_CHECK compilation pass to change the << operator to void.
// This class is copied from glog.
class Voidify {
 public:
  Voidify() {}
  // This has to be an operator with a precedence lower than << but
  // higher than ?:
  void operator&(ArrowLogBase&) {}
};

}  // namespace arrow

#endif  // ARROW_UTIL_LOGGING_H
