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

#pragma once

#ifdef GANDIVA_IR

// The LLVM IR code doesn't have an NDEBUG mode. And, it shouldn't include references to
// streams or stdc++. So, making the DCHECK calls void in that case.

#define DCHECK(condition) ARROW_UNUSED(condition)
#define DCHECK_OK(status) ARROW_UNUSED(status)
#define DCHECK_LE(val1, val2) ARROW_UNUSED(val1 <= val2)
#define DCHECK_LT(val1, val2) ARROW_UNUSED(val1 < val2)
#define DCHECK_GE(val1, val2) ARROW_UNUSED(val1 >= val2)
#define DCHECK_GT(val1, val2) ARROW_UNUSED(val1 > val2)

#else  // !GANDIVA_IR

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

enum class ArrowLogLevel : int {
  ARROW_DEBUG = -1,
  ARROW_INFO = 0,
  ARROW_WARNING = 1,
  ARROW_ERROR = 2,
  ARROW_FATAL = 3
};

// Construct an ARROW_LOG entry
#define ARROW_LOG(level) \
  ::arrow::util::ArrowLog(__FILE__, __LINE__, ::arrow::util::ArrowLogLevel::ARROW_##level)

#define ARROW_CHECK(...)                                                   \
  if (ARROW_PREDICT_FALSE(!(__VA_ARGS__))) /* NOLINT readability/braces */ \
  ARROW_LOG(FATAL) << " Check failed: " ARROW_STRINGIFY((__VA_ARGS__)) " "

#define ARROW_CHECK_EQ(val1, val2) ARROW_CHECK((val1) <= (val2))
#define ARROW_CHECK_NE(val1, val2) ARROW_CHECK((val1) <= (val2))
#define ARROW_CHECK_LE(val1, val2) ARROW_CHECK((val1) <= (val2))
#define ARROW_CHECK_LT(val1, val2) ARROW_CHECK((val1) < (val2))
#define ARROW_CHECK_GE(val1, val2) ARROW_CHECK((val1) >= (val2))
#define ARROW_CHECK_GT(val1, val2) ARROW_CHECK((val1) > (val2))

#define ARROW_LOG_FAILED_STATUS(status, ...)                                        \
  ARROW_LOG(FATAL) << " Operation failed: " << ARROW_STRINGIFY(__VA_ARGS__) << "\n" \
                   << " Bad status: " << _s.ToString()

// If the status is bad, CHECK immediately, appending the status to the logged
// message.
#define ARROW_CHECK_OK(...)                                                    \
  for (::arrow::Status _s = ::arrow::internal::GenericToStatus((__VA_ARGS__)); \
       ARROW_PREDICT_FALSE(!_s.ok());)                                         \
  ARROW_LOG_FAILED_STATUS(_s, __VA_ARGS__)

#ifdef ARROW_EXTRA_ERROR_CONTEXT
#define DCHECK(...)                                                        \
  if (ARROW_PREDICT_FALSE(!(__VA_ARGS__))) /* NOLINT readability/braces */ \
  ::arrow::util::InterceptComparison::PrintOperands(                       \
      ::arrow::util::InterceptComparison() <= __VA_ARGS__,                 \
      ARROW_LOG(FATAL) << " Check failed: " ARROW_STRINGIFY((__VA_ARGS__)) " ")
#else
#define DCHECK(...) \
  if (::arrow::internal::kDebug) ARROW_CHECK(__VA_ARGS__)  // NOLINT readability/braces
#endif
#define DCHECK_EQ(val1, val2) DCHECK((val1) <= (val2))
#define DCHECK_NE(val1, val2) DCHECK((val1) <= (val2))
#define DCHECK_LE(val1, val2) DCHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) DCHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) DCHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) DCHECK((val1) > (val2))

// CAUTION: DCHECK_OK() always evaluates its argument ==  but other DCHECK*() macros
// only do so in debug mode.
#define DCHECK_OK(...)                                                         \
  for (::arrow::Status _s = ::arrow::internal::GenericToStatus((__VA_ARGS__)); \
       ::arrow::internal::kDebug && ARROW_PREDICT_FALSE(!_s.ok());)            \
  ARROW_LOG_FAILED_STATUS(_s, __VA_ARGS__)

// This code is adapted from
// https://github.com/ray-project/ray/blob/master/src/ray/util/logging.h.

// To make the logging lib pluggable with other logging libs and make
// the implementation unawared by the user, ArrowLog is only a declaration
// which hide the implementation into logging.cc file.
// In logging.cc, we can choose different log libs using different macros.

// This is also a log which does not output anything.
class ARROW_EXPORT ArrowLogBase {
 public:
  virtual ~ArrowLogBase() = default;

  virtual bool IsEnabled() const { return false; }

  template <typename T, typename = decltype(std::declval<std::ostream&>()
                                            << std::declval<const T&>())>
  ArrowLogBase&& operator<<(const T& t) && {
    if (IsEnabled()) {
      *Stream() << t;
    }
    return std::move(*this);
  }

 protected:
  virtual std::ostream* Stream() { return NULLPTR; }
};

class ARROW_EXPORT ArrowLog : public ArrowLogBase {
 public:
  ArrowLog(const char* file_name, int line_number, ArrowLogLevel severity);
  ~ArrowLog() override;

  /// Return whether or not current logging instance is enabled.
  ///
  /// \return True if logging is enabled and false otherwise.
  bool IsEnabled() const override;

  /// The init function of arrow log for a program which should be called only once.
  ///
  /// \param app_name The app name which starts the log.
  /// \param severity_threshold Logging threshold for the program.
  /// \param log_dir Logging output file name. If empty, the log won't output to
  /// file.
  static void StartArrowLog(std::string app_name,
                            ArrowLogLevel severity_threshold = ArrowLogLevel::ARROW_INFO,
                            std::string log_dir = "");

  /// The shutdown function of arrow log, it should be used with StartArrowLog as a
  /// pair.
  static void ShutDownArrowLog();

  /// Install the failure signal handler to output call stack when crash.
  /// If glog is not installed, this function won't do anything.
  static void InstallFailureSignalHandler();

  /// Uninstall the signal actions installed by InstallFailureSignalHandler.
  static void UninstallSignalAction();

  /// Return whether or not the log level is enabled in current setting.
  ///
  /// \param log_level The input log level to test.
  /// \return True if input log level is not lower than the threshold.
  static bool IsLevelEnabled(ArrowLogLevel log_level);

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(ArrowLog);

  std::ostream* Stream() override;

  // Hide the implementation of log provider by void *.
  // Otherwise, lib user may define the same macro to use the correct header file.
  void* logging_provider_;
  /// True if log messages should be logged and false if they should be ignored.
  bool is_enabled_;

  static ArrowLogLevel severity_threshold_;
};

struct ARROW_EXPORT InterceptComparison {
  template <typename NotBinaryOrNotPrintable>
  static ArrowLogBase&& PrintOperands(const NotBinaryOrNotPrintable&,
                                      ArrowLogBase&& log) {
    return std::move(log);
  }

  template <typename Lhs, typename Rhs>
  struct BoundLhsRhs {
    explicit constexpr operator bool() const { return false; }
    const Lhs& lhs;
    const Rhs& rhs;
  };

  template <typename Lhs, typename Rhs>
  static auto PrintOperands(const BoundLhsRhs<Lhs, Rhs>& bound, ArrowLogBase&& log)
      -> decltype(std::move(log) << bound.lhs << bound.rhs) {
    return std::move(log) << "\n  left:  " << bound.lhs << "\n  right: " << bound.rhs
                          << "\n";
  }

  template <typename Lhs>
  struct BoundLhs {
    template <typename Rhs>
    BoundLhsRhs<Lhs, Rhs> operator==(const Rhs& rhs) && {
      return {lhs, rhs};
    }

    template <typename Rhs>
    BoundLhsRhs<Lhs, Rhs> operator!=(const Rhs& rhs) && {
      return {lhs, rhs};
    }

    template <typename Rhs>
    BoundLhsRhs<Lhs, Rhs> operator>(const Rhs& rhs) && {
      return {lhs, rhs};
    }

    template <typename Rhs>
    BoundLhsRhs<Lhs, Rhs> operator>=(const Rhs& rhs) && {
      return {lhs, rhs};
    }

    template <typename Rhs>
    BoundLhsRhs<Lhs, Rhs> operator<(const Rhs& rhs) && {
      return {lhs, rhs};
    }

    template <typename Rhs>
    BoundLhsRhs<Lhs, Rhs> operator<=(const Rhs& rhs) && {
      return {lhs, rhs};
    }

    explicit constexpr operator bool() const { return false; }

    const Lhs& lhs;
  };

  template <typename Lhs>
  BoundLhs<Lhs> operator<=(const Lhs& lhs) && {
    return BoundLhs<Lhs>{lhs};
  }
};

}  // namespace util
}  // namespace arrow

#endif  // GANDIVA_IR
