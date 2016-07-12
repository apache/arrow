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

#ifndef PARQUET_UTIL_LOGGING_H
#define PARQUET_UTIL_LOGGING_H

#include <iostream>

namespace parquet {

// Stubbed versions of macros defined in glog/logging.h, intended for
// environments where glog headers aren't available.
//
// Add more as needed.

// Log levels. LOG ignores them, so their values are abitrary.

#define PARQUET_INFO 0
#define PARQUET_WARNING 1
#define PARQUET_ERROR 2
#define PARQUET_FATAL 3

#define PARQUET_LOG_INTERNAL(level) parquet::internal::CerrLog(level)
#define PARQUET_LOG(level) PARQUET_LOG_INTERNAL(PARQUET_##level)
#define PARQUET_IGNORE_EXPR(expr) ((void)(expr));

#define PARQUET_CHECK(condition) \
  (condition) ? 0 : PARQUET_LOG(FATAL) << "Check failed: " #condition " "

#ifdef NDEBUG
#define PARQUET_DFATAL PARQUET_WARNING

#define DCHECK(condition)        \
  PARQUET_IGNORE_EXPR(condition) \
  while (false)                  \
  parquet::internal::NullLog()
#define DCHECK_EQ(val1, val2) \
  PARQUET_IGNORE_EXPR(val1)   \
  while (false)               \
  parquet::internal::NullLog()
#define DCHECK_NE(val1, val2) \
  PARQUET_IGNORE_EXPR(val1)   \
  while (false)               \
  parquet::internal::NullLog()
#define DCHECK_LE(val1, val2) \
  PARQUET_IGNORE_EXPR(val1)   \
  while (false)               \
  parquet::internal::NullLog()
#define DCHECK_LT(val1, val2) \
  PARQUET_IGNORE_EXPR(val1)   \
  while (false)               \
  parquet::internal::NullLog()
#define DCHECK_GE(val1, val2) \
  PARQUET_IGNORE_EXPR(val1)   \
  while (false)               \
  parquet::internal::NullLog()
#define DCHECK_GT(val1, val2) \
  PARQUET_IGNORE_EXPR(val1)   \
  while (false)               \
  parquet::internal::NullLog()

#else
#define PARQUET_DFATAL PARQUET_FATAL

#define DCHECK(condition) PARQUET_CHECK(condition)
#define DCHECK_EQ(val1, val2) PARQUET_CHECK((val1) == (val2))
#define DCHECK_NE(val1, val2) PARQUET_CHECK((val1) != (val2))
#define DCHECK_LE(val1, val2) PARQUET_CHECK((val1) <= (val2))
#define DCHECK_LT(val1, val2) PARQUET_CHECK((val1) < (val2))
#define DCHECK_GE(val1, val2) PARQUET_CHECK((val1) >= (val2))
#define DCHECK_GT(val1, val2) PARQUET_CHECK((val1) > (val2))

#endif  // NDEBUG

namespace internal {

class NullLog {
 public:
  template <class T>
  NullLog& operator<<(const T& t) {
    return *this;
  }
};

class CerrLog {
 public:
  CerrLog(int severity)  // NOLINT(runtime/explicit)
      : severity_(severity),
        has_logged_(false) {}

  ~CerrLog() {
    if (has_logged_) { std::cerr << std::endl; }
    if (severity_ == PARQUET_FATAL) { exit(1); }
  }

  template <class T>
  CerrLog& operator<<(const T& t) {
    has_logged_ = true;
    std::cerr << t;
    return *this;
  }

 private:
  const int severity_;
  bool has_logged_;
};

}  // namespace internal

}  // namespace parquet

#endif  // PARQUET_UTIL_LOGGING_H
