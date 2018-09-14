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

#ifndef GANDIVA_STATUS_H
#define GANDIVA_STATUS_H

#include <assert.h>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/util/macros.h"

#define GANDIVA_RETURN_NOT_OK(status)                                                    \
  do {                                                                                   \
    Status _status = (status);                                                           \
    if (!_status.ok()) {                                                                 \
      std::stringstream ss;                                                              \
      ss << __FILE__ << ":" << __LINE__ << " code: " << _status.CodeAsString() << " \n " \
         << _status.message();                                                           \
      return Status(_status.code(), ss.str());                                           \
    }                                                                                    \
  } while (0)

#define GANDIVA_RETURN_FAILURE_IF_FALSE(condition, status)                               \
  do {                                                                                   \
    if (!(condition)) {                                                                  \
      Status _status = (status);                                                         \
      std::stringstream ss;                                                              \
      ss << __FILE__ << ":" << __LINE__ << " code: " << _status.CodeAsString() << " \n " \
         << _status.message();                                                           \
      return Status(_status.code(), ss.str());                                           \
    }                                                                                    \
  } while (0)

// Check arrow status & convert to gandiva status on error.
#define GANDIVA_RETURN_ARROW_NOT_OK(astatus)                      \
  do {                                                            \
    if (!(astatus).ok()) {                                        \
      return Status(StatusCode::ArrowError, (astatus).message()); \
    }                                                             \
  } while (0)

namespace gandiva {

enum class StatusCode : char {
  OK = 0,
  Invalid = 1,
  CodeGenError = 2,
  ArrowError = 3,
  ExpressionValidationError = 4,
};

class Status {
 public:
  // Create a success status.
  Status() : state_(NULL) {}
  ~Status() { delete state_; }

  Status(StatusCode code, const std::string& msg);

  // Copy the specified status.
  Status(const Status& s);
  Status& operator=(const Status& s);

  // Move the specified status.
  Status(Status&& s);
  Status& operator=(Status&& s);

  // AND the statuses.
  Status operator&(const Status& s) const;
  Status operator&(Status&& s) const;
  Status& operator&=(const Status& s);
  Status& operator&=(Status&& s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status CodeGenError(const std::string& msg) {
    return Status(StatusCode::CodeGenError, msg);
  }

  static Status Invalid(const std::string& msg) {
    return Status(StatusCode::Invalid, msg);
  }

  static Status ArrowError(const std::string& msg) {
    return Status(StatusCode::ArrowError, msg);
  }

  static Status ExpressionValidationError(const std::string& msg) {
    return Status(StatusCode::ExpressionValidationError, msg);
  }

  // Returns true if the status indicates success.
  bool ok() const { return (state_ == NULL); }

  bool IsCodeGenError() const { return code() == StatusCode::CodeGenError; }

  bool IsInvalid() const { return code() == StatusCode::Invalid; }

  bool IsArrowError() const { return code() == StatusCode::ArrowError; }

  bool IsExpressionValidationError() const {
    return code() == StatusCode::ExpressionValidationError;
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  // Return a string representation of the status code, without the message
  // text or posix code information.
  std::string CodeAsString() const;

  StatusCode code() const { return ok() ? StatusCode::OK : state_->code; }

  std::string message() const { return ok() ? "" : state_->msg; }

 private:
  struct State {
    StatusCode code;
    std::string msg;
  };
  // OK status has a `NULL` state_.  Otherwise, `state_` points to
  // a `State` structure containing the error code and message(s)
  State* state_;

  void CopyFrom(const Status& s);
  void MoveFrom(Status& s);
};

static inline std::ostream& operator<<(std::ostream& os, const Status& x) {
  os << x.ToString();
  return os;
}

inline Status::Status(const Status& s)
    : state_((s.state_ == NULL) ? NULL : new State(*s.state_)) {}

inline Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    CopyFrom(s);
  }
  return *this;
}

inline Status::Status(Status&& s) : state_(s.state_) { s.state_ = NULL; }

inline Status& Status::operator=(Status&& s) {
  MoveFrom(s);
  return *this;
}

inline Status Status::operator&(const Status& s) const {
  if (ok()) {
    return s;
  } else {
    return *this;
  }
}

inline Status Status::operator&(Status&& s) const {
  if (ok()) {
    return std::move(s);
  } else {
    return *this;
  }
}

inline Status& Status::operator&=(const Status& s) {
  if (ok() && !s.ok()) {
    CopyFrom(s);
  }
  return *this;
}

inline Status& Status::operator&=(Status&& s) {
  if (ok() && !s.ok()) {
    MoveFrom(s);
  }
  return *this;
}

inline Status::Status(StatusCode code, const std::string& msg) {
  assert(code != StatusCode::OK);
  state_ = new State;
  state_->code = code;
  state_->msg = msg;
}

inline void Status::CopyFrom(const Status& s) {
  delete state_;
  if (s.state_ == NULLPTR) {
    state_ = NULLPTR;
  } else {
    state_ = new State(*s.state_);
  }
}

inline std::string Status::CodeAsString() const {
  if (state_ == NULLPTR) {
    return "OK";
  }

  const char* type;
  switch (code()) {
    case StatusCode::OK:
      type = "OK";
      break;
    case StatusCode::CodeGenError:
      type = "CodeGenError";
      break;
    case StatusCode::Invalid:
      type = "Invalid";
      break;
    case StatusCode::ExpressionValidationError:
      type = "ExpressionValidationError";
      break;
    default:
      type = "Unknown";
      break;
  }
  return std::string(type);
}

inline void Status::MoveFrom(Status& s) {
  delete state_;
  state_ = s.state_;
  s.state_ = NULL;
}

inline std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (state_ == NULL) {
    return result;
  }
  result += ": ";
  result += state_->msg;
  return result;
}

}  // namespace gandiva

#endif  // GANDIVA_STATUS_H
