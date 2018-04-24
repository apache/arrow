// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

// Adapted from Apache Kudu, TensorFlow

#ifndef ARROW_STATUS_H_
#define ARROW_STATUS_H_

#include <cstring>
#include <iosfwd>
#include <string>

#ifdef ARROW_EXTRA_ERROR_CONTEXT
#include <sstream>
#endif

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define RETURN_NOT_OK(s)                                                            \
  do {                                                                              \
    ::arrow::Status _s = (s);                                                       \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                                            \
      std::stringstream ss;                                                         \
      ss << __FILE__ << ":" << __LINE__ << " code: " << #s << "\n" << _s.message(); \
      return Status(_s.code(), ss.str());                                           \
    }                                                                               \
  } while (0)

#else

#define RETURN_NOT_OK(s)                 \
  do {                                   \
    ::arrow::Status _s = (s);            \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      return _s;                         \
    }                                    \
  } while (false)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

#define RETURN_NOT_OK_ELSE(s, else_) \
  do {                               \
    ::arrow::Status _s = (s);        \
    if (!_s.ok()) {                  \
      else_;                         \
      return _s;                     \
    }                                \
  } while (false)

// This is used by other codebases. The macros above
// should probably have that prefix, but there is a
// lot of code that already uses that macro and changing
// it would be of no benefit.
#define ARROW_RETURN_NOT_OK(s) RETURN_NOT_OK(s)

namespace arrow {

enum class StatusCode : char {
  OK = 0,
  OutOfMemory = 1,
  KeyError = 2,
  TypeError = 3,
  Invalid = 4,
  IOError = 5,
  UnknownError = 9,
  NotImplemented = 10,
  SerializationError = 11,
  PythonError = 12,
  PlasmaObjectExists = 20,
  PlasmaObjectNonexistent = 21,
  PlasmaStoreFull = 22,
  PlasmaObjectAlreadySealed = 23,
};

#if defined(__clang__)
// Only clang supports warn_unused_result as a type annotation.
class ARROW_MUST_USE_RESULT ARROW_EXPORT Status;
#endif

class ARROW_EXPORT Status {
 public:
  // Create a success status.
  Status() : state_(NULL) {}
  ~Status() { delete state_; }

  Status(StatusCode code, const std::string& msg);

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status OutOfMemory(const std::string& msg) {
    return Status(StatusCode::OutOfMemory, msg);
  }

  static Status KeyError(const std::string& msg) {
    return Status(StatusCode::KeyError, msg);
  }

  static Status TypeError(const std::string& msg) {
    return Status(StatusCode::TypeError, msg);
  }

  static Status UnknownError(const std::string& msg) {
    return Status(StatusCode::UnknownError, msg);
  }

  static Status NotImplemented(const std::string& msg) {
    return Status(StatusCode::NotImplemented, msg);
  }

  static Status Invalid(const std::string& msg) {
    return Status(StatusCode::Invalid, msg);
  }

  static Status IOError(const std::string& msg) {
    return Status(StatusCode::IOError, msg);
  }

  static Status SerializationError(const std::string& msg) {
    return Status(StatusCode::SerializationError, msg);
  }

  static Status PlasmaObjectExists(const std::string& msg) {
    return Status(StatusCode::PlasmaObjectExists, msg);
  }

  static Status PlasmaObjectNonexistent(const std::string& msg) {
    return Status(StatusCode::PlasmaObjectNonexistent, msg);
  }

  static Status PlasmaObjectAlreadySealed(const std::string& msg) {
    return Status(StatusCode::PlasmaObjectAlreadySealed, msg);
  }

  static Status PlasmaStoreFull(const std::string& msg) {
    return Status(StatusCode::PlasmaStoreFull, msg);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  bool IsOutOfMemory() const { return code() == StatusCode::OutOfMemory; }
  bool IsKeyError() const { return code() == StatusCode::KeyError; }
  bool IsInvalid() const { return code() == StatusCode::Invalid; }
  bool IsIOError() const { return code() == StatusCode::IOError; }
  bool IsTypeError() const { return code() == StatusCode::TypeError; }
  bool IsUnknownError() const { return code() == StatusCode::UnknownError; }
  bool IsNotImplemented() const { return code() == StatusCode::NotImplemented; }
  // An object could not be serialized or deserialized.
  bool IsSerializationError() const { return code() == StatusCode::SerializationError; }
  // An error is propagated from a nested Python function.
  bool IsPythonError() const { return code() == StatusCode::PythonError; }
  // An object with this object ID already exists in the plasma store.
  bool IsPlasmaObjectExists() const { return code() == StatusCode::PlasmaObjectExists; }
  // An object was requested that doesn't exist in the plasma store.
  bool IsPlasmaObjectNonexistent() const {
    return code() == StatusCode::PlasmaObjectNonexistent;
  }
  // An already sealed object is tried to be sealed again.
  bool IsPlasmaObjectAlreadySealed() const {
    return code() == StatusCode::PlasmaObjectAlreadySealed;
  }
  // An object is too large to fit into the plasma store.
  bool IsPlasmaStoreFull() const { return code() == StatusCode::PlasmaStoreFull; }

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

  void CopyFrom(const State* s);
};

static inline std::ostream& operator<<(std::ostream& os, const Status& x) {
  os << x.ToString();
  return os;
}

inline Status::Status(const Status& s)
    : state_((s.state_ == NULL) ? NULL : new State(*s.state_)) {}

inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    CopyFrom(s.state_);
  }
}

}  // namespace arrow

#endif  // ARROW_STATUS_H_
