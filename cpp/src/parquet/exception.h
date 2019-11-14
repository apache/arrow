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

#ifndef PARQUET_EXCEPTION_H
#define PARQUET_EXCEPTION_H

#include <exception>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/status.h"
#include "parquet/platform.h"

// PARQUET-1085
#if !defined(ARROW_UNUSED)
#define ARROW_UNUSED(x) UNUSED(x)
#endif

#define PARQUET_CATCH_NOT_OK(s)                    \
  try {                                            \
    (s);                                           \
  } catch (const ::parquet::ParquetException& e) { \
    return ::arrow::Status::IOError(e.what());     \
  }

#define PARQUET_IGNORE_NOT_OK(s) \
  do {                           \
    ::arrow::Status _s = (s);    \
    ARROW_UNUSED(_s);            \
  } while (0)

#define PARQUET_THROW_NOT_OK(s)                    \
  do {                                             \
    ::arrow::Status _s = (s);                      \
    if (!_s.ok()) {                                \
      std::stringstream ss;                        \
      ss << "Arrow error: " << _s.ToString();      \
      throw ::parquet::ParquetException(ss.str()); \
    }                                              \
  } while (0)

namespace parquet {

class ParquetException : public std::exception {
 public:
  PARQUET_NORETURN static void EofException(const std::string& msg = "") {
    std::stringstream ss;
    ss << "Unexpected end of stream";
    if (!msg.empty()) {
      ss << ": " << msg;
    }
    throw ParquetException(ss.str());
  }

  PARQUET_NORETURN static void NYI(const std::string& msg = "") {
    std::stringstream ss;
    ss << "Not yet implemented: " << msg << ".";
    throw ParquetException(ss.str());
  }

  explicit ParquetException(const char* msg) : msg_(msg) {}

  explicit ParquetException(const std::string& msg) : msg_(msg) {}

  explicit ParquetException(const char* msg, std::exception&) : msg_(msg) {}

  ~ParquetException() throw() override {}

  const char* what() const throw() override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class ParquetStatusException : public ParquetException {
 public:
  explicit ParquetStatusException(::arrow::Status status)
      : ParquetException(status.ToString()), status_(std::move(status)) {}

  /// Return an error status for out-of-memory conditions
  template <typename... Args>
  PARQUET_NORETURN static void OutOfMemory(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::OutOfMemory(std::forward<Args>(args)...));
  }

  /// Return an error status for failed key lookups (e.g. column name in a table)
  template <typename... Args>
  PARQUET_NORETURN static void KeyError(Args&&... args) {
    throw ParquetStatusException(::arrow::Status::KeyError(std::forward<Args>(args)...));
  }

  /// Return an error status for type errors (such as mismatching data types)
  template <typename... Args>
  PARQUET_NORETURN static void TypeError(Args&&... args) {
    throw ParquetStatusException(::arrow::Status::TypeError(std::forward<Args>(args)...));
  }

  /// Return an error status for unknown errors
  template <typename... Args>
  PARQUET_NORETURN static void UnknownError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::UnknownError(std::forward<Args>(args)...));
  }

  /// Return an error status when an operation or a combination of operation and
  /// data types is unimplemented
  template <typename... Args>
  PARQUET_NORETURN static void NotImplemented(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::NotImplemented(std::forward<Args>(args)...));
  }

  /// Return an error status for invalid data (for example a string that fails parsing)
  template <typename... Args>
  PARQUET_NORETURN static void Invalid(Args&&... args) {
    throw ParquetStatusException(::arrow::Status::Invalid(std::forward<Args>(args)...));
  }

  /// Return an error status when an index is out of bounds
  template <typename... Args>
  PARQUET_NORETURN static void IndexError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::IndexError(std::forward<Args>(args)...));
  }

  /// Return an error status when a container's capacity would exceed its limits
  template <typename... Args>
  PARQUET_NORETURN static void CapacityError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::CapacityError(std::forward<Args>(args)...));
  }

  /// Return an error status when some IO-related operation failed
  template <typename... Args>
  PARQUET_NORETURN static void IOError(Args&&... args) {
    throw ParquetStatusException(::arrow::Status::IOError(std::forward<Args>(args)...));
  }

  /// Return an error status when some (de)serialization operation failed
  template <typename... Args>
  PARQUET_NORETURN static void SerializationError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::SerializationError(std::forward<Args>(args)...));
  }

  template <typename... Args>
  PARQUET_NORETURN static void RError(Args&&... args) {
    throw ParquetStatusException(::arrow::Status::RError(std::forward<Args>(args)...));
  }

  template <typename... Args>
  PARQUET_NORETURN static void CodeGenError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::CodeGenError(std::forward<Args>(args)...));
  }

  template <typename... Args>
  PARQUET_NORETURN static void ExpressionValidationError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::ExpressionValidationError(std::forward<Args>(args)...));
  }

  template <typename... Args>
  PARQUET_NORETURN static void ExecutionError(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::ExecutionError(std::forward<Args>(args)...));
  }

  template <typename... Args>
  PARQUET_NORETURN static void AlreadyExists(Args&&... args) {
    throw ParquetStatusException(
        ::arrow::Status::AlreadyExists(std::forward<Args>(args)...));
  }

  const ::arrow::Status& status() const { return status_; }

 private:
  ::arrow::Status status_;
};

template <typename StatusReturnBlock>
void ThrowNotOk(StatusReturnBlock&& b) {
  PARQUET_THROW_NOT_OK(b());
}

}  // namespace parquet

#endif  // PARQUET_EXCEPTION_H
