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

#include "arrow/status.h"

#include "parquet/util/macros.h"
#include "parquet/util/visibility.h"

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

#define PARQUET_THROW_NOT_OK(s)                     \
  do {                                              \
    ::arrow::Status _s = (s);                       \
    if (!_s.ok()) {                                 \
      std::stringstream ss;                         \
      ss << "Arrow error: " << _s.ToString();       \
      ::parquet::ParquetException::Throw(ss.str()); \
    }                                               \
  } while (0)

namespace parquet {

class PARQUET_EXPORT ParquetException : public std::exception {
 public:
  PARQUET_NORETURN static void EofException();
  PARQUET_NORETURN static void NYI(const std::string& msg);
  PARQUET_NORETURN static void Throw(const std::string& msg);

  explicit ParquetException(const char* msg);
  explicit ParquetException(const std::string& msg);
  explicit ParquetException(const char* msg, exception& e);

  virtual ~ParquetException() throw();
  virtual const char* what() const throw();

 private:
  std::string msg_;
};

}  // namespace parquet

#endif  // PARQUET_EXCEPTION_H
