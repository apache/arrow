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

#ifndef PYARROW_UTIL_DATETIME_H
#define PYARROW_UTIL_DATETIME_H

#include <algorithm>
#include <chrono>

#include "arrow/python/platform.h"
#include "arrow/python/visibility.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

// By default, PyDateTimeAPI is a *static* variable.  This forces
// PyDateTime_IMPORT to be called in every C/C++ module using the
// C datetime API.  This is error-prone and potentially costly.
// Instead, we redefine PyDateTimeAPI to point to a global variable,
// which is initialized once by calling InitDatetime().
#define PyDateTimeAPI ::arrow::py::internal::datetime_api

namespace arrow {
namespace py {
namespace internal {

extern PyDateTime_CAPI* datetime_api;

ARROW_PYTHON_EXPORT
void InitDatetime();

ARROW_PYTHON_EXPORT
inline int64_t PyTime_to_us(PyObject* pytime) {
  return (static_cast<int64_t>(PyDateTime_TIME_GET_HOUR(pytime)) * 3600000000LL +
          static_cast<int64_t>(PyDateTime_TIME_GET_MINUTE(pytime)) * 60000000LL +
          static_cast<int64_t>(PyDateTime_TIME_GET_SECOND(pytime)) * 1000000LL +
          PyDateTime_TIME_GET_MICROSECOND(pytime));
}

ARROW_PYTHON_EXPORT
inline int64_t PyTime_to_s(PyObject* pytime) { return PyTime_to_us(pytime) / 1000000; }

ARROW_PYTHON_EXPORT
inline int64_t PyTime_to_ms(PyObject* pytime) { return PyTime_to_us(pytime) / 1000; }

ARROW_PYTHON_EXPORT
inline int64_t PyTime_to_ns(PyObject* pytime) { return PyTime_to_us(pytime) * 1000; }

ARROW_PYTHON_EXPORT
Status PyTime_from_int(int64_t val, const TimeUnit::type unit, PyObject** out);

ARROW_PYTHON_EXPORT
Status PyDate_from_int(int64_t val, const DateUnit unit, PyObject** out);

ARROW_PYTHON_EXPORT
Status PyDateTime_from_int(int64_t val, const TimeUnit::type unit, PyObject** out);

using TimePoint =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

ARROW_PYTHON_EXPORT
Status PyDateTime_from_TimePoint(TimePoint val, PyObject** out);

ARROW_PYTHON_EXPORT
int64_t PyDate_to_days(PyDateTime_Date* pydate);

ARROW_PYTHON_EXPORT
inline int64_t PyDate_to_ms(PyDateTime_Date* pydate) {
  return PyDate_to_days(pydate) * 24 * 3600 * 1000;
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_s(PyDateTime_DateTime* pydatetime) {
  int64_t total_seconds = 0;
  total_seconds += PyDateTime_DATE_GET_SECOND(pydatetime);
  total_seconds += PyDateTime_DATE_GET_MINUTE(pydatetime) * 60;
  total_seconds += PyDateTime_DATE_GET_HOUR(pydatetime) * 3600;

  return total_seconds +
         (PyDate_to_ms(reinterpret_cast<PyDateTime_Date*>(pydatetime)) / 1000LL);
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_ms(PyDateTime_DateTime* pydatetime) {
  int64_t date_ms = PyDateTime_to_s(pydatetime) * 1000;
  int ms = PyDateTime_DATE_GET_MICROSECOND(pydatetime) / 1000;
  return date_ms + ms;
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_us(PyDateTime_DateTime* pydatetime) {
  int64_t ms = PyDateTime_to_s(pydatetime) * 1000;
  int us = PyDateTime_DATE_GET_MICROSECOND(pydatetime);
  return ms * 1000 + us;
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_ns(PyDateTime_DateTime* pydatetime) {
  return PyDateTime_to_us(pydatetime) * 1000;
}

}  // namespace internal
}  // namespace py
}  // namespace arrow

#endif  // PYARROW_UTIL_DATETIME_H
