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

#include <algorithm>
#include <chrono>

#include "arrow/python/common.h"
#include "arrow/python/platform.h"
#include "arrow/python/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"

// By default, PyDateTimeAPI is a *static* variable.  This forces
// PyDateTime_IMPORT to be called in every C/C++ module using the
// C datetime API.  This is error-prone and potentially costly.
// Instead, we redefine PyDateTimeAPI to point to a global variable,
// which is initialized once by calling InitDatetime().
#ifdef PYPY_VERSION
#  include "datetime.h"
#else
#  define PyDateTimeAPI ::arrow::py::internal::datetime_api

// Under Py_LIMITED_API (the cp311-abi3 build) CPython's <datetime.h> is
// entirely absent (it lives behind #ifndef Py_LIMITED_API), so neither the
// PyDateTime_CAPI struct nor the PyDate_Check / PyDate_FromDate macros are
// provided. The datetime C-API is still exposed at runtime through the
// "datetime.datetime_CAPI" capsule (which InitDatetime() imports and stores in
// datetime_api); the struct layout is a documented, stable part of that
// capsule contract. Define the struct and the check/constructor macros we use
// so the same code compiles in the limited-API build. The layout must stay in
// sync with CPython's Modules/_datetimemodule.c datetime_capsule.
#  ifdef Py_LIMITED_API
#    ifndef DATETIME_H
#      define DATETIME_H
#    endif
#    ifndef PyDateTime_CAPSULE_NAME
#      define PyDateTime_CAPSULE_NAME "datetime.datetime_CAPI"
#    endif
typedef struct {
  /* type objects */
  PyTypeObject* DateType;
  PyTypeObject* DateTimeType;
  PyTypeObject* TimeType;
  PyTypeObject* DeltaType;
  PyTypeObject* TZInfoType;

  /* singletons */
  PyObject* TimeZone_UTC;

  /* constructors */
  PyObject* (*Date_FromDate)(int, int, int, PyTypeObject*);
  PyObject* (*DateTime_FromDateAndTime)(int, int, int, int, int, int, int,
                                        PyObject*, PyTypeObject*);
  PyObject* (*Time_FromTime)(int, int, int, int, PyObject*, PyTypeObject*);
  PyObject* (*Delta_FromDelta)(int, int, int, int, PyTypeObject*);
  PyObject* (*TimeZone_FromTimeZone)(PyObject* offset, PyObject* name);

  /* constructors for the DB API */
  PyObject* (*DateTime_FromTimestamp)(PyObject*, PyObject*, PyObject*);
  PyObject* (*Date_FromTimestamp)(PyObject*, PyObject*);

  /* PEP 495 constructors */
  PyObject* (*DateTime_FromDateAndTimeAndFold)(int, int, int, int, int, int,
                                               int, PyObject*, int,
                                               PyTypeObject*);
  PyObject* (*Time_FromTimeAndFold)(int, int, int, int, PyObject*, int,
                                    PyTypeObject*);
} PyDateTime_CAPI;

#    define PyDate_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->DateType)
#    define PyDate_CheckExact(op) Py_IS_TYPE(op, PyDateTimeAPI->DateType)
#    define PyDateTime_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->DateTimeType)
#    define PyDateTime_CheckExact(op) Py_IS_TYPE(op, PyDateTimeAPI->DateTimeType)
#    define PyTime_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->TimeType)
#    define PyTime_CheckExact(op) Py_IS_TYPE(op, PyDateTimeAPI->TimeType)
#    define PyDelta_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->DeltaType)
#    define PyDelta_CheckExact(op) Py_IS_TYPE(op, PyDateTimeAPI->DeltaType)
#    define PyTZInfo_Check(op) PyObject_TypeCheck(op, PyDateTimeAPI->TZInfoType)
#    define PyTZInfo_CheckExact(op) Py_IS_TYPE(op, PyDateTimeAPI->TZInfoType)
#    define PyDateTime_TimeZone_UTC PyDateTimeAPI->TimeZone_UTC
#    define PyDate_FromDate(year, month, day) \
      PyDateTimeAPI->Date_FromDate(year, month, day, PyDateTimeAPI->DateType)
#    define PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, usec) \
      PyDateTimeAPI->DateTime_FromDateAndTime(year, month, day, hour, min, sec, \
                                              usec, Py_None, \
                                              PyDateTimeAPI->DateTimeType)
#    define PyTime_FromTime(hour, minute, second, usecond) \
      PyDateTimeAPI->Time_FromTime(hour, minute, second, usecond, Py_None, \
                                   PyDateTimeAPI->TimeType)
#    define PyDelta_FromDSU(days, seconds, useconds) \
      PyDateTimeAPI->Delta_FromDelta(days, seconds, useconds, 1, \
                                     PyDateTimeAPI->DeltaType)
#  endif  // Py_LIMITED_API
#endif  // !PYPY_VERSION

namespace arrow {
using internal::AddWithOverflow;
using internal::MultiplyWithOverflow;
namespace py {
namespace internal {

#ifndef PYPY_VERSION
extern PyDateTime_CAPI* datetime_api;

ARROW_PYTHON_EXPORT
void InitDatetime();
#endif

// Returns the MonthDayNano namedtuple type (increments the reference count).
ARROW_PYTHON_EXPORT
PyObject* NewMonthDayNanoTupleType();

// Reads an integer field ("year", "hour", "days", ...) off a Python
// date/time/datetime/timedelta object through the stable C-API. The fast
// datetime struct-field accessors (PyDateTime_TIME_GET_HOUR etc.) are hidden
// under Py_LIMITED_API (abi3), so attribute access is the portable path.
// Callers pass a type-checked object, so the field is guaranteed present and
// integral; a failure is a genuine invariant violation.
ARROW_PYTHON_EXPORT
inline int64_t PyDatetimeField(PyObject* obj, const char* name) {
  OwnedRef field(PyObject_GetAttrString(obj, name));
  if (ARROW_PREDICT_FALSE(field.obj() == nullptr)) {
    Py_FatalError("arrow: failed to read datetime field");
  }
  long long v = PyLong_AsLongLong(field.obj());
  if (ARROW_PREDICT_FALSE(v == -1 && PyErr_Occurred())) {
    Py_FatalError("arrow: datetime field is not an integer");
  }
  return v;
}

ARROW_PYTHON_EXPORT
inline int64_t PyTime_to_us(PyObject* pytime) {
  return (PyDatetimeField(pytime, "hour") * 3600000000LL +
          PyDatetimeField(pytime, "minute") * 60000000LL +
          PyDatetimeField(pytime, "second") * 1000000LL +
          PyDatetimeField(pytime, "microsecond"));
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

// WARNING: This function returns a naive datetime.
ARROW_PYTHON_EXPORT
Status PyDateTime_from_int(int64_t val, const TimeUnit::type unit, PyObject** out);

// This declaration must be the same as in filesystem/filesystem.h
using TimePoint =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

ARROW_PYTHON_EXPORT
int64_t PyDate_to_days(PyObject* pydate);

ARROW_PYTHON_EXPORT
inline int64_t PyDate_to_s(PyObject* pydate) { return PyDate_to_days(pydate) * 86400LL; }

ARROW_PYTHON_EXPORT
inline int64_t PyDate_to_ms(PyObject* pydate) {
  return PyDate_to_days(pydate) * 86400000LL;
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_s(PyObject* pydatetime) {
  return (PyDate_to_s(pydatetime) + PyDatetimeField(pydatetime, "hour") * 3600LL +
          PyDatetimeField(pydatetime, "minute") * 60LL +
          PyDatetimeField(pydatetime, "second"));
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_ms(PyObject* pydatetime) {
  return (PyDateTime_to_s(pydatetime) * 1000LL +
          PyDatetimeField(pydatetime, "microsecond") / 1000);
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_us(PyObject* pydatetime) {
  return (PyDateTime_to_s(pydatetime) * 1000000LL +
          PyDatetimeField(pydatetime, "microsecond"));
}

ARROW_PYTHON_EXPORT
inline int64_t PyDateTime_to_ns(PyObject* pydatetime) {
  return PyDateTime_to_us(pydatetime) * 1000LL;
}

ARROW_PYTHON_EXPORT
inline TimePoint PyDateTime_to_TimePoint(PyObject* pydatetime) {
  return TimePoint(TimePoint::duration(PyDateTime_to_ns(pydatetime)));
}

ARROW_PYTHON_EXPORT
inline int64_t TimePoint_to_ns(TimePoint val) { return val.time_since_epoch().count(); }

ARROW_PYTHON_EXPORT
inline TimePoint TimePoint_from_s(double val) {
  return TimePoint(TimePoint::duration(static_cast<int64_t>(1e9 * val)));
}

ARROW_PYTHON_EXPORT
inline TimePoint TimePoint_from_ns(int64_t val) {
  return TimePoint(TimePoint::duration(val));
}

ARROW_PYTHON_EXPORT
inline int64_t PyDelta_to_s(PyObject* pytimedelta) {
  return (PyDatetimeField(pytimedelta, "days") * 86400LL +
          PyDatetimeField(pytimedelta, "seconds"));
}

ARROW_PYTHON_EXPORT
inline int64_t PyDelta_to_ms(PyObject* pytimedelta) {
  return (PyDelta_to_s(pytimedelta) * 1000LL +
          PyDatetimeField(pytimedelta, "microseconds") / 1000);
}

ARROW_PYTHON_EXPORT
inline Result<int64_t> PyDelta_to_us(PyObject* pytimedelta) {
  int64_t result = PyDelta_to_s(pytimedelta);
  if (MultiplyWithOverflow(result, 1000000LL, &result)) {
    return Status::Invalid("Timedelta too large to fit in 64-bit integer");
  }
  if (AddWithOverflow(result, PyDatetimeField(pytimedelta, "microseconds"), &result)) {
    return Status::Invalid("Timedelta too large to fit in 64-bit integer");
  }
  return result;
}

ARROW_PYTHON_EXPORT
inline Result<int64_t> PyDelta_to_ns(PyObject* pytimedelta) {
  ARROW_ASSIGN_OR_RAISE(int64_t result, PyDelta_to_us(pytimedelta));
  if (MultiplyWithOverflow(result, 1000LL, &result)) {
    return Status::Invalid("Timedelta too large to fit in 64-bit integer");
  }
  return result;
}

ARROW_PYTHON_EXPORT
Result<int64_t> PyDateTime_utcoffset_s(PyObject* pydatetime);

/// \brief Convert a time zone name into a time zone object.
///
/// Supported input strings are:
/// * As used in the Olson time zone database (the "tz database" or
///   "tzdata"), such as "America/New_York"
/// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
/// GIL must be held when calling this method.
ARROW_PYTHON_EXPORT
Result<PyObject*> StringToTzinfo(const std::string& tz, bool prefer_zoneinfo = true);

/// \brief Convert a time zone object to a string representation.
///
/// The output strings are:
/// * An absolute time zone offset of the form +XX:XX or -XX:XX, such as +07:30
///   if the input object is either an instance of pytz._FixedOffset or
///   datetime.timedelta
/// * The timezone's name if the input object's tzname() method returns with a
///   non-empty timezone name such as "UTC" or "America/New_York"
///
/// GIL must be held when calling this method.
ARROW_PYTHON_EXPORT
Result<std::string> TzinfoToString(PyObject* pytzinfo);

/// \brief Convert MonthDayNano to a python namedtuple.
///
/// Return a named tuple (pyarrow.MonthDayNano) containing attributes
/// "months", "days", "nanoseconds" in the given order
/// with values extracted from the fields on interval.
///
/// GIL must be held when calling this method.
ARROW_PYTHON_EXPORT
PyObject* MonthDayNanoIntervalToNamedTuple(
    const MonthDayNanoIntervalType::MonthDayNanos& interval);

/// \brief Convert the given Array to a PyList object containing
/// pyarrow.MonthDayNano objects.
ARROW_PYTHON_EXPORT
Result<PyObject*> MonthDayNanoIntervalArrayToPyList(
    const MonthDayNanoIntervalArray& array);

/// \brief Convert the Scalar object to a pyarrow.MonthDayNano (or None if
/// is isn't valid).
ARROW_PYTHON_EXPORT
Result<PyObject*> MonthDayNanoIntervalScalarToPyObject(
    const MonthDayNanoIntervalScalar& scalar);

}  // namespace internal
}  // namespace py
}  // namespace arrow
