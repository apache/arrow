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

// Functions for converting between pandas's NumPy-based data representation
// and Arrow data structures

#include "arrow/python/arrow_to_python.h"

#include "arrow/python/datetime.h"
#include "arrow/python/helpers.h"
#include "arrow/result_internal.h"
#include "arrow/scalar.h"

namespace arrow {
namespace py {
namespace {

Status CheckInterval(const DataType& datatype) {
  if (datatype.id() != Type::INTERVAL_MONTH_DAY_NANO) {
    return Status::NotImplemented(
        "Only MonthDayIntervalNanoIntervalType supported. Provided.",
        datatype.ToString());
  }
  return Status::OK();
}

// Wrapper around a Python list object that mimics dereference and assignment
// operations.
struct PyListAssigner {
 public:
  explicit PyListAssigner(PyObject* list) : list_(list) { DCHECK(PyList_Check(list_)); }

  PyListAssigner& operator*() { return *this; }

  void operator=(PyObject* obj) {
    if (ARROW_PREDICT_FALSE(PyList_SetItem(list_, current_index_, obj) == -1)) {
      Py_FatalError("list did not have the correct preallocated size.");
    }
  }

  PyListAssigner& operator++() {
    current_index_++;
    return *this;
  }

  PyListAssigner& operator+=(int64_t offset) {
    current_index_ += offset;
    return *this;
  }

 private:
  PyObject* list_;
  int64_t current_index_ = 0;
};

// Args and Kwargs are passed in to avoid reallocation for batch conversion.
template <typename OutputType>
Status ConvertToDateOffset(const MonthDayNanoIntervalType::MonthDayNanos& interval,
                           PyObject* date_offset_constructor, PyObject* args,
                           PyObject* kwargs, OutputType out) {
  DCHECK(internal::BorrowPandasDataOffsetType());
  RETURN_IF_PYERROR();
  // TimeDelta objects do not add nanoseconds component to timestamp.
  // so convert microseconds and remainder to preserve data
  // but give users more expected results.
  int64_t microseconds = interval.nanoseconds / 1000;
  int64_t nanoseconds;
  if (interval.nanoseconds >= 0) {
    nanoseconds = interval.nanoseconds % 1000;
  } else {
    nanoseconds = -((-interval.nanoseconds) % 1000);
  }

  PyDict_SetItemString(kwargs, "months", PyLong_FromLong(interval.months));
  PyDict_SetItemString(kwargs, "days", PyLong_FromLong(interval.days));
  PyDict_SetItemString(kwargs, "microseconds", PyLong_FromLongLong(microseconds));
  PyDict_SetItemString(kwargs, "nanoseconds", PyLong_FromLongLong(nanoseconds));
  *out = PyObject_Call(internal::BorrowPandasDataOffsetType(), args, kwargs);
  RETURN_IF_PYERROR();
  return Status::OK();
}

}  // namespace

Result<PyObject*> ArrowToPython::ToPyList(const Array& array) {
  RETURN_NOT_OK(Init());
  RETURN_NOT_OK(CheckInterval(*array.type()));
  OwnedRef out_list(PyList_New(array.length()));
  RETURN_IF_PYERROR();
  PyListAssigner out_objects(out_list.obj());
  auto& interval_array =
      arrow::internal::checked_cast<const MonthDayNanoIntervalArray&>(array);
  PyObject* date_offset_type = internal::BorrowPandasDataOffsetType();
  if (date_offset_type != nullptr) {
    OwnedRef args(PyTuple_New(0));
    OwnedRef kwargs(PyDict_New());
    RETURN_IF_PYERROR();
    RETURN_NOT_OK(internal::WriteArrayObjects(
        interval_array,
        [&](const MonthDayNanoIntervalType::MonthDayNanos& interval,
            PyListAssigner& out) {
          return ConvertToDateOffset(interval, date_offset_type, args.obj(), kwargs.obj(),
                                     out);
        },
        out_objects));
  } else {
    RETURN_NOT_OK(internal::WriteArrayObjects(
        interval_array,
        [&](const MonthDayNanoIntervalType::MonthDayNanos& interval,
            PyListAssigner& out) {
          ASSIGN_OR_RAISE(PyObject * tuple,
                          internal::MonthDayNanoIntervalToNamedTuple(interval));
          if (ARROW_PREDICT_FALSE(tuple == nullptr)) {
            RETURN_IF_PYERROR();
          }

          *out = tuple;
          return Status::OK();
        },
        out_objects));
  }
  return out_list.detach();
}

Status ArrowToPython::ToNumpyObjectArray(const ArrowToPythonObjectOptions& options,
                                         const ChunkedArray& array,
                                         PyObject** out_objects) {
  RETURN_NOT_OK(Init());
  RETURN_NOT_OK(CheckInterval(*array.type()));
  OwnedRef args(PyTuple_New(0));
  OwnedRef kwargs(PyDict_New());
  RETURN_IF_PYERROR();
  return internal::ConvertAsPyObjects<MonthDayNanoIntervalType>(
      options, array,
      [&](const MonthDayNanoIntervalType::MonthDayNanos& interval, PyObject** out) {
        return ConvertToDateOffset(interval, internal::BorrowPandasDataOffsetType(),
                                   args.obj(), kwargs.obj(), out);
      },
      out_objects);
}

Result<PyObject*> ArrowToPython::ToLogical(const Scalar& scalar) {
  // Pandas's DateOffset is the best type in the python ecosystem to
  // for MonthDayNano interval type so use that if it is available.
  // Otherwise use the primitive type. (this logical is similar to how
  // we handle timestamps today, since datetime.datetime doesn't support nanos).
  // In this case timedelta doesn't support months, years or nanos.
  RETURN_NOT_OK(Init());
  if (internal::BorrowPandasDataOffsetType() != nullptr) {
    RETURN_NOT_OK(CheckInterval(*scalar.type));
    if (!scalar.is_valid) {
      Py_INCREF(Py_None);
      return Py_None;
    }
    OwnedRef args(PyTuple_New(0));
    OwnedRef kwargs(PyDict_New());
    RETURN_IF_PYERROR();
    PyObject* out;
    RETURN_NOT_OK(ConvertToDateOffset(
        arrow::internal::checked_cast<const MonthDayNanoIntervalScalar&>(scalar).value,
        internal::BorrowPandasDataOffsetType(), args.obj(), kwargs.obj(), &out));
    return out;
  } else {
    return ToPrimitive(scalar);
  }
}

Result<PyObject*> ArrowToPython::ToPrimitive(const Scalar& scalar) {
  RETURN_NOT_OK(Init());
  RETURN_NOT_OK(CheckInterval(*scalar.type));
  if (scalar.is_valid) {
    return internal::MonthDayNanoIntervalToNamedTuple(
        arrow::internal::checked_cast<const MonthDayNanoIntervalScalar&>(scalar).value);
  } else {
    Py_INCREF(Py_None);
    return Py_None;
  }
}

Status ArrowToPython::Init() {
  // relies on GIL for interpretation.
  internal::InitPandasStaticData();
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
