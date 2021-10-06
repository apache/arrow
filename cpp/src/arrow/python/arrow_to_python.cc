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

#include "arrow/python/arrow_to_python_internal.h"
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

}  // namespace

Result<PyObject*> ArrowToPython::ToPyList(const Array& array) {
  RETURN_NOT_OK(CheckInterval(*array.type()));
  OwnedRef out_list(PyList_New(array.length()));
  RETURN_IF_PYERROR();
  PyListAssigner out_objects(out_list.obj());
  auto& interval_array =
      arrow::internal::checked_cast<const MonthDayNanoIntervalArray&>(array);
  RETURN_NOT_OK(internal::WriteArrayObjects(
      interval_array,
      [&](const MonthDayNanoIntervalType::MonthDayNanos& interval, PyListAssigner& out) {
        PyObject* tuple = internal::MonthDayNanoIntervalToNamedTuple(interval);
        if (ARROW_PREDICT_FALSE(tuple == nullptr)) {
          RETURN_IF_PYERROR();
        }

        *out = tuple;
        return Status::OK();
      },
      out_objects));
  return out_list.detach();
}

Result<PyObject*> ArrowToPython::ToPyObject(const Scalar& scalar) {
  RETURN_NOT_OK(CheckInterval(*scalar.type));
  if (scalar.is_valid) {
    return internal::MonthDayNanoIntervalToNamedTuple(
        arrow::internal::checked_cast<const MonthDayNanoIntervalScalar&>(scalar).value);
  } else {
    Py_INCREF(Py_None);
    return Py_None;
  }
}

}  // namespace py
}  // namespace arrow
