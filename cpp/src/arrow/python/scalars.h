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

#ifndef ARROW_PYTHON_SCALARS_H
#define ARROW_PYTHON_SCALARS_H

#include <arrow/api.h>

#include <Python.h>
#include "numpy_interop.h"
#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

#include "sequence.h"

namespace arrow {

Status AppendScalar(PyObject* obj, SequenceBuilder& builder) {
  if (PyArray_IsScalar(obj, Bool)) {
    return builder.AppendBool(((PyBoolScalarObject*)obj)->obval != 0);
  } else if (PyArray_IsScalar(obj, Float)) {
    return builder.AppendFloat(((PyFloatScalarObject*)obj)->obval);
  } else if (PyArray_IsScalar(obj, Double)) {
    return builder.AppendDouble(((PyDoubleScalarObject*)obj)->obval);
  }
  int64_t value = 0;
  if (PyArray_IsScalar(obj, Byte)) {
    value = ((PyByteScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, UByte)) {
    value = ((PyUByteScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, Short)) {
    value = ((PyShortScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, UShort)) {
    value = ((PyUShortScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, Int)) {
    value = ((PyIntScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, UInt)) {
    value = ((PyUIntScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, Long)) {
    value = ((PyLongScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, ULong)) {
    value = ((PyULongScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, LongLong)) {
    value = ((PyLongLongScalarObject*)obj)->obval;
  } else if (PyArray_IsScalar(obj, ULongLong)) {
    value = ((PyULongLongScalarObject*)obj)->obval;
  } else {
    DCHECK(false) << "scalar type not recognized";
  }
  return builder.AppendInt64(value);
}

}  // namespace arrow

#endif  // PYTHON_ARROW_SCALARS_H
