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

#include <Python.h>

#include "pyarrow/adapters/builtin.h"

#include <arrow/api.h>

#include "pyarrow/status.h"

namespace pyarrow {

using arrow::DataType;
using arrow::LogicalType;

static inline bool IsPyInteger(PyObject* obj) {
#if PYARROW_IS_PY2
  return PyLong_Check(obj) || PyInt_Check(obj);
#else
  return PyLong_Check(obj);
#endif
}

static inline bool IsPyBaseString(PyObject* obj) {
#if PYARROW_IS_PY2
  return PyString_Check(obj) || PyUnicode_Check(obj);
#else
  return PyUnicode_Check(obj);
#endif
}

class ScalarTypeInfer {
 public:
  ScalarTypeInfer() :
      none_count_(0),
      bool_count_(0),
      int_count_(0),
      float_count_(0),
      string_count_(0) {}

  void Visit(PyObject* obj) {
    if (obj == Py_None) {
      ++none_count_;
    } else if (PyFloat_Check(obj)) {
      ++float_count_;
    } else if (IsPyInteger(obj)) {
      ++int_count_;
    } else if (IsPyBaseString(obj)) {
      ++string_count_;
    } else {
      // TODO(wesm): accumulate error information somewhere
    }
  }

  std::shared_ptr<DataType> GetType() {
    // TODO(wesm): handling mixed-type cases

    if (float_count_) {
      return arrow::DOUBLE;
    } else if (int_count_) {
      // TODO(wesm): tighter type later
      return arrow::INT64;
    } else if (bool_count_) {
      // TODO(wesm): tighter type later
      return arrow::BOOL;
    } else if (string_count_) {
      return arrow::STRING;
    } else {
      return arrow::NA;
    }
  }

 private:
  int64_t none_count_;
  int64_t bool_count_;
  int64_t int_count_;
  int64_t float_count_;
  int64_t string_count_;

  // Place to accumulate errors
  // std::vector<Status> errors_;
};

// Non-exhaustive type inference
static Status InferArrowType(PyObject* obj, int64_t* size,
    std::shared_ptr<DataType>* out_type) {
  *size = PySequence_Size(obj);
  if (PyErr_Occurred()) {
    // Not a sequence
    PyErr_Clear();
    return Status::TypeError("Object is not a sequence");
  }

  // For 0-length sequences, refuse to guess
  if (*size == 0) {
    *out_type = arrow::NA;
  }

  ScalarTypeInfer inferer;

  for (int64_t i = 0; i < *size; ++i) {
    // TODO(wesm): Error checking?
    // TODO(wesm): Specialize for PyList_GET_ITEM?
    OwnedRef item_ref(PySequence_GetItem(obj, i));
    PyObject* item = item_ref.obj();

    if (PyList_Check(item) || PyDict_Check(item)) {
      // TODO(wesm): inferring types for collections
      return Status::NotImplemented("No type inference for collections");
    } else {
      inferer.Visit(obj);
    }
  }

  *out_type = inferer.GetType();
  return Status::OK();
}

// Marshal Python sequence (list, tuple, etc.) to Arrow array
class SeqConverter {
 public:
  SeqConverter();

  virtual Status AppendData(PyObject* seq) = 0;

 private:
  // Borrowed reference for now
  PyObject* obj_;
};

class BooleanConverter : SeqConverter {
 public:

  Status AppendData(PyObject* obj) override {
    return Status::OK();
  }
};

template <typename T>
class IntegerConverter : SeqConverter {
 public:

  Status AppendData(PyObject* obj) override {
    return Status::OK();
  }
};

template <typename T>
class FloatingConverter : SeqConverter {
 public:

  Status AppendData(PyObject* obj) override {
    return Status::OK();
  }
};

class StringConverter : SeqConverter {
 public:

  Status AppendData(PyObject* obj) override {
    return Status::OK();
  }

 private:
  arrow::StringBuilder builder_;
};

class ListConverter : SeqConverter {
 public:

  Status AppendData(PyObject* obj) override {
    return Status::OK();
  }

 private:
  arrow::ListBuilder builder_;
};

Status GetConverter(const std::shared_ptr<DataType>& type,
    std::shared_ptr<SeqConverter>* out) {
  switch (type->type) {
    case LogicalType::BOOL:
      break;
    case LogicalType::INT64:
      break;
    case LogicalType::DOUBLE:
      break;
    case LogicalType::STRING:
      break;
    case LogicalType::LIST:
    case LogicalType::STRUCT:
    default:
      return Status::NotImplemented("No type converter implemetned");
      break;
  }
  return Status::OK();
}

Status ConvertPySequence(PyObject* obj, std::shared_ptr<arrow::Array>* out) {
  std::shared_ptr<DataType> type;
  int64_t size;
  RETURN_NOT_OK(InferArrowType(obj, &size, &type));

  std::shared_ptr<SeqConverter> converter;
  RETURN_NOT_OK(GetConverter(type, &converter));
  RETURN_NOT_OK(converter->AppendData(obj));

  return Status::OK();
}

} // namespace pyarrow
