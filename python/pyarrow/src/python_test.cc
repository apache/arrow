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

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "platform.h"

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/table.h"
#include "arrow/util/decimal.h"

#include "arrow_to_pandas.h"
#include "decimal.h"
#include "helpers.h"
#include "numpy_convert.h"
#include "numpy_interop.h"
#include "python_to_arrow.h"
#include "arrow/util/logging.h"

#define ASSERT_EQ(x, y) { \
  auto&& _left = (x); \
  auto&& _right = (y); \
  if (_left != _right) { \
    return Status::Invalid("Expected equality but ", _left, " != ", _right); \
  } \
}
#define ASSERT_NE(x, y){ \
  auto&& _left = (x); \
  auto&& _right = (y); \
  if (_left == _right) { \
    return Status::Invalid("Expected inequality but ", _left, " == ", _right); \
  } \
}
#define ASSERT_FALSE_PY(error){ \
  auto&& _err = (error); \
  if (_err != NULL){ \
    return Status::Invalid("An error occurred: ", _err); \
  } \
}
#define ASSERT_TRUE_PY(error){ \
  auto&& _err = (error); \
  if (_err == NULL){ \
    return Status::Invalid("Expected an error but did not got one."); \
  } \
}
#define ASSERT_FALSE(error){ \
  auto&& _err = (error); \
  if (_err != false){ \
    return Status::Invalid("An error occurred: ", _err); \
  } \
}
#define ASSERT_TRUE(error){ \
  auto&& _err = (error); \
  if (_err == false){ \
    return Status::Invalid("Expected an error but did not got one."); \
  } \
}
#define ASSERT_OK(expr){ \
  for (::arrow::Status _st = ::arrow::internal::GenericToStatus((expr)); !_st.ok();) \
  return Status::Invalid(expr, "failed with", _st.ToString()); \
}

namespace arrow {

namespace py {

Status TestOwnedRefMoves() {
  std::vector<OwnedRef> vec;
  PyObject *u, *v;
  u = PyList_New(0);
  v = PyList_New(0);

  {
    OwnedRef ref(u);
    vec.push_back(std::move(ref));
    ASSERT_EQ(ref.obj(), nullptr);
  }
  vec.emplace_back(v);
  ASSERT_EQ(Py_REFCNT(u), 1);
  ASSERT_EQ(Py_REFCNT(v), 1);
  return Status::OK();
}

Status TestOwnedRefNoGILMoves() {
  PyAcquireGIL lock;
  lock.release();

  {
    std::vector<OwnedRef> vec;
    PyObject *u, *v;
    {
      lock.acquire();
      u = PyList_New(0);
      v = PyList_New(0);
      lock.release();
    }
    {
      OwnedRefNoGIL ref(u);
      vec.push_back(std::move(ref));
      ASSERT_EQ(ref.obj(), nullptr);
    }
    vec.emplace_back(v);
    ASSERT_EQ(Py_REFCNT(u), 1);
    ASSERT_EQ(Py_REFCNT(v), 1);
    return Status::OK();
  }
}

std::string FormatPythonException(const std::string& exc_class_name) {
  std::stringstream ss;
  ss << "Python exception: ";
  ss << exc_class_name;
  return ss.str();
}

Status TestCheckPyErrorStatus() {
  Status st;
  std::string expected_detail = "";

  // auto check_error = [](Status& st, const char* expected_message = "some error",
  //                       std::string expected_detail = "") {
  //   st = CheckPyError();
  //   ASSERT_EQ(st.message(), expected_message);
  //   ASSERT_FALSE_PY(PyErr_Occurred());
  //   if (expected_detail.size() > 0) {
  //     auto detail = st.detail();
  //     ASSERT_NE(detail, nullptr);
  //     ASSERT_EQ(detail->ToString(), expected_detail);
  //   }
  // };

  for (PyObject* exc_type : {PyExc_Exception, PyExc_SyntaxError}) {
    PyErr_SetString(exc_type, "some error");
    //check_error(st);
    st = CheckPyError();
    ASSERT_EQ(st.message(), "some error");
    ASSERT_FALSE_PY(PyErr_Occurred());
    ASSERT_TRUE(st.IsUnknownError());
  }

  PyErr_SetString(PyExc_TypeError, "some error");
  // check_error(st, "some error", FormatPythonException("TypeError"));
  st = CheckPyError();
  expected_detail = FormatPythonException("TypeError");
  ASSERT_EQ(st.message(), "some error");
  ASSERT_FALSE_PY(PyErr_Occurred());
  if (expected_detail.size() > 0) {
    auto detail = st.detail();
    ASSERT_NE(detail, nullptr);
    ASSERT_EQ(detail->ToString(), expected_detail);
  }
  ASSERT_TRUE(st.IsTypeError());

  PyErr_SetString(PyExc_ValueError, "some error");
  // check_error(st);
  st = CheckPyError();
  ASSERT_EQ(st.message(), "some error");
  ASSERT_FALSE_PY(PyErr_Occurred());
  ASSERT_TRUE(st.IsInvalid());

  PyErr_SetString(PyExc_KeyError, "some error");
  // check_error(st, "'some error'");
  st = CheckPyError();
  ASSERT_EQ(st.message(), "'some error'");
  ASSERT_FALSE_PY(PyErr_Occurred());
  ASSERT_TRUE(st.IsKeyError());

  for (PyObject* exc_type : {PyExc_OSError, PyExc_IOError}) {
    PyErr_SetString(exc_type, "some error");
    //  check_error(st);
    st = CheckPyError();
    ASSERT_EQ(st.message(), "some error");
    ASSERT_FALSE_PY(PyErr_Occurred());
    ASSERT_TRUE(st.IsIOError());
  }

  PyErr_SetString(PyExc_NotImplementedError, "some error");
  // check_error(st, "some error", FormatPythonException("NotImplementedError"));
  st = CheckPyError();
  expected_detail = FormatPythonException("NotImplementedError");
  ASSERT_EQ(st.message(), "some error");
  ASSERT_FALSE_PY(PyErr_Occurred());
  if (expected_detail.size() > 0) {
    auto detail = st.detail();
    ASSERT_NE(detail, nullptr);
    ASSERT_EQ(detail->ToString(), expected_detail);
  }
  ASSERT_TRUE(st.IsNotImplemented());

  // No override if a specific status code is given
  PyErr_SetString(PyExc_TypeError, "some error");
  st = CheckPyError(StatusCode::SerializationError);
  ASSERT_TRUE(st.IsSerializationError());
  ASSERT_EQ(st.message(), "some error");
  ASSERT_FALSE_PY(PyErr_Occurred());

  return Status::OK();
}

Status TestCheckPyErrorStatusNoGIL() {
  PyAcquireGIL lock;
  {
    Status st;
    PyErr_SetString(PyExc_ZeroDivisionError, "zzzt");
    st = ConvertPyError();
    ASSERT_FALSE_PY(PyErr_Occurred());
    lock.release();
    ASSERT_TRUE(st.IsUnknownError());
    ASSERT_EQ(st.message(), "zzzt");
    ASSERT_EQ(st.detail()->ToString(), FormatPythonException("ZeroDivisionError"));
    return Status::OK();
  }
}

Status TestRestorePyErrorBasics() {
  PyErr_SetString(PyExc_ZeroDivisionError, "zzzt");
  auto st = ConvertPyError();
  ASSERT_FALSE_PY(PyErr_Occurred());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "zzzt");
  ASSERT_EQ(st.detail()->ToString(), FormatPythonException("ZeroDivisionError"));

  RestorePyError(st);
  ASSERT_TRUE_PY(PyErr_Occurred());
  PyObject* exc_type;
  PyObject* exc_value;
  PyObject* exc_traceback;
  PyErr_Fetch(&exc_type, &exc_value, &exc_traceback);
  ASSERT_TRUE(PyErr_GivenExceptionMatches(exc_type, PyExc_ZeroDivisionError));
  std::string py_message;
  ASSERT_OK(internal::PyObject_StdStringStr(exc_value, &py_message));
  ASSERT_EQ(py_message, "zzzt");

  return Status::OK();
}

Status TestPyBufferInvalidInputObject() {
  std::shared_ptr<Buffer> res;
  PyObject* input = Py_None;
  auto old_refcnt = Py_REFCNT(input);
  {
    Status st = PyBuffer::FromPyObject(input).status();
    // ASSERT_TRUE(IsPyError(st)) << st.ToString();
    ASSERT_FALSE_PY(PyErr_Occurred());
  }
  ASSERT_EQ(old_refcnt, Py_REFCNT(input));
  return Status::OK();
}

// Because of how it is declared, the Numpy C API instance initialized
// within libarrow_python.dll may not be visible in this test under Windows
// ("unresolved external symbol arrow_ARRAY_API referenced").
#ifndef _WIN32
Status TestPyBufferNumpyArray() {
  const npy_intp dims[1] = {10};

  OwnedRef arr_ref(PyArray_SimpleNew(1, dims, NPY_FLOAT));
  PyObject* arr = arr_ref.obj();
  ASSERT_NE(arr, nullptr);
  auto old_refcnt = Py_REFCNT(arr);

  // ASSERT_OK_AND_ASSIGN(auto buf, PyBuffer::FromPyObject(arr));
  // ASSERT_OK(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__).status())
  auto buf = std::move(PyBuffer::FromPyObject(arr)).ValueOrDie();

  ASSERT_TRUE(buf->is_cpu());
  ASSERT_EQ(buf->data(), PyArray_DATA(reinterpret_cast<PyArrayObject*>(arr)));
  ASSERT_TRUE(buf->is_mutable());
  ASSERT_EQ(buf->mutable_data(), buf->data());
  ASSERT_EQ(old_refcnt + 1, Py_REFCNT(arr));
  buf.reset();
  ASSERT_EQ(old_refcnt, Py_REFCNT(arr));

  // Read-only
  PyArray_CLEARFLAGS(reinterpret_cast<PyArrayObject*>(arr), NPY_ARRAY_WRITEABLE);
  // ASSERT_OK_AND_ASSIGN(buf, PyBuffer::FromPyObject(arr));
  buf = std::move(PyBuffer::FromPyObject(arr)).ValueOrDie();
  ASSERT_TRUE(buf->is_cpu());
  ASSERT_EQ(buf->data(), PyArray_DATA(reinterpret_cast<PyArrayObject*>(arr)));
  ASSERT_FALSE(buf->is_mutable());
  ASSERT_EQ(old_refcnt + 1, Py_REFCNT(arr));
  buf.reset();
  ASSERT_EQ(old_refcnt, Py_REFCNT(arr));

  return Status::OK();
}

Status TestNumPyBufferNumpyArray() {
  npy_intp dims[1] = {10};

  OwnedRef arr_ref(PyArray_SimpleNew(1, dims, NPY_FLOAT));
  PyObject* arr = arr_ref.obj();
  ASSERT_NE(arr, nullptr);
  auto old_refcnt = Py_REFCNT(arr);

  auto buf = std::make_shared<NumPyBuffer>(arr);
  ASSERT_TRUE(buf->is_cpu());
  ASSERT_EQ(buf->data(), PyArray_DATA(reinterpret_cast<PyArrayObject*>(arr)));
  ASSERT_TRUE(buf->is_mutable());
  ASSERT_EQ(buf->mutable_data(), buf->data());
  ASSERT_EQ(old_refcnt + 1, Py_REFCNT(arr));
  buf.reset();
  ASSERT_EQ(old_refcnt, Py_REFCNT(arr));

  // Read-only
  PyArray_CLEARFLAGS(reinterpret_cast<PyArrayObject*>(arr), NPY_ARRAY_WRITEABLE);
  buf = std::make_shared<NumPyBuffer>(arr);
  ASSERT_TRUE(buf->is_cpu());
  ASSERT_EQ(buf->data(), PyArray_DATA(reinterpret_cast<PyArrayObject*>(arr)));
  ASSERT_FALSE(buf->is_mutable());
  ASSERT_EQ(old_refcnt + 1, Py_REFCNT(arr));
  buf.reset();
  ASSERT_EQ(old_refcnt, Py_REFCNT(arr));

  return Status::OK();
}
#endif

}  // namespace py
}  // namespace arrow
