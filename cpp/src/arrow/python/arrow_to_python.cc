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

#include "arrow/python/arrow_to_python.h"

#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"

namespace arrow {
namespace py {

#if PY_MAJOR_VERSION >= 3
#define PyInt_FromLong PyLong_FromLong
#endif

Status get_value(std::shared_ptr<Array> arr, int32_t index, int32_t type, PyObject* base,
                 const std::vector<std::shared_ptr<Tensor>>& tensors, PyObject** result) {
  switch (arr->type()->id()) {
    case Type::BOOL:
      *result =
          PyBool_FromLong(std::static_pointer_cast<BooleanArray>(arr)->Value(index));
      return Status::OK();
    case Type::INT64:
      *result = PyInt_FromLong(std::static_pointer_cast<Int64Array>(arr)->Value(index));
      return Status::OK();
    case Type::BINARY: {
      int32_t nchars;
      const uint8_t* str =
          std::static_pointer_cast<BinaryArray>(arr)->GetValue(index, &nchars);
      *result = PyBytes_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return Status::OK();
    }
    case Type::STRING: {
      int32_t nchars;
      const uint8_t* str =
          std::static_pointer_cast<StringArray>(arr)->GetValue(index, &nchars);
      *result = PyUnicode_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return Status::OK();
    }
    case Type::FLOAT:
      *result =
          PyFloat_FromDouble(std::static_pointer_cast<FloatArray>(arr)->Value(index));
      return Status::OK();
    case Type::DOUBLE:
      *result =
          PyFloat_FromDouble(std::static_pointer_cast<DoubleArray>(arr)->Value(index));
      return Status::OK();
    case Type::STRUCT: {
      auto s = std::static_pointer_cast<StructArray>(arr);
      auto l = std::static_pointer_cast<ListArray>(s->field(0));
      if (s->type()->child(0)->name() == "list") {
        return DeserializeList(l->values(), l->value_offset(index),
                               l->value_offset(index + 1), base, tensors, result);
      } else if (s->type()->child(0)->name() == "tuple") {
        return DeserializeTuple(l->values(), l->value_offset(index),
                                l->value_offset(index + 1), base, tensors, result);
      } else if (s->type()->child(0)->name() == "dict") {
        return DeserializeDict(l->values(), l->value_offset(index),
                               l->value_offset(index + 1), base, tensors, result);
      } else {
        DCHECK(false) << "error";
      }
    }
    // We use an Int32Builder here to distinguish the tensor indices from
    // the Type::INT64 above (see tensor_indices_ in sequence.h).
    case Type::INT32: {
      return DeserializeArray(arr, index, base, tensors, result);
    }
    default:
      DCHECK(false) << "union tag not recognized " << type;
  }
  return Status::OK();
}

#define DESERIALIZE_SEQUENCE(CREATE, SET_ITEM)                              \
  auto data = std::dynamic_pointer_cast<UnionArray>(array);                 \
  int32_t size = array->length();                                           \
  PyObject* result = CREATE(stop_idx - start_idx);                          \
  auto types = std::make_shared<Int8Array>(size, data->type_ids());         \
  auto offsets = std::make_shared<Int32Array>(size, data->value_offsets()); \
  for (int32_t i = start_idx; i < stop_idx; ++i) {                          \
    if (data->IsNull(i)) {                                                  \
      Py_INCREF(Py_None);                                                   \
      SET_ITEM(result, i - start_idx, Py_None);                             \
    } else {                                                                \
      int32_t offset = offsets->Value(i);                                   \
      int8_t type = types->Value(i);                                        \
      std::shared_ptr<Array> arr = data->child(type);                       \
      PyObject* value;                                                      \
      RETURN_NOT_OK(get_value(arr, offset, type, base, tensors, &value));   \
      SET_ITEM(result, i - start_idx, value);                               \
    }                                                                       \
  }                                                                         \
  *out = result;                                                            \
  return Status::OK();

Status DeserializeList(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
                       PyObject* base,
                       const std::vector<std::shared_ptr<Tensor>>& tensors,
                       PyObject** out) {
  DESERIALIZE_SEQUENCE(PyList_New, PyList_SetItem)
}

Status DeserializeTuple(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
                        PyObject* base,
                        const std::vector<std::shared_ptr<Tensor>>& tensors,
                        PyObject** out) {
  DESERIALIZE_SEQUENCE(PyTuple_New, PyTuple_SetItem)
}

Status DeserializeDict(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
                       PyObject* base,
                       const std::vector<std::shared_ptr<Tensor>>& tensors,
                       PyObject** out) {
  auto data = std::dynamic_pointer_cast<StructArray>(array);
  // TODO(pcm): error handling, get rid of the temporary copy of the list
  PyObject *keys, *vals;
  PyObject* result = PyDict_New();
  ARROW_RETURN_NOT_OK(
      DeserializeList(data->field(0), start_idx, stop_idx, base, tensors, &keys));
  ARROW_RETURN_NOT_OK(
      DeserializeList(data->field(1), start_idx, stop_idx, base, tensors, &vals));
  for (int32_t i = start_idx; i < stop_idx; ++i) {
    PyDict_SetItem(result, PyList_GetItem(keys, i - start_idx),
                   PyList_GetItem(vals, i - start_idx));
  }
  Py_XDECREF(keys);  // PyList_GetItem(keys, ...) incremented the reference count
  Py_XDECREF(vals);  // PyList_GetItem(vals, ...) incremented the reference count
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(result, py_type) && pyarrow_deserialize_callback) {
    PyObject* arglist = Py_BuildValue("(O)", result);
    // The result of the call to PyObject_CallObject will be passed to Python
    // and its reference count will be decremented by the interpreter.
    PyObject* callback_result =
        PyObject_CallObject(pyarrow_deserialize_callback, arglist);
    Py_XDECREF(arglist);
    Py_XDECREF(result);
    result = callback_result;
    if (!callback_result) {
      RETURN_IF_PYERROR();
    }
  }
  *out = result;
  return Status::OK();
}

Status DeserializeArray(std::shared_ptr<Array> array, int32_t offset, PyObject* base,
                        const std::vector<std::shared_ptr<arrow::Tensor>>& tensors,
                        PyObject** out) {
  DCHECK(array);
  int32_t index = std::static_pointer_cast<Int32Array>(array)->Value(offset);
  RETURN_NOT_OK(py::TensorToNdarray(*tensors[index], base, out));
  /* Mark the array as immutable. */
  PyObject* flags = PyObject_GetAttrString(*out, "flags");
  DCHECK(flags != NULL) << "Could not mark Numpy array immutable";
  int flag_set = PyObject_SetAttrString(flags, "writeable", Py_False);
  DCHECK(flag_set == 0) << "Could not mark Numpy array immutable";
  Py_XDECREF(flags);
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
