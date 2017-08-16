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

Status GetValue(std::shared_ptr<Array> arr, int32_t index, int32_t type, PyObject* base,
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
      return CheckPyError();
    }
    case Type::STRING: {
      int32_t nchars;
      const uint8_t* str =
          std::static_pointer_cast<StringArray>(arr)->GetValue(index, &nchars);
      *result = PyUnicode_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return CheckPyError();
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

template<typename CreateFn, typename SetItemFn>
Status DeserializeSequence(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
                           PyObject* base,
                           const std::vector<std::shared_ptr<Tensor>>& tensors,
                           CreateFn create_fn, SetItemFn set_item_fn,
                           PyObject** out) {
  auto data = std::dynamic_pointer_cast<UnionArray>(array);
  int32_t size = array->length();
  ScopedRef result(create_fn(stop_idx - start_idx));
  auto types = std::make_shared<Int8Array>(size, data->type_ids());
  auto offsets = std::make_shared<Int32Array>(size, data->value_offsets());
  for (int32_t i = start_idx; i < stop_idx; ++i) {
    if (data->IsNull(i)) {
      Py_INCREF(Py_None);
      set_item_fn(result.get(), i - start_idx, Py_None);
    } else {
      int32_t offset = offsets->Value(i);
      int8_t type = types->Value(i);
      std::shared_ptr<Array> arr = data->child(type);
      PyObject* value;
      RETURN_NOT_OK(GetValue(arr, offset, type, base, tensors, &value));
      set_item_fn(result.get(), i - start_idx, value);
    }
  }
  *out = result.release();
  return Status::OK();
}

Status DeserializeList(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
                       PyObject* base,
                       const std::vector<std::shared_ptr<Tensor>>& tensors,
                       PyObject** out) {
  return DeserializeSequence(array, start_idx, stop_idx, base, tensors, PyList_New, PyList_SetItem, out);
}

Status DeserializeTuple(std::shared_ptr<Array> array, int32_t start_idx, int32_t stop_idx,
                        PyObject* base,
                        const std::vector<std::shared_ptr<Tensor>>& tensors,
                        PyObject** out) {
  return DeserializeSequence(array, start_idx, stop_idx, base, tensors, PyTuple_New, PyTuple_SetItem, out);
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
    PyDict_SetItem(result, PyList_GET_ITEM(keys, i - start_idx),
                   PyList_GET_ITEM(vals, i - start_idx));
  }
  // PyDict_SetItem behaves differently from PyList_SetItem and PyTuple_SetItem.
  // The latter two steal references whereas PyDict_SetItem does not. So we need
  // to steal it by hand here.
  Py_XDECREF(keys);
  Py_XDECREF(vals);
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(result, py_type)) {
    PyObject* callback_result;
    CallCustomCallback(pyarrow_deserialize_callback, result, &callback_result);
    Py_XDECREF(result);
    result = callback_result;
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
