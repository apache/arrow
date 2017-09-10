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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/reader.h"
#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/python_to_arrow.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {

Status CallDeserializeCallback(PyObject* context, PyObject* value,
                               PyObject** deserialized_object);

Status DeserializeTuple(PyObject* context, const Array& array, int64_t start_idx,
                        int64_t stop_idx, PyObject* base,
                        const std::vector<std::shared_ptr<Tensor>>& tensors,
                        PyObject** out);

Status DeserializeList(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base,
                       const std::vector<std::shared_ptr<Tensor>>& tensors,
                       PyObject** out);

Status DeserializeSet(PyObject* context, const Array& array, int64_t start_idx,
                      int64_t stop_idx, PyObject* base,
                      const std::vector<std::shared_ptr<Tensor>>& tensors,
                      PyObject** out);

Status DeserializeDict(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base,
                       const std::vector<std::shared_ptr<Tensor>>& tensors,
                       PyObject** out) {
  const auto& data = static_cast<const StructArray&>(array);
  ScopedRef keys, vals;
  ScopedRef result(PyDict_New());
  RETURN_NOT_OK(DeserializeList(context, *data.field(0), start_idx, stop_idx, base,
                                tensors, keys.ref()));
  RETURN_NOT_OK(DeserializeList(context, *data.field(1), start_idx, stop_idx, base,
                                tensors, vals.ref()));
  for (int64_t i = start_idx; i < stop_idx; ++i) {
    // PyDict_SetItem behaves differently from PyList_SetItem and PyTuple_SetItem.
    // The latter two steal references whereas PyDict_SetItem does not. So we need
    // to make sure the reference count is decremented by letting the ScopedRef
    // go out of scope at the end.
    PyDict_SetItem(result.get(), PyList_GET_ITEM(keys.get(), i - start_idx),
                   PyList_GET_ITEM(vals.get(), i - start_idx));
  }
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(result.get(), py_type)) {
    RETURN_NOT_OK(CallDeserializeCallback(context, result.get(), out));
  } else {
    *out = result.release();
  }
  return Status::OK();
}

Status DeserializeArray(const Array& array, int64_t offset, PyObject* base,
                        const std::vector<std::shared_ptr<arrow::Tensor>>& tensors,
                        PyObject** out) {
  int32_t index = static_cast<const Int32Array&>(array).Value(offset);
  RETURN_NOT_OK(py::TensorToNdarray(*tensors[index], base, out));
  // Mark the array as immutable
  ScopedRef flags(PyObject_GetAttrString(*out, "flags"));
  DCHECK(flags.get() != NULL) << "Could not mark Numpy array immutable";
  Py_INCREF(Py_False);
  int flag_set = PyObject_SetAttrString(flags.get(), "writeable", Py_False);
  DCHECK(flag_set == 0) << "Could not mark Numpy array immutable";
  return Status::OK();
}

Status GetValue(PyObject* context, const Array& arr, int64_t index, int32_t type,
                PyObject* base, const std::vector<std::shared_ptr<Tensor>>& tensors,
                PyObject** result) {
  switch (arr.type()->id()) {
    case Type::BOOL:
      *result = PyBool_FromLong(static_cast<const BooleanArray&>(arr).Value(index));
      return Status::OK();
    case Type::INT64:
      *result = PyLong_FromSsize_t(static_cast<const Int64Array&>(arr).Value(index));
      return Status::OK();
    case Type::BINARY: {
      int32_t nchars;
      const uint8_t* str = static_cast<const BinaryArray&>(arr).GetValue(index, &nchars);
      *result = PyBytes_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return CheckPyError();
    }
    case Type::STRING: {
      int32_t nchars;
      const uint8_t* str = static_cast<const StringArray&>(arr).GetValue(index, &nchars);
      *result = PyUnicode_FromStringAndSize(reinterpret_cast<const char*>(str), nchars);
      return CheckPyError();
    }
    case Type::FLOAT:
      *result = PyFloat_FromDouble(static_cast<const FloatArray&>(arr).Value(index));
      return Status::OK();
    case Type::DOUBLE:
      *result = PyFloat_FromDouble(static_cast<const DoubleArray&>(arr).Value(index));
      return Status::OK();
    case Type::STRUCT: {
      const auto& s = static_cast<const StructArray&>(arr);
      const auto& l = static_cast<const ListArray&>(*s.field(0));
      if (s.type()->child(0)->name() == "list") {
        return DeserializeList(context, *l.values(), l.value_offset(index),
                               l.value_offset(index + 1), base, tensors, result);
      } else if (s.type()->child(0)->name() == "tuple") {
        return DeserializeTuple(context, *l.values(), l.value_offset(index),
                                l.value_offset(index + 1), base, tensors, result);
      } else if (s.type()->child(0)->name() == "dict") {
        return DeserializeDict(context, *l.values(), l.value_offset(index),
                               l.value_offset(index + 1), base, tensors, result);
      } else if (s.type()->child(0)->name() == "set") {
        return DeserializeSet(context, *l.values(), l.value_offset(index),
                              l.value_offset(index + 1), base, tensors, result);
      } else {
        DCHECK(false) << "unexpected StructArray type " << s.type()->child(0)->name();
      }
    }
    // We use an Int32Builder here to distinguish the tensor indices from
    // the Type::INT64 above (see tensor_indices_ in SequenceBuilder).
    case Type::INT32: {
      return DeserializeArray(arr, index, base, tensors, result);
    }
    default:
      DCHECK(false) << "union tag " << type << " not recognized";
  }
  return Status::OK();
}

#define DESERIALIZE_SEQUENCE(CREATE_FN, SET_ITEM_FN)                                  \
  const auto& data = static_cast<const UnionArray&>(array);                           \
  int64_t size = array.length();                                                      \
  ScopedRef result(CREATE_FN(stop_idx - start_idx));                                  \
  auto types = std::make_shared<Int8Array>(size, data.type_ids());                    \
  auto offsets = std::make_shared<Int32Array>(size, data.value_offsets());            \
  for (int64_t i = start_idx; i < stop_idx; ++i) {                                    \
    if (data.IsNull(i)) {                                                             \
      Py_INCREF(Py_None);                                                             \
      SET_ITEM_FN(result.get(), i - start_idx, Py_None);                              \
    } else {                                                                          \
      int64_t offset = offsets->Value(i);                                             \
      int8_t type = types->Value(i);                                                  \
      PyObject* value;                                                                \
      RETURN_NOT_OK(                                                                  \
          GetValue(context, *data.child(type), offset, type, base, tensors, &value)); \
      SET_ITEM_FN(result.get(), i - start_idx, value);                                \
    }                                                                                 \
  }                                                                                   \
  *out = result.release();                                                            \
  return Status::OK()

Status DeserializeList(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base,
                       const std::vector<std::shared_ptr<Tensor>>& tensors,
                       PyObject** out) {
  DESERIALIZE_SEQUENCE(PyList_New, PyList_SET_ITEM);
}

Status DeserializeTuple(PyObject* context, const Array& array, int64_t start_idx,
                        int64_t stop_idx, PyObject* base,
                        const std::vector<std::shared_ptr<Tensor>>& tensors,
                        PyObject** out) {
  DESERIALIZE_SEQUENCE(PyTuple_New, PyTuple_SET_ITEM);
}

Status DeserializeSet(PyObject* context, const Array& array, int64_t start_idx,
                      int64_t stop_idx, PyObject* base,
                      const std::vector<std::shared_ptr<Tensor>>& tensors,
                      PyObject** out) {
  const auto& data = static_cast<const UnionArray&>(array);
  int64_t size = array.length();
  ScopedRef result(PySet_New(nullptr));
  auto types = std::make_shared<Int8Array>(size, data.type_ids());
  auto offsets = std::make_shared<Int32Array>(size, data.value_offsets());
  for (int64_t i = start_idx; i < stop_idx; ++i) {
    if (data.IsNull(i)) {
      Py_INCREF(Py_None);
      if (PySet_Add(result.get(), Py_None) < 0) {
        RETURN_IF_PYERROR();
      }
    } else {
      int64_t offset = offsets->Value(i);
      int8_t type = types->Value(i);
      PyObject* value;
      RETURN_NOT_OK(
          GetValue(context, *data.child(type), offset, type, base, tensors, &value));
      if (PySet_Add(result.get(), value) < 0) {
        RETURN_IF_PYERROR();
      }
    }
  }
  *out = result.release();
  return Status::OK();
}

Status ReadSerializedObject(io::RandomAccessFile* src, SerializedPyObject* out) {
  int64_t offset;
  int64_t bytes_read;
  int32_t num_tensors;
  // Read number of tensors
  RETURN_NOT_OK(
      src->Read(sizeof(int32_t), &bytes_read, reinterpret_cast<uint8_t*>(&num_tensors)));

  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(ipc::RecordBatchStreamReader::Open(src, &reader));
  RETURN_NOT_OK(reader->ReadNext(&out->batch));

  RETURN_NOT_OK(src->Tell(&offset));
  offset += 4;  // Skip the end-of-stream message
  for (int i = 0; i < num_tensors; ++i) {
    std::shared_ptr<Tensor> tensor;
    RETURN_NOT_OK(ipc::ReadTensor(offset, src, &tensor));
    out->tensors.push_back(tensor);
    RETURN_NOT_OK(src->Tell(&offset));
  }
  return Status::OK();
}

Status DeserializeObject(PyObject* context, const SerializedPyObject& obj, PyObject* base,
                         PyObject** out) {
  PyAcquireGIL lock;
  return DeserializeList(context, *obj.batch->column(0), 0, obj.batch->num_rows(), base,
                         obj.tensors, out);
}

}  // namespace py
}  // namespace arrow
