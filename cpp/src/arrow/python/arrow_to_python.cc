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

#include "arrow/python/numpy_interop.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/python_to_arrow.h"
#include "arrow/python/util/datetime.h"

namespace arrow {
namespace py {

Status CallDeserializeCallback(PyObject* context, PyObject* value,
                               PyObject** deserialized_object);

Status DeserializeTuple(PyObject* context, const Array& array, int64_t start_idx,
                        int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                        PyObject** out);

Status DeserializeList(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                       PyObject** out);

Status DeserializeSet(PyObject* context, const Array& array, int64_t start_idx,
                      int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                      PyObject** out);

Status DeserializeDict(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                       PyObject** out) {
  const auto& data = static_cast<const StructArray&>(array);
  OwnedRef keys, vals;
  OwnedRef result(PyDict_New());
  RETURN_IF_PYERROR();

  DCHECK_EQ(2, data.num_fields());

  RETURN_NOT_OK(DeserializeList(context, *data.field(0), start_idx, stop_idx, base, blobs,
                                keys.ref()));
  RETURN_NOT_OK(DeserializeList(context, *data.field(1), start_idx, stop_idx, base, blobs,
                                vals.ref()));
  for (int64_t i = start_idx; i < stop_idx; ++i) {
    // PyDict_SetItem behaves differently from PyList_SetItem and PyTuple_SetItem.
    // The latter two steal references whereas PyDict_SetItem does not. So we need
    // to make sure the reference count is decremented by letting the OwnedRef
    // go out of scope at the end.
    PyDict_SetItem(result.obj(), PyList_GET_ITEM(keys.obj(), i - start_idx),
                   PyList_GET_ITEM(vals.obj(), i - start_idx));
  }
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(result.obj(), py_type)) {
    RETURN_NOT_OK(CallDeserializeCallback(context, result.obj(), out));
  } else {
    *out = result.detach();
  }
  return Status::OK();
}

Status DeserializeArray(const Array& array, int64_t offset, PyObject* base,
                        const SerializedPyObject& blobs, PyObject** out) {
  int32_t index = static_cast<const Int32Array&>(array).Value(offset);
  RETURN_NOT_OK(py::TensorToNdarray(*blobs.tensors[index], base, out));
  // Mark the array as immutable
  OwnedRef flags(PyObject_GetAttrString(*out, "flags"));
  DCHECK(flags.obj() != NULL) << "Could not mark Numpy array immutable";
  Py_INCREF(Py_False);
  int flag_set = PyObject_SetAttrString(flags.obj(), "writeable", Py_False);
  DCHECK(flag_set == 0) << "Could not mark Numpy array immutable";
  return Status::OK();
}

Status GetValue(PyObject* context, const UnionArray& parent, const Array& arr,
                int64_t index, int32_t type, PyObject* base,
                const SerializedPyObject& blobs, PyObject** result) {
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
    case Type::HALF_FLOAT: {
      *result = PyArrayScalar_New(Half);
      RETURN_IF_PYERROR();

      npy_half halffloat = static_cast<const HalfFloatArray&>(arr).Value(index);
      PyArrayScalar_ASSIGN(*result, Half, halffloat);
      return Status::OK();
    }
    case Type::FLOAT:
      *result = PyFloat_FromDouble(static_cast<const FloatArray&>(arr).Value(index));
      return Status::OK();
    case Type::DOUBLE:
      *result = PyFloat_FromDouble(static_cast<const DoubleArray&>(arr).Value(index));
      return Status::OK();
    case Type::DATE64: {
      RETURN_NOT_OK(PyDateTime_from_int(static_cast<const Date64Array&>(arr).Value(index),
                                        TimeUnit::MICRO, result));
      RETURN_IF_PYERROR();
      return Status::OK();
    }
    case Type::STRUCT: {
      const auto& s = static_cast<const StructArray&>(arr);
      const auto& l = static_cast<const ListArray&>(*s.field(0));
      if (s.type()->child(0)->name() == "list") {
        return DeserializeList(context, *l.values(), l.value_offset(index),
                               l.value_offset(index + 1), base, blobs, result);
      } else if (s.type()->child(0)->name() == "tuple") {
        return DeserializeTuple(context, *l.values(), l.value_offset(index),
                                l.value_offset(index + 1), base, blobs, result);
      } else if (s.type()->child(0)->name() == "dict") {
        return DeserializeDict(context, *l.values(), l.value_offset(index),
                               l.value_offset(index + 1), base, blobs, result);
      } else if (s.type()->child(0)->name() == "set") {
        return DeserializeSet(context, *l.values(), l.value_offset(index),
                              l.value_offset(index + 1), base, blobs, result);
      } else {
        DCHECK(false) << "unexpected StructArray type " << s.type()->child(0)->name();
      }
    }
    default: {
      const std::string& child_name = parent.type()->child(type)->name();
      if (child_name == "tensor") {
        return DeserializeArray(arr, index, base, blobs, result);
      } else if (child_name == "buffer") {
        int32_t ref = static_cast<const Int32Array&>(arr).Value(index);
        *result = wrap_buffer(blobs.buffers[ref]);
        return Status::OK();
      } else {
        DCHECK(false) << "union tag " << type << " with child name '" << child_name
                      << "' not recognized";
      }
    }
  }
  return Status::OK();
}

#define DESERIALIZE_SEQUENCE(CREATE_FN, SET_ITEM_FN)                                     \
  const auto& data = static_cast<const UnionArray&>(array);                              \
  OwnedRef result(CREATE_FN(stop_idx - start_idx));                                      \
  const uint8_t* type_ids = data.raw_type_ids();                                         \
  const int32_t* value_offsets = data.raw_value_offsets();                               \
  for (int64_t i = start_idx; i < stop_idx; ++i) {                                       \
    if (data.IsNull(i)) {                                                                \
      Py_INCREF(Py_None);                                                                \
      SET_ITEM_FN(result.obj(), i - start_idx, Py_None);                                 \
    } else {                                                                             \
      int64_t offset = value_offsets[i];                                                 \
      uint8_t type = type_ids[i];                                                        \
      PyObject* value;                                                                   \
      RETURN_NOT_OK(GetValue(context, data, *data.UnsafeChild(type), offset, type, base, \
                             blobs, &value));                                            \
      SET_ITEM_FN(result.obj(), i - start_idx, value);                                   \
    }                                                                                    \
  }                                                                                      \
  *out = result.detach();                                                                \
  return Status::OK()

Status DeserializeList(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                       PyObject** out) {
  DESERIALIZE_SEQUENCE(PyList_New, PyList_SET_ITEM);
}

Status DeserializeTuple(PyObject* context, const Array& array, int64_t start_idx,
                        int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                        PyObject** out) {
  DESERIALIZE_SEQUENCE(PyTuple_New, PyTuple_SET_ITEM);
}

Status DeserializeSet(PyObject* context, const Array& array, int64_t start_idx,
                      int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                      PyObject** out) {
  const auto& data = static_cast<const UnionArray&>(array);
  OwnedRef result(PySet_New(nullptr));
  const uint8_t* type_ids = data.raw_type_ids();
  const int32_t* value_offsets = data.raw_value_offsets();
  for (int64_t i = start_idx; i < stop_idx; ++i) {
    if (data.IsNull(i)) {
      Py_INCREF(Py_None);
      if (PySet_Add(result.obj(), Py_None) < 0) {
        RETURN_IF_PYERROR();
      }
    } else {
      int32_t offset = value_offsets[i];
      int8_t type = type_ids[i];
      PyObject* value;
      RETURN_NOT_OK(GetValue(context, data, *data.UnsafeChild(type), offset, type, base,
                             blobs, &value));
      if (PySet_Add(result.obj(), value) < 0) {
        RETURN_IF_PYERROR();
      }
    }
  }
  *out = result.detach();
  return Status::OK();
}

Status ReadSerializedObject(io::RandomAccessFile* src, SerializedPyObject* out) {
  int64_t offset;
  int64_t bytes_read;
  int32_t num_tensors;
  int32_t num_buffers;
  // Read number of tensors
  RETURN_NOT_OK(
      src->Read(sizeof(int32_t), &bytes_read, reinterpret_cast<uint8_t*>(&num_tensors)));
  RETURN_NOT_OK(
      src->Read(sizeof(int32_t), &bytes_read, reinterpret_cast<uint8_t*>(&num_buffers)));

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

  for (int i = 0; i < num_buffers; ++i) {
    int64_t size;
    RETURN_NOT_OK(src->ReadAt(offset, sizeof(int64_t), &bytes_read,
                              reinterpret_cast<uint8_t*>(&size)));
    RETURN_NOT_OK(src->Tell(&offset));
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(src->ReadAt(offset, size, &buffer));
    out->buffers.push_back(buffer);
    RETURN_NOT_OK(src->Tell(&offset));
  }

  return Status::OK();
}

Status DeserializeObject(PyObject* context, const SerializedPyObject& obj, PyObject* base,
                         PyObject** out) {
  PyAcquireGIL lock;
  PyDateTime_IMPORT;
  import_pyarrow();
  return DeserializeList(context, *obj.batch->column(0), 0, obj.batch->num_rows(), base,
                         obj, out);
}

Status GetSerializedFromComponents(int num_tensors, int num_buffers, PyObject* data,
                                   SerializedPyObject* out) {
  PyAcquireGIL gil;
  const Py_ssize_t data_length = PyList_Size(data);
  RETURN_IF_PYERROR();

  const Py_ssize_t expected_data_length = 1 + num_tensors * 2 + num_buffers;
  if (data_length != expected_data_length) {
    return Status::Invalid("Invalid number of buffers in data");
  }

  auto GetBuffer = [&data](Py_ssize_t index, std::shared_ptr<Buffer>* out) {
    PyObject* py_buf = PyList_GET_ITEM(data, index);
    return unwrap_buffer(py_buf, out);
  };

  Py_ssize_t buffer_index = 0;

  // Read the union batch describing object structure
  {
    std::shared_ptr<Buffer> data_buffer;
    RETURN_NOT_OK(GetBuffer(buffer_index++, &data_buffer));
    gil.release();
    io::BufferReader buf_reader(data_buffer);
    std::shared_ptr<RecordBatchReader> reader;
    RETURN_NOT_OK(ipc::RecordBatchStreamReader::Open(&buf_reader, &reader));
    RETURN_NOT_OK(reader->ReadNext(&out->batch));
    gil.acquire();
  }

  // Zero-copy reconstruct tensors
  for (int i = 0; i < num_tensors; ++i) {
    std::shared_ptr<Buffer> metadata;
    std::shared_ptr<Buffer> body;
    std::shared_ptr<Tensor> tensor;
    RETURN_NOT_OK(GetBuffer(buffer_index++, &metadata));
    RETURN_NOT_OK(GetBuffer(buffer_index++, &body));

    ipc::Message message(metadata, body);

    RETURN_NOT_OK(ReadTensor(message, &tensor));
    out->tensors.emplace_back(std::move(tensor));
  }

  // Unwrap and append buffers
  for (int i = 0; i < num_buffers; ++i) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(GetBuffer(buffer_index++, &buffer));
    out->buffers.emplace_back(std::move(buffer));
  }

  return Status::OK();
}

}  // namespace py
}  // namespace arrow
