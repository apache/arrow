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

#include "arrow/python/deserialize.h"

#include "arrow/python/numpy_interop.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/util.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/parsing.h"

#include "arrow/python/common.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/serialize.h"
#include "arrow/python/util/datetime.h"

namespace arrow {

using internal::checked_cast;
using internal::StringConverter;

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
  const auto& data = checked_cast<const StructArray&>(array);
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
    int ret = PyDict_SetItem(result.obj(), PyList_GET_ITEM(keys.obj(), i - start_idx),
                             PyList_GET_ITEM(vals.obj(), i - start_idx));
    if (ret != 0) {
      return ConvertPyError();
    }
  }
  static PyObject* py_type = PyUnicode_FromString("_pytype_");
  if (PyDict_Contains(result.obj(), py_type)) {
    RETURN_NOT_OK(CallDeserializeCallback(context, result.obj(), out));
  } else {
    *out = result.detach();
  }
  return Status::OK();
}

Status DeserializeArray(int32_t index, PyObject* base, const SerializedPyObject& blobs,
                        PyObject** out) {
  RETURN_NOT_OK(py::TensorToNdarray(blobs.ndarrays[index], base, out));
  // Mark the array as immutable
  OwnedRef flags(PyObject_GetAttrString(*out, "flags"));
  if (flags.obj() == NULL) {
    return ConvertPyError();
  }
  if (PyObject_SetAttrString(flags.obj(), "writeable", Py_False) < 0) {
    return ConvertPyError();
  }
  return Status::OK();
}

Status GetValue(PyObject* context, const Array& arr, int64_t index, int8_t type,
                PyObject* base, const SerializedPyObject& blobs, PyObject** result) {
  switch (type) {
    case PythonType::BOOL:
      *result = PyBool_FromLong(checked_cast<const BooleanArray&>(arr).Value(index));
      return Status::OK();
    case PythonType::PY2INT:
    case PythonType::INT: {
#if PY_MAJOR_VERSION < 3
      if (type == PythonType::PY2INT) {
        *result = PyInt_FromSsize_t(checked_cast<const Int64Array&>(arr).Value(index));
        return Status::OK();
      }
#endif
      *result = PyLong_FromSsize_t(checked_cast<const Int64Array&>(arr).Value(index));
      return Status::OK();
    }
    case PythonType::BYTES: {
      auto view = checked_cast<const BinaryArray&>(arr).GetView(index);
      *result = PyBytes_FromStringAndSize(view.data(), view.length());
      return CheckPyError();
    }
    case PythonType::STRING: {
      auto view = checked_cast<const StringArray&>(arr).GetView(index);
      *result = PyUnicode_FromStringAndSize(view.data(), view.length());
      return CheckPyError();
    }
    case PythonType::HALF_FLOAT: {
      *result = PyHalf_FromHalf(checked_cast<const HalfFloatArray&>(arr).Value(index));
      RETURN_IF_PYERROR();
      return Status::OK();
    }
    case PythonType::FLOAT:
      *result = PyFloat_FromDouble(checked_cast<const FloatArray&>(arr).Value(index));
      return Status::OK();
    case PythonType::DOUBLE:
      *result = PyFloat_FromDouble(checked_cast<const DoubleArray&>(arr).Value(index));
      return Status::OK();
    case PythonType::DATE64: {
      RETURN_NOT_OK(PyDateTime_from_int(
          checked_cast<const Date64Array&>(arr).Value(index), TimeUnit::MICRO, result));
      RETURN_IF_PYERROR();
      return Status::OK();
    }
    case PythonType::LIST: {
      const auto& l = checked_cast<const ListArray&>(arr);
      return DeserializeList(context, *l.values(), l.value_offset(index),
                             l.value_offset(index + 1), base, blobs, result);
    }
    case PythonType::DICT: {
      const auto& l = checked_cast<const ListArray&>(arr);
      return DeserializeDict(context, *l.values(), l.value_offset(index),
                             l.value_offset(index + 1), base, blobs, result);
    }
    case PythonType::TUPLE: {
      const auto& l = checked_cast<const ListArray&>(arr);
      return DeserializeTuple(context, *l.values(), l.value_offset(index),
                              l.value_offset(index + 1), base, blobs, result);
    }
    case PythonType::SET: {
      const auto& l = checked_cast<const ListArray&>(arr);
      return DeserializeSet(context, *l.values(), l.value_offset(index),
                            l.value_offset(index + 1), base, blobs, result);
    }
    case PythonType::TENSOR: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      *result = wrap_tensor(blobs.tensors[ref]);
      return Status::OK();
    }
    case PythonType::NDARRAY: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      return DeserializeArray(ref, base, blobs, result);
    }
    case PythonType::BUFFER: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      *result = wrap_buffer(blobs.buffers[ref]);
      return Status::OK();
    }
    default: { ARROW_CHECK(false) << "union tag " << type << "' not recognized"; }
  }
  return Status::OK();
}

Status GetPythonTypes(const UnionArray& data, std::vector<int8_t>* result) {
  ARROW_CHECK(result != nullptr);
  auto type = data.type();
  for (int i = 0; i < type->num_children(); ++i) {
    StringConverter<Int8Type> converter;
    int8_t tag = 0;
    const std::string& data = type->child(i)->name();
    if (!converter(data.c_str(), data.size(), &tag)) {
      return Status::SerializationError("Cannot convert string: \"",
                                        type->child(i)->name(), "\" to int8_t");
    }
    result->push_back(tag);
  }
  return Status::OK();
}

template <typename CreateSequenceFn, typename SetItemFn>
Status DeserializeSequence(PyObject* context, const Array& array, int64_t start_idx,
                           int64_t stop_idx, PyObject* base,
                           const SerializedPyObject& blobs,
                           CreateSequenceFn&& create_sequence, SetItemFn&& set_item,
                           PyObject** out) {
  const auto& data = checked_cast<const UnionArray&>(array);
  OwnedRef result(create_sequence(stop_idx - start_idx));
  RETURN_IF_PYERROR();
  const uint8_t* type_ids = data.raw_type_ids();
  const int32_t* value_offsets = data.raw_value_offsets();
  std::vector<int8_t> python_types;
  RETURN_NOT_OK(GetPythonTypes(data, &python_types));
  for (int64_t i = start_idx; i < stop_idx; ++i) {
    if (data.IsNull(i)) {
      Py_INCREF(Py_None);
      RETURN_NOT_OK(set_item(result.obj(), i - start_idx, Py_None));
    } else {
      int64_t offset = value_offsets[i];
      uint8_t type = type_ids[i];
      PyObject* value;
      RETURN_NOT_OK(GetValue(context, *data.UnsafeChild(type), offset,
                             python_types[type_ids[i]], base, blobs, &value));
      RETURN_NOT_OK(set_item(result.obj(), i - start_idx, value));
    }
  }
  *out = result.detach();
  return Status::OK();
}

Status DeserializeList(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                       PyObject** out) {
  return DeserializeSequence(context, array, start_idx, stop_idx, base, blobs,
                             [](int64_t size) { return PyList_New(size); },
                             [](PyObject* seq, int64_t index, PyObject* item) {
                               PyList_SET_ITEM(seq, index, item);
                               return Status::OK();
                             },
                             out);
}

Status DeserializeTuple(PyObject* context, const Array& array, int64_t start_idx,
                        int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                        PyObject** out) {
  return DeserializeSequence(context, array, start_idx, stop_idx, base, blobs,
                             [](int64_t size) { return PyTuple_New(size); },
                             [](PyObject* seq, int64_t index, PyObject* item) {
                               PyTuple_SET_ITEM(seq, index, item);
                               return Status::OK();
                             },
                             out);
}

Status DeserializeSet(PyObject* context, const Array& array, int64_t start_idx,
                      int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                      PyObject** out) {
  return DeserializeSequence(context, array, start_idx, stop_idx, base, blobs,
                             [](int64_t size) { return PySet_New(nullptr); },
                             [](PyObject* seq, int64_t index, PyObject* item) {
                               int err = PySet_Add(seq, item);
                               Py_DECREF(item);
                               if (err < 0) {
                                 RETURN_IF_PYERROR();
                               }
                               return Status::OK();
                             },
                             out);
}

Status ReadSerializedObject(io::RandomAccessFile* src, SerializedPyObject* out) {
  int64_t bytes_read;
  int32_t num_tensors;
  int32_t num_ndarrays;
  int32_t num_buffers;

  // Read number of tensors
  RETURN_NOT_OK(
      src->Read(sizeof(int32_t), &bytes_read, reinterpret_cast<uint8_t*>(&num_tensors)));
  RETURN_NOT_OK(
      src->Read(sizeof(int32_t), &bytes_read, reinterpret_cast<uint8_t*>(&num_ndarrays)));
  RETURN_NOT_OK(
      src->Read(sizeof(int32_t), &bytes_read, reinterpret_cast<uint8_t*>(&num_buffers)));

  // Align stream to 8-byte offset
  RETURN_NOT_OK(ipc::AlignStream(src, ipc::kArrowIpcAlignment));
  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(ipc::RecordBatchStreamReader::Open(src, &reader));
  RETURN_NOT_OK(reader->ReadNext(&out->batch));

  /// Skip EOS marker
  RETURN_NOT_OK(src->Advance(4));

  /// Align stream so tensor bodies are 64-byte aligned
  RETURN_NOT_OK(ipc::AlignStream(src, ipc::kTensorAlignment));

  for (int i = 0; i < num_tensors; ++i) {
    std::shared_ptr<Tensor> tensor;
    RETURN_NOT_OK(ipc::ReadTensor(src, &tensor));
    RETURN_NOT_OK(ipc::AlignStream(src, ipc::kTensorAlignment));
    out->tensors.push_back(tensor);
  }

  for (int i = 0; i < num_ndarrays; ++i) {
    std::shared_ptr<Tensor> ndarray;
    RETURN_NOT_OK(ipc::ReadTensor(src, &ndarray));
    RETURN_NOT_OK(ipc::AlignStream(src, ipc::kTensorAlignment));
    out->ndarrays.push_back(ndarray);
  }

  int64_t offset = -1;
  RETURN_NOT_OK(src->Tell(&offset));
  for (int i = 0; i < num_buffers; ++i) {
    int64_t size;
    RETURN_NOT_OK(src->ReadAt(offset, sizeof(int64_t), &bytes_read,
                              reinterpret_cast<uint8_t*>(&size)));
    offset += sizeof(int64_t);
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(src->ReadAt(offset, size, &buffer));
    out->buffers.push_back(buffer);
    offset += size;
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

Status GetSerializedFromComponents(int num_tensors, int num_ndarrays, int num_buffers,
                                   PyObject* data, SerializedPyObject* out) {
  PyAcquireGIL gil;
  const Py_ssize_t data_length = PyList_Size(data);
  RETURN_IF_PYERROR();

  const Py_ssize_t expected_data_length =
      1 + num_tensors * 2 + num_ndarrays * 2 + num_buffers;
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

    RETURN_NOT_OK(ipc::ReadTensor(message, &tensor));
    out->tensors.emplace_back(std::move(tensor));
  }

  // Zero-copy reconstruct tensors for numpy ndarrays
  for (int i = 0; i < num_ndarrays; ++i) {
    std::shared_ptr<Buffer> metadata;
    std::shared_ptr<Buffer> body;
    std::shared_ptr<Tensor> tensor;
    RETURN_NOT_OK(GetBuffer(buffer_index++, &metadata));
    RETURN_NOT_OK(GetBuffer(buffer_index++, &body));

    ipc::Message message(metadata, body);

    RETURN_NOT_OK(ipc::ReadTensor(message, &tensor));
    out->ndarrays.emplace_back(std::move(tensor));
  }

  // Unwrap and append buffers
  for (int i = 0; i < num_buffers; ++i) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(GetBuffer(buffer_index++, &buffer));
    out->buffers.emplace_back(std::move(buffer));
  }

  return Status::OK();
}

Status DeserializeNdarray(const SerializedPyObject& object,
                          std::shared_ptr<Tensor>* out) {
  if (object.ndarrays.size() != 1) {
    return Status::Invalid("Object is not an Ndarray");
  }
  *out = object.ndarrays[0];
  return Status::OK();
}

Status NdarrayFromBuffer(std::shared_ptr<Buffer> src, std::shared_ptr<Tensor>* out) {
  io::BufferReader reader(src);
  SerializedPyObject object;
  RETURN_NOT_OK(ReadSerializedObject(&reader, &object));
  return DeserializeNdarray(object, out);
}

}  // namespace py
}  // namespace arrow
