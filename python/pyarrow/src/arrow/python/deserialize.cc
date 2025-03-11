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
#include <string>
#include <utility>
#include <vector>

#include <numpy/arrayobject.h>
#include <numpy/arrayscalars.h>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/util.h"
#include "arrow/ipc/writer.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

#include "arrow/python/common.h"
#include "arrow/python/datetime.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/serialize.h"
#include "arrow/python/vendored/pythoncapi_compat.h"

namespace arrow {

using internal::checked_cast;
using internal::ParseValue;

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
    PyObject* key = PyList_GetItemRef(keys.obj(), i - start_idx);
    RETURN_IF_PYERROR();
    OwnedRef keyref(key);
    PyObject* val = PyList_GetItemRef(vals.obj(), i - start_idx);
    RETURN_IF_PYERROR();
    OwnedRef valref(val);
    int ret = PyDict_SetItem(result.obj(), key, val);
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
    case PythonType::NONE:
      Py_INCREF(Py_None);
      *result = Py_None;
      return Status::OK();
    case PythonType::BOOL:
      *result = PyBool_FromLong(checked_cast<const BooleanArray&>(arr).Value(index));
      return Status::OK();
    case PythonType::PY2INT:
    case PythonType::INT: {
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
      RETURN_NOT_OK(internal::PyDateTime_from_int(
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
    case PythonType::SPARSECOOTENSOR: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      const std::shared_ptr<SparseCOOTensor>& sparse_coo_tensor =
          arrow::internal::checked_pointer_cast<SparseCOOTensor>(
              blobs.sparse_tensors[ref]);
      *result = wrap_sparse_coo_tensor(sparse_coo_tensor);
      return Status::OK();
    }
    case PythonType::SPARSECSRMATRIX: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      const std::shared_ptr<SparseCSRMatrix>& sparse_csr_matrix =
          arrow::internal::checked_pointer_cast<SparseCSRMatrix>(
              blobs.sparse_tensors[ref]);
      *result = wrap_sparse_csr_matrix(sparse_csr_matrix);
      return Status::OK();
    }
    case PythonType::SPARSECSCMATRIX: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      const std::shared_ptr<SparseCSCMatrix>& sparse_csc_matrix =
          arrow::internal::checked_pointer_cast<SparseCSCMatrix>(
              blobs.sparse_tensors[ref]);
      *result = wrap_sparse_csc_matrix(sparse_csc_matrix);
      return Status::OK();
    }
    case PythonType::SPARSECSFTENSOR: {
      int32_t ref = checked_cast<const Int32Array&>(arr).Value(index);
      const std::shared_ptr<SparseCSFTensor>& sparse_csf_tensor =
          arrow::internal::checked_pointer_cast<SparseCSFTensor>(
              blobs.sparse_tensors[ref]);
      *result = wrap_sparse_csf_tensor(sparse_csf_tensor);
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
    default: {
      ARROW_CHECK(false) << "union tag " << type << "' not recognized";
    }
  }
  return Status::OK();
}

Status GetPythonTypes(const UnionArray& data, std::vector<int8_t>* result) {
  ARROW_CHECK(result != nullptr);
  auto type = data.type();
  for (int i = 0; i < type->num_fields(); ++i) {
    int8_t tag = 0;
    const std::string& data = type->field(i)->name();
    if (!ParseValue<Int8Type>(data.c_str(), data.size(), &tag)) {
      return Status::SerializationError("Cannot convert string: \"",
                                        type->field(i)->name(), "\" to int8_t");
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
  const auto& data = checked_cast<const DenseUnionArray&>(array);
  OwnedRef result(create_sequence(stop_idx - start_idx));
  RETURN_IF_PYERROR();
  const int8_t* type_codes = data.raw_type_codes();
  const int32_t* value_offsets = data.raw_value_offsets();
  std::vector<int8_t> python_types;
  RETURN_NOT_OK(GetPythonTypes(data, &python_types));
  for (int64_t i = start_idx; i < stop_idx; ++i) {
    const int64_t offset = value_offsets[i];
    const uint8_t type = type_codes[i];
    PyObject* value;
    RETURN_NOT_OK(GetValue(context, *data.field(type), offset, python_types[type], base,
                           blobs, &value));
    RETURN_NOT_OK(set_item(result.obj(), i - start_idx, value));
  }
  *out = result.detach();
  return Status::OK();
}

Status DeserializeList(PyObject* context, const Array& array, int64_t start_idx,
                       int64_t stop_idx, PyObject* base, const SerializedPyObject& blobs,
                       PyObject** out) {
  return DeserializeSequence(
      context, array, start_idx, stop_idx, base, blobs,
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
  return DeserializeSequence(
      context, array, start_idx, stop_idx, base, blobs,
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
  return DeserializeSequence(
      context, array, start_idx, stop_idx, base, blobs,
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

}  // namespace py
}  // namespace arrow
