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

#include "arrow/python/numpy_interop.h"

#include "arrow/python/numpy_convert.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/tensor.h"
#include "arrow/type.h"

#include "arrow/python/common.h"
#include "arrow/python/type_traits.h"

namespace arrow {
namespace py {

bool is_contiguous(PyObject* array) {
  if (PyArray_Check(array)) {
    return (PyArray_FLAGS(reinterpret_cast<PyArrayObject*>(array)) &
            (NPY_ARRAY_C_CONTIGUOUS | NPY_ARRAY_F_CONTIGUOUS)) != 0;
  } else {
    return false;
  }
}

int cast_npy_type_compat(int type_num) {
// Both LONGLONG and INT64 can be observed in the wild, which is buggy. We set
// U/LONGLONG to U/INT64 so things work properly.

#if (NPY_INT64 == NPY_LONGLONG) && (NPY_SIZEOF_LONGLONG == 8)
  if (type_num == NPY_LONGLONG) {
    type_num = NPY_INT64;
  }
  if (type_num == NPY_ULONGLONG) {
    type_num = NPY_UINT64;
  }
#endif

  return type_num;
}

NumPyBuffer::NumPyBuffer(PyObject* ao) : Buffer(nullptr, 0) {
  arr_ = ao;
  Py_INCREF(ao);

  if (PyArray_Check(ao)) {
    PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(ao);
    data_ = reinterpret_cast<const uint8_t*>(PyArray_DATA(ndarray));
    size_ = PyArray_SIZE(ndarray) * PyArray_DESCR(ndarray)->elsize;
    capacity_ = size_;

    if (PyArray_FLAGS(ndarray) & NPY_ARRAY_WRITEABLE) {
      is_mutable_ = true;
    }
  }
}

NumPyBuffer::~NumPyBuffer() { Py_XDECREF(arr_); }

#define TO_ARROW_TYPE_CASE(NPY_NAME, FACTORY) \
  case NPY_##NPY_NAME:                        \
    *out = FACTORY();                         \
    break;

Status GetTensorType(PyObject* dtype, std::shared_ptr<DataType>* out) {
  PyArray_Descr* descr = reinterpret_cast<PyArray_Descr*>(dtype);
  int type_num = cast_npy_type_compat(descr->type_num);

  switch (type_num) {
    TO_ARROW_TYPE_CASE(BOOL, uint8);
    TO_ARROW_TYPE_CASE(INT8, int8);
    TO_ARROW_TYPE_CASE(INT16, int16);
    TO_ARROW_TYPE_CASE(INT32, int32);
    TO_ARROW_TYPE_CASE(INT64, int64);
#if (NPY_INT64 != NPY_LONGLONG)
    TO_ARROW_TYPE_CASE(LONGLONG, int64);
#endif
    TO_ARROW_TYPE_CASE(UINT8, uint8);
    TO_ARROW_TYPE_CASE(UINT16, uint16);
    TO_ARROW_TYPE_CASE(UINT32, uint32);
    TO_ARROW_TYPE_CASE(UINT64, uint64);
#if (NPY_UINT64 != NPY_ULONGLONG)
    TO_ARROW_CASE(ULONGLONG);
#endif
    TO_ARROW_TYPE_CASE(FLOAT16, float16);
    TO_ARROW_TYPE_CASE(FLOAT32, float32);
    TO_ARROW_TYPE_CASE(FLOAT64, float64);
    default: {
      std::stringstream ss;
      ss << "Unsupported numpy type " << descr->type_num << std::endl;
      return Status::NotImplemented(ss.str());
    }
  }
  return Status::OK();
}

Status GetNumPyType(const DataType& type, int* type_num) {
#define NUMPY_TYPE_CASE(ARROW_NAME, NPY_NAME) \
  case Type::ARROW_NAME:                      \
    *type_num = NPY_##NPY_NAME;               \
    break;

  switch (type.id()) {
    NUMPY_TYPE_CASE(UINT8, UINT8);
    NUMPY_TYPE_CASE(INT8, INT8);
    NUMPY_TYPE_CASE(UINT16, UINT16);
    NUMPY_TYPE_CASE(INT16, INT16);
    NUMPY_TYPE_CASE(UINT32, UINT32);
    NUMPY_TYPE_CASE(INT32, INT32);
    NUMPY_TYPE_CASE(UINT64, UINT64);
    NUMPY_TYPE_CASE(INT64, INT64);
    NUMPY_TYPE_CASE(HALF_FLOAT, FLOAT16);
    NUMPY_TYPE_CASE(FLOAT, FLOAT32);
    NUMPY_TYPE_CASE(DOUBLE, FLOAT64);
    default: {
      std::stringstream ss;
      ss << "Unsupported tensor type: " << type.ToString() << std::endl;
      return Status::NotImplemented(ss.str());
    }
  }
#undef NUMPY_TYPE_CASE

  return Status::OK();
}

Status NumPyDtypeToArrow(PyObject* dtype, std::shared_ptr<DataType>* out) {
  PyArray_Descr* descr = reinterpret_cast<PyArray_Descr*>(dtype);

  int type_num = cast_npy_type_compat(descr->type_num);

  switch (type_num) {
    TO_ARROW_TYPE_CASE(BOOL, boolean);
    TO_ARROW_TYPE_CASE(INT8, int8);
    TO_ARROW_TYPE_CASE(INT16, int16);
    TO_ARROW_TYPE_CASE(INT32, int32);
    TO_ARROW_TYPE_CASE(INT64, int64);
#if (NPY_INT64 != NPY_LONGLONG)
    TO_ARROW_TYPE_CASE(LONGLONG, int64);
#endif
    TO_ARROW_TYPE_CASE(UINT8, uint8);
    TO_ARROW_TYPE_CASE(UINT16, uint16);
    TO_ARROW_TYPE_CASE(UINT32, uint32);
    TO_ARROW_TYPE_CASE(UINT64, uint64);
#if (NPY_UINT64 != NPY_ULONGLONG)
    TO_ARROW_CASE(ULONGLONG);
#endif
    TO_ARROW_TYPE_CASE(FLOAT32, float32);
    TO_ARROW_TYPE_CASE(FLOAT64, float64);
    TO_ARROW_TYPE_CASE(STRING, binary);
    TO_ARROW_TYPE_CASE(UNICODE, utf8);
    case NPY_DATETIME: {
      auto date_dtype =
          reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(descr->c_metadata);
      switch (date_dtype->meta.base) {
        case NPY_FR_s:
          *out = timestamp(TimeUnit::SECOND);
          break;
        case NPY_FR_ms:
          *out = timestamp(TimeUnit::MILLI);
          break;
        case NPY_FR_us:
          *out = timestamp(TimeUnit::MICRO);
          break;
        case NPY_FR_ns:
          *out = timestamp(TimeUnit::NANO);
          break;
        case NPY_FR_D:
          *out = date32();
          break;
        default:
          return Status::NotImplemented("Unsupported datetime64 time unit");
      }
    } break;
    default: {
      std::stringstream ss;
      ss << "Unsupported numpy type " << descr->type_num << std::endl;
      return Status::NotImplemented(ss.str());
    }
  }

  return Status::OK();
}

#undef TO_ARROW_TYPE_CASE

Status NdarrayToTensor(MemoryPool* pool, PyObject* ao, std::shared_ptr<Tensor>* out) {
  PyAcquireGIL lock;

  if (!PyArray_Check(ao)) {
    return Status::TypeError("Did not pass ndarray object");
  }

  PyArrayObject* ndarray = reinterpret_cast<PyArrayObject*>(ao);

  // TODO(wesm): What do we want to do with non-contiguous memory and negative strides?

  int ndim = PyArray_NDIM(ndarray);

  std::shared_ptr<Buffer> data = std::make_shared<NumPyBuffer>(ao);
  std::vector<int64_t> shape(ndim);
  std::vector<int64_t> strides(ndim);

  npy_intp* array_strides = PyArray_STRIDES(ndarray);
  npy_intp* array_shape = PyArray_SHAPE(ndarray);
  for (int i = 0; i < ndim; ++i) {
    if (array_strides[i] < 0) {
      return Status::Invalid("Negative ndarray strides not supported");
    }
    shape[i] = array_shape[i];
    strides[i] = array_strides[i];
  }

  std::shared_ptr<DataType> type;
  RETURN_NOT_OK(
      GetTensorType(reinterpret_cast<PyObject*>(PyArray_DESCR(ndarray)), &type));
  *out = std::make_shared<Tensor>(type, data, shape, strides);
  return Status::OK();
}

Status TensorToNdarray(const Tensor& tensor, PyObject* base, PyObject** out) {
  PyAcquireGIL lock;

  int type_num;
  RETURN_NOT_OK(GetNumPyType(*tensor.type(), &type_num));
  PyArray_Descr* dtype = PyArray_DescrNewFromType(type_num);
  RETURN_IF_PYERROR();

  std::vector<npy_intp> npy_shape(tensor.ndim());
  std::vector<npy_intp> npy_strides(tensor.ndim());

  for (int i = 0; i < tensor.ndim(); ++i) {
    npy_shape[i] = tensor.shape()[i];
    npy_strides[i] = tensor.strides()[i];
  }

  const void* immutable_data = nullptr;
  if (tensor.data()) {
    immutable_data = tensor.data()->data();
  }

  // Remove const =(
  void* mutable_data = const_cast<void*>(immutable_data);

  int array_flags = 0;
  if (tensor.is_row_major()) {
    array_flags |= NPY_ARRAY_C_CONTIGUOUS;
  }
  if (tensor.is_column_major()) {
    array_flags |= NPY_ARRAY_F_CONTIGUOUS;
  }
  if (tensor.is_mutable()) {
    array_flags |= NPY_ARRAY_WRITEABLE;
  }

  PyObject* result =
      PyArray_NewFromDescr(&PyArray_Type, dtype, tensor.ndim(), npy_shape.data(),
                           npy_strides.data(), mutable_data, array_flags, nullptr);
  RETURN_IF_PYERROR()

  if (base != Py_None) {
    PyArray_SetBaseObject(reinterpret_cast<PyArrayObject*>(result), base);
    Py_XINCREF(base);
  }
  *out = result;
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
