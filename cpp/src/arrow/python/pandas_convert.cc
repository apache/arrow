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

// Functions for pandas conversion via NumPy

#include <Python.h>

#include "arrow/python/numpy_interop.h"
#include "arrow/python/pandas_convert.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "arrow/array.h"
#include "arrow/loader.h"
#include "arrow/python/builtin_convert.h"
#include "arrow/python/common.h"
#include "arrow/python/config.h"
#include "arrow/python/type_traits.h"
#include "arrow/python/util/datetime.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace py {

// ----------------------------------------------------------------------
// Utility code

int cast_npy_type_compat(int type_num) {
// Both LONGLONG and INT64 can be observed in the wild, which is buggy. We set
// U/LONGLONG to U/INT64 so things work properly.

#if (NPY_INT64 == NPY_LONGLONG) && (NPY_SIZEOF_LONGLONG == 8)
  if (type_num == NPY_LONGLONG) { type_num = NPY_INT64; }
  if (type_num == NPY_ULONGLONG) { type_num = NPY_UINT64; }
#endif

  return type_num;
}

static inline bool PyObject_is_null(const PyObject* obj) {
  return obj == Py_None || obj == numpy_nan;
}

static inline bool PyObject_is_string(const PyObject* obj) {
#if PY_MAJOR_VERSION >= 3
  return PyUnicode_Check(obj) || PyBytes_Check(obj);
#else
  return PyString_Check(obj) || PyUnicode_Check(obj);
#endif
}

template <int TYPE>
static int64_t ValuesToBitmap(const void* data, int64_t length, uint8_t* bitmap) {
  typedef npy_traits<TYPE> traits;
  typedef typename traits::value_type T;

  int64_t null_count = 0;
  const T* values = reinterpret_cast<const T*>(data);

  // TODO(wesm): striding
  for (int i = 0; i < length; ++i) {
    if (traits::isnull(values[i])) {
      ++null_count;
    } else {
      BitUtil::SetBit(bitmap, i);
    }
  }

  return null_count;
}

// Returns null count
static int64_t MaskToBitmap(PyArrayObject* mask, int64_t length, uint8_t* bitmap) {
  int64_t null_count = 0;
  const uint8_t* mask_values = static_cast<const uint8_t*>(PyArray_DATA(mask));
  // TODO(wesm): strided null mask
  for (int i = 0; i < length; ++i) {
    if (mask_values[i]) {
      ++null_count;
    } else {
      BitUtil::SetBit(bitmap, i);
    }
  }
  return null_count;
}

template <int TYPE>
static int64_t ValuesToValidBytes(
    const void* data, int64_t length, uint8_t* valid_bytes) {
  typedef npy_traits<TYPE> traits;
  typedef typename traits::value_type T;

  int64_t null_count = 0;
  const T* values = reinterpret_cast<const T*>(data);

  // TODO(wesm): striding
  for (int i = 0; i < length; ++i) {
    valid_bytes[i] = !traits::isnull(values[i]);
    if (traits::isnull(values[i])) null_count++;
  }

  return null_count;
}

Status CheckFlatNumpyArray(PyArrayObject* numpy_array, int np_type) {
  if (PyArray_NDIM(numpy_array) != 1) {
    return Status::Invalid("only handle 1-dimensional arrays");
  }

  if (PyArray_DESCR(numpy_array)->type_num != np_type) {
    return Status::Invalid("can only handle exact conversions");
  }

  npy_intp* astrides = PyArray_STRIDES(numpy_array);
  if (astrides[0] != PyArray_DESCR(numpy_array)->elsize) {
    return Status::Invalid("No support for strided arrays in lists yet");
  }
  return Status::OK();
}

Status AppendObjectStrings(int64_t objects_length, StringBuilder* builder,
    PyObject** objects, bool* have_bytes) {
  PyObject* obj;

  for (int64_t i = 0; i < objects_length; ++i) {
    obj = objects[i];
    if (PyUnicode_Check(obj)) {
      obj = PyUnicode_AsUTF8String(obj);
      if (obj == NULL) {
        PyErr_Clear();
        return Status::TypeError("failed converting unicode to UTF8");
      }
      const int32_t length = static_cast<int32_t>(PyBytes_GET_SIZE(obj));
      Status s = builder->Append(PyBytes_AS_STRING(obj), length);
      Py_DECREF(obj);
      if (!s.ok()) { return s; }
    } else if (PyBytes_Check(obj)) {
      *have_bytes = true;
      const int32_t length = static_cast<int32_t>(PyBytes_GET_SIZE(obj));
      RETURN_NOT_OK(builder->Append(PyBytes_AS_STRING(obj), length));
    } else if (PyObject_is_null(obj)) {
      RETURN_NOT_OK(builder->AppendNull());
    } else {
      return InvalidConversion(obj, "string or bytes");
    }
  }

  return Status::OK();
}

static Status AppendObjectFixedWidthBytes(int64_t objects_length, int byte_width,
    FixedSizeBinaryBuilder* builder, PyObject** objects) {
  PyObject* obj;

  for (int64_t i = 0; i < objects_length; ++i) {
    obj = objects[i];
    if (PyUnicode_Check(obj)) {
      obj = PyUnicode_AsUTF8String(obj);
      if (obj == NULL) {
        PyErr_Clear();
        return Status::TypeError("failed converting unicode to UTF8");
      }

      RETURN_NOT_OK(CheckPythonBytesAreFixedLength(obj, byte_width));
      Status s =
          builder->Append(reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(obj)));
      Py_DECREF(obj);
      RETURN_NOT_OK(s);
    } else if (PyBytes_Check(obj)) {
      RETURN_NOT_OK(CheckPythonBytesAreFixedLength(obj, byte_width));
      RETURN_NOT_OK(
          builder->Append(reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(obj))));
    } else if (PyObject_is_null(obj)) {
      RETURN_NOT_OK(builder->AppendNull());
    } else {
      return InvalidConversion(obj, "string or bytes");
    }
  }

  return Status::OK();
}

template <typename T>
struct WrapBytes {};

template <>
struct WrapBytes<StringArray> {
  static inline PyObject* Wrap(const uint8_t* data, int64_t length) {
    return PyUnicode_FromStringAndSize(reinterpret_cast<const char*>(data), length);
  }
};

template <>
struct WrapBytes<BinaryArray> {
  static inline PyObject* Wrap(const uint8_t* data, int64_t length) {
    return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(data), length);
  }
};

template <>
struct WrapBytes<FixedSizeBinaryArray> {
  static inline PyObject* Wrap(const uint8_t* data, int64_t length) {
    return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(data), length);
  }
};

static inline bool ListTypeSupported(const Type::type type_id) {
  switch (type_id) {
    case Type::UINT8:
    case Type::INT8:
    case Type::UINT16:
    case Type::INT16:
    case Type::UINT32:
    case Type::INT32:
    case Type::INT64:
    case Type::UINT64:
    case Type::FLOAT:
    case Type::DOUBLE:
    case Type::STRING:
    case Type::TIMESTAMP:
      // The above types are all supported.
      return true;
    default:
      break;
  }
  return false;
}

// ----------------------------------------------------------------------
// Conversion from NumPy-in-Pandas to Arrow

class PandasConverter : public TypeVisitor {
 public:
  PandasConverter(
      MemoryPool* pool, PyObject* ao, PyObject* mo, const std::shared_ptr<DataType>& type)
      : pool_(pool),
        type_(type),
        arr_(reinterpret_cast<PyArrayObject*>(ao)),
        mask_(nullptr) {
    if (mo != nullptr && mo != Py_None) { mask_ = reinterpret_cast<PyArrayObject*>(mo); }
    length_ = static_cast<int64_t>(PyArray_SIZE(arr_));
  }

  bool is_strided() const {
    npy_intp* astrides = PyArray_STRIDES(arr_);
    return astrides[0] != PyArray_DESCR(arr_)->elsize;
  }

  Status InitNullBitmap() {
    int64_t null_bytes = BitUtil::BytesForBits(length_);

    null_bitmap_ = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(null_bitmap_->Resize(null_bytes));

    null_bitmap_data_ = null_bitmap_->mutable_data();
    memset(null_bitmap_data_, 0, static_cast<size_t>(null_bytes));

    return Status::OK();
  }

  // ----------------------------------------------------------------------
  // Traditional visitor conversion for non-object arrays

  template <typename ArrowType>
  Status ConvertData(std::shared_ptr<Buffer>* data);

  template <typename ArrowType>
  Status VisitNative() {
    using traits = arrow_traits<ArrowType::type_id>;

    if (mask_ != nullptr || traits::supports_nulls) { RETURN_NOT_OK(InitNullBitmap()); }

    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(ConvertData<ArrowType>(&data));

    int64_t null_count = 0;
    if (mask_ != nullptr) {
      null_count = MaskToBitmap(mask_, length_, null_bitmap_data_);
    } else if (traits::supports_nulls) {
      // TODO(wesm): this presumes the NumPy C type and arrow C type are the
      // same
      null_count = ValuesToBitmap<traits::npy_type>(
          PyArray_DATA(arr_), length_, null_bitmap_data_);
    }

    std::vector<FieldMetadata> fields(1);
    fields[0].length = length_;
    fields[0].null_count = null_count;
    fields[0].offset = 0;

    return LoadArray(type_, fields, {null_bitmap_, data}, &out_);
  }

#define VISIT_NATIVE(TYPE) \
  Status Visit(const TYPE& type) override { return VisitNative<TYPE>(); }

  VISIT_NATIVE(BooleanType);
  VISIT_NATIVE(Int8Type);
  VISIT_NATIVE(Int16Type);
  VISIT_NATIVE(Int32Type);
  VISIT_NATIVE(Int64Type);
  VISIT_NATIVE(UInt8Type);
  VISIT_NATIVE(UInt16Type);
  VISIT_NATIVE(UInt32Type);
  VISIT_NATIVE(UInt64Type);
  VISIT_NATIVE(FloatType);
  VISIT_NATIVE(DoubleType);
  VISIT_NATIVE(TimestampType);

#undef VISIT_NATIVE

  Status Convert(std::shared_ptr<Array>* out) {
    if (PyArray_NDIM(arr_) != 1) {
      return Status::Invalid("only handle 1-dimensional arrays");
    }
    // TODO(wesm): strided arrays
    if (is_strided()) { return Status::Invalid("no support for strided data yet"); }

    if (type_ == nullptr) { return Status::Invalid("Must pass data type"); }

    // Visit the type to perform conversion
    RETURN_NOT_OK(type_->Accept(this));

    *out = out_;
    return Status::OK();
  }

  // ----------------------------------------------------------------------
  // Conversion logic for various object dtype arrays

  template <int ITEM_TYPE, typename ArrowType>
  Status ConvertTypedLists(
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out);

  Status ConvertObjectStrings(std::shared_ptr<Array>* out);
  Status ConvertObjectFixedWidthBytes(
      const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out);
  Status ConvertBooleans(std::shared_ptr<Array>* out);
  Status ConvertDates(std::shared_ptr<Array>* out);
  Status ConvertLists(const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out);
  Status ConvertObjects(std::shared_ptr<Array>* out);

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  PyArrayObject* arr_;
  PyArrayObject* mask_;
  int64_t length_;

  // Used in visitor pattern
  std::shared_ptr<Array> out_;

  std::shared_ptr<ResizableBuffer> null_bitmap_;
  uint8_t* null_bitmap_data_;
};

template <typename ArrowType>
inline Status PandasConverter::ConvertData(std::shared_ptr<Buffer>* data) {
  using traits = arrow_traits<ArrowType::type_id>;

  // Handle LONGLONG->INT64 and other fun things
  int type_num_compat = cast_npy_type_compat(PyArray_DESCR(arr_)->type_num);

  if (traits::npy_type != type_num_compat) {
    return Status::NotImplemented("NumPy type casts not yet implemented");
  }

  *data = std::make_shared<NumPyBuffer>(arr_);
  return Status::OK();
}

template <>
inline Status PandasConverter::ConvertData<BooleanType>(std::shared_ptr<Buffer>* data) {
  int64_t nbytes = BitUtil::BytesForBits(length_);
  auto buffer = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(buffer->Resize(nbytes));

  const uint8_t* values = reinterpret_cast<const uint8_t*>(PyArray_DATA(arr_));

  uint8_t* bitmap = buffer->mutable_data();

  memset(bitmap, 0, nbytes);
  for (int i = 0; i < length_; ++i) {
    if (values[i] > 0) { BitUtil::SetBit(bitmap, i); }
  }

  *data = buffer;
  return Status::OK();
}

Status InvalidConversion(PyObject* obj, const std::string& expected_type_name) {
  OwnedRef type(PyObject_Type(obj));
  RETURN_IF_PYERROR();
  DCHECK_NE(type.obj(), nullptr);

  OwnedRef type_name(PyObject_GetAttrString(type.obj(), "__name__"));
  RETURN_IF_PYERROR();
  DCHECK_NE(type_name.obj(), nullptr);

  OwnedRef bytes_obj(PyUnicode_AsUTF8String(type_name.obj()));
  RETURN_IF_PYERROR();
  DCHECK_NE(bytes_obj.obj(), nullptr);

  Py_ssize_t size = PyBytes_GET_SIZE(bytes_obj.obj());
  const char* bytes = PyBytes_AS_STRING(bytes_obj.obj());

  DCHECK_NE(bytes, nullptr) << "bytes from type(...).__name__ were null";

  std::string cpp_type_name(bytes, size);

  std::stringstream ss;
  ss << "Python object of type " << cpp_type_name << " is not None and is not a "
     << expected_type_name << " object";
  return Status::TypeError(ss.str());
}

Status PandasConverter::ConvertDates(std::shared_ptr<Array>* out) {
  PyAcquireGIL lock;

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  Date64Builder date_builder(pool_);
  RETURN_NOT_OK(date_builder.Resize(length_));

  Status s;
  PyObject* obj;
  for (int64_t i = 0; i < length_; ++i) {
    obj = objects[i];
    if (PyDate_CheckExact(obj)) {
      PyDateTime_Date* pydate = reinterpret_cast<PyDateTime_Date*>(obj);
      date_builder.Append(PyDate_to_ms(pydate));
    } else if (PyObject_is_null(obj)) {
      date_builder.AppendNull();
    } else {
      return InvalidConversion(obj, "date");
    }
  }
  return date_builder.Finish(out);
}

Status PandasConverter::ConvertObjectStrings(std::shared_ptr<Array>* out) {
  PyAcquireGIL lock;

  // The output type at this point is inconclusive because there may be bytes
  // and unicode mixed in the object array

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  StringBuilder builder(pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  Status s;
  bool have_bytes = false;
  RETURN_NOT_OK(AppendObjectStrings(length_, &builder, objects, &have_bytes));
  RETURN_NOT_OK(builder.Finish(out));

  if (have_bytes) {
    const auto& arr = static_cast<const StringArray&>(*out->get());
    *out = std::make_shared<BinaryArray>(arr.length(), arr.value_offsets(), arr.data(),
        arr.null_bitmap(), arr.null_count());
  }
  return Status::OK();
}

Status PandasConverter::ConvertObjectFixedWidthBytes(
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  PyAcquireGIL lock;

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  FixedSizeBinaryBuilder builder(pool_, type);
  RETURN_NOT_OK(builder.Resize(length_));
  RETURN_NOT_OK(AppendObjectFixedWidthBytes(length_,
      std::dynamic_pointer_cast<FixedSizeBinaryType>(builder.type())->byte_width(),
      &builder, objects));
  RETURN_NOT_OK(builder.Finish(out));
  return Status::OK();
}

Status PandasConverter::ConvertBooleans(std::shared_ptr<Array>* out) {
  PyAcquireGIL lock;

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));

  int64_t nbytes = BitUtil::BytesForBits(length_);
  auto data = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(data->Resize(nbytes));
  uint8_t* bitmap = data->mutable_data();
  memset(bitmap, 0, nbytes);

  int64_t null_count = 0;
  PyObject* obj;
  for (int64_t i = 0; i < length_; ++i) {
    obj = objects[i];
    if (obj == Py_True) {
      BitUtil::SetBit(bitmap, i);
      BitUtil::SetBit(null_bitmap_data_, i);
    } else if (obj == Py_False) {
      BitUtil::SetBit(null_bitmap_data_, i);
    } else if (PyObject_is_null(obj)) {
      ++null_count;
    } else {
      return InvalidConversion(obj, "bool");
    }
  }

  *out = std::make_shared<BooleanArray>(length_, data, null_bitmap_, null_count);

  return Status::OK();
}

Status PandasConverter::ConvertObjects(std::shared_ptr<Array>* out) {
  // Python object arrays are annoying, since we could have one of:
  //
  // * Strings
  // * Booleans with nulls
  // * Mixed type (not supported at the moment by arrow format)
  //
  // Additionally, nulls may be encoded either as np.nan or None. So we have to
  // do some type inference and conversion

  RETURN_NOT_OK(InitNullBitmap());

  // TODO: mask not supported here
  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  const PyObject** objects;
  {
    PyAcquireGIL lock;
    objects = reinterpret_cast<const PyObject**>(PyArray_DATA(arr_));
    PyDateTime_IMPORT;
  }

  if (type_) {
    switch (type_->type) {
      case Type::STRING:
        return ConvertObjectStrings(out);
      case Type::FIXED_SIZE_BINARY:
        return ConvertObjectFixedWidthBytes(type_, out);
      case Type::BOOL:
        return ConvertBooleans(out);
      case Type::DATE64:
        return ConvertDates(out);
      case Type::LIST: {
        const auto& list_field = static_cast<const ListType&>(*type_);
        return ConvertLists(list_field.value_field()->type, out);
      }
      default:
        return Status::TypeError("No known conversion to Arrow type");
    }
  } else {
    for (int64_t i = 0; i < length_; ++i) {
      if (PyObject_is_null(objects[i])) {
        continue;
      } else if (PyObject_is_string(objects[i])) {
        return ConvertObjectStrings(out);
      } else if (PyBool_Check(objects[i])) {
        return ConvertBooleans(out);
      } else if (PyDate_CheckExact(objects[i])) {
        return ConvertDates(out);
      } else {
        return InvalidConversion(
            const_cast<PyObject*>(objects[i]), "string, bool, or date");
      }
    }
  }

  return Status::TypeError("Unable to infer type of object array, were all null");
}

template <int ITEM_TYPE, typename ArrowType>
inline Status PandasConverter::ConvertTypedLists(
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  typedef npy_traits<ITEM_TYPE> traits;
  typedef typename traits::value_type T;
  typedef typename traits::BuilderClass BuilderT;

  PyAcquireGIL lock;

  auto value_builder = std::make_shared<BuilderT>(pool_, type);
  ListBuilder list_builder(pool_, value_builder);
  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  for (int64_t i = 0; i < length_; ++i) {
    if (PyObject_is_null(objects[i])) {
      RETURN_NOT_OK(list_builder.AppendNull());
    } else if (PyArray_Check(objects[i])) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(objects[i]);
      RETURN_NOT_OK(list_builder.Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, ITEM_TYPE));

      int64_t size = PyArray_DIM(numpy_array, 0);
      auto data = reinterpret_cast<const T*>(PyArray_DATA(numpy_array));
      if (traits::supports_nulls) {
        null_bitmap_->Resize(size, false);
        // TODO(uwe): A bitmap would be more space-efficient but the Builder API doesn't
        // currently support this.
        // ValuesToBitmap<ITEM_TYPE>(data, size, null_bitmap_->mutable_data());
        ValuesToValidBytes<ITEM_TYPE>(data, size, null_bitmap_->mutable_data());
        RETURN_NOT_OK(value_builder->Append(data, size, null_bitmap_->data()));
      } else {
        RETURN_NOT_OK(value_builder->Append(data, size));
      }

    } else if (PyList_Check(objects[i])) {
      int64_t size;
      std::shared_ptr<DataType> inferred_type;
      RETURN_NOT_OK(list_builder.Append(true));
      RETURN_NOT_OK(InferArrowTypeAndSize(objects[i], &size, &inferred_type));
      if (inferred_type->type != type->type) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to " << type->ToString();
        return Status::TypeError(ss.str());
      }
      RETURN_NOT_OK(AppendPySequence(objects[i], type, value_builder));
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  }
  return list_builder.Finish(out);
}

template <>
inline Status PandasConverter::ConvertTypedLists<NPY_OBJECT, StringType>(
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  PyAcquireGIL lock;
  // TODO: If there are bytes involed, convert to Binary representation
  bool have_bytes = false;

  auto value_builder = std::make_shared<StringBuilder>(pool_);
  ListBuilder list_builder(pool_, value_builder);
  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  for (int64_t i = 0; i < length_; ++i) {
    if (PyObject_is_null(objects[i])) {
      RETURN_NOT_OK(list_builder.AppendNull());
    } else if (PyArray_Check(objects[i])) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(objects[i]);
      RETURN_NOT_OK(list_builder.Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, NPY_OBJECT));

      int64_t size = static_cast<int64_t>(PyArray_DIM(numpy_array, 0));
      auto data = reinterpret_cast<PyObject**>(PyArray_DATA(numpy_array));
      RETURN_NOT_OK(AppendObjectStrings(size, value_builder.get(), data, &have_bytes));
    } else if (PyList_Check(objects[i])) {
      int64_t size;
      std::shared_ptr<DataType> inferred_type;
      RETURN_NOT_OK(list_builder.Append(true));
      RETURN_NOT_OK(InferArrowTypeAndSize(objects[i], &size, &inferred_type));
      if (inferred_type->type != Type::STRING) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to STRING.";
        return Status::TypeError(ss.str());
      }
      RETURN_NOT_OK(AppendPySequence(objects[i], inferred_type, value_builder));
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  }
  return list_builder.Finish(out);
}

#define LIST_CASE(TYPE, NUMPY_TYPE, ArrowType)                  \
  case Type::TYPE: {                                            \
    return ConvertTypedLists<NUMPY_TYPE, ArrowType>(type, out); \
  }

Status PandasConverter::ConvertLists(
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  switch (type->type) {
    LIST_CASE(UINT8, NPY_UINT8, UInt8Type)
    LIST_CASE(INT8, NPY_INT8, Int8Type)
    LIST_CASE(UINT16, NPY_UINT16, UInt16Type)
    LIST_CASE(INT16, NPY_INT16, Int16Type)
    LIST_CASE(UINT32, NPY_UINT32, UInt32Type)
    LIST_CASE(INT32, NPY_INT32, Int32Type)
    LIST_CASE(UINT64, NPY_UINT64, UInt64Type)
    LIST_CASE(INT64, NPY_INT64, Int64Type)
    LIST_CASE(TIMESTAMP, NPY_DATETIME, TimestampType)
    LIST_CASE(FLOAT, NPY_FLOAT, FloatType)
    LIST_CASE(DOUBLE, NPY_DOUBLE, DoubleType)
    LIST_CASE(STRING, NPY_OBJECT, StringType)
    default:
      return Status::TypeError("Unknown list item type");
  }

  return Status::TypeError("Unknown list type");
}

Status PandasToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  PandasConverter converter(pool, ao, mo, type);
  return converter.Convert(out);
}

Status PandasObjectsToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  PandasConverter converter(pool, ao, mo, type);
  return converter.ConvertObjects(out);
}

Status PandasDtypeToArrow(PyObject* dtype, std::shared_ptr<DataType>* out) {
  PyArray_Descr* descr = reinterpret_cast<PyArray_Descr*>(dtype);

  int type_num = cast_npy_type_compat(descr->type_num);

#define TO_ARROW_TYPE_CASE(NPY_NAME, FACTORY) \
  case NPY_##NPY_NAME:                        \
    *out = FACTORY();                         \
    break;

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
    case NPY_DATETIME: {
      auto date_dtype =
          reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(descr->c_metadata);
      TimeUnit unit;
      switch (date_dtype->meta.base) {
        case NPY_FR_s:
          unit = TimeUnit::SECOND;
          break;
        case NPY_FR_ms:
          unit = TimeUnit::MILLI;
          break;
        case NPY_FR_us:
          unit = TimeUnit::MICRO;
          break;
        case NPY_FR_ns:
          unit = TimeUnit::NANO;
          break;
        default:
          return Status::NotImplemented("Unsupported datetime64 time unit");
      }
      *out = timestamp(unit);
    } break;
    default: {
      std::stringstream ss;
      ss << "Unsupported numpy type " << descr->type_num << std::endl;
      return Status::NotImplemented(ss.str());
    }
  }

#undef TO_ARROW_TYPE_CASE

  return Status::OK();
}

// ----------------------------------------------------------------------
// pandas 0.x DataFrame conversion internals

inline void set_numpy_metadata(int type, DataType* datatype, PyArrayObject* out) {
  if (type == NPY_DATETIME) {
    PyArray_Descr* descr = PyArray_DESCR(out);
    auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(descr->c_metadata);
    if (datatype->type == Type::TIMESTAMP) {
      auto timestamp_type = static_cast<TimestampType*>(datatype);

      switch (timestamp_type->unit) {
        case TimestampType::Unit::SECOND:
          date_dtype->meta.base = NPY_FR_s;
          break;
        case TimestampType::Unit::MILLI:
          date_dtype->meta.base = NPY_FR_ms;
          break;
        case TimestampType::Unit::MICRO:
          date_dtype->meta.base = NPY_FR_us;
          break;
        case TimestampType::Unit::NANO:
          date_dtype->meta.base = NPY_FR_ns;
          break;
      }
    } else {
      // datatype->type == Type::DATE64
      date_dtype->meta.base = NPY_FR_D;
    }
  }
}

class PandasBlock {
 public:
  enum type {
    OBJECT,
    UINT8,
    INT8,
    UINT16,
    INT16,
    UINT32,
    INT32,
    UINT64,
    INT64,
    FLOAT,
    DOUBLE,
    BOOL,
    DATETIME,
    DATETIME_WITH_TZ,
    CATEGORICAL
  };

  PandasBlock(int64_t num_rows, int num_columns)
      : num_rows_(num_rows), num_columns_(num_columns) {}
  virtual ~PandasBlock() {}

  virtual Status Allocate() = 0;
  virtual Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) = 0;

  PyObject* block_arr() const { return block_arr_.obj(); }

  virtual Status GetPyResult(PyObject** output) {
    PyObject* result = PyDict_New();
    RETURN_IF_PYERROR();

    PyDict_SetItemString(result, "block", block_arr_.obj());
    PyDict_SetItemString(result, "placement", placement_arr_.obj());

    *output = result;

    return Status::OK();
  }

 protected:
  Status AllocateNDArray(int npy_type, int ndim = 2) {
    PyAcquireGIL lock;

    PyObject* block_arr;
    if (ndim == 2) {
      npy_intp block_dims[2] = {num_columns_, num_rows_};
      block_arr = PyArray_SimpleNew(2, block_dims, npy_type);
    } else {
      npy_intp block_dims[1] = {num_rows_};
      block_arr = PyArray_SimpleNew(1, block_dims, npy_type);
    }

    if (block_arr == NULL) {
      // TODO(wesm): propagating Python exception
      return Status::OK();
    }

    npy_intp placement_dims[1] = {num_columns_};
    PyObject* placement_arr = PyArray_SimpleNew(1, placement_dims, NPY_INT64);
    if (placement_arr == NULL) {
      // TODO(wesm): propagating Python exception
      return Status::OK();
    }

    block_arr_.reset(block_arr);
    placement_arr_.reset(placement_arr);

    block_data_ = reinterpret_cast<uint8_t*>(
        PyArray_DATA(reinterpret_cast<PyArrayObject*>(block_arr)));

    placement_data_ = reinterpret_cast<int64_t*>(
        PyArray_DATA(reinterpret_cast<PyArrayObject*>(placement_arr)));

    return Status::OK();
  }

  int64_t num_rows_;
  int num_columns_;

  OwnedRef block_arr_;
  uint8_t* block_data_;

  // ndarray<int32>
  OwnedRef placement_arr_;
  int64_t* placement_data_;

 private:
  DISALLOW_COPY_AND_ASSIGN(PandasBlock);
};

template <typename T>
inline void ConvertIntegerWithNulls(const ChunkedArray& data, double* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const T*>(prim_arr->data()->data());
    // Upcast to double, set NaN as appropriate

    for (int i = 0; i < arr->length(); ++i) {
      *out_values++ = prim_arr->IsNull(i) ? NAN : static_cast<double>(in_values[i]);
    }
  }
}

template <typename T>
inline void ConvertIntegerNoNullsSameType(const ChunkedArray& data, T* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const T*>(prim_arr->data()->data());
    memcpy(out_values, in_values, sizeof(T) * arr->length());
    out_values += arr->length();
  }
}

template <typename InType, typename OutType>
inline void ConvertIntegerNoNullsCast(const ChunkedArray& data, OutType* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const InType*>(prim_arr->data()->data());
    for (int64_t i = 0; i < arr->length(); ++i) {
      *out_values = in_values[i];
    }
  }
}

static Status ConvertBooleanWithNulls(const ChunkedArray& data, PyObject** out_values) {
  PyAcquireGIL lock;
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto bool_arr = static_cast<BooleanArray*>(arr.get());

    for (int64_t i = 0; i < arr->length(); ++i) {
      if (bool_arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values++ = Py_None;
      } else if (bool_arr->Value(i)) {
        // True
        Py_INCREF(Py_True);
        *out_values++ = Py_True;
      } else {
        // False
        Py_INCREF(Py_False);
        *out_values++ = Py_False;
      }
    }
  }
  return Status::OK();
}

static void ConvertBooleanNoNulls(const ChunkedArray& data, uint8_t* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto bool_arr = static_cast<BooleanArray*>(arr.get());
    for (int64_t i = 0; i < arr->length(); ++i) {
      *out_values++ = static_cast<uint8_t>(bool_arr->Value(i));
    }
  }
}

template <typename ArrayType>
inline Status ConvertBinaryLike(const ChunkedArray& data, PyObject** out_values) {
  PyAcquireGIL lock;
  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = static_cast<ArrayType*>(data.chunk(c).get());

    const uint8_t* data_ptr;
    int32_t length;
    const bool has_nulls = data.null_count() > 0;
    for (int64_t i = 0; i < arr->length(); ++i) {
      if (has_nulls && arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values = Py_None;
      } else {
        data_ptr = arr->GetValue(i, &length);
        *out_values = WrapBytes<ArrayType>::Wrap(data_ptr, length);
        if (*out_values == nullptr) {
          PyErr_Clear();
          std::stringstream ss;
          ss << "Wrapping "
             << std::string(reinterpret_cast<const char*>(data_ptr), length) << " failed";
          return Status::UnknownError(ss.str());
        }
      }
      ++out_values;
    }
  }
  return Status::OK();
}

inline Status ConvertFixedSizeBinary(const ChunkedArray& data, PyObject** out_values) {
  PyAcquireGIL lock;
  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = static_cast<FixedSizeBinaryArray*>(data.chunk(c).get());

    const uint8_t* data_ptr;
    int32_t length =
        std::dynamic_pointer_cast<FixedSizeBinaryType>(arr->type())->byte_width();
    const bool has_nulls = data.null_count() > 0;
    for (int64_t i = 0; i < arr->length(); ++i) {
      if (has_nulls && arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values = Py_None;
      } else {
        data_ptr = arr->GetValue(i);
        *out_values = WrapBytes<FixedSizeBinaryArray>::Wrap(data_ptr, length);
        if (*out_values == nullptr) {
          PyErr_Clear();
          std::stringstream ss;
          ss << "Wrapping "
             << std::string(reinterpret_cast<const char*>(data_ptr), length) << " failed";
          return Status::UnknownError(ss.str());
        }
      }
      ++out_values;
    }
  }
  return Status::OK();
}

template <typename ArrowType>
inline Status ConvertListsLike(
    const std::shared_ptr<Column>& col, PyObject** out_values) {
  const ChunkedArray& data = *col->data().get();
  auto list_type = std::static_pointer_cast<ListType>(col->type());

  // Get column of underlying value arrays
  std::vector<std::shared_ptr<Array>> value_arrays;
  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = std::static_pointer_cast<ListArray>(data.chunk(c));
    value_arrays.emplace_back(arr->values());
  }
  auto flat_column = std::make_shared<Column>(list_type->value_field(), value_arrays);
  // TODO(ARROW-489): Currently we don't have a Python reference for single columns.
  //    Storing a reference to the whole Array would be to expensive.
  PyObject* numpy_array;
  RETURN_NOT_OK(ConvertColumnToPandas(flat_column, nullptr, &numpy_array));

  PyAcquireGIL lock;

  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = std::static_pointer_cast<ListArray>(data.chunk(c));

    const bool has_nulls = data.null_count() > 0;
    for (int64_t i = 0; i < arr->length(); ++i) {
      if (has_nulls && arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values = Py_None;
      } else {
        PyObject* start = PyLong_FromLong(arr->value_offset(i));
        PyObject* end = PyLong_FromLong(arr->value_offset(i + 1));
        PyObject* slice = PySlice_New(start, end, NULL);
        *out_values = PyObject_GetItem(numpy_array, slice);
        Py_DECREF(start);
        Py_DECREF(end);
        Py_DECREF(slice);
      }
      ++out_values;
    }
  }

  Py_XDECREF(numpy_array);
  return Status::OK();
}

template <typename T>
inline void ConvertNumericNullable(const ChunkedArray& data, T na_value, T* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const T*>(prim_arr->data()->data());

    const uint8_t* valid_bits = arr->null_bitmap_data();

    if (arr->null_count() > 0) {
      for (int64_t i = 0; i < arr->length(); ++i) {
        *out_values++ = BitUtil::BitNotSet(valid_bits, i) ? na_value : in_values[i];
      }
    } else {
      memcpy(out_values, in_values, sizeof(T) * arr->length());
      out_values += arr->length();
    }
  }
}

template <typename InType, typename OutType>
inline void ConvertNumericNullableCast(
    const ChunkedArray& data, OutType na_value, OutType* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const InType*>(prim_arr->data()->data());

    for (int64_t i = 0; i < arr->length(); ++i) {
      *out_values++ = arr->IsNull(i) ? na_value : static_cast<OutType>(in_values[i]);
    }
  }
}

template <typename T>
inline void ConvertDates(const ChunkedArray& data, T na_value, T* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const T*>(prim_arr->data()->data());

    for (int64_t i = 0; i < arr->length(); ++i) {
      // There are 1000 * 60 * 60 * 24 = 86400000ms in a day
      *out_values++ = arr->IsNull(i) ? na_value : in_values[i] / 86400000;
    }
  }
}

template <typename InType, int SHIFT>
inline void ConvertDatetimeNanos(const ChunkedArray& data, int64_t* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const InType*>(prim_arr->data()->data());

    for (int64_t i = 0; i < arr->length(); ++i) {
      *out_values++ = arr->IsNull(i) ? kPandasTimestampNull
                                     : (static_cast<int64_t>(in_values[i]) * SHIFT);
    }
  }
}

#define CONVERTLISTSLIKE_CASE(ArrowType, ArrowEnum)                \
  case Type::ArrowEnum:                                            \
    RETURN_NOT_OK((ConvertListsLike<ArrowType>(col, out_buffer))); \
    break;

class ObjectBlock : public PandasBlock {
 public:
  using PandasBlock::PandasBlock;
  virtual ~ObjectBlock() {}

  Status Allocate() override { return AllocateNDArray(NPY_OBJECT); }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->type;

    PyObject** out_buffer =
        reinterpret_cast<PyObject**>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col->data().get();

    if (type == Type::BOOL) {
      RETURN_NOT_OK(ConvertBooleanWithNulls(data, out_buffer));
    } else if (type == Type::BINARY) {
      RETURN_NOT_OK(ConvertBinaryLike<BinaryArray>(data, out_buffer));
    } else if (type == Type::STRING) {
      RETURN_NOT_OK(ConvertBinaryLike<StringArray>(data, out_buffer));
    } else if (type == Type::FIXED_SIZE_BINARY) {
      RETURN_NOT_OK(ConvertFixedSizeBinary(data, out_buffer));
    } else if (type == Type::LIST) {
      auto list_type = std::static_pointer_cast<ListType>(col->type());
      switch (list_type->value_type()->type) {
        CONVERTLISTSLIKE_CASE(UInt8Type, UINT8)
        CONVERTLISTSLIKE_CASE(Int8Type, INT8)
        CONVERTLISTSLIKE_CASE(UInt16Type, UINT16)
        CONVERTLISTSLIKE_CASE(Int16Type, INT16)
        CONVERTLISTSLIKE_CASE(UInt32Type, UINT32)
        CONVERTLISTSLIKE_CASE(Int32Type, INT32)
        CONVERTLISTSLIKE_CASE(UInt64Type, UINT64)
        CONVERTLISTSLIKE_CASE(Int64Type, INT64)
        CONVERTLISTSLIKE_CASE(TimestampType, TIMESTAMP)
        CONVERTLISTSLIKE_CASE(FloatType, FLOAT)
        CONVERTLISTSLIKE_CASE(DoubleType, DOUBLE)
        CONVERTLISTSLIKE_CASE(StringType, STRING)
        default: {
          std::stringstream ss;
          ss << "Not implemented type for lists: " << list_type->value_type()->ToString();
          return Status::NotImplemented(ss.str());
        }
      }
    } else {
      std::stringstream ss;
      ss << "Unsupported type for object array output: " << col->type()->ToString();
      return Status::NotImplemented(ss.str());
    }

    placement_data_[rel_placement] = abs_placement;
    return Status::OK();
  }
};

template <int ARROW_TYPE, typename C_TYPE>
class IntBlock : public PandasBlock {
 public:
  using PandasBlock::PandasBlock;

  Status Allocate() override {
    return AllocateNDArray(arrow_traits<ARROW_TYPE>::npy_type);
  }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->type;

    C_TYPE* out_buffer =
        reinterpret_cast<C_TYPE*>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col->data().get();

    if (type != ARROW_TYPE) { return Status::NotImplemented(col->type()->ToString()); }

    ConvertIntegerNoNullsSameType<C_TYPE>(data, out_buffer);
    placement_data_[rel_placement] = abs_placement;
    return Status::OK();
  }
};

using UInt8Block = IntBlock<Type::UINT8, uint8_t>;
using Int8Block = IntBlock<Type::INT8, int8_t>;
using UInt16Block = IntBlock<Type::UINT16, uint16_t>;
using Int16Block = IntBlock<Type::INT16, int16_t>;
using UInt32Block = IntBlock<Type::UINT32, uint32_t>;
using Int32Block = IntBlock<Type::INT32, int32_t>;
using UInt64Block = IntBlock<Type::UINT64, uint64_t>;
using Int64Block = IntBlock<Type::INT64, int64_t>;

class Float32Block : public PandasBlock {
 public:
  using PandasBlock::PandasBlock;

  Status Allocate() override { return AllocateNDArray(NPY_FLOAT32); }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->type;

    if (type != Type::FLOAT) { return Status::NotImplemented(col->type()->ToString()); }

    float* out_buffer = reinterpret_cast<float*>(block_data_) + rel_placement * num_rows_;

    ConvertNumericNullable<float>(*col->data().get(), NAN, out_buffer);
    placement_data_[rel_placement] = abs_placement;
    return Status::OK();
  }
};

class Float64Block : public PandasBlock {
 public:
  using PandasBlock::PandasBlock;

  Status Allocate() override { return AllocateNDArray(NPY_FLOAT64); }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->type;

    double* out_buffer =
        reinterpret_cast<double*>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col->data().get();

#define INTEGER_CASE(IN_TYPE)                         \
  ConvertIntegerWithNulls<IN_TYPE>(data, out_buffer); \
  break;

    switch (type) {
      case Type::UINT8:
        INTEGER_CASE(uint8_t);
      case Type::INT8:
        INTEGER_CASE(int8_t);
      case Type::UINT16:
        INTEGER_CASE(uint16_t);
      case Type::INT16:
        INTEGER_CASE(int16_t);
      case Type::UINT32:
        INTEGER_CASE(uint32_t);
      case Type::INT32:
        INTEGER_CASE(int32_t);
      case Type::UINT64:
        INTEGER_CASE(uint64_t);
      case Type::INT64:
        INTEGER_CASE(int64_t);
      case Type::FLOAT:
        ConvertNumericNullableCast<float, double>(data, NAN, out_buffer);
        break;
      case Type::DOUBLE:
        ConvertNumericNullable<double>(data, NAN, out_buffer);
        break;
      default:
        return Status::NotImplemented(col->type()->ToString());
    }

#undef INTEGER_CASE

    placement_data_[rel_placement] = abs_placement;
    return Status::OK();
  }
};

class BoolBlock : public PandasBlock {
 public:
  using PandasBlock::PandasBlock;

  Status Allocate() override { return AllocateNDArray(NPY_BOOL); }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->type;

    if (type != Type::BOOL) { return Status::NotImplemented(col->type()->ToString()); }

    uint8_t* out_buffer =
        reinterpret_cast<uint8_t*>(block_data_) + rel_placement * num_rows_;

    ConvertBooleanNoNulls(*col->data().get(), out_buffer);
    placement_data_[rel_placement] = abs_placement;
    return Status::OK();
  }
};

class DatetimeBlock : public PandasBlock {
 public:
  using PandasBlock::PandasBlock;

  Status AllocateDatetime(int ndim) {
    RETURN_NOT_OK(AllocateNDArray(NPY_DATETIME, ndim));

    PyAcquireGIL lock;
    auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(
        PyArray_DESCR(reinterpret_cast<PyArrayObject*>(block_arr_.obj()))->c_metadata);
    date_dtype->meta.base = NPY_FR_ns;
    return Status::OK();
  }

  Status Allocate() override { return AllocateDatetime(2); }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->type;

    int64_t* out_buffer =
        reinterpret_cast<int64_t*>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col.get()->data();

    if (type == Type::DATE64) {
      // Date64Type is millisecond timestamp stored as int64_t
      // TODO(wesm): Do we want to make sure to zero out the milliseconds?
      ConvertDatetimeNanos<int64_t, 1000000L>(data, out_buffer);
    } else if (type == Type::TIMESTAMP) {
      auto ts_type = static_cast<TimestampType*>(col->type().get());

      if (ts_type->unit == TimeUnit::NANO) {
        ConvertNumericNullable<int64_t>(data, kPandasTimestampNull, out_buffer);
      } else if (ts_type->unit == TimeUnit::MICRO) {
        ConvertDatetimeNanos<int64_t, 1000L>(data, out_buffer);
      } else if (ts_type->unit == TimeUnit::MILLI) {
        ConvertDatetimeNanos<int64_t, 1000000L>(data, out_buffer);
      } else if (ts_type->unit == TimeUnit::SECOND) {
        ConvertDatetimeNanos<int64_t, 1000000000L>(data, out_buffer);
      } else {
        return Status::NotImplemented("Unsupported time unit");
      }
    } else {
      return Status::NotImplemented(col->type()->ToString());
    }

    placement_data_[rel_placement] = abs_placement;
    return Status::OK();
  }
};

class DatetimeTZBlock : public DatetimeBlock {
 public:
  DatetimeTZBlock(const std::string& timezone, int64_t num_rows)
      : DatetimeBlock(num_rows, 1), timezone_(timezone) {}

  // Like Categorical, the internal ndarray is 1-dimensional
  Status Allocate() override { return AllocateDatetime(1); }

  Status GetPyResult(PyObject** output) override {
    PyObject* result = PyDict_New();
    RETURN_IF_PYERROR();

    PyObject* py_tz = PyUnicode_FromStringAndSize(
        timezone_.c_str(), static_cast<Py_ssize_t>(timezone_.size()));
    RETURN_IF_PYERROR();

    PyDict_SetItemString(result, "block", block_arr_.obj());
    PyDict_SetItemString(result, "timezone", py_tz);
    PyDict_SetItemString(result, "placement", placement_arr_.obj());

    *output = result;

    return Status::OK();
  }

 private:
  std::string timezone_;
};

template <int ARROW_INDEX_TYPE>
class CategoricalBlock : public PandasBlock {
 public:
  explicit CategoricalBlock(int64_t num_rows) : PandasBlock(num_rows, 1) {}

  Status Allocate() override {
    constexpr int npy_type = arrow_traits<ARROW_INDEX_TYPE>::npy_type;

    if (!(npy_type == NPY_INT8 || npy_type == NPY_INT16 || npy_type == NPY_INT32 ||
            npy_type == NPY_INT64)) {
      return Status::Invalid("Category indices must be signed integers");
    }
    return AllocateNDArray(npy_type, 1);
  }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    using T = typename arrow_traits<ARROW_INDEX_TYPE>::T;

    T* out_values = reinterpret_cast<T*>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col->data().get();

    for (int c = 0; c < data.num_chunks(); c++) {
      const std::shared_ptr<Array> arr = data.chunk(c);
      const auto& dict_arr = static_cast<const DictionaryArray&>(*arr);
      const auto& indices = static_cast<const PrimitiveArray&>(*dict_arr.indices());
      auto in_values = reinterpret_cast<const T*>(indices.data()->data());

      // Null is -1 in CategoricalBlock
      for (int i = 0; i < arr->length(); ++i) {
        *out_values++ = indices.IsNull(i) ? -1 : in_values[i];
      }
    }

    placement_data_[rel_placement] = abs_placement;

    auto dict_type = static_cast<const DictionaryType*>(col->type().get());

    PyObject* dict;
    RETURN_NOT_OK(ConvertArrayToPandas(dict_type->dictionary(), nullptr, &dict));
    dictionary_.reset(dict);

    return Status::OK();
  }

  Status GetPyResult(PyObject** output) override {
    PyObject* result = PyDict_New();
    RETURN_IF_PYERROR();

    PyDict_SetItemString(result, "block", block_arr_.obj());
    PyDict_SetItemString(result, "dictionary", dictionary_.obj());
    PyDict_SetItemString(result, "placement", placement_arr_.obj());

    *output = result;

    return Status::OK();
  }

 protected:
  OwnedRef dictionary_;
};

Status MakeBlock(PandasBlock::type type, int64_t num_rows, int num_columns,
    std::shared_ptr<PandasBlock>* block) {
#define BLOCK_CASE(NAME, TYPE)                              \
  case PandasBlock::NAME:                                   \
    *block = std::make_shared<TYPE>(num_rows, num_columns); \
    break;

  switch (type) {
    BLOCK_CASE(OBJECT, ObjectBlock);
    BLOCK_CASE(UINT8, UInt8Block);
    BLOCK_CASE(INT8, Int8Block);
    BLOCK_CASE(UINT16, UInt16Block);
    BLOCK_CASE(INT16, Int16Block);
    BLOCK_CASE(UINT32, UInt32Block);
    BLOCK_CASE(INT32, Int32Block);
    BLOCK_CASE(UINT64, UInt64Block);
    BLOCK_CASE(INT64, Int64Block);
    BLOCK_CASE(FLOAT, Float32Block);
    BLOCK_CASE(DOUBLE, Float64Block);
    BLOCK_CASE(BOOL, BoolBlock);
    BLOCK_CASE(DATETIME, DatetimeBlock);
    default:
      return Status::NotImplemented("Unsupported block type");
  }

#undef BLOCK_CASE

  return (*block)->Allocate();
}

static inline Status MakeCategoricalBlock(const std::shared_ptr<DataType>& type,
    int64_t num_rows, std::shared_ptr<PandasBlock>* block) {
  // All categoricals become a block with a single column
  auto dict_type = static_cast<const DictionaryType*>(type.get());
  switch (dict_type->index_type()->type) {
    case Type::INT8:
      *block = std::make_shared<CategoricalBlock<Type::INT8>>(num_rows);
      break;
    case Type::INT16:
      *block = std::make_shared<CategoricalBlock<Type::INT16>>(num_rows);
      break;
    case Type::INT32:
      *block = std::make_shared<CategoricalBlock<Type::INT32>>(num_rows);
      break;
    case Type::INT64:
      *block = std::make_shared<CategoricalBlock<Type::INT64>>(num_rows);
      break;
    default: {
      std::stringstream ss;
      ss << "Categorical index type not implemented: "
         << dict_type->index_type()->ToString();
      return Status::NotImplemented(ss.str());
    }
  }
  return (*block)->Allocate();
}

using BlockMap = std::unordered_map<int, std::shared_ptr<PandasBlock>>;

// Construct the exact pandas 0.x "BlockManager" memory layout
//
// * For each column determine the correct output pandas type
// * Allocate 2D blocks (ncols x nrows) for each distinct data type in output
// * Allocate  block placement arrays
// * Write Arrow columns out into each slice of memory; populate block
// * placement arrays as we go
class DataFrameBlockCreator {
 public:
  explicit DataFrameBlockCreator(const std::shared_ptr<Table>& table) : table_(table) {}

  Status Convert(int nthreads, PyObject** output) {
    column_types_.resize(table_->num_columns());
    column_block_placement_.resize(table_->num_columns());
    type_counts_.clear();
    blocks_.clear();

    RETURN_NOT_OK(CreateBlocks());
    RETURN_NOT_OK(WriteTableToBlocks(nthreads));

    return GetResultList(output);
  }

  Status CreateBlocks() {
    for (int i = 0; i < table_->num_columns(); ++i) {
      std::shared_ptr<Column> col = table_->column(i);
      PandasBlock::type output_type;

      Type::type column_type = col->type()->type;
      switch (column_type) {
        case Type::BOOL:
          output_type = col->null_count() > 0 ? PandasBlock::OBJECT : PandasBlock::BOOL;
          break;
        case Type::UINT8:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT8;
          break;
        case Type::INT8:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT8;
          break;
        case Type::UINT16:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT16;
          break;
        case Type::INT16:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT16;
          break;
        case Type::UINT32:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT32;
          break;
        case Type::INT32:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT32;
          break;
        case Type::INT64:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT64;
          break;
        case Type::UINT64:
          output_type = col->null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT64;
          break;
        case Type::FLOAT:
          output_type = PandasBlock::FLOAT;
          break;
        case Type::DOUBLE:
          output_type = PandasBlock::DOUBLE;
          break;
        case Type::STRING:
        case Type::BINARY:
        case Type::FIXED_SIZE_BINARY:
          output_type = PandasBlock::OBJECT;
          break;
        case Type::DATE64:
          output_type = PandasBlock::DATETIME;
          break;
        case Type::TIMESTAMP: {
          const auto& ts_type = static_cast<const TimestampType&>(*col->type());
          if (ts_type.timezone != "") {
            output_type = PandasBlock::DATETIME_WITH_TZ;
          } else {
            output_type = PandasBlock::DATETIME;
          }
        } break;
        case Type::LIST: {
          auto list_type = std::static_pointer_cast<ListType>(col->type());
          if (!ListTypeSupported(list_type->value_type()->type)) {
            std::stringstream ss;
            ss << "Not implemented type for lists: "
               << list_type->value_type()->ToString();
            return Status::NotImplemented(ss.str());
          }
          output_type = PandasBlock::OBJECT;
        } break;
        case Type::DICTIONARY:
          output_type = PandasBlock::CATEGORICAL;
          break;
        default:
          return Status::NotImplemented(col->type()->ToString());
      }

      int block_placement = 0;
      std::shared_ptr<PandasBlock> block;
      if (output_type == PandasBlock::CATEGORICAL) {
        RETURN_NOT_OK(MakeCategoricalBlock(col->type(), table_->num_rows(), &block));
        categorical_blocks_[i] = block;
      } else if (output_type == PandasBlock::DATETIME_WITH_TZ) {
        const auto& ts_type = static_cast<const TimestampType&>(*col->type());
        block = std::make_shared<DatetimeTZBlock>(ts_type.timezone, table_->num_rows());
        RETURN_NOT_OK(block->Allocate());
        datetimetz_blocks_[i] = block;
      } else {
        auto it = type_counts_.find(output_type);
        if (it != type_counts_.end()) {
          block_placement = it->second;
          // Increment count
          it->second += 1;
        } else {
          // Add key to map
          type_counts_[output_type] = 1;
        }
      }

      column_types_[i] = output_type;
      column_block_placement_[i] = block_placement;
    }

    // Create normal non-categorical blocks
    for (const auto& it : type_counts_) {
      PandasBlock::type type = static_cast<PandasBlock::type>(it.first);
      std::shared_ptr<PandasBlock> block;
      RETURN_NOT_OK(MakeBlock(type, table_->num_rows(), it.second, &block));
      blocks_[type] = block;
    }
    return Status::OK();
  }

  Status WriteTableToBlocks(int nthreads) {
    auto WriteColumn = [this](int i) {
      std::shared_ptr<Column> col = this->table_->column(i);
      PandasBlock::type output_type = this->column_types_[i];

      int rel_placement = this->column_block_placement_[i];

      std::shared_ptr<PandasBlock> block;
      if (output_type == PandasBlock::CATEGORICAL) {
        auto it = this->categorical_blocks_.find(i);
        if (it == this->blocks_.end()) {
          return Status::KeyError("No categorical block allocated");
        }
        block = it->second;
      } else if (output_type == PandasBlock::DATETIME_WITH_TZ) {
        auto it = this->datetimetz_blocks_.find(i);
        if (it == this->datetimetz_blocks_.end()) {
          return Status::KeyError("No datetimetz block allocated");
        }
        block = it->second;
      } else {
        auto it = this->blocks_.find(output_type);
        if (it == this->blocks_.end()) { return Status::KeyError("No block allocated"); }
        block = it->second;
      }
      return block->Write(col, i, rel_placement);
    };

    nthreads = std::min<int>(nthreads, table_->num_columns());

    if (nthreads == 1) {
      for (int i = 0; i < table_->num_columns(); ++i) {
        RETURN_NOT_OK(WriteColumn(i));
      }
    } else {
      std::vector<std::thread> thread_pool;
      thread_pool.reserve(nthreads);
      std::atomic<int> task_counter(0);

      std::mutex error_mtx;
      bool error_occurred = false;
      Status error;

      for (int thread_id = 0; thread_id < nthreads; ++thread_id) {
        thread_pool.emplace_back(
            [this, &error, &error_occurred, &error_mtx, &task_counter, &WriteColumn]() {
              int column_num;
              while (!error_occurred) {
                column_num = task_counter.fetch_add(1);
                if (column_num >= this->table_->num_columns()) { break; }
                Status s = WriteColumn(column_num);
                if (!s.ok()) {
                  std::lock_guard<std::mutex> lock(error_mtx);
                  error_occurred = true;
                  error = s;
                  break;
                }
              }
            });
      }
      for (auto&& thread : thread_pool) {
        thread.join();
      }

      if (error_occurred) { return error; }
    }
    return Status::OK();
  }

  Status AppendBlocks(const BlockMap& blocks, PyObject* list) {
    for (const auto& it : blocks) {
      PyObject* item;
      RETURN_NOT_OK(it.second->GetPyResult(&item));
      if (PyList_Append(list, item) < 0) { RETURN_IF_PYERROR(); }
    }
    return Status::OK();
  }

  Status GetResultList(PyObject** out) {
    PyAcquireGIL lock;

    PyObject* result = PyList_New(0);
    RETURN_IF_PYERROR();

    RETURN_NOT_OK(AppendBlocks(blocks_, result));
    RETURN_NOT_OK(AppendBlocks(categorical_blocks_, result));
    RETURN_NOT_OK(AppendBlocks(datetimetz_blocks_, result));

    *out = result;
    return Status::OK();
  }

 private:
  std::shared_ptr<Table> table_;

  // column num -> block type id
  std::vector<PandasBlock::type> column_types_;

  // column num -> relative placement within internal block
  std::vector<int> column_block_placement_;

  // block type -> type count
  std::unordered_map<int, int> type_counts_;

  // block type -> block
  BlockMap blocks_;

  // column number -> categorical block
  BlockMap categorical_blocks_;

  // column number -> datetimetz block
  BlockMap datetimetz_blocks_;
};

class ArrowDeserializer {
 public:
  ArrowDeserializer(const std::shared_ptr<Column>& col, PyObject* py_ref)
      : col_(col), data_(*col->data().get()), py_ref_(py_ref) {}

  Status AllocateOutput(int type) {
    PyAcquireGIL lock;

    npy_intp dims[1] = {col_->length()};
    result_ = PyArray_SimpleNew(1, dims, type);
    arr_ = reinterpret_cast<PyArrayObject*>(result_);

    if (arr_ == NULL) {
      // Error occurred, trust that SimpleNew set the error state
      return Status::OK();
    }

    set_numpy_metadata(type, col_->type().get(), arr_);

    return Status::OK();
  }

  template <int TYPE>
  Status ConvertValuesZeroCopy(int npy_type, std::shared_ptr<Array> arr) {
    typedef typename arrow_traits<TYPE>::T T;

    auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
    auto in_values = reinterpret_cast<const T*>(prim_arr->data()->data());

    // Zero-Copy. We can pass the data pointer directly to NumPy.
    void* data = const_cast<T*>(in_values);

    PyAcquireGIL lock;

    // Zero-Copy. We can pass the data pointer directly to NumPy.
    npy_intp dims[1] = {col_->length()};
    result_ = PyArray_SimpleNewFromData(1, dims, npy_type, data);
    arr_ = reinterpret_cast<PyArrayObject*>(result_);

    if (arr_ == NULL) {
      // Error occurred, trust that SimpleNew set the error state
      return Status::OK();
    }

    set_numpy_metadata(npy_type, col_->type().get(), arr_);

    if (PyArray_SetBaseObject(arr_, py_ref_) == -1) {
      // Error occurred, trust that SetBaseObject set the error state
      return Status::OK();
    } else {
      // PyArray_SetBaseObject steals our reference to py_ref_
      Py_INCREF(py_ref_);
    }

    // Arrow data is immutable.
    PyArray_CLEARFLAGS(arr_, NPY_ARRAY_WRITEABLE);

    return Status::OK();
  }

  // ----------------------------------------------------------------------
  // Allocate new array and deserialize. Can do a zero copy conversion for some
  // types

  Status Convert(PyObject** out) {
#define CONVERT_CASE(TYPE)                      \
  case Type::TYPE: {                            \
    RETURN_NOT_OK(ConvertValues<Type::TYPE>()); \
  } break;

    switch (col_->type()->type) {
      CONVERT_CASE(BOOL);
      CONVERT_CASE(INT8);
      CONVERT_CASE(INT16);
      CONVERT_CASE(INT32);
      CONVERT_CASE(INT64);
      CONVERT_CASE(UINT8);
      CONVERT_CASE(UINT16);
      CONVERT_CASE(UINT32);
      CONVERT_CASE(UINT64);
      CONVERT_CASE(FLOAT);
      CONVERT_CASE(DOUBLE);
      CONVERT_CASE(BINARY);
      CONVERT_CASE(STRING);
      CONVERT_CASE(FIXED_SIZE_BINARY);
      CONVERT_CASE(DATE64);
      CONVERT_CASE(TIMESTAMP);
      CONVERT_CASE(DICTIONARY);
      CONVERT_CASE(LIST);
      default: {
        std::stringstream ss;
        ss << "Arrow type reading not implemented for " << col_->type()->ToString();
        return Status::NotImplemented(ss.str());
      }
    }

#undef CONVERT_CASE

    *out = result_;
    return Status::OK();
  }

  template <int TYPE>
  inline typename std::enable_if<
      (TYPE != Type::DATE64) & arrow_traits<TYPE>::is_numeric_nullable, Status>::type
  ConvertValues() {
    typedef typename arrow_traits<TYPE>::T T;
    int npy_type = arrow_traits<TYPE>::npy_type;

    if (data_.num_chunks() == 1 && data_.null_count() == 0 && py_ref_ != nullptr) {
      return ConvertValuesZeroCopy<TYPE>(npy_type, data_.chunk(0));
    }

    RETURN_NOT_OK(AllocateOutput(npy_type));
    auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));
    ConvertNumericNullable<T>(data_, arrow_traits<TYPE>::na_value, out_values);

    return Status::OK();
  }

  template <int TYPE>
  inline typename std::enable_if<TYPE == Type::DATE64, Status>::type ConvertValues() {
    typedef typename arrow_traits<TYPE>::T T;

    RETURN_NOT_OK(AllocateOutput(arrow_traits<TYPE>::npy_type));
    auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));
    ConvertDates<T>(data_, arrow_traits<TYPE>::na_value, out_values);
    return Status::OK();
  }

  // Integer specialization
  template <int TYPE>
  inline
      typename std::enable_if<arrow_traits<TYPE>::is_numeric_not_nullable, Status>::type
      ConvertValues() {
    typedef typename arrow_traits<TYPE>::T T;
    int npy_type = arrow_traits<TYPE>::npy_type;

    if (data_.num_chunks() == 1 && data_.null_count() == 0 && py_ref_ != nullptr) {
      return ConvertValuesZeroCopy<TYPE>(npy_type, data_.chunk(0));
    }

    if (data_.null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_FLOAT64));
      auto out_values = reinterpret_cast<double*>(PyArray_DATA(arr_));
      ConvertIntegerWithNulls<T>(data_, out_values);
    } else {
      RETURN_NOT_OK(AllocateOutput(arrow_traits<TYPE>::npy_type));
      auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));
      ConvertIntegerNoNullsSameType<T>(data_, out_values);
    }

    return Status::OK();
  }

  // Boolean specialization
  template <int TYPE>
  inline typename std::enable_if<arrow_traits<TYPE>::is_boolean, Status>::type
  ConvertValues() {
    if (data_.null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
      auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
      RETURN_NOT_OK(ConvertBooleanWithNulls(data_, out_values));
    } else {
      RETURN_NOT_OK(AllocateOutput(arrow_traits<TYPE>::npy_type));
      auto out_values = reinterpret_cast<uint8_t*>(PyArray_DATA(arr_));
      ConvertBooleanNoNulls(data_, out_values);
    }
    return Status::OK();
  }

  // UTF8 strings
  template <int TYPE>
  inline typename std::enable_if<TYPE == Type::STRING, Status>::type ConvertValues() {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertBinaryLike<StringArray>(data_, out_values);
  }

  // Binary strings
  template <int T2>
  inline typename std::enable_if<T2 == Type::BINARY, Status>::type ConvertValues() {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertBinaryLike<BinaryArray>(data_, out_values);
  }

  // Fixed length binary strings
  template <int TYPE>
  inline typename std::enable_if<TYPE == Type::FIXED_SIZE_BINARY, Status>::type
  ConvertValues() {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertFixedSizeBinary(data_, out_values);
  }

#define CONVERTVALUES_LISTSLIKE_CASE(ArrowType, ArrowEnum) \
  case Type::ArrowEnum:                                    \
    return ConvertListsLike<ArrowType>(col_, out_values);

  template <int T2>
  inline typename std::enable_if<T2 == Type::LIST, Status>::type ConvertValues() {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    auto list_type = std::static_pointer_cast<ListType>(col_->type());
    switch (list_type->value_type()->type) {
      CONVERTVALUES_LISTSLIKE_CASE(UInt8Type, UINT8)
      CONVERTVALUES_LISTSLIKE_CASE(Int8Type, INT8)
      CONVERTVALUES_LISTSLIKE_CASE(UInt16Type, UINT16)
      CONVERTVALUES_LISTSLIKE_CASE(Int16Type, INT16)
      CONVERTVALUES_LISTSLIKE_CASE(UInt32Type, UINT32)
      CONVERTVALUES_LISTSLIKE_CASE(Int32Type, INT32)
      CONVERTVALUES_LISTSLIKE_CASE(UInt64Type, UINT64)
      CONVERTVALUES_LISTSLIKE_CASE(Int64Type, INT64)
      CONVERTVALUES_LISTSLIKE_CASE(TimestampType, TIMESTAMP)
      CONVERTVALUES_LISTSLIKE_CASE(FloatType, FLOAT)
      CONVERTVALUES_LISTSLIKE_CASE(DoubleType, DOUBLE)
      CONVERTVALUES_LISTSLIKE_CASE(StringType, STRING)
      default: {
        std::stringstream ss;
        ss << "Not implemented type for lists: " << list_type->value_type()->ToString();
        return Status::NotImplemented(ss.str());
      }
    }
  }

  template <int TYPE>
  inline typename std::enable_if<TYPE == Type::DICTIONARY, Status>::type ConvertValues() {
    std::shared_ptr<PandasBlock> block;
    RETURN_NOT_OK(MakeCategoricalBlock(col_->type(), col_->length(), &block));
    RETURN_NOT_OK(block->Write(col_, 0, 0));

    auto dict_type = static_cast<const DictionaryType*>(col_->type().get());

    PyAcquireGIL lock;
    result_ = PyDict_New();
    RETURN_IF_PYERROR();

    PyObject* dictionary;
    RETURN_NOT_OK(ConvertArrayToPandas(dict_type->dictionary(), nullptr, &dictionary));

    PyDict_SetItemString(result_, "indices", block->block_arr());
    PyDict_SetItemString(result_, "dictionary", dictionary);

    return Status::OK();
  }

 private:
  std::shared_ptr<Column> col_;
  const ChunkedArray& data_;
  PyObject* py_ref_;
  PyArrayObject* arr_;
  PyObject* result_;
};

Status ConvertArrayToPandas(
    const std::shared_ptr<Array>& arr, PyObject* py_ref, PyObject** out) {
  static std::string dummy_name = "dummy";
  auto field = std::make_shared<Field>(dummy_name, arr->type());
  auto col = std::make_shared<Column>(field, arr);
  return ConvertColumnToPandas(col, py_ref, out);
}

Status ConvertColumnToPandas(
    const std::shared_ptr<Column>& col, PyObject* py_ref, PyObject** out) {
  ArrowDeserializer converter(col, py_ref);
  return converter.Convert(out);
}

Status ConvertTableToPandas(
    const std::shared_ptr<Table>& table, int nthreads, PyObject** out) {
  DataFrameBlockCreator helper(table);
  return helper.Convert(nthreads, out);
}

}  // namespace py
}  // namespace arrow
