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

#define ARROW_NO_DEFAULT_MEMORY_POOL

#include "arrow/python/numpy_interop.h"

#include "arrow/python/pandas_to_arrow.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int128.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/visitor_inline.h"

#include "arrow/python/builtin_convert.h"
#include "arrow/python/common.h"
#include "arrow/python/config.h"
#include "arrow/python/helpers.h"
#include "arrow/python/numpy-internal.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/type_traits.h"
#include "arrow/python/util/datetime.h"

namespace arrow {

using internal::ArrayData;
using internal::MakeArray;

namespace py {

using internal::NumPyTypeSize;

// ----------------------------------------------------------------------
// Conversion utilities

static inline bool PyFloat_isnan(const PyObject* obj) {
  if (PyFloat_Check(obj)) {
    double val = PyFloat_AS_DOUBLE(obj);
    return val != val;
  } else {
    return false;
  }
}
static inline bool PandasObjectIsNull(const PyObject* obj) {
  return obj == Py_None || obj == numpy_nan || PyFloat_isnan(obj);
}

static inline bool PyObject_is_string(const PyObject* obj) {
#if PY_MAJOR_VERSION >= 3
  return PyUnicode_Check(obj) || PyBytes_Check(obj);
#else
  return PyString_Check(obj) || PyUnicode_Check(obj);
#endif
}

static inline bool PyObject_is_float(const PyObject* obj) { return PyFloat_Check(obj); }

static inline bool PyObject_is_integer(const PyObject* obj) {
  return (!PyBool_Check(obj)) && PyArray_IsIntegerScalar(obj);
}

template <int TYPE>
static int64_t ValuesToBitmap(PyArrayObject* arr, uint8_t* bitmap) {
  typedef internal::npy_traits<TYPE> traits;
  typedef typename traits::value_type T;

  int64_t null_count = 0;

  Ndarray1DIndexer<T> values(arr);
  for (int i = 0; i < values.size(); ++i) {
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

  Ndarray1DIndexer<uint8_t> mask_values(mask);
  for (int i = 0; i < length; ++i) {
    if (mask_values[i]) {
      ++null_count;
    } else {
      BitUtil::SetBit(bitmap, i);
    }
  }
  return null_count;
}

template <int TYPE, typename BuilderType>
static Status AppendNdarrayToBuilder(PyArrayObject* array, BuilderType* builder) {
  typedef internal::npy_traits<TYPE> traits;
  typedef typename traits::value_type T;

  // TODO(wesm): Vector append when not strided
  Ndarray1DIndexer<T> values(array);
  if (traits::supports_nulls) {
    for (int64_t i = 0; i < values.size(); ++i) {
      if (traits::isnull(values[i])) {
        RETURN_NOT_OK(builder->AppendNull());
      } else {
        RETURN_NOT_OK(builder->Append(values[i]));
      }
    }
  } else {
    for (int64_t i = 0; i < values.size(); ++i) {
      RETURN_NOT_OK(builder->Append(values[i]));
    }
  }
  return Status::OK();
}

Status CheckFlatNumpyArray(PyArrayObject* numpy_array, int np_type) {
  if (PyArray_NDIM(numpy_array) != 1) {
    return Status::Invalid("only handle 1-dimensional arrays");
  }

  const int received_type = PyArray_DESCR(numpy_array)->type_num;
  if (received_type != np_type) {
    std::stringstream ss;
    ss << "trying to convert NumPy type " << GetNumPyTypeName(np_type) << " but got "
       << GetNumPyTypeName(received_type);
    return Status::Invalid(ss.str());
  }

  return Status::OK();
}

constexpr int64_t kBinaryMemoryLimit = std::numeric_limits<int32_t>::max();

/// Append as many string objects from NumPy arrays to a `StringBuilder` as we
/// can fit
///
/// \param[in] offset starting offset for appending
/// \param[out] values_consumed ending offset where we stopped appending. Will
/// be length of arr if fully consumed
/// \param[out] have_bytes true if we encountered any PyBytes object
static Status AppendObjectStrings(PyArrayObject* arr, PyArrayObject* mask, int64_t offset,
                                  StringBuilder* builder, int64_t* end_offset,
                                  bool* have_bytes) {
  PyObject* obj;

  Ndarray1DIndexer<PyObject*> objects(arr);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask != nullptr) {
    mask_values.Init(mask);
    have_mask = true;
  }

  for (; offset < objects.size(); ++offset) {
    OwnedRef tmp_obj;
    obj = objects[offset];
    if ((have_mask && mask_values[offset]) || PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder->AppendNull());
      continue;
    } else if (PyUnicode_Check(obj)) {
      obj = PyUnicode_AsUTF8String(obj);
      if (obj == NULL) {
        PyErr_Clear();
        return Status::Invalid("failed converting unicode to UTF8");
      }
      tmp_obj.reset(obj);
    } else if (PyBytes_Check(obj)) {
      *have_bytes = true;
    } else {
      std::stringstream ss;
      ss << "Error converting to Python objects to String/UTF8: ";
      RETURN_NOT_OK(InvalidConversion(obj, "str, bytes", &ss));
      return Status::Invalid(ss.str());
    }

    const int32_t length = static_cast<int32_t>(PyBytes_GET_SIZE(obj));
    if (ARROW_PREDICT_FALSE(builder->value_data_length() + length > kBinaryMemoryLimit)) {
      break;
    }
    RETURN_NOT_OK(builder->Append(PyBytes_AS_STRING(obj), length));
  }

  // If we consumed the whole array, this will be the length of arr
  *end_offset = offset;
  return Status::OK();
}

static Status AppendObjectFixedWidthBytes(PyArrayObject* arr, PyArrayObject* mask,
                                          int byte_width, int64_t offset,
                                          FixedSizeBinaryBuilder* builder,
                                          int64_t* end_offset) {
  PyObject* obj;

  Ndarray1DIndexer<PyObject*> objects(arr);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask != nullptr) {
    mask_values.Init(mask);
    have_mask = true;
  }

  for (; offset < objects.size(); ++offset) {
    OwnedRef tmp_obj;
    obj = objects[offset];
    if ((have_mask && mask_values[offset]) || PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder->AppendNull());
      continue;
    } else if (PyUnicode_Check(obj)) {
      obj = PyUnicode_AsUTF8String(obj);
      if (obj == NULL) {
        PyErr_Clear();
        return Status::Invalid("failed converting unicode to UTF8");
      }

      tmp_obj.reset(obj);
    } else if (!PyBytes_Check(obj)) {
      std::stringstream ss;
      ss << "Error converting to Python objects to FixedSizeBinary: ";
      RETURN_NOT_OK(InvalidConversion(obj, "str, bytes", &ss));
      return Status::Invalid(ss.str());
    }

    RETURN_NOT_OK(CheckPythonBytesAreFixedLength(obj, byte_width));
    if (ARROW_PREDICT_FALSE(builder->value_data_length() + byte_width >
                            kBinaryMemoryLimit)) {
      break;
    }
    RETURN_NOT_OK(
        builder->Append(reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(obj))));
  }

  // If we consumed the whole array, this will be the length of arr
  *end_offset = offset;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Conversion from NumPy-in-Pandas to Arrow

class PandasConverter {
 public:
  PandasConverter(MemoryPool* pool, PyObject* ao, PyObject* mo,
                  const std::shared_ptr<DataType>& type)
      : pool_(pool),
        type_(type),
        arr_(reinterpret_cast<PyArrayObject*>(ao)),
        mask_(nullptr) {
    if (mo != nullptr && mo != Py_None) {
      mask_ = reinterpret_cast<PyArrayObject*>(mo);
    }
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

  template <typename T>
  Status PushBuilderResult(T* builder) {
    std::shared_ptr<Array> out;
    RETURN_NOT_OK(builder->Finish(&out));
    out_arrays_.emplace_back(out);
    return Status::OK();
  }

  Status PushArray(const std::shared_ptr<ArrayData>& data) {
    std::shared_ptr<Array> result;
    RETURN_NOT_OK(MakeArray(data, &result));
    out_arrays_.emplace_back(std::move(result));
    return Status::OK();
  }

  template <typename ArrowType>
  Status VisitNative() {
    using traits = internal::arrow_traits<ArrowType::type_id>;

    if (mask_ != nullptr || traits::supports_nulls) {
      RETURN_NOT_OK(InitNullBitmap());
    }

    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(ConvertData<ArrowType>(&data));

    int64_t null_count = 0;
    if (mask_ != nullptr) {
      null_count = MaskToBitmap(mask_, length_, null_bitmap_data_);
    } else if (traits::supports_nulls) {
      // TODO(wesm): this presumes the NumPy C type and arrow C type are the
      // same
      null_count = ValuesToBitmap<traits::npy_type>(arr_, null_bitmap_data_);
    }

    BufferVector buffers = {null_bitmap_, data};
    return PushArray(
        std::make_shared<ArrayData>(type_, length_, std::move(buffers), null_count, 0));
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value ||
                              std::is_same<BooleanType, T>::value,
                          Status>::type
  Visit(const T& type) {
    return VisitNative<T>();
  }

  Status Visit(const Date32Type& type) { return VisitNative<Date32Type>(); }
  Status Visit(const Date64Type& type) { return VisitNative<Int64Type>(); }
  Status Visit(const TimestampType& type) { return VisitNative<TimestampType>(); }
  Status Visit(const Time32Type& type) { return VisitNative<Int32Type>(); }
  Status Visit(const Time64Type& type) { return VisitNative<Int64Type>(); }

  Status TypeNotImplemented(std::string type_name) {
    std::stringstream ss;
    ss << "PandasConverter doesn't implement <" << type_name << "> conversion. ";
    return Status::NotImplemented(ss.str());
  }

  Status Visit(const NullType& type) { return TypeNotImplemented(type.ToString()); }

  Status Visit(const BinaryType& type) { return TypeNotImplemented(type.ToString()); }

  Status Visit(const FixedSizeBinaryType& type) {
    return TypeNotImplemented(type.ToString());
  }

  Status Visit(const DecimalType& type) { return TypeNotImplemented(type.ToString()); }

  Status Visit(const DictionaryType& type) { return TypeNotImplemented(type.ToString()); }

  Status Visit(const NestedType& type) { return TypeNotImplemented(type.ToString()); }

  Status Convert() {
    if (PyArray_NDIM(arr_) != 1) {
      return Status::Invalid("only handle 1-dimensional arrays");
    }

    if (type_ == nullptr) {
      return Status::Invalid("Must pass data type");
    }

    // Visit the type to perform conversion
    return VisitTypeInline(*type_, this);
  }

  const std::vector<std::shared_ptr<Array>>& result() const { return out_arrays_; }

  // ----------------------------------------------------------------------
  // Conversion logic for various object dtype arrays

  template <int ITEM_TYPE, typename ArrowType>
  Status ConvertTypedLists(const std::shared_ptr<DataType>& type, ListBuilder* builder,
                           PyObject* list);

  template <typename ArrowType>
  Status ConvertDates();

  Status ConvertBooleans();
  Status ConvertObjectStrings();
  Status ConvertObjectFloats();
  Status ConvertObjectFixedWidthBytes(const std::shared_ptr<DataType>& type);
  Status ConvertObjectIntegers();
  Status ConvertLists(const std::shared_ptr<DataType>& type);
  Status ConvertLists(const std::shared_ptr<DataType>& type, ListBuilder* builder,
                      PyObject* list);
  Status ConvertObjects();
  Status ConvertDecimals();
  Status ConvertTimes();

 protected:
  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  PyArrayObject* arr_;
  PyArrayObject* mask_;
  int64_t length_;

  // Used in visitor pattern
  std::vector<std::shared_ptr<Array>> out_arrays_;

  std::shared_ptr<ResizableBuffer> null_bitmap_;
  uint8_t* null_bitmap_data_;
};

template <typename T, typename T2>
void CopyStrided(T* input_data, int64_t length, int64_t stride, T2* output_data) {
  // Passing input_data as non-const is a concession to PyObject*
  int64_t j = 0;
  for (int64_t i = 0; i < length; ++i) {
    output_data[i] = static_cast<T2>(input_data[j]);
    j += stride;
  }
}

template <>
void CopyStrided<PyObject*, PyObject*>(PyObject** input_data, int64_t length,
                                       int64_t stride, PyObject** output_data) {
  int64_t j = 0;
  for (int64_t i = 0; i < length; ++i) {
    output_data[i] = input_data[j];
    if (output_data[i] != nullptr) {
      Py_INCREF(output_data[i]);
    }
    j += stride;
  }
}

template <typename ArrowType>
inline Status PandasConverter::ConvertData(std::shared_ptr<Buffer>* data) {
  using traits = internal::arrow_traits<ArrowType::type_id>;
  using T = typename traits::T;

  // Handle LONGLONG->INT64 and other fun things
  int type_num_compat = cast_npy_type_compat(PyArray_DESCR(arr_)->type_num);

  if (NumPyTypeSize(traits::npy_type) != NumPyTypeSize(type_num_compat)) {
    std::stringstream ss;
    ss << "NumPy type casts not yet implemented, type sizes differ: ";
    ss << NumPyTypeSize(traits::npy_type) << " compared to "
       << NumPyTypeSize(type_num_compat);
    return Status::NotImplemented(ss.str());
  }

  if (is_strided()) {
    // Strided, must copy into new contiguous memory
    const int64_t stride = PyArray_STRIDES(arr_)[0];
    const int64_t stride_elements = stride / sizeof(T);

    auto new_buffer = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(new_buffer->Resize(sizeof(T) * length_));
    CopyStrided(reinterpret_cast<T*>(PyArray_DATA(arr_)), length_, stride_elements,
                reinterpret_cast<T*>(new_buffer->mutable_data()));
    *data = new_buffer;
  } else {
    // Can zero-copy
    *data = std::make_shared<NumPyBuffer>(reinterpret_cast<PyObject*>(arr_));
  }
  return Status::OK();
}

template <>
inline Status PandasConverter::ConvertData<Date32Type>(std::shared_ptr<Buffer>* data) {
  // Handle LONGLONG->INT64 and other fun things
  int type_num_compat = cast_npy_type_compat(PyArray_DESCR(arr_)->type_num);
  int type_size = NumPyTypeSize(type_num_compat);

  if (type_size == 4) {
    // Source and target are INT32, so can refer to the main implementation.
    return ConvertData<Int32Type>(data);
  } else if (type_size == 8) {
    // We need to scale down from int64 to int32
    auto new_buffer = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(new_buffer->Resize(sizeof(int32_t) * length_));

    auto input = reinterpret_cast<const int64_t*>(PyArray_DATA(arr_));
    auto output = reinterpret_cast<int32_t*>(new_buffer->mutable_data());

    if (is_strided()) {
      // Strided, must copy into new contiguous memory
      const int64_t stride = PyArray_STRIDES(arr_)[0];
      const int64_t stride_elements = stride / sizeof(int64_t);
      CopyStrided(input, length_, stride_elements, output);
    } else {
      // TODO(wesm): int32 overflow checks
      for (int64_t i = 0; i < length_; ++i) {
        *output++ = static_cast<int32_t>(*input++);
      }
    }
    *data = new_buffer;
  } else {
    std::stringstream ss;
    ss << "Cannot convert NumPy array of element size ";
    ss << type_size << " to a Date32 array";
    return Status::NotImplemented(ss.str());
  }

  return Status::OK();
}

template <>
inline Status PandasConverter::ConvertData<BooleanType>(std::shared_ptr<Buffer>* data) {
  int64_t nbytes = BitUtil::BytesForBits(length_);
  auto buffer = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(buffer->Resize(nbytes));

  Ndarray1DIndexer<uint8_t> values(arr_);

  uint8_t* bitmap = buffer->mutable_data();

  memset(bitmap, 0, nbytes);
  for (int i = 0; i < length_; ++i) {
    if (values[i] > 0) {
      BitUtil::SetBit(bitmap, i);
    }
  }

  *data = buffer;
  return Status::OK();
}

template <typename T>
struct UnboxDate {};

template <>
struct UnboxDate<Date32Type> {
  static int32_t Unbox(PyObject* obj) {
    return PyDate_to_days(reinterpret_cast<PyDateTime_Date*>(obj));
  }
};

template <>
struct UnboxDate<Date64Type> {
  static int64_t Unbox(PyObject* obj) {
    return PyDate_to_ms(reinterpret_cast<PyDateTime_Date*>(obj));
  }
};

template <typename ArrowType>
Status PandasConverter::ConvertDates() {
  PyAcquireGIL lock;

  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;

  Ndarray1DIndexer<PyObject*> objects(arr_);

  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  BuilderType builder(pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  /// We have to run this in this compilation unit, since we cannot use the
  /// datetime API otherwise
  PyDateTime_IMPORT;

  PyObject* obj;
  for (int64_t i = 0; i < length_; ++i) {
    obj = objects[i];
    if (PyDate_CheckExact(obj)) {
      RETURN_NOT_OK(builder.Append(UnboxDate<ArrowType>::Unbox(obj)));
    } else if (PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Date: ";
      RETURN_NOT_OK(InvalidConversion(obj, "datetime.date", &ss));
      return Status::Invalid(ss.str());
    }
  }

  return PushBuilderResult(&builder);
}

Status PandasConverter::ConvertDecimals() {
  PyAcquireGIL lock;

  // Import the decimal module and Decimal class
  OwnedRef decimal;
  OwnedRef Decimal;
  RETURN_NOT_OK(ImportModule("decimal", &decimal));
  RETURN_NOT_OK(ImportFromModule(decimal, "Decimal", &Decimal));

  Ndarray1DIndexer<PyObject*> objects(arr_);
  PyObject* object = objects[0];

  int precision;
  int scale;

  RETURN_NOT_OK(InferDecimalPrecisionAndScale(object, &precision, &scale));

  type_ = std::make_shared<DecimalType>(precision, scale);

  DecimalBuilder builder(type_, pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  for (int64_t i = 0; i < length_; ++i) {
    object = objects[i];
    if (PyObject_IsInstance(object, Decimal.obj())) {
      std::string string;
      RETURN_NOT_OK(PythonDecimalToString(object, &string));

      Int128 value;
      RETURN_NOT_OK(DecimalUtil::FromString(string, &value));
      RETURN_NOT_OK(builder.Append(value));
    } else if (PandasObjectIsNull(object)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Decimal: ";
      RETURN_NOT_OK(InvalidConversion(object, "decimal.Decimal", &ss));
      return Status::Invalid(ss.str());
    }
  }
  return PushBuilderResult(&builder);
}

Status PandasConverter::ConvertTimes() {
  // Convert array of datetime.time objects to Arrow
  PyAcquireGIL lock;
  PyDateTime_IMPORT;

  Ndarray1DIndexer<PyObject*> objects(arr_);

  // datetime.time stores microsecond resolution
  Time64Builder builder(::arrow::time64(TimeUnit::MICRO), pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  PyObject* obj;
  for (int64_t i = 0; i < length_; ++i) {
    obj = objects[i];
    if (PyTime_Check(obj)) {
      RETURN_NOT_OK(builder.Append(PyTime_to_us(obj)));
    } else if (PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Time: ";
      RETURN_NOT_OK(InvalidConversion(obj, "datetime.time", &ss));
      return Status::Invalid(ss.str());
    }
  }
  return PushBuilderResult(&builder);
}

Status PandasConverter::ConvertObjectStrings() {
  PyAcquireGIL lock;

  // The output type at this point is inconclusive because there may be bytes
  // and unicode mixed in the object array
  StringBuilder builder(pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  bool global_have_bytes = false;
  int64_t offset = 0;
  while (offset < length_) {
    bool chunk_have_bytes = false;
    RETURN_NOT_OK(
        AppendObjectStrings(arr_, mask_, offset, &builder, &offset, &chunk_have_bytes));

    global_have_bytes = global_have_bytes | chunk_have_bytes;
    std::shared_ptr<Array> chunk;
    RETURN_NOT_OK(builder.Finish(&chunk));
    out_arrays_.emplace_back(std::move(chunk));
  }

  // If we saw PyBytes, convert everything to BinaryArray
  if (global_have_bytes) {
    for (size_t i = 0; i < out_arrays_.size(); ++i) {
      auto binary_data = out_arrays_[i]->data()->ShallowCopy();
      binary_data->type = ::arrow::binary();
      out_arrays_[i] = std::make_shared<BinaryArray>(binary_data);
    }
  }
  return Status::OK();
}

Status PandasConverter::ConvertObjectFloats() {
  PyAcquireGIL lock;

  Ndarray1DIndexer<PyObject*> objects(arr_);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask_ != nullptr) {
    mask_values.Init(mask_);
    have_mask = true;
  }

  DoubleBuilder builder(pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  PyObject* obj;
  for (int64_t i = 0; i < objects.size(); ++i) {
    obj = objects[i];
    if ((have_mask && mask_values[i]) || PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else if (PyFloat_Check(obj)) {
      double val = PyFloat_AsDouble(obj);
      RETURN_IF_PYERROR();
      RETURN_NOT_OK(builder.Append(val));
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Double: ";
      RETURN_NOT_OK(InvalidConversion(obj, "float", &ss));
      return Status::Invalid(ss.str());
    }
  }

  return PushBuilderResult(&builder);
}

Status PandasConverter::ConvertObjectIntegers() {
  PyAcquireGIL lock;

  Int64Builder builder(pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  Ndarray1DIndexer<PyObject*> objects(arr_);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask_ != nullptr) {
    mask_values.Init(mask_);
    have_mask = true;
  }

  PyObject* obj;
  for (int64_t i = 0; i < objects.size(); ++i) {
    obj = objects[i];
    if ((have_mask && mask_values[i]) || PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder.AppendNull());
    } else if (PyObject_is_integer(obj)) {
      const int64_t val = static_cast<int64_t>(PyLong_AsLong(obj));
      RETURN_IF_PYERROR();
      RETURN_NOT_OK(builder.Append(val));
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Int64: ";
      RETURN_NOT_OK(InvalidConversion(obj, "integer", &ss));
      return Status::Invalid(ss.str());
    }
  }

  return PushBuilderResult(&builder);
}

Status PandasConverter::ConvertObjectFixedWidthBytes(
    const std::shared_ptr<DataType>& type) {
  PyAcquireGIL lock;

  int32_t byte_width = static_cast<const FixedSizeBinaryType&>(*type).byte_width();

  // The output type at this point is inconclusive because there may be bytes
  // and unicode mixed in the object array
  FixedSizeBinaryBuilder builder(type, pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  int64_t offset = 0;
  while (offset < length_) {
    RETURN_NOT_OK(
        AppendObjectFixedWidthBytes(arr_, mask_, byte_width, offset, &builder, &offset));

    std::shared_ptr<Array> chunk;
    RETURN_NOT_OK(builder.Finish(&chunk));
    out_arrays_.emplace_back(std::move(chunk));
  }
  return Status::OK();
}

Status PandasConverter::ConvertBooleans() {
  PyAcquireGIL lock;

  Ndarray1DIndexer<PyObject*> objects(arr_);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask_ != nullptr) {
    mask_values.Init(mask_);
    have_mask = true;
  }

  int64_t nbytes = BitUtil::BytesForBits(length_);
  auto data = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(data->Resize(nbytes));
  uint8_t* bitmap = data->mutable_data();
  memset(bitmap, 0, nbytes);

  int64_t null_count = 0;
  PyObject* obj;
  for (int64_t i = 0; i < length_; ++i) {
    obj = objects[i];
    if ((have_mask && mask_values[i]) || PandasObjectIsNull(obj)) {
      ++null_count;
    } else if (obj == Py_True) {
      BitUtil::SetBit(bitmap, i);
      BitUtil::SetBit(null_bitmap_data_, i);
    } else if (obj == Py_False) {
      BitUtil::SetBit(null_bitmap_data_, i);
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Boolean: ";
      RETURN_NOT_OK(InvalidConversion(obj, "bool", &ss));
      return Status::Invalid(ss.str());
    }
  }

  out_arrays_.push_back(
      std::make_shared<BooleanArray>(length_, data, null_bitmap_, null_count));
  return Status::OK();
}

Status PandasConverter::ConvertObjects() {
  // Python object arrays are annoying, since we could have one of:
  //
  // * Strings
  // * Booleans with nulls
  // * decimal.Decimals
  // * Mixed type (not supported at the moment by arrow format)
  //
  // Additionally, nulls may be encoded either as np.nan or None. So we have to
  // do some type inference and conversion

  RETURN_NOT_OK(InitNullBitmap());

  Ndarray1DIndexer<PyObject*> objects;

  PyAcquireGIL lock;
  objects.Init(arr_);
  PyDateTime_IMPORT;
  lock.release();

  // This means we received an explicit type from the user
  if (type_) {
    switch (type_->id()) {
      case Type::STRING:
        return ConvertObjectStrings();
      case Type::FIXED_SIZE_BINARY:
        return ConvertObjectFixedWidthBytes(type_);
      case Type::BOOL:
        return ConvertBooleans();
      case Type::DATE32:
        return ConvertDates<Date32Type>();
      case Type::DATE64:
        return ConvertDates<Date64Type>();
      case Type::LIST: {
        const auto& list_field = static_cast<const ListType&>(*type_);
        return ConvertLists(list_field.value_field()->type());
      }
      case Type::DECIMAL:
        return ConvertDecimals();
      default:
        return Status::TypeError("No known conversion to Arrow type");
    }
  } else {
    // Re-acquire GIL
    lock.acquire();

    OwnedRef decimal;
    OwnedRef Decimal;
    RETURN_NOT_OK(ImportModule("decimal", &decimal));
    RETURN_NOT_OK(ImportFromModule(decimal, "Decimal", &Decimal));

    for (int64_t i = 0; i < length_; ++i) {
      PyObject* obj = objects[i];
      if (PandasObjectIsNull(obj)) {
        continue;
      } else if (PyObject_is_string(obj)) {
        return ConvertObjectStrings();
      } else if (PyObject_is_float(obj)) {
        return ConvertObjectFloats();
      } else if (PyBool_Check(obj)) {
        return ConvertBooleans();
      } else if (PyObject_is_integer(obj)) {
        return ConvertObjectIntegers();
      } else if (PyDate_CheckExact(obj)) {
        // We could choose Date32 or Date64
        return ConvertDates<Date32Type>();
      } else if (PyTime_Check(obj)) {
        return ConvertTimes();
      } else if (PyObject_IsInstance(const_cast<PyObject*>(obj), Decimal.obj())) {
        return ConvertDecimals();
      } else if (PyList_Check(obj) || PyArray_Check(obj)) {
        std::shared_ptr<DataType> inferred_type;
        RETURN_NOT_OK(InferArrowType(obj, &inferred_type));
        return ConvertLists(inferred_type);
      } else {
        const std::string supported_types =
            "string, bool, float, int, date, time, decimal, list, array";
        std::stringstream ss;
        ss << "Error inferring Arrow type for Python object array. ";
        RETURN_NOT_OK(InvalidConversion(obj, supported_types, &ss));
        return Status::Invalid(ss.str());
      }
    }
  }

  out_arrays_.push_back(std::make_shared<NullArray>(length_));
  return Status::OK();
}

template <typename T>
Status LoopPySequence(PyObject* sequence, T func) {
  if (PySequence_Check(sequence)) {
    OwnedRef ref;
    Py_ssize_t size = PySequence_Size(sequence);
    if (PyArray_Check(sequence)) {
      auto array = reinterpret_cast<PyArrayObject*>(sequence);
      Ndarray1DIndexer<PyObject*> objects(array);
      for (int64_t i = 0; i < size; ++i) {
        RETURN_NOT_OK(func(objects[i]));
      }
    } else {
      for (int64_t i = 0; i < size; ++i) {
        ref.reset(PySequence_GetItem(sequence, i));
        RETURN_NOT_OK(func(ref.obj()));
      }
    }
  } else if (PyObject_HasAttrString(sequence, "__iter__")) {
    OwnedRef iter = OwnedRef(PyObject_GetIter(sequence));
    PyObject* item;
    while ((item = PyIter_Next(iter.obj()))) {
      OwnedRef ref = OwnedRef(item);
      RETURN_NOT_OK(func(ref.obj()));
    }
  } else {
    return Status::TypeError("Object is not a sequence or iterable");
  }

  return Status::OK();
}

template <int ITEM_TYPE, typename ArrowType>
inline Status PandasConverter::ConvertTypedLists(const std::shared_ptr<DataType>& type,
                                                 ListBuilder* builder, PyObject* list) {
  typedef internal::npy_traits<ITEM_TYPE> traits;
  typedef typename traits::BuilderClass BuilderT;

  PyAcquireGIL lock;

  // TODO: mask not supported here
  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  BuilderT* value_builder = static_cast<BuilderT*>(builder->value_builder());

  auto foreach_item = [&](PyObject* object) {
    if (PandasObjectIsNull(object)) {
      return builder->AppendNull();
    } else if (PyArray_Check(object)) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(object);
      RETURN_NOT_OK(builder->Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, ITEM_TYPE));

      return AppendNdarrayToBuilder<ITEM_TYPE, BuilderT>(numpy_array, value_builder);
    } else if (PyList_Check(object)) {
      int64_t size;
      std::shared_ptr<DataType> inferred_type;
      RETURN_NOT_OK(builder->Append(true));
      RETURN_NOT_OK(InferArrowTypeAndSize(object, &size, &inferred_type));
      if (inferred_type->id() != Type::NA && inferred_type->id() != type->id()) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to " << type->ToString();
        return Status::TypeError(ss.str());
      }
      return AppendPySequence(object, size, type, value_builder);
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  };

  return LoopPySequence(list, foreach_item);
}

template <>
inline Status PandasConverter::ConvertTypedLists<NPY_OBJECT, NullType>(
    const std::shared_ptr<DataType>& type, ListBuilder* builder, PyObject* list) {
  PyAcquireGIL lock;

  // TODO: mask not supported here
  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  auto value_builder = static_cast<NullBuilder*>(builder->value_builder());

  auto foreach_item = [&](PyObject* object) {
    if (PandasObjectIsNull(object)) {
      return builder->AppendNull();
    } else if (PyArray_Check(object)) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(object);
      RETURN_NOT_OK(builder->Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, NPY_OBJECT));

      for (int64_t i = 0; i < static_cast<int64_t>(PyArray_SIZE(numpy_array)); ++i) {
        RETURN_NOT_OK(value_builder->AppendNull());
      }
      return Status::OK();
    } else if (PyList_Check(object)) {
      RETURN_NOT_OK(builder->Append(true));
      const Py_ssize_t size = PySequence_Size(object);
      for (Py_ssize_t i = 0; i < size; ++i) {
        RETURN_NOT_OK(value_builder->AppendNull());
      }
      return Status::OK();
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  };

  return LoopPySequence(list, foreach_item);
}

template <>
inline Status PandasConverter::ConvertTypedLists<NPY_OBJECT, StringType>(
    const std::shared_ptr<DataType>& type, ListBuilder* builder, PyObject* list) {
  PyAcquireGIL lock;
  // TODO: If there are bytes involed, convert to Binary representation
  bool have_bytes = false;

  // TODO: mask not supported here
  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  auto value_builder = static_cast<StringBuilder*>(builder->value_builder());

  auto foreach_item = [&](PyObject* object) {
    if (PandasObjectIsNull(object)) {
      return builder->AppendNull();
    } else if (PyArray_Check(object)) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(object);
      RETURN_NOT_OK(builder->Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, NPY_OBJECT));

      int64_t offset = 0;
      RETURN_NOT_OK(AppendObjectStrings(numpy_array, nullptr, 0, value_builder, &offset,
                                        &have_bytes));
      if (offset < PyArray_SIZE(numpy_array)) {
        return Status::Invalid("Array cell value exceeded 2GB");
      }
      return Status::OK();
    } else if (PyList_Check(object)) {
      int64_t size;
      std::shared_ptr<DataType> inferred_type;
      RETURN_NOT_OK(builder->Append(true));
      RETURN_NOT_OK(InferArrowTypeAndSize(object, &size, &inferred_type));
      if (inferred_type->id() != Type::NA && inferred_type->id() != Type::STRING) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to STRING.";
        return Status::TypeError(ss.str());
      }
      return AppendPySequence(object, size, inferred_type, value_builder);
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  };

  return LoopPySequence(list, foreach_item);
}

#define LIST_CASE(TYPE, NUMPY_TYPE, ArrowType)                            \
  case Type::TYPE: {                                                      \
    return ConvertTypedLists<NUMPY_TYPE, ArrowType>(type, builder, list); \
  }

Status PandasConverter::ConvertLists(const std::shared_ptr<DataType>& type,
                                     ListBuilder* builder, PyObject* list) {
  switch (type->id()) {
    LIST_CASE(NA, NPY_OBJECT, NullType)
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
    case Type::LIST: {
      const ListType& list_type = static_cast<const ListType&>(*type);
      auto value_builder = static_cast<ListBuilder*>(builder->value_builder());

      auto foreach_item = [&](PyObject* object) {
        if (PandasObjectIsNull(object)) {
          return builder->AppendNull();
        } else {
          RETURN_NOT_OK(builder->Append(true));
          return ConvertLists(list_type.value_type(), value_builder, object);
        }
      };

      return LoopPySequence(list, foreach_item);
    }
    default: {
      std::stringstream ss;
      ss << "Unknown list item type: ";
      ss << type->ToString();
      return Status::TypeError(ss.str());
    }
  }
}

Status PandasConverter::ConvertLists(const std::shared_ptr<DataType>& type) {
  std::unique_ptr<ArrayBuilder> array_builder;
  RETURN_NOT_OK(MakeBuilder(pool_, arrow::list(type), &array_builder));
  ListBuilder* list_builder = static_cast<ListBuilder*>(array_builder.get());
  RETURN_NOT_OK(ConvertLists(type, list_builder, reinterpret_cast<PyObject*>(arr_)));
  return PushBuilderResult(list_builder);
}

Status PandasToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
                     const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  PandasConverter converter(pool, ao, mo, type);
  RETURN_NOT_OK(converter.Convert());
  *out = converter.result()[0];
  return Status::OK();
}

Status PandasObjectsToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
                            const std::shared_ptr<DataType>& type,
                            std::shared_ptr<ChunkedArray>* out) {
  PandasConverter converter(pool, ao, mo, type);
  RETURN_NOT_OK(converter.ConvertObjects());
  *out = std::make_shared<ChunkedArray>(converter.result());
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
