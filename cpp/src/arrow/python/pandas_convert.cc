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
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
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
namespace py {

// ----------------------------------------------------------------------
// Utility code

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

static inline bool PyObject_is_float(const PyObject* obj) {
  return PyFloat_Check(obj);
}

static inline bool PyObject_is_integer(const PyObject* obj) {
  return (!PyBool_Check(obj)) && PyArray_IsIntegerScalar(obj);
}

template <int TYPE>
static int64_t ValuesToBitmap(PyArrayObject* arr, uint8_t* bitmap) {
  typedef npy_traits<TYPE> traits;
  typedef typename traits::value_type T;

  int64_t null_count = 0;

  Ndarray1DIndexer<T> values(arr);

  // TODO(wesm): striding
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

static Status AppendObjectStrings(
    PyArrayObject* arr, PyArrayObject* mask, StringBuilder* builder, bool* have_bytes) {
  PyObject* obj;

  Ndarray1DIndexer<PyObject*> objects(arr);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask != nullptr) {
    mask_values.Init(mask);
    have_mask = true;
  }

  for (int64_t i = 0; i < objects.size(); ++i) {
    obj = objects[i];
    if ((have_mask && mask_values[i]) || PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder->AppendNull());
    } else if (PyUnicode_Check(obj)) {
      obj = PyUnicode_AsUTF8String(obj);
      if (obj == NULL) {
        PyErr_Clear();
        return Status::Invalid("failed converting unicode to UTF8");
      }
      const int32_t length = static_cast<int32_t>(PyBytes_GET_SIZE(obj));
      Status s = builder->Append(PyBytes_AS_STRING(obj), length);
      Py_DECREF(obj);
      if (!s.ok()) { return s; }
    } else if (PyBytes_Check(obj)) {
      *have_bytes = true;
      const int32_t length = static_cast<int32_t>(PyBytes_GET_SIZE(obj));
      RETURN_NOT_OK(builder->Append(PyBytes_AS_STRING(obj), length));
    } else {
      return InvalidConversion(obj, "string or bytes");
    }
  }

  return Status::OK();
}

static Status AppendObjectFixedWidthBytes(PyArrayObject* arr, PyArrayObject* mask,
    int byte_width, FixedSizeBinaryBuilder* builder) {
  PyObject* obj;

  Ndarray1DIndexer<PyObject*> objects(arr);
  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask != nullptr) {
    mask_values.Init(mask);
    have_mask = true;
  }

  for (int64_t i = 0; i < objects.size(); ++i) {
    obj = objects[i];
    if ((have_mask && mask_values[i]) || PandasObjectIsNull(obj)) {
      RETURN_NOT_OK(builder->AppendNull());
    } else if (PyUnicode_Check(obj)) {
      obj = PyUnicode_AsUTF8String(obj);
      if (obj == NULL) {
        PyErr_Clear();
        return Status::Invalid("failed converting unicode to UTF8");
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

class PandasConverter {
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
      null_count = ValuesToBitmap<traits::npy_type>(arr_, null_bitmap_data_);
    }

    std::vector<FieldMetadata> fields(1);
    fields[0].length = length_;
    fields[0].null_count = null_count;
    fields[0].offset = 0;

    return LoadArray(type_, fields, {null_bitmap_, data}, &out_);
  }

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value ||
                              std::is_same<BooleanType, T>::value,
      Status>::type
  Visit(const T& type) {
    return VisitNative<T>();
  }

  Status Visit(const Date32Type& type) { return VisitNative<Int32Type>(); }
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

    if (type_ == nullptr) { return Status::Invalid("Must pass data type"); }

    // Visit the type to perform conversion
    return VisitTypeInline(*type_, this);
  }

  std::shared_ptr<Array> result() const { return out_; }

  // ----------------------------------------------------------------------
  // Conversion logic for various object dtype arrays

  template <int ITEM_TYPE, typename ArrowType>
  Status ConvertTypedLists(const std::shared_ptr<DataType>& type);

  template <typename ArrowType>
  Status ConvertDates();

  Status ConvertBooleans();
  Status ConvertObjectStrings();
  Status ConvertObjectFloats();
  Status ConvertObjectFixedWidthBytes(const std::shared_ptr<DataType>& type);
  Status ConvertObjectIntegers();
  Status ConvertLists(const std::shared_ptr<DataType>& type);
  Status ConvertObjects();
  Status ConvertDecimals();

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

template <typename T>
void CopyStrided(T* input_data, int64_t length, int64_t stride, T* output_data) {
  // Passing input_data as non-const is a concession to PyObject*
  int64_t j = 0;
  for (int64_t i = 0; i < length; ++i) {
    output_data[i] = input_data[j];
    j += stride;
  }
}

template <>
void CopyStrided<PyObject*>(
    PyObject** input_data, int64_t length, int64_t stride, PyObject** output_data) {
  int64_t j = 0;
  for (int64_t i = 0; i < length; ++i) {
    output_data[i] = input_data[j];
    if (output_data[i] != nullptr) { Py_INCREF(output_data[i]); }
    j += stride;
  }
}

template <typename ArrowType>
inline Status PandasConverter::ConvertData(std::shared_ptr<Buffer>* data) {
  using traits = arrow_traits<ArrowType::type_id>;
  using T = typename traits::T;

  // Handle LONGLONG->INT64 and other fun things
  int type_num_compat = cast_npy_type_compat(PyArray_DESCR(arr_)->type_num);

  if (numpy_type_size(traits::npy_type) != numpy_type_size(type_num_compat)) {
    return Status::NotImplemented("NumPy type casts not yet implemented");
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
inline Status PandasConverter::ConvertData<BooleanType>(std::shared_ptr<Buffer>* data) {
  int64_t nbytes = BitUtil::BytesForBits(length_);
  auto buffer = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(buffer->Resize(nbytes));

  Ndarray1DIndexer<uint8_t> values(arr_);

  uint8_t* bitmap = buffer->mutable_data();

  memset(bitmap, 0, nbytes);
  for (int i = 0; i < length_; ++i) {
    if (values[i] > 0) { BitUtil::SetBit(bitmap, i); }
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

  BuilderType date_builder(pool_);
  RETURN_NOT_OK(date_builder.Resize(length_));

  /// We have to run this in this compilation unit, since we cannot use the
  /// datetime API otherwise
  PyDateTime_IMPORT;

  Status s;
  PyObject* obj;
  for (int64_t i = 0; i < length_; ++i) {
    obj = objects[i];
    if (PyDate_CheckExact(obj)) {
      date_builder.Append(UnboxDate<ArrowType>::Unbox(obj));
    } else if (PandasObjectIsNull(obj)) {
      date_builder.AppendNull();
    } else {
      return InvalidConversion(obj, "date");
    }
  }
  return date_builder.Finish(&out_);
}

#define CONVERT_DECIMAL_CASE(bit_width, builder, object)         \
  case bit_width: {                                              \
    decimal::Decimal##bit_width d;                               \
    std::string string_out;                                      \
    RETURN_NOT_OK(PythonDecimalToString((object), &string_out)); \
    RETURN_NOT_OK(FromString(string_out, &d));                   \
    RETURN_NOT_OK((builder).Append(d));                          \
    break;                                                       \
  }

Status PandasConverter::ConvertDecimals() {
  PyAcquireGIL lock;

  // Import the decimal module and Decimal class
  OwnedRef decimal;
  OwnedRef Decimal;
  RETURN_NOT_OK(ImportModule("decimal", &decimal));
  RETURN_NOT_OK(ImportFromModule(decimal, "Decimal", &Decimal));

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  PyObject* object = objects[0];

  int precision;
  int scale;

  RETURN_NOT_OK(InferDecimalPrecisionAndScale(object, &precision, &scale));

  type_ = std::make_shared<DecimalType>(precision, scale);

  const int bit_width = std::dynamic_pointer_cast<DecimalType>(type_)->bit_width();
  DecimalBuilder decimal_builder(pool_, type_);

  RETURN_NOT_OK(decimal_builder.Resize(length_));

  for (int64_t i = 0; i < length_; ++i) {
    object = objects[i];
    if (PyObject_IsInstance(object, Decimal.obj())) {
      switch (bit_width) {
        CONVERT_DECIMAL_CASE(32, decimal_builder, object)
        CONVERT_DECIMAL_CASE(64, decimal_builder, object)
        CONVERT_DECIMAL_CASE(128, decimal_builder, object)
        default:
          break;
      }
    } else if (PandasObjectIsNull(object)) {
      decimal_builder.AppendNull();
    } else {
      return InvalidConversion(object, "decimal.Decimal");
    }
  }
  return decimal_builder.Finish(&out_);
}

#undef CONVERT_DECIMAL_CASE

Status PandasConverter::ConvertObjectStrings() {
  PyAcquireGIL lock;

  // The output type at this point is inconclusive because there may be bytes
  // and unicode mixed in the object array

  StringBuilder builder(pool_);
  RETURN_NOT_OK(builder.Resize(length_));

  Status s;
  bool have_bytes = false;
  RETURN_NOT_OK(AppendObjectStrings(arr_, mask_, &builder, &have_bytes));
  RETURN_NOT_OK(builder.Finish(&out_));

  if (have_bytes) {
    const auto& arr = static_cast<const StringArray&>(*out_);
    out_ = std::make_shared<BinaryArray>(arr.length(), arr.value_offsets(), arr.data(),
        arr.null_bitmap(), arr.null_count());
  }
  return Status::OK();
}

Status PandasConverter::ConvertObjectFloats() {
  PyAcquireGIL lock;

  DoubleBuilder builder(pool_);
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
    } else if (PyFloat_Check(obj)) {
      double val = PyFloat_AsDouble(obj);
      RETURN_IF_PYERROR();
      RETURN_NOT_OK(builder.Append(val));
    } else {
      return InvalidConversion(obj, "float");
    }
  }

  return builder.Finish(&out_);
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
      return InvalidConversion(obj, "integer");
    }
  }

  return builder.Finish(&out_);
}

Status PandasConverter::ConvertObjectFixedWidthBytes(
    const std::shared_ptr<DataType>& type) {
  PyAcquireGIL lock;

  Ndarray1DIndexer<PyObject*> objects(arr_);

  int32_t value_size = static_cast<const FixedSizeBinaryType&>(*type).byte_width();

  FixedSizeBinaryBuilder builder(pool_, type);
  RETURN_NOT_OK(builder.Resize(length_));
  RETURN_NOT_OK(AppendObjectFixedWidthBytes(arr_, mask_, value_size, &builder));
  RETURN_NOT_OK(builder.Finish(&out_));
  return Status::OK();
}

template <typename T>
Status validate_precision(int precision) {
  constexpr static const int maximum_precision = decimal::DecimalPrecision<T>::maximum;
  if (!(precision > 0 && precision <= maximum_precision)) {
    std::stringstream ss;
    ss << "Invalid precision: " << precision << ". Minimum is 1, maximum is "
       << maximum_precision;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

template <typename T>
Status RawDecimalToString(
    const uint8_t* bytes, int precision, int scale, std::string* result) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(result, nullptr);
  RETURN_NOT_OK(validate_precision<T>(precision));
  decimal::Decimal<T> decimal;
  FromBytes(bytes, &decimal);
  *result = ToString(decimal, precision, scale);
  return Status::OK();
}

template Status RawDecimalToString<int32_t>(
    const uint8_t*, int, int, std::string* result);
template Status RawDecimalToString<int64_t>(
    const uint8_t*, int, int, std::string* result);

Status RawDecimalToString(const uint8_t* bytes, int precision, int scale,
    bool is_negative, std::string* result) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(result, nullptr);
  RETURN_NOT_OK(validate_precision<boost::multiprecision::int128_t>(precision));
  decimal::Decimal128 decimal;
  FromBytes(bytes, is_negative, &decimal);
  *result = ToString(decimal, precision, scale);
  return Status::OK();
}

static Status ConvertDecimals(const ChunkedArray& data, PyObject** out_values) {
  PyAcquireGIL lock;
  OwnedRef decimal_ref;
  OwnedRef Decimal_ref;
  RETURN_NOT_OK(ImportModule("decimal", &decimal_ref));
  RETURN_NOT_OK(ImportFromModule(decimal_ref, "Decimal", &Decimal_ref));
  PyObject* Decimal = Decimal_ref.obj();

  for (int c = 0; c < data.num_chunks(); c++) {
    auto* arr(static_cast<arrow::DecimalArray*>(data.chunk(c).get()));
    auto type(std::dynamic_pointer_cast<arrow::DecimalType>(arr->type()));
    const int precision = type->precision();
    const int scale = type->scale();
    const int bit_width = type->bit_width();

    for (int64_t i = 0; i < arr->length(); ++i) {
      if (arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values++ = Py_None;
      } else {
        const uint8_t* raw_value = arr->GetValue(i);
        std::string s;
        switch (bit_width) {
          case 32:
            RETURN_NOT_OK(RawDecimalToString<int32_t>(raw_value, precision, scale, &s));
            break;
          case 64:
            RETURN_NOT_OK(RawDecimalToString<int64_t>(raw_value, precision, scale, &s));
            break;
          case 128:
            RETURN_NOT_OK(
                RawDecimalToString(raw_value, precision, scale, arr->IsNegative(i), &s));
            break;
          default:
            break;
        }
        RETURN_NOT_OK(DecimalFromString(Decimal, s, out_values++));
      }
    }
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
      return InvalidConversion(obj, "bool");
    }
  }

  out_ = std::make_shared<BooleanArray>(length_, data, null_bitmap_, null_count);

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
      if (PandasObjectIsNull(objects[i])) {
        continue;
      } else if (PyObject_is_string(objects[i])) {
        return ConvertObjectStrings();
      } else if (PyObject_is_float(objects[i])) {
        return ConvertObjectFloats();
      } else if (PyBool_Check(objects[i])) {
        return ConvertBooleans();
      } else if (PyObject_is_integer(objects[i])) {
        return ConvertObjectIntegers();
      } else if (PyDate_CheckExact(objects[i])) {
        // We could choose Date32 or Date64
        return ConvertDates<Date32Type>();
      } else if (PyObject_IsInstance(const_cast<PyObject*>(objects[i]), Decimal.obj())) {
        return ConvertDecimals();
      } else {
        return InvalidConversion(
            const_cast<PyObject*>(objects[i]), "string, bool, float, int, date, decimal");
      }
    }
  }

  out_ = std::make_shared<NullArray>(length_);
  return Status::OK();
}

template <int ITEM_TYPE, typename ArrowType>
inline Status PandasConverter::ConvertTypedLists(const std::shared_ptr<DataType>& type) {
  typedef npy_traits<ITEM_TYPE> traits;
  typedef typename traits::value_type T;
  typedef typename traits::BuilderClass BuilderT;

  PyAcquireGIL lock;

  // TODO: mask not supported here
  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  if (is_strided()) {
    return Status::NotImplemented("strided arrays not implemented for lists");
  }

  ListBuilder list_builder(
      pool_, std::unique_ptr<ArrayBuilder>(new BuilderT(pool_, type)));
  BuilderT* value_builder = static_cast<BuilderT*>(list_builder.value_builder());

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  for (int64_t i = 0; i < length_; ++i) {
    if (PandasObjectIsNull(objects[i])) {
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
      if (inferred_type->id() != type->id()) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to " << type->ToString();
        return Status::TypeError(ss.str());
      }
      RETURN_NOT_OK(AppendPySequence(objects[i], size, type, value_builder));
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  }
  return list_builder.Finish(&out_);
}

template <>
inline Status PandasConverter::ConvertTypedLists<NPY_OBJECT, StringType>(
    const std::shared_ptr<DataType>& type) {
  PyAcquireGIL lock;
  // TODO: If there are bytes involed, convert to Binary representation
  bool have_bytes = false;

  // TODO: mask not supported here
  if (mask_ != nullptr) {
    return Status::NotImplemented("mask not supported in object conversions yet");
  }

  if (is_strided()) {
    return Status::NotImplemented("strided arrays not implemented for lists");
  }

  ListBuilder list_builder(
      pool_, std::unique_ptr<ArrayBuilder>(new StringBuilder(pool_)));
  auto value_builder = static_cast<StringBuilder*>(list_builder.value_builder());

  PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
  for (int64_t i = 0; i < length_; ++i) {
    if (PandasObjectIsNull(objects[i])) {
      RETURN_NOT_OK(list_builder.AppendNull());
    } else if (PyArray_Check(objects[i])) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(objects[i]);
      RETURN_NOT_OK(list_builder.Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, NPY_OBJECT));

      RETURN_NOT_OK(
          AppendObjectStrings(numpy_array, nullptr, value_builder, &have_bytes));
    } else if (PyList_Check(objects[i])) {
      int64_t size;
      std::shared_ptr<DataType> inferred_type;
      RETURN_NOT_OK(list_builder.Append(true));
      RETURN_NOT_OK(InferArrowTypeAndSize(objects[i], &size, &inferred_type));
      if (inferred_type->id() != Type::STRING) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to STRING.";
        return Status::TypeError(ss.str());
      }
      RETURN_NOT_OK(AppendPySequence(objects[i], size, inferred_type, value_builder));
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  }
  return list_builder.Finish(&out_);
}

#define LIST_CASE(TYPE, NUMPY_TYPE, ArrowType)             \
  case Type::TYPE: {                                       \
    return ConvertTypedLists<NUMPY_TYPE, ArrowType>(type); \
  }

Status PandasConverter::ConvertLists(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
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
  RETURN_NOT_OK(converter.Convert());
  *out = converter.result();
  return Status::OK();
}

Status PandasObjectsToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo,
    const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) {
  PandasConverter converter(pool, ao, mo, type);
  RETURN_NOT_OK(converter.ConvertObjects());
  *out = converter.result();
  return Status::OK();
}

// ----------------------------------------------------------------------
// pandas 0.x DataFrame conversion internals

inline void set_numpy_metadata(int type, DataType* datatype, PyArray_Descr* out) {
  if (type == NPY_DATETIME) {
    auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(out->c_metadata);
    if (datatype->id() == Type::TIMESTAMP) {
      auto timestamp_type = static_cast<TimestampType*>(datatype);

      switch (timestamp_type->unit()) {
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

static inline PyArray_Descr* GetSafeNumPyDtype(int type) {
  if (type == NPY_DATETIME) {
    // It is not safe to mutate the result of DescrFromType
    return PyArray_DescrNewFromType(type);
  } else {
    return PyArray_DescrFromType(type);
  }
}
static inline PyObject* NewArray1DFromType(
    DataType* arrow_type, int type, int64_t length, void* data) {
  npy_intp dims[1] = {length};

  PyArray_Descr* descr = GetSafeNumPyDtype(type);
  if (descr == nullptr) {
    // Error occurred, trust error state is set
    return nullptr;
  }

  set_numpy_metadata(type, arrow_type, descr);
  return PyArray_NewFromDescr(&PyArray_Type, descr, 1, dims, nullptr, data,
      NPY_ARRAY_OWNDATA | NPY_ARRAY_CARRAY | NPY_ARRAY_WRITEABLE, nullptr);
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
    DECIMAL,
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

    PyArray_Descr* descr = GetSafeNumPyDtype(npy_type);

    PyObject* block_arr;
    if (ndim == 2) {
      npy_intp block_dims[2] = {num_columns_, num_rows_};
      block_arr = PyArray_SimpleNewFromDescr(2, block_dims, descr);
    } else {
      npy_intp block_dims[1] = {num_rows_};
      block_arr = PyArray_SimpleNewFromDescr(1, block_dims, descr);
    }

    if (block_arr == NULL) {
      // TODO(wesm): propagating Python exception
      return Status::OK();
    }

    PyArray_ENABLEFLAGS(reinterpret_cast<PyArrayObject*>(block_arr), NPY_ARRAY_OWNDATA);

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

template <typename Type>
inline Status ConvertBinaryLike(const ChunkedArray& data, PyObject** out_values) {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
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

inline Status ConvertNulls(const ChunkedArray& data, PyObject** out_values) {
  PyAcquireGIL lock;
  for (int c = 0; c < data.num_chunks(); c++) {
    std::shared_ptr<Array> arr = data.chunk(c);

    for (int64_t i = 0; i < arr->length(); ++i) {
      // All values are null
      Py_INCREF(Py_None);
      *out_values = Py_None;
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

inline Status ConvertStruct(const ChunkedArray& data, PyObject** out_values) {
  PyAcquireGIL lock;
  if (data.num_chunks() <= 0) { return Status::OK(); }
  // ChunkedArray has at least one chunk
  auto arr = static_cast<const StructArray*>(data.chunk(0).get());
  // Use it to cache the struct type and number of fields for all chunks
  auto num_fields = arr->fields().size();
  auto array_type = arr->type();
  std::vector<OwnedRef> fields_data(num_fields);
  OwnedRef dict_item;
  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = static_cast<const StructArray*>(data.chunk(c).get());
    // Convert the struct arrays first
    for (size_t i = 0; i < num_fields; i++) {
      PyObject* numpy_array;
      RETURN_NOT_OK(
          ConvertArrayToPandas(arr->field(static_cast<int>(i)), nullptr, &numpy_array));
      fields_data[i].reset(numpy_array);
    }

    // Construct a dictionary for each row
    const bool has_nulls = data.null_count() > 0;
    for (int64_t i = 0; i < arr->length(); ++i) {
      if (has_nulls && arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values = Py_None;
      } else {
        // Build the new dict object for the row
        dict_item.reset(PyDict_New());
        RETURN_IF_PYERROR();
        for (size_t field_idx = 0; field_idx < num_fields; ++field_idx) {
          OwnedRef field_value;
          auto name = array_type->child(static_cast<int>(field_idx))->name();
          if (!arr->field(static_cast<int>(field_idx))->IsNull(i)) {
            // Value exists in child array, obtain it
            auto array = reinterpret_cast<PyArrayObject*>(fields_data[field_idx].obj());
            auto ptr = reinterpret_cast<const char*>(PyArray_GETPTR1(array, i));
            field_value.reset(PyArray_GETITEM(array, ptr));
            RETURN_IF_PYERROR();
          } else {
            // Translate the Null to a None
            Py_INCREF(Py_None);
            field_value.reset(Py_None);
          }
          // PyDict_SetItemString does not steal the value reference
          auto setitem_result =
              PyDict_SetItemString(dict_item.obj(), name.c_str(), field_value.obj());
          RETURN_IF_PYERROR();
          DCHECK_EQ(setitem_result, 0);
        }
        *out_values = dict_item.obj();
        // Grant ownership to the resulting array
        Py_INCREF(*out_values);
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

template <typename InType, int64_t SHIFT>
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
  Status Allocate() override { return AllocateNDArray(NPY_OBJECT); }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
      int64_t rel_placement) override {
    Type::type type = col->type()->id();

    PyObject** out_buffer =
        reinterpret_cast<PyObject**>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col->data().get();

    if (type == Type::BOOL) {
      RETURN_NOT_OK(ConvertBooleanWithNulls(data, out_buffer));
    } else if (type == Type::BINARY) {
      RETURN_NOT_OK(ConvertBinaryLike<BinaryType>(data, out_buffer));
    } else if (type == Type::STRING) {
      RETURN_NOT_OK(ConvertBinaryLike<StringType>(data, out_buffer));
    } else if (type == Type::FIXED_SIZE_BINARY) {
      RETURN_NOT_OK(ConvertFixedSizeBinary(data, out_buffer));
    } else if (type == Type::DECIMAL) {
      RETURN_NOT_OK(ConvertDecimals(data, out_buffer));
    } else if (type == Type::NA) {
      RETURN_NOT_OK(ConvertNulls(data, out_buffer));
    } else if (type == Type::LIST) {
      auto list_type = std::static_pointer_cast<ListType>(col->type());
      switch (list_type->value_type()->id()) {
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
    } else if (type == Type::STRUCT) {
      RETURN_NOT_OK(ConvertStruct(data, out_buffer));
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
    Type::type type = col->type()->id();

    C_TYPE* out_buffer =
        reinterpret_cast<C_TYPE*>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col->data().get();

    if (type != ARROW_TYPE) {
      std::stringstream ss;
      ss << "Cannot write Arrow data of type " << col->type()->ToString();
      ss << " to a Pandas int" << sizeof(C_TYPE) << " block.";
      return Status::NotImplemented(ss.str());
    }

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
    Type::type type = col->type()->id();

    if (type != Type::FLOAT) {
      std::stringstream ss;
      ss << "Cannot write Arrow data of type " << col->type()->ToString();
      ss << " to a Pandas float32 block.";
      return Status::NotImplemented(ss.str());
    }

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
    Type::type type = col->type()->id();

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
        std::stringstream ss;
        ss << "Cannot write Arrow data of type " << col->type()->ToString();
        ss << " to a Pandas float64 block.";
        return Status::NotImplemented(ss.str());
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
    Type::type type = col->type()->id();

    if (type != Type::BOOL) {
      std::stringstream ss;
      ss << "Cannot write Arrow data of type " << col->type()->ToString();
      ss << " to a Pandas boolean block.";
      return Status::NotImplemented(ss.str());
    }

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
    Type::type type = col->type()->id();

    int64_t* out_buffer =
        reinterpret_cast<int64_t*>(block_data_) + rel_placement * num_rows_;

    const ChunkedArray& data = *col.get()->data();

    if (type == Type::DATE32) {
      // Convert from days since epoch to datetime64[ns]
      ConvertDatetimeNanos<int32_t, kNanosecondsInDay>(data, out_buffer);
    } else if (type == Type::DATE64) {
      // Date64Type is millisecond timestamp stored as int64_t
      // TODO(wesm): Do we want to make sure to zero out the milliseconds?
      ConvertDatetimeNanos<int64_t, 1000000L>(data, out_buffer);
    } else if (type == Type::TIMESTAMP) {
      auto ts_type = static_cast<TimestampType*>(col->type().get());

      if (ts_type->unit() == TimeUnit::NANO) {
        ConvertNumericNullable<int64_t>(data, kPandasTimestampNull, out_buffer);
      } else if (ts_type->unit() == TimeUnit::MICRO) {
        ConvertDatetimeNanos<int64_t, 1000L>(data, out_buffer);
      } else if (ts_type->unit() == TimeUnit::MILLI) {
        ConvertDatetimeNanos<int64_t, 1000000L>(data, out_buffer);
      } else if (ts_type->unit() == TimeUnit::SECOND) {
        ConvertDatetimeNanos<int64_t, 1000000000L>(data, out_buffer);
      } else {
        return Status::NotImplemented("Unsupported time unit");
      }
    } else {
      std::stringstream ss;
      ss << "Cannot write Arrow data of type " << col->type()->ToString();
      ss << " to a Pandas datetime block.";
      return Status::NotImplemented(ss.str());
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
    BLOCK_CASE(DECIMAL, ObjectBlock);
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
  switch (dict_type->index_type()->id()) {
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

      Type::type column_type = col->type()->id();
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
        case Type::DATE32:
          output_type = PandasBlock::DATETIME;
          break;
        case Type::DATE64:
          output_type = PandasBlock::DATETIME;
          break;
        case Type::TIMESTAMP: {
          const auto& ts_type = static_cast<const TimestampType&>(*col->type());
          if (ts_type.timezone() != "") {
            output_type = PandasBlock::DATETIME_WITH_TZ;
          } else {
            output_type = PandasBlock::DATETIME;
          }
        } break;
        case Type::LIST: {
          auto list_type = std::static_pointer_cast<ListType>(col->type());
          if (!ListTypeSupported(list_type->value_type()->id())) {
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
        case Type::DECIMAL:
          output_type = PandasBlock::DECIMAL;
          break;
        case Type::NA:
        case Type::STRUCT:
          output_type = PandasBlock::OBJECT;
          break;
        default:
          std::stringstream ss;
          ss << "No known equivalent Pandas block for Arrow data of type ";
          ss << col->type()->ToString() << " is known.";
          return Status::NotImplemented(ss.str());
      }

      int block_placement = 0;
      std::shared_ptr<PandasBlock> block;
      if (output_type == PandasBlock::CATEGORICAL) {
        RETURN_NOT_OK(MakeCategoricalBlock(col->type(), table_->num_rows(), &block));
        categorical_blocks_[i] = block;
      } else if (output_type == PandasBlock::DATETIME_WITH_TZ) {
        const auto& ts_type = static_cast<const TimestampType&>(*col->type());
        block = std::make_shared<DatetimeTZBlock>(ts_type.timezone(), table_->num_rows());
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

      // ARROW-1017; PyList_Append increments object refcount
      Py_DECREF(item);
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

    result_ = NewArray1DFromType(col_->type().get(), type, col_->length(), nullptr);
    arr_ = reinterpret_cast<PyArrayObject*>(result_);
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
    result_ = NewArray1DFromType(col_->type().get(), npy_type, col_->length(), data);
    arr_ = reinterpret_cast<PyArrayObject*>(result_);

    if (arr_ == NULL) {
      // Error occurred, trust that error set
      return Status::OK();
    }

    if (PyArray_SetBaseObject(arr_, py_ref_) == -1) {
      // Error occurred, trust that SetBaseObject set the error state
      return Status::OK();
    } else {
      // PyArray_SetBaseObject steals our reference to py_ref_
      Py_INCREF(py_ref_);
    }

    // Arrow data is immutable.
    PyArray_CLEARFLAGS(arr_, NPY_ARRAY_WRITEABLE);

    // Arrow data is owned by another
    PyArray_CLEARFLAGS(arr_, NPY_ARRAY_OWNDATA);

    return Status::OK();
  }

  // ----------------------------------------------------------------------
  // Allocate new array and deserialize. Can do a zero copy conversion for some
  // types

  template <typename Type>
  typename std::enable_if<std::is_base_of<FloatingPoint, Type>::value, Status>::type
  Visit(const Type& type) {
    constexpr int TYPE = Type::type_id;
    using traits = arrow_traits<TYPE>;

    typedef typename traits::T T;
    int npy_type = traits::npy_type;

    if (data_.num_chunks() == 1 && data_.null_count() == 0 && py_ref_ != nullptr) {
      return ConvertValuesZeroCopy<TYPE>(npy_type, data_.chunk(0));
    }

    RETURN_NOT_OK(AllocateOutput(npy_type));
    auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));
    ConvertNumericNullable<T>(data_, traits::na_value, out_values);

    return Status::OK();
  }

  template <typename Type>
  typename std::enable_if<std::is_base_of<DateType, Type>::value ||
                              std::is_base_of<TimestampType, Type>::value,
      Status>::type
  Visit(const Type& type) {
    constexpr int TYPE = Type::type_id;
    using traits = arrow_traits<TYPE>;

    typedef typename traits::T T;

    RETURN_NOT_OK(AllocateOutput(traits::npy_type));
    auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));

    constexpr T na_value = traits::na_value;
    constexpr int64_t kShift = traits::npy_shift;

    for (int c = 0; c < data_.num_chunks(); c++) {
      const std::shared_ptr<Array> arr = data_.chunk(c);
      auto prim_arr = static_cast<PrimitiveArray*>(arr.get());
      auto in_values = reinterpret_cast<const T*>(prim_arr->data()->data());

      for (int64_t i = 0; i < arr->length(); ++i) {
        *out_values++ = arr->IsNull(i) ? na_value : in_values[i] / kShift;
      }
    }
    return Status::OK();
  }

  template <typename Type>
  typename std::enable_if<std::is_base_of<TimeType, Type>::value, Status>::type Visit(
      const Type& type) {
    return Status::NotImplemented("Don't know how to serialize Arrow time type to NumPy");
  }

  // Integer specialization
  template <typename Type>
  typename std::enable_if<std::is_base_of<Integer, Type>::value, Status>::type Visit(
      const Type& type) {
    constexpr int TYPE = Type::type_id;
    using traits = arrow_traits<TYPE>;

    typedef typename traits::T T;

    if (data_.num_chunks() == 1 && data_.null_count() == 0 && py_ref_ != nullptr) {
      return ConvertValuesZeroCopy<TYPE>(traits::npy_type, data_.chunk(0));
    }

    if (data_.null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_FLOAT64));
      auto out_values = reinterpret_cast<double*>(PyArray_DATA(arr_));
      ConvertIntegerWithNulls<T>(data_, out_values);
    } else {
      RETURN_NOT_OK(AllocateOutput(traits::npy_type));
      auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));
      ConvertIntegerNoNullsSameType<T>(data_, out_values);
    }

    return Status::OK();
  }

  // Boolean specialization
  Status Visit(const BooleanType& type) {
    if (data_.null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
      auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
      RETURN_NOT_OK(ConvertBooleanWithNulls(data_, out_values));
    } else {
      RETURN_NOT_OK(AllocateOutput(arrow_traits<Type::BOOL>::npy_type));
      auto out_values = reinterpret_cast<uint8_t*>(PyArray_DATA(arr_));
      ConvertBooleanNoNulls(data_, out_values);
    }
    return Status::OK();
  }

  // UTF8 strings
  template <typename Type>
  typename std::enable_if<std::is_base_of<BinaryType, Type>::value, Status>::type Visit(
      const Type& type) {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertBinaryLike<Type>(data_, out_values);
  }

  // Fixed length binary strings
  Status Visit(const FixedSizeBinaryType& type) {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertFixedSizeBinary(data_, out_values);
  }

  Status Visit(const DecimalType& type) {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertDecimals(data_, out_values);
  }

  Status Visit(const ListType& type) {
#define CONVERTVALUES_LISTSLIKE_CASE(ArrowType, ArrowEnum) \
  case Type::ArrowEnum:                                    \
    return ConvertListsLike<ArrowType>(col_, out_values);

    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    auto list_type = std::static_pointer_cast<ListType>(col_->type());
    switch (list_type->value_type()->id()) {
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
      CONVERTVALUES_LISTSLIKE_CASE(DecimalType, DECIMAL)
      default: {
        std::stringstream ss;
        ss << "Not implemented type for lists: " << list_type->value_type()->ToString();
        return Status::NotImplemented(ss.str());
      }
    }
#undef CONVERTVALUES_LISTSLIKE_CASE
  }

  Status Visit(const DictionaryType& type) {
    std::shared_ptr<PandasBlock> block;
    RETURN_NOT_OK(MakeCategoricalBlock(col_->type(), col_->length(), &block));
    RETURN_NOT_OK(block->Write(col_, 0, 0));

    auto dict_type = static_cast<const DictionaryType*>(col_->type().get());

    PyAcquireGIL lock;
    result_ = PyDict_New();
    RETURN_IF_PYERROR();

    PyObject* dictionary;

    // Release GIL before calling ConvertArrayToPandas, will be reacquired
    // there if needed
    lock.release();
    RETURN_NOT_OK(ConvertArrayToPandas(dict_type->dictionary(), nullptr, &dictionary));
    lock.acquire();

    PyDict_SetItemString(result_, "indices", block->block_arr());
    PyDict_SetItemString(result_, "dictionary", dictionary);

    return Status::OK();
  }

  Status Visit(const NullType& type) {
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertNulls(data_, out_values);
  }

  Status Visit(const StructType& type) {
    AllocateOutput(NPY_OBJECT);
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return ConvertStruct(data_, out_values);
  }

  Status Visit(const UnionType& type) { return Status::NotImplemented("union type"); }

  Status Convert(PyObject** out) {
    RETURN_NOT_OK(VisitTypeInline(*col_->type(), this));
    *out = result_;
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
