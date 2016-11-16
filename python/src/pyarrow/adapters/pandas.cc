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

#include "pyarrow/numpy_interop.h"

#include "pyarrow/adapters/pandas.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/api.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/status.h"

#include "pyarrow/common.h"
#include "pyarrow/config.h"

namespace pyarrow {

using arrow::Array;
using arrow::Column;
using arrow::Field;
using arrow::DataType;
using arrow::Status;

namespace BitUtil = arrow::BitUtil;

// ----------------------------------------------------------------------
// Serialization

template <int TYPE>
struct npy_traits {
};

template <>
struct npy_traits<NPY_BOOL> {
  typedef uint8_t value_type;
  using TypeClass = arrow::BooleanType;

  static constexpr bool supports_nulls = false;
  static inline bool isnull(uint8_t v) {
    return false;
  }
};

#define NPY_INT_DECL(TYPE, CapType, T)              \
  template <>                                       \
  struct npy_traits<NPY_##TYPE> {                   \
    typedef T value_type;                           \
    using TypeClass = arrow::CapType##Type;         \
                                                    \
    static constexpr bool supports_nulls = false;   \
    static inline bool isnull(T v) {                \
      return false;                                 \
    }                                               \
  };

NPY_INT_DECL(INT8, Int8, int8_t);
NPY_INT_DECL(INT16, Int16, int16_t);
NPY_INT_DECL(INT32, Int32, int32_t);
NPY_INT_DECL(INT64, Int64, int64_t);
NPY_INT_DECL(UINT8, UInt8, uint8_t);
NPY_INT_DECL(UINT16, UInt16, uint16_t);
NPY_INT_DECL(UINT32, UInt32, uint32_t);
NPY_INT_DECL(UINT64, UInt64, uint64_t);

template <>
struct npy_traits<NPY_FLOAT32> {
  typedef float value_type;
  using TypeClass = arrow::FloatType;

  static constexpr bool supports_nulls = true;

  static inline bool isnull(float v) {
    return v != v;
  }
};

template <>
struct npy_traits<NPY_FLOAT64> {
  typedef double value_type;
  using TypeClass = arrow::DoubleType;

  static constexpr bool supports_nulls = true;

  static inline bool isnull(double v) {
    return v != v;
  }
};

template <>
struct npy_traits<NPY_DATETIME> {
  typedef int64_t value_type;
  using TypeClass = arrow::TimestampType;

  static constexpr bool supports_nulls = true;

  static inline bool isnull(int64_t v) {
    // NaT = -2**63
    // = -0x8000000000000000
    // = -9223372036854775808;
    // = std::numeric_limits<int64_t>::min()
    return v == std::numeric_limits<int64_t>::min();
  }
};

template <>
struct npy_traits<NPY_OBJECT> {
  typedef PyObject* value_type;
  static constexpr bool supports_nulls = true;
};

template <int TYPE>
class ArrowSerializer {
 public:
  ArrowSerializer(arrow::MemoryPool* pool, PyArrayObject* arr, PyArrayObject* mask) :
      pool_(pool),
      arr_(arr),
      mask_(mask) {
    length_ = PyArray_SIZE(arr_);
  }

  Status Convert(std::shared_ptr<Array>* out);

  int stride() const {
    return PyArray_STRIDES(arr_)[0];
  }

  Status InitNullBitmap() {
    int null_bytes = BitUtil::BytesForBits(length_);

    null_bitmap_ = std::make_shared<arrow::PoolBuffer>(pool_);
    RETURN_NOT_OK(null_bitmap_->Resize(null_bytes));

    null_bitmap_data_ = null_bitmap_->mutable_data();
    memset(null_bitmap_data_, 0, null_bytes);

    return Status::OK();
  }

  bool is_strided() const {
    npy_intp* astrides = PyArray_STRIDES(arr_);
    return astrides[0] != PyArray_DESCR(arr_)->elsize;
  }

 private:
  Status ConvertData();

  Status ConvertObjectStrings(std::shared_ptr<Array>* out) {
    PyAcquireGIL lock;

    PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    arrow::TypePtr string_type(new arrow::StringType());
    arrow::StringBuilder string_builder(pool_, string_type);
    RETURN_NOT_OK(string_builder.Resize(length_));

    Status s;
    PyObject* obj;
    for (int64_t i = 0; i < length_; ++i) {
      obj = objects[i];
      if (PyUnicode_Check(obj)) {
        obj = PyUnicode_AsUTF8String(obj);
        if (obj == NULL) {
          PyErr_Clear();
          return Status::TypeError("failed converting unicode to UTF8");
        }
        const int32_t length = PyBytes_GET_SIZE(obj);
        s = string_builder.Append(PyBytes_AS_STRING(obj), length);
        Py_DECREF(obj);
        if (!s.ok()) {
          return s;
        }
      } else if (PyBytes_Check(obj)) {
        const int32_t length = PyBytes_GET_SIZE(obj);
        RETURN_NOT_OK(string_builder.Append(PyBytes_AS_STRING(obj), length));
      } else {
        string_builder.AppendNull();
      }
    }
    return string_builder.Finish(out);
  }

  Status ConvertBooleans(std::shared_ptr<Array>* out) {
    PyAcquireGIL lock;

    PyObject** objects = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));

    int nbytes = BitUtil::BytesForBits(length_);
    auto data = std::make_shared<arrow::PoolBuffer>(pool_);
    RETURN_NOT_OK(data->Resize(nbytes));
    uint8_t* bitmap = data->mutable_data();
    memset(bitmap, 0, nbytes);

    int64_t null_count = 0;
    for (int64_t i = 0; i < length_; ++i) {
      if (objects[i] == Py_True) {
        BitUtil::SetBit(bitmap, i);
        BitUtil::SetBit(null_bitmap_data_, i);
      } else if (objects[i] != Py_False) {
        ++null_count;
      } else {
        BitUtil::SetBit(null_bitmap_data_, i);
      }
    }

    *out = std::make_shared<arrow::BooleanArray>(length_, data, null_count,
        null_bitmap_);

    return Status::OK();
  }

  Status MakeDataType(std::shared_ptr<DataType>* out);

  arrow::MemoryPool* pool_;

  PyArrayObject* arr_;
  PyArrayObject* mask_;

  int64_t length_;

  std::shared_ptr<arrow::Buffer> data_;
  std::shared_ptr<arrow::ResizableBuffer> null_bitmap_;
  uint8_t* null_bitmap_data_;
};

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

template <int TYPE>
inline Status ArrowSerializer<TYPE>::MakeDataType(std::shared_ptr<DataType>* out) {
  out->reset(new typename npy_traits<TYPE>::TypeClass());
  return Status::OK();
}

template <>
inline Status ArrowSerializer<NPY_DATETIME>::MakeDataType(std::shared_ptr<DataType>* out) {
  PyArray_Descr* descr = PyArray_DESCR(arr_);
  auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(descr->c_metadata);
  arrow::TimestampType::Unit unit;

  switch (date_dtype->meta.base) {
      case NPY_FR_s:
          unit = arrow::TimestampType::Unit::SECOND;
          break;
      case NPY_FR_ms:
          unit = arrow::TimestampType::Unit::MILLI;
          break;
      case NPY_FR_us:
          unit = arrow::TimestampType::Unit::MICRO;
          break;
      case NPY_FR_ns:
          unit = arrow::TimestampType::Unit::NANO;
          break;
      default:
          return Status::Invalid("Unknown NumPy datetime unit");
  }

  out->reset(new arrow::TimestampType(unit));
  return Status::OK();
}

template <int TYPE>
inline Status ArrowSerializer<TYPE>::Convert(std::shared_ptr<Array>* out) {
  typedef npy_traits<TYPE> traits;

  if (mask_ != nullptr || traits::supports_nulls) {
    RETURN_NOT_OK(InitNullBitmap());
  }

  int64_t null_count = 0;
  if (mask_ != nullptr) {
    null_count = MaskToBitmap(mask_, length_, null_bitmap_data_);
  } else if (traits::supports_nulls) {
    null_count = ValuesToBitmap<TYPE>(PyArray_DATA(arr_), length_, null_bitmap_data_);
  }

  RETURN_NOT_OK(ConvertData());
  std::shared_ptr<DataType> type;
  RETURN_NOT_OK(MakeDataType(&type));
  RETURN_NOT_OK(MakePrimitiveArray(type, length_, data_, null_count, null_bitmap_, out));
  return Status::OK();
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

static inline bool PyObject_is_bool(const PyObject* obj) {
#if PY_MAJOR_VERSION >= 3
  return PyString_Check(obj) || PyBytes_Check(obj);
#else
  return PyString_Check(obj) || PyUnicode_Check(obj);
#endif
}

template <>
inline Status ArrowSerializer<NPY_OBJECT>::Convert(std::shared_ptr<Array>* out) {
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
  const PyObject** objects = reinterpret_cast<const PyObject**>(PyArray_DATA(arr_));

  for (int64_t i = 0; i < length_; ++i) {
    if (PyObject_is_null(objects[i])) {
      continue;
    } else if (PyObject_is_string(objects[i])) {
      return ConvertObjectStrings(out);
    } else if (PyBool_Check(objects[i])) {
      return ConvertBooleans(out);
    } else {
      return Status::TypeError("unhandled python type");
    }
  }

  return Status::TypeError("Unable to infer type of object array, were all null");
}

template <int TYPE>
inline Status ArrowSerializer<TYPE>::ConvertData() {
  // TODO(wesm): strided arrays
  if (is_strided()) {
    return Status::Invalid("no support for strided data yet");
  }

  data_ = std::make_shared<NumPyBuffer>(arr_);
  return Status::OK();
}

template <>
inline Status ArrowSerializer<NPY_BOOL>::ConvertData() {
  if (is_strided()) {
    return Status::Invalid("no support for strided data yet");
  }

  int nbytes = BitUtil::BytesForBits(length_);
  auto buffer = std::make_shared<arrow::PoolBuffer>(pool_);
  RETURN_NOT_OK(buffer->Resize(nbytes));

  const uint8_t* values = reinterpret_cast<const uint8_t*>(PyArray_DATA(arr_));

  uint8_t* bitmap = buffer->mutable_data();

  memset(bitmap, 0, nbytes);
  for (int i = 0; i < length_; ++i) {
    if (values[i] > 0) {
      BitUtil::SetBit(bitmap, i);
    }
  }

  data_ = buffer;

  return Status::OK();
}

template <>
inline Status ArrowSerializer<NPY_OBJECT>::ConvertData() {
  return Status::TypeError("NYI");
}


#define TO_ARROW_CASE(TYPE)                                     \
  case NPY_##TYPE:                                              \
    {                                                           \
      ArrowSerializer<NPY_##TYPE> converter(pool, arr, mask);   \
      RETURN_NOT_OK(converter.Convert(out));                    \
    }                                                           \
    break;

Status PandasMaskedToArrow(arrow::MemoryPool* pool, PyObject* ao, PyObject* mo,
    std::shared_ptr<Array>* out) {
  PyArrayObject* arr = reinterpret_cast<PyArrayObject*>(ao);
  PyArrayObject* mask = nullptr;

  if (mo != nullptr) {
    mask = reinterpret_cast<PyArrayObject*>(mo);
  }

  if (PyArray_NDIM(arr) != 1) {
    return Status::Invalid("only handle 1-dimensional arrays");
  }

  switch(PyArray_DESCR(arr)->type_num) {
    TO_ARROW_CASE(BOOL);
    TO_ARROW_CASE(INT8);
    TO_ARROW_CASE(INT16);
    TO_ARROW_CASE(INT32);
    TO_ARROW_CASE(INT64);
    TO_ARROW_CASE(UINT8);
    TO_ARROW_CASE(UINT16);
    TO_ARROW_CASE(UINT32);
    TO_ARROW_CASE(UINT64);
    TO_ARROW_CASE(FLOAT32);
    TO_ARROW_CASE(FLOAT64);
    TO_ARROW_CASE(DATETIME);
    TO_ARROW_CASE(OBJECT);
    default:
      std::stringstream ss;
      ss << "unsupported type " << PyArray_DESCR(arr)->type_num
         << std::endl;
      return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

Status PandasToArrow(arrow::MemoryPool* pool, PyObject* ao,
    std::shared_ptr<Array>* out) {
  return PandasMaskedToArrow(pool, ao, nullptr, out);
}

// ----------------------------------------------------------------------
// Deserialization

template <int TYPE>
struct arrow_traits {
};

template <>
struct arrow_traits<arrow::Type::BOOL> {
  static constexpr int npy_type = NPY_BOOL;
  static constexpr bool supports_nulls = false;
  static constexpr bool is_boolean = true;
  static constexpr bool is_pandas_numeric_not_nullable = false;
  static constexpr bool is_pandas_numeric_nullable = false;
};

#define INT_DECL(TYPE)                                           \
  template <>                                                    \
  struct arrow_traits<arrow::Type::TYPE> {                       \
    static constexpr int npy_type = NPY_##TYPE;                  \
    static constexpr bool supports_nulls = false;                \
    static constexpr double na_value = NAN;                      \
    static constexpr bool is_boolean = false;                    \
    static constexpr bool is_pandas_numeric_not_nullable = true; \
    static constexpr bool is_pandas_numeric_nullable = false;    \
    typedef typename npy_traits<NPY_##TYPE>::value_type T;       \
  };

INT_DECL(INT8);
INT_DECL(INT16);
INT_DECL(INT32);
INT_DECL(INT64);
INT_DECL(UINT8);
INT_DECL(UINT16);
INT_DECL(UINT32);
INT_DECL(UINT64);

template <>
struct arrow_traits<arrow::Type::FLOAT> {
  static constexpr int npy_type = NPY_FLOAT32;
  static constexpr bool supports_nulls = true;
  static constexpr float na_value = NAN;
  static constexpr bool is_boolean = false;
  static constexpr bool is_pandas_numeric_not_nullable = false;
  static constexpr bool is_pandas_numeric_nullable = true;
  typedef typename npy_traits<NPY_FLOAT32>::value_type T;
};

template <>
struct arrow_traits<arrow::Type::DOUBLE> {
  static constexpr int npy_type = NPY_FLOAT64;
  static constexpr bool supports_nulls = true;
  static constexpr double na_value = NAN;
  static constexpr bool is_boolean = false;
  static constexpr bool is_pandas_numeric_not_nullable = false;
  static constexpr bool is_pandas_numeric_nullable = true;
  typedef typename npy_traits<NPY_FLOAT64>::value_type T;
};

template <>
struct arrow_traits<arrow::Type::TIMESTAMP> {
  static constexpr int npy_type = NPY_DATETIME;
  static constexpr bool supports_nulls = true;
  static constexpr int64_t na_value = std::numeric_limits<int64_t>::min();
  static constexpr bool is_boolean = false;
  static constexpr bool is_pandas_numeric_not_nullable = false;
  static constexpr bool is_pandas_numeric_nullable = true;
  typedef typename npy_traits<NPY_DATETIME>::value_type T;
};

template <>
struct arrow_traits<arrow::Type::STRING> {
  static constexpr int npy_type = NPY_OBJECT;
  static constexpr bool supports_nulls = true;
  static constexpr bool is_boolean = false;
  static constexpr bool is_pandas_numeric_not_nullable = false;
  static constexpr bool is_pandas_numeric_nullable = false;
};


static inline PyObject* make_pystring(const uint8_t* data, int32_t length) {
#if PY_MAJOR_VERSION >= 3
  return PyUnicode_FromStringAndSize(reinterpret_cast<const char*>(data), length);
#else
  return PyString_FromStringAndSize(reinterpret_cast<const char*>(data), length);
#endif
}

inline void set_numpy_metadata(int type, DataType* datatype, PyArrayObject* out) {
  if (type == NPY_DATETIME) {
    auto timestamp_type = static_cast<arrow::TimestampType*>(datatype);
    // We only support ms resolution at the moment
    PyArray_Descr* descr = PyArray_DESCR(out);
    auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(descr->c_metadata);

    switch (timestamp_type->unit) {
      case arrow::TimestampType::Unit::SECOND:
        date_dtype->meta.base = NPY_FR_s;
        break;
      case arrow::TimestampType::Unit::MILLI:
        date_dtype->meta.base = NPY_FR_ms;
        break;
      case arrow::TimestampType::Unit::MICRO:
        date_dtype->meta.base = NPY_FR_us;
        break;
      case arrow::TimestampType::Unit::NANO:
        date_dtype->meta.base = NPY_FR_ns;
        break;
    }
  }
}

template <int TYPE>
class ArrowDeserializer {
 public:
  ArrowDeserializer(const std::shared_ptr<Column>& col, PyObject* py_ref) :
      col_(col), py_ref_(py_ref) {}

  Status Convert(PyObject** out) {
    const std::shared_ptr<arrow::ChunkedArray> data = col_->data();
    if (data->num_chunks() > 1) {
      return Status::NotImplemented("Chunked column conversion NYI");
    }

    auto chunk = data->chunk(0);

    RETURN_NOT_OK(ConvertValues<TYPE>(chunk));
    *out = reinterpret_cast<PyObject*>(out_);
    return Status::OK();
  }

  Status AllocateOutput(int type) {
    PyAcquireGIL lock;

    npy_intp dims[1] = {col_->length()};
    out_ = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNew(1, dims, type));

    if (out_ == NULL) {
      // Error occurred, trust that SimpleNew set the error state
      return Status::OK();
    }

    set_numpy_metadata(type, col_->type().get(), out_);

    return Status::OK();
  }

  Status OutputFromData(int type, void* data) {
    PyAcquireGIL lock;

    // Zero-Copy. We can pass the data pointer directly to NumPy.
    npy_intp dims[1] = {col_->length()};
    out_ = reinterpret_cast<PyArrayObject*>(PyArray_SimpleNewFromData(1, dims,
                type, data));

    if (out_ == NULL) {
      // Error occurred, trust that SimpleNew set the error state
      return Status::OK();
    }

    set_numpy_metadata(type, col_->type().get(), out_);

    if (PyArray_SetBaseObject(out_, py_ref_) == -1) {
      // Error occurred, trust that SetBaseObject set the error state
      return Status::OK();
    } else {
      // PyArray_SetBaseObject steals our reference to py_ref_
      Py_INCREF(py_ref_);
    }

    // Arrow data is immutable.
    PyArray_CLEARFLAGS(out_, NPY_ARRAY_WRITEABLE);

    return Status::OK();
  }

  template <int T2>
  inline typename std::enable_if<
    arrow_traits<T2>::is_pandas_numeric_nullable, Status>::type
  ConvertValues(const std::shared_ptr<Array>& arr) {
    typedef typename arrow_traits<T2>::T T;

    arrow::PrimitiveArray* prim_arr = static_cast<arrow::PrimitiveArray*>(
        arr.get());
    const T* in_values = reinterpret_cast<const T*>(prim_arr->data()->data());

    if (arr->null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(arrow_traits<T2>::npy_type));

      T* out_values = reinterpret_cast<T*>(PyArray_DATA(out_));
      for (int64_t i = 0; i < arr->length(); ++i) {
        out_values[i] = arr->IsNull(i) ? arrow_traits<T2>::na_value : in_values[i];
      }
    } else {
      // Zero-Copy. We can pass the data pointer directly to NumPy.
      void* data = const_cast<T*>(in_values);
      int type = arrow_traits<TYPE>::npy_type;
      RETURN_NOT_OK(OutputFromData(type, data));
    }

    return Status::OK();
  }

  // Integer specialization
  template <int T2>
  inline typename std::enable_if<
    arrow_traits<T2>::is_pandas_numeric_not_nullable, Status>::type
  ConvertValues(const std::shared_ptr<Array>& arr) {
    typedef typename arrow_traits<T2>::T T;

    arrow::PrimitiveArray* prim_arr = static_cast<arrow::PrimitiveArray*>(
        arr.get());

    const T* in_values = reinterpret_cast<const T*>(prim_arr->data()->data());

    if (arr->null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_FLOAT64));

      // Upcast to double, set NaN as appropriate
      double* out_values = reinterpret_cast<double*>(PyArray_DATA(out_));
      for (int i = 0; i < arr->length(); ++i) {
        out_values[i] = prim_arr->IsNull(i) ? NAN : in_values[i];
      }
    } else {
      // Zero-Copy. We can pass the data pointer directly to NumPy.
      void* data = const_cast<T*>(in_values);
      int type = arrow_traits<TYPE>::npy_type;
      RETURN_NOT_OK(OutputFromData(type, data));
    }

    return Status::OK();
  }

  // Boolean specialization
  template <int T2>
  inline typename std::enable_if<
    arrow_traits<T2>::is_boolean, Status>::type
  ConvertValues(const std::shared_ptr<Array>& arr) {
    PyAcquireGIL lock;

    arrow::BooleanArray* bool_arr = static_cast<arrow::BooleanArray*>(arr.get());

    if (arr->null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));

      PyObject** out_values = reinterpret_cast<PyObject**>(PyArray_DATA(out_));
      for (int64_t i = 0; i < arr->length(); ++i) {
        if (bool_arr->IsNull(i)) {
          Py_INCREF(Py_None);
          out_values[i] = Py_None;
        } else if (bool_arr->Value(i)) {
          // True
          Py_INCREF(Py_True);
          out_values[i] = Py_True;
        } else {
          // False
          Py_INCREF(Py_False);
          out_values[i] = Py_False;
        }
      }
    } else {
      RETURN_NOT_OK(AllocateOutput(arrow_traits<TYPE>::npy_type));

      uint8_t* out_values = reinterpret_cast<uint8_t*>(PyArray_DATA(out_));
      for (int64_t i = 0; i < arr->length(); ++i) {
        out_values[i] = static_cast<uint8_t>(bool_arr->Value(i));
      }
    }

    return Status::OK();
  }

  // UTF8
  template <int T2>
  inline typename std::enable_if<
    T2 == arrow::Type::STRING, Status>::type
  ConvertValues(const std::shared_ptr<Array>& arr) {
    PyAcquireGIL lock;

    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));

    PyObject** out_values = reinterpret_cast<PyObject**>(PyArray_DATA(out_));

    arrow::StringArray* string_arr = static_cast<arrow::StringArray*>(arr.get());

    const uint8_t* data;
    int32_t length;
    if (arr->null_count() > 0) {
      for (int64_t i = 0; i < arr->length(); ++i) {
        if (string_arr->IsNull(i)) {
          Py_INCREF(Py_None);
          out_values[i] = Py_None;
        } else {
          data = string_arr->GetValue(i, &length);

          out_values[i] = make_pystring(data, length);
          if (out_values[i] == nullptr) {
            return Status::UnknownError("String initialization failed");
          }
        }
      }
    } else {
      for (int64_t i = 0; i < arr->length(); ++i) {
        data = string_arr->GetValue(i, &length);
        out_values[i] = make_pystring(data, length);
        if (out_values[i] == nullptr) {
          return Status::UnknownError("String initialization failed");
        }
      }
    }
    return Status::OK();
  }
 private:
  std::shared_ptr<Column> col_;
  PyObject* py_ref_;
  PyArrayObject* out_;
};

#define FROM_ARROW_CASE(TYPE)                                       \
  case arrow::Type::TYPE:                                           \
    {                                                               \
      ArrowDeserializer<arrow::Type::TYPE> converter(col, py_ref);  \
      return converter.Convert(out);                                \
    }                                                               \
    break;

Status ConvertArrayToPandas(const std::shared_ptr<Array>& arr, PyObject* py_ref,
        PyObject** out) {
  static std::string dummy_name = "dummy";
  auto field = std::make_shared<Field>(dummy_name, arr->type());
  auto col = std::make_shared<Column>(field, arr);
  return ConvertColumnToPandas(col, py_ref, out);
}

Status ConvertColumnToPandas(const std::shared_ptr<Column>& col, PyObject* py_ref,
        PyObject** out) {
  switch(col->type()->type) {
    FROM_ARROW_CASE(BOOL);
    FROM_ARROW_CASE(INT8);
    FROM_ARROW_CASE(INT16);
    FROM_ARROW_CASE(INT32);
    FROM_ARROW_CASE(INT64);
    FROM_ARROW_CASE(UINT8);
    FROM_ARROW_CASE(UINT16);
    FROM_ARROW_CASE(UINT32);
    FROM_ARROW_CASE(UINT64);
    FROM_ARROW_CASE(FLOAT);
    FROM_ARROW_CASE(DOUBLE);
    FROM_ARROW_CASE(STRING);
    FROM_ARROW_CASE(TIMESTAMP);
    default:
      return Status::NotImplemented("Arrow type reading not implemented");
  }
  return Status::OK();
}

} // namespace pyarrow
