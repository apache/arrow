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

#include "arrow/python/arrow_to_pandas.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/parallel.h"
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

using internal::kPandasTimestampNull;
using internal::kNanosecondsInDay;

// ----------------------------------------------------------------------
// Utility code

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

static inline bool ListTypeSupported(const DataType& type) {
  switch (type.id()) {
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
    case Type::LIST: {
      const ListType& list_type = static_cast<const ListType&>(type);
      return ListTypeSupported(*list_type.value_type());
    }
    default:
      break;
  }
  return false;
}
// ----------------------------------------------------------------------
// PyCapsule code for setting ndarray base to reference C++ object

struct ArrowCapsule {
  std::shared_ptr<Array> array;
};

namespace {

void ArrowCapsule_Destructor(PyObject* capsule) {
  delete reinterpret_cast<ArrowCapsule*>(PyCapsule_GetPointer(capsule, "arrow"));
}

}  // namespace

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
static inline PyObject* NewArray1DFromType(DataType* arrow_type, int type, int64_t length,
                                           void* data) {
  npy_intp dims[1] = {length};

  PyArray_Descr* descr = GetSafeNumPyDtype(type);
  if (descr == nullptr) {
    // Error occurred, trust error state is set
    return nullptr;
  }

  set_numpy_metadata(type, arrow_type, descr);
  return PyArray_NewFromDescr(&PyArray_Type, descr, 1, dims, nullptr, data,
                              NPY_ARRAY_OWNDATA | NPY_ARRAY_CARRAY | NPY_ARRAY_WRITEABLE,
                              nullptr);
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

  PandasBlock(PandasOptions options, int64_t num_rows, int num_columns)
      : num_rows_(num_rows), num_columns_(num_columns), options_(options) {}
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

  PandasOptions options_;

  // ndarray<int32>
  OwnedRef placement_arr_;
  int64_t* placement_data_;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(PandasBlock);
};

template <typename T>
inline void ConvertIntegerWithNulls(PandasOptions options, const ChunkedArray& data,
                                    double* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const PrimitiveArray&>(*data.chunk(c));
    auto in_values = reinterpret_cast<const T*>(arr.raw_values());
    // Upcast to double, set NaN as appropriate

    for (int i = 0; i < arr.length(); ++i) {
      *out_values++ = arr.IsNull(i) ? NAN : static_cast<double>(in_values[i]);
    }
  }
}

template <typename T>
inline void ConvertIntegerNoNullsSameType(PandasOptions options, const ChunkedArray& data,
                                          T* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const PrimitiveArray&>(*data.chunk(c));
    auto in_values = reinterpret_cast<const T*>(arr.raw_values());
    memcpy(out_values, in_values, sizeof(T) * arr.length());
    out_values += arr.length();
  }
}

template <typename InType, typename OutType>
inline void ConvertIntegerNoNullsCast(PandasOptions options, const ChunkedArray& data,
                                      OutType* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const PrimitiveArray&>(*data.chunk(c));
    auto in_values = reinterpret_cast<const InType*>(arr.raw_values());
    for (int64_t i = 0; i < arr.length(); ++i) {
      *out_values = in_values[i];
    }
  }
}

static Status ConvertBooleanWithNulls(PandasOptions options, const ChunkedArray& data,
                                      PyObject** out_values) {
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

static void ConvertBooleanNoNulls(PandasOptions options, const ChunkedArray& data,
                                  uint8_t* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const std::shared_ptr<Array> arr = data.chunk(c);
    auto bool_arr = static_cast<BooleanArray*>(arr.get());
    for (int64_t i = 0; i < arr->length(); ++i) {
      *out_values++ = static_cast<uint8_t>(bool_arr->Value(i));
    }
  }
}

template <typename Type>
inline Status ConvertBinaryLike(PandasOptions options, const ChunkedArray& data,
                                PyObject** out_values) {
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

inline Status ConvertNulls(PandasOptions options, const ChunkedArray& data,
                           PyObject** out_values) {
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

inline Status ConvertFixedSizeBinary(PandasOptions options, const ChunkedArray& data,
                                     PyObject** out_values) {
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

inline Status ConvertStruct(PandasOptions options, const ChunkedArray& data,
                            PyObject** out_values) {
  PyAcquireGIL lock;
  if (data.num_chunks() <= 0) {
    return Status::OK();
  }
  // ChunkedArray has at least one chunk
  auto arr = static_cast<const StructArray*>(data.chunk(0).get());
  // Use it to cache the struct type and number of fields for all chunks
  int32_t num_fields = arr->num_fields();
  auto array_type = arr->type();
  std::vector<OwnedRef> fields_data(num_fields);
  OwnedRef dict_item;
  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = static_cast<const StructArray*>(data.chunk(c).get());
    // Convert the struct arrays first
    for (int32_t i = 0; i < num_fields; i++) {
      PyObject* numpy_array;
      RETURN_NOT_OK(ConvertArrayToPandas(options, arr->field(static_cast<int>(i)),
                                         nullptr, &numpy_array));
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
        for (int32_t field_idx = 0; field_idx < num_fields; ++field_idx) {
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
inline Status ConvertListsLike(PandasOptions options, const std::shared_ptr<Column>& col,
                               PyObject** out_values) {
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
  RETURN_NOT_OK(ConvertColumnToPandas(options, flat_column, nullptr, &numpy_array));

  PyAcquireGIL lock;

  int64_t chunk_offset = 0;
  for (int c = 0; c < data.num_chunks(); c++) {
    auto arr = std::static_pointer_cast<ListArray>(data.chunk(c));

    const bool has_nulls = data.null_count() > 0;
    for (int64_t i = 0; i < arr->length(); ++i) {
      if (has_nulls && arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values = Py_None;
      } else {
        PyObject* start = PyLong_FromLongLong(arr->value_offset(i) + chunk_offset);
        PyObject* end = PyLong_FromLongLong(arr->value_offset(i + 1) + chunk_offset);
        PyObject* slice = PySlice_New(start, end, NULL);
        *out_values = PyObject_GetItem(numpy_array, slice);
        Py_DECREF(start);
        Py_DECREF(end);
        Py_DECREF(slice);
      }
      ++out_values;
    }

    chunk_offset += arr->values()->length();
  }

  Py_XDECREF(numpy_array);
  return Status::OK();
}

template <typename T>
inline void ConvertNumericNullable(const ChunkedArray& data, T na_value, T* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const PrimitiveArray&>(*data.chunk(c));
    auto in_values = reinterpret_cast<const T*>(arr.raw_values());

    const uint8_t* valid_bits = arr.null_bitmap_data();

    if (arr.null_count() > 0) {
      for (int64_t i = 0; i < arr.length(); ++i) {
        *out_values++ = BitUtil::BitNotSet(valid_bits, i) ? na_value : in_values[i];
      }
    } else {
      memcpy(out_values, in_values, sizeof(T) * arr.length());
      out_values += arr.length();
    }
  }
}

template <typename InType, typename OutType>
inline void ConvertNumericNullableCast(const ChunkedArray& data, OutType na_value,
                                       OutType* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const PrimitiveArray&>(*data.chunk(c));
    auto in_values = reinterpret_cast<const InType*>(arr.raw_values());

    for (int64_t i = 0; i < arr.length(); ++i) {
      *out_values++ = arr.IsNull(i) ? na_value : static_cast<OutType>(in_values[i]);
    }
  }
}

template <typename InType, int64_t SHIFT>
inline void ConvertDatetimeNanos(const ChunkedArray& data, int64_t* out_values) {
  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const PrimitiveArray&>(*data.chunk(c));
    auto in_values = reinterpret_cast<const InType*>(arr.raw_values());

    for (int64_t i = 0; i < arr.length(); ++i) {
      *out_values++ = arr.IsNull(i) ? kPandasTimestampNull
                                    : (static_cast<int64_t>(in_values[i]) * SHIFT);
    }
  }
}

template <typename TYPE>
static Status ConvertTimes(PandasOptions options, const ChunkedArray& data,
                           PyObject** out_values) {
  using ArrayType = typename TypeTraits<TYPE>::ArrayType;

  PyAcquireGIL lock;
  OwnedRef time_ref;

  PyDateTime_IMPORT;

  for (int c = 0; c < data.num_chunks(); c++) {
    const auto& arr = static_cast<const ArrayType&>(*data.chunk(c));
    auto type = std::dynamic_pointer_cast<TYPE>(arr.type());
    DCHECK(type);

    const TimeUnit::type unit = type->unit();

    for (int64_t i = 0; i < arr.length(); ++i) {
      if (arr.IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values++ = Py_None;
      } else {
        RETURN_NOT_OK(PyTime_from_int(arr.Value(i), unit, out_values++));
        RETURN_IF_PYERROR();
      }
    }
  }

  return Status::OK();
}

static Status RawDecimalToString(const uint8_t* bytes, int precision, int scale,
                                 std::string* result) {
  DCHECK_NE(result, nullptr);
  Decimal128 decimal(bytes);
  *result = decimal.ToString(precision, scale);
  return Status::OK();
}

static Status ConvertDecimals(PandasOptions options, const ChunkedArray& data,
                              PyObject** out_values) {
  PyAcquireGIL lock;
  OwnedRef decimal_ref;
  OwnedRef Decimal_ref;
  RETURN_NOT_OK(internal::ImportModule("decimal", &decimal_ref));
  RETURN_NOT_OK(internal::ImportFromModule(decimal_ref, "Decimal", &Decimal_ref));
  PyObject* Decimal = Decimal_ref.obj();

  for (int c = 0; c < data.num_chunks(); c++) {
    auto* arr(static_cast<arrow::DecimalArray*>(data.chunk(c).get()));
    auto type(std::dynamic_pointer_cast<arrow::DecimalType>(arr->type()));
    const int precision = type->precision();
    const int scale = type->scale();

    for (int64_t i = 0; i < arr->length(); ++i) {
      if (arr->IsNull(i)) {
        Py_INCREF(Py_None);
        *out_values++ = Py_None;
      } else {
        const uint8_t* raw_value = arr->GetValue(i);
        std::string decimal_string;
        RETURN_NOT_OK(RawDecimalToString(raw_value, precision, scale, &decimal_string));
        *out_values++ = internal::DecimalFromString(Decimal, decimal_string);
        RETURN_IF_PYERROR();
      }
    }
  }

  return Status::OK();
}

#define CONVERTLISTSLIKE_CASE(ArrowType, ArrowEnum)                          \
  case Type::ArrowEnum:                                                      \
    RETURN_NOT_OK((ConvertListsLike<ArrowType>(options_, col, out_buffer))); \
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
      RETURN_NOT_OK(ConvertBooleanWithNulls(options_, data, out_buffer));
    } else if (type == Type::BINARY) {
      RETURN_NOT_OK(ConvertBinaryLike<BinaryType>(options_, data, out_buffer));
    } else if (type == Type::STRING) {
      RETURN_NOT_OK(ConvertBinaryLike<StringType>(options_, data, out_buffer));
    } else if (type == Type::FIXED_SIZE_BINARY) {
      RETURN_NOT_OK(ConvertFixedSizeBinary(options_, data, out_buffer));
    } else if (type == Type::TIME32) {
      RETURN_NOT_OK(ConvertTimes<Time32Type>(options_, data, out_buffer));
    } else if (type == Type::TIME64) {
      RETURN_NOT_OK(ConvertTimes<Time64Type>(options_, data, out_buffer));
    } else if (type == Type::DECIMAL) {
      RETURN_NOT_OK(ConvertDecimals(options_, data, out_buffer));
    } else if (type == Type::NA) {
      RETURN_NOT_OK(ConvertNulls(options_, data, out_buffer));
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
        CONVERTLISTSLIKE_CASE(ListType, LIST)
        default: {
          std::stringstream ss;
          ss << "Not implemented type for conversion from List to Pandas ObjectBlock: "
             << list_type->value_type()->ToString();
          return Status::NotImplemented(ss.str());
        }
      }
    } else if (type == Type::STRUCT) {
      RETURN_NOT_OK(ConvertStruct(options_, data, out_buffer));
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
    return AllocateNDArray(internal::arrow_traits<ARROW_TYPE>::npy_type);
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

    ConvertIntegerNoNullsSameType<C_TYPE>(options_, data, out_buffer);
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

#define INTEGER_CASE(IN_TYPE)                                   \
  ConvertIntegerWithNulls<IN_TYPE>(options_, data, out_buffer); \
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

    ConvertBooleanNoNulls(options_, *col->data().get(), out_buffer);
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
  DatetimeTZBlock(PandasOptions options, const std::string& timezone, int64_t num_rows)
      : DatetimeBlock(options, num_rows, 1), timezone_(timezone) {}

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

class CategoricalBlock : public PandasBlock {
 public:
  explicit CategoricalBlock(PandasOptions options, MemoryPool* pool, int64_t num_rows)
      : PandasBlock(options, num_rows, 1), pool_(pool) {}

  Status Allocate() override {
    return Status::NotImplemented(
        "CategoricalBlock allocation happens when calling Write");
  }

  template <int ARROW_INDEX_TYPE>
  Status WriteIndices(const std::shared_ptr<Column>& col) {
    using TRAITS = internal::arrow_traits<ARROW_INDEX_TYPE>;
    using T = typename TRAITS::T;
    constexpr int npy_type = TRAITS::npy_type;

    const ChunkedArray& data = *col->data().get();

    // Sniff the first chunk
    const std::shared_ptr<Array> arr_first = data.chunk(0);
    const auto& dict_arr_first = static_cast<const DictionaryArray&>(*arr_first);
    const auto& indices_first =
        static_cast<const PrimitiveArray&>(*dict_arr_first.indices());

    if (data.num_chunks() == 1 && indices_first.null_count() == 0) {
      RETURN_NOT_OK(AllocateNDArrayFromIndices<T>(npy_type, indices_first));
    } else {
      if (options_.zero_copy_only) {
        std::stringstream ss;
        ss << "Needed to copy " << data.num_chunks() << " chunks with "
           << indices_first.null_count() << " indices nulls, but zero_copy_only was True";
        return Status::Invalid(ss.str());
      }
      RETURN_NOT_OK(AllocateNDArray(npy_type, 1));

      // No relative placement offset because a single column
      T* out_values = reinterpret_cast<T*>(block_data_);

      for (int c = 0; c < data.num_chunks(); c++) {
        const std::shared_ptr<Array> arr = data.chunk(c);
        const auto& dict_arr = static_cast<const DictionaryArray&>(*arr);

        const auto& indices = static_cast<const PrimitiveArray&>(*dict_arr.indices());
        auto in_values = reinterpret_cast<const T*>(indices.raw_values());

        // Null is -1 in CategoricalBlock
        for (int i = 0; i < arr->length(); ++i) {
          *out_values++ = indices.IsNull(i) ? -1 : in_values[i];
        }
      }
    }

    return Status::OK();
  }

  Status Write(const std::shared_ptr<Column>& col, int64_t abs_placement,
               int64_t rel_placement) override {
    std::shared_ptr<Column> converted_col;
    if (options_.strings_to_categorical &&
        (col->type()->id() == Type::STRING || col->type()->id() == Type::BINARY)) {
      RETURN_NOT_OK(EncodeColumnToDictionary(static_cast<const Column&>(*col), pool_,
                                             &converted_col));
    } else {
      converted_col = col;
    }

    const auto& dict_type = static_cast<const DictionaryType&>(*converted_col->type());

    switch (dict_type.index_type()->id()) {
      case Type::INT8:
        RETURN_NOT_OK(WriteIndices<Type::INT8>(converted_col));
        break;
      case Type::INT16:
        RETURN_NOT_OK(WriteIndices<Type::INT16>(converted_col));
        break;
      case Type::INT32:
        RETURN_NOT_OK(WriteIndices<Type::INT32>(converted_col));
        break;
      case Type::INT64:
        RETURN_NOT_OK(WriteIndices<Type::INT64>(converted_col));
        break;
      default: {
        std::stringstream ss;
        ss << "Categorical index type not supported: "
           << dict_type.index_type()->ToString();
        return Status::NotImplemented(ss.str());
      }
    }

    placement_data_[rel_placement] = abs_placement;
    PyObject* dict;
    RETURN_NOT_OK(ConvertArrayToPandas(options_, dict_type.dictionary(), nullptr, &dict));
    dictionary_.reset(dict);
    ordered_ = dict_type.ordered();

    return Status::OK();
  }

  Status GetPyResult(PyObject** output) override {
    PyObject* result = PyDict_New();
    RETURN_IF_PYERROR();

    PyDict_SetItemString(result, "block", block_arr_.obj());
    PyDict_SetItemString(result, "dictionary", dictionary_.obj());
    PyDict_SetItemString(result, "placement", placement_arr_.obj());

    PyObject* py_ordered = ordered_ ? Py_True : Py_False;
    Py_INCREF(py_ordered);
    PyDict_SetItemString(result, "ordered", py_ordered);

    *output = result;

    return Status::OK();
  }

  PyObject* dictionary() const { return dictionary_.obj(); }

 protected:
  template <typename T>
  Status AllocateNDArrayFromIndices(int npy_type, const PrimitiveArray& indices) {
    npy_intp block_dims[1] = {num_rows_};

    auto in_values = reinterpret_cast<const T*>(indices.raw_values());
    void* data = const_cast<T*>(in_values);

    PyAcquireGIL lock;

    PyArray_Descr* descr = GetSafeNumPyDtype(npy_type);
    if (descr == nullptr) {
      // Error occurred, trust error state is set
      return Status::OK();
    }

    PyObject* block_arr = PyArray_NewFromDescr(&PyArray_Type, descr, 1, block_dims,
                                               nullptr, data, NPY_ARRAY_CARRAY, nullptr);

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

  MemoryPool* pool_;
  OwnedRef dictionary_;
  bool ordered_;
};

Status MakeBlock(PandasOptions options, PandasBlock::type type, int64_t num_rows,
                 int num_columns, std::shared_ptr<PandasBlock>* block) {
#define BLOCK_CASE(NAME, TYPE)                                       \
  case PandasBlock::NAME:                                            \
    *block = std::make_shared<TYPE>(options, num_rows, num_columns); \
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

using BlockMap = std::unordered_map<int, std::shared_ptr<PandasBlock>>;

static Status GetPandasBlockType(const Column& col, const PandasOptions& options,
                                 PandasBlock::type* output_type) {
  switch (col.type()->id()) {
    case Type::BOOL:
      *output_type = col.null_count() > 0 ? PandasBlock::OBJECT : PandasBlock::BOOL;
      break;
    case Type::UINT8:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT8;
      break;
    case Type::INT8:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT8;
      break;
    case Type::UINT16:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT16;
      break;
    case Type::INT16:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT16;
      break;
    case Type::UINT32:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT32;
      break;
    case Type::INT32:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT32;
      break;
    case Type::INT64:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::INT64;
      break;
    case Type::UINT64:
      *output_type = col.null_count() > 0 ? PandasBlock::DOUBLE : PandasBlock::UINT64;
      break;
    case Type::FLOAT:
      *output_type = PandasBlock::FLOAT;
      break;
    case Type::DOUBLE:
      *output_type = PandasBlock::DOUBLE;
      break;
    case Type::STRING:
    case Type::BINARY:
      if (options.strings_to_categorical) {
        *output_type = PandasBlock::CATEGORICAL;
        break;
      }
    case Type::NA:
    case Type::FIXED_SIZE_BINARY:
    case Type::STRUCT:
    case Type::TIME32:
    case Type::TIME64:
    case Type::DECIMAL:
      *output_type = PandasBlock::OBJECT;
      break;
    case Type::DATE32:
      *output_type = PandasBlock::DATETIME;
      break;
    case Type::DATE64:
      *output_type = PandasBlock::DATETIME;
      break;
    case Type::TIMESTAMP: {
      const auto& ts_type = static_cast<const TimestampType&>(*col.type());
      if (ts_type.timezone() != "") {
        *output_type = PandasBlock::DATETIME_WITH_TZ;
      } else {
        *output_type = PandasBlock::DATETIME;
      }
    } break;
    case Type::LIST: {
      auto list_type = std::static_pointer_cast<ListType>(col.type());
      if (!ListTypeSupported(*list_type->value_type())) {
        std::stringstream ss;
        ss << "Not implemented type for list in DataFrameBlock: "
           << list_type->value_type()->ToString();
        return Status::NotImplemented(ss.str());
      }
      *output_type = PandasBlock::OBJECT;
    } break;
    case Type::DICTIONARY:
      *output_type = PandasBlock::CATEGORICAL;
      break;
    default:
      std::stringstream ss;
      ss << "No known equivalent Pandas block for Arrow data of type ";
      ss << col.type()->ToString() << " is known.";
      return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

// Construct the exact pandas 0.x "BlockManager" memory layout
//
// * For each column determine the correct output pandas type
// * Allocate 2D blocks (ncols x nrows) for each distinct data type in output
// * Allocate  block placement arrays
// * Write Arrow columns out into each slice of memory; populate block
// * placement arrays as we go
class DataFrameBlockCreator {
 public:
  explicit DataFrameBlockCreator(const PandasOptions& options,
                                 const std::shared_ptr<Table>& table, MemoryPool* pool)
      : table_(table), options_(options), pool_(pool) {}

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
      PandasBlock::type output_type = PandasBlock::OBJECT;
      RETURN_NOT_OK(GetPandasBlockType(*col, options_, &output_type));

      int block_placement = 0;
      std::shared_ptr<PandasBlock> block;
      if (output_type == PandasBlock::CATEGORICAL) {
        block = std::make_shared<CategoricalBlock>(options_, pool_, table_->num_rows());
        categorical_blocks_[i] = block;
      } else if (output_type == PandasBlock::DATETIME_WITH_TZ) {
        const auto& ts_type = static_cast<const TimestampType&>(*col->type());
        block = std::make_shared<DatetimeTZBlock>(options_, ts_type.timezone(),
                                                  table_->num_rows());
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
    for (const auto& it : this->type_counts_) {
      PandasBlock::type type = static_cast<PandasBlock::type>(it.first);
      std::shared_ptr<PandasBlock> block;
      RETURN_NOT_OK(
          MakeBlock(this->options_, type, this->table_->num_rows(), it.second, &block));
      this->blocks_[type] = block;
    }
    return Status::OK();
  }

  Status GetBlock(int i, std::shared_ptr<PandasBlock>* block) {
    PandasBlock::type output_type = this->column_types_[i];

    if (output_type == PandasBlock::CATEGORICAL) {
      auto it = this->categorical_blocks_.find(i);
      if (it == this->blocks_.end()) {
        return Status::KeyError("No categorical block allocated");
      }
      *block = it->second;
    } else if (output_type == PandasBlock::DATETIME_WITH_TZ) {
      auto it = this->datetimetz_blocks_.find(i);
      if (it == this->datetimetz_blocks_.end()) {
        return Status::KeyError("No datetimetz block allocated");
      }
      *block = it->second;
    } else {
      auto it = this->blocks_.find(output_type);
      if (it == this->blocks_.end()) {
        return Status::KeyError("No block allocated");
      }
      *block = it->second;
    }
    return Status::OK();
  }

  Status WriteTableToBlocks(int nthreads) {
    auto WriteColumn = [this](int i) {
      std::shared_ptr<PandasBlock> block;
      RETURN_NOT_OK(this->GetBlock(i, &block));
      return block->Write(this->table_->column(i), i, this->column_block_placement_[i]);
    };

    int num_tasks = table_->num_columns();
    nthreads = std::min<int>(nthreads, num_tasks);
    if (nthreads == 1) {
      for (int i = 0; i < num_tasks; ++i) {
        RETURN_NOT_OK(WriteColumn(i));
      }
    } else {
      RETURN_NOT_OK(ParallelFor(nthreads, num_tasks, WriteColumn));
    }
    return Status::OK();
  }

  Status AppendBlocks(const BlockMap& blocks, PyObject* list) {
    for (const auto& it : blocks) {
      PyObject* item;
      RETURN_NOT_OK(it.second->GetPyResult(&item));
      if (PyList_Append(list, item) < 0) {
        RETURN_IF_PYERROR();
      }

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

  PandasOptions options_;

  // Memory pool for dictionary encoding
  MemoryPool* pool_;

  // block type -> block
  BlockMap blocks_;

  // column number -> categorical block
  BlockMap categorical_blocks_;

  // column number -> datetimetz block
  BlockMap datetimetz_blocks_;
};

class ArrowDeserializer {
 public:
  ArrowDeserializer(PandasOptions options, const std::shared_ptr<Column>& col,
                    PyObject* py_ref)
      : col_(col), data_(*col->data().get()), options_(options), py_ref_(py_ref) {}

  Status AllocateOutput(int type) {
    PyAcquireGIL lock;

    result_ = NewArray1DFromType(col_->type().get(), type, col_->length(), nullptr);
    arr_ = reinterpret_cast<PyArrayObject*>(result_);
    return Status::OK();
  }

  template <int TYPE>
  Status ConvertValuesZeroCopy(PandasOptions options, int npy_type,
                               std::shared_ptr<Array> arr) {
    typedef typename internal::arrow_traits<TYPE>::T T;

    const auto& prim_arr = static_cast<const PrimitiveArray&>(*arr);
    auto in_values = reinterpret_cast<const T*>(prim_arr.raw_values());

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

    PyObject* base;
    if (py_ref_ == nullptr) {
      ArrowCapsule* capsule = new ArrowCapsule;
      capsule->array = arr;
      base = PyCapsule_New(reinterpret_cast<void*>(capsule), "arrow",
                           &ArrowCapsule_Destructor);
      if (base == nullptr) {
        delete capsule;
        RETURN_IF_PYERROR();
      }
    } else {
      base = py_ref_;
    }

    if (PyArray_SetBaseObject(arr_, base) == -1) {
      // Error occurred, trust that SetBaseObject set the error state
      return Status::OK();
    } else {
      // PyArray_SetBaseObject steals our reference to base
      Py_INCREF(base);
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
    using traits = internal::arrow_traits<TYPE>;

    typedef typename traits::T T;
    int npy_type = traits::npy_type;

    if (data_.num_chunks() == 1 && data_.null_count() == 0) {
      return ConvertValuesZeroCopy<TYPE>(options_, npy_type, data_.chunk(0));
    } else if (options_.zero_copy_only) {
      std::stringstream ss;
      ss << "Needed to copy " << data_.num_chunks() << " chunks with "
         << data_.null_count() << " nulls, but zero_copy_only was True";
      return Status::Invalid(ss.str());
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
    if (options_.zero_copy_only) {
      return Status::Invalid("Copy Needed, but zero_copy_only was True");
    }

    constexpr int TYPE = Type::type_id;
    using traits = internal::arrow_traits<TYPE>;
    using c_type = typename Type::c_type;

    typedef typename traits::T T;

    RETURN_NOT_OK(AllocateOutput(traits::npy_type));
    auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));

    constexpr T na_value = traits::na_value;
    constexpr int64_t kShift = traits::npy_shift;

    for (int c = 0; c < data_.num_chunks(); c++) {
      const auto& arr = static_cast<const PrimitiveArray&>(*data_.chunk(c));
      auto in_values = reinterpret_cast<const c_type*>(arr.raw_values());

      for (int64_t i = 0; i < arr.length(); ++i) {
        *out_values++ = arr.IsNull(i) ? na_value : static_cast<T>(in_values[i]) / kShift;
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
    using traits = internal::arrow_traits<TYPE>;

    typedef typename traits::T T;

    if (data_.num_chunks() == 1 && data_.null_count() == 0) {
      return ConvertValuesZeroCopy<TYPE>(options_, traits::npy_type, data_.chunk(0));
    } else if (options_.zero_copy_only) {
      std::stringstream ss;
      ss << "Needed to copy " << data_.num_chunks() << " chunks with "
         << data_.null_count() << " nulls, but zero_copy_only was True";
      return Status::Invalid(ss.str());
    }

    if (data_.null_count() > 0) {
      RETURN_NOT_OK(AllocateOutput(NPY_FLOAT64));
      auto out_values = reinterpret_cast<double*>(PyArray_DATA(arr_));
      ConvertIntegerWithNulls<T>(options_, data_, out_values);
    } else {
      RETURN_NOT_OK(AllocateOutput(traits::npy_type));
      auto out_values = reinterpret_cast<T*>(PyArray_DATA(arr_));
      ConvertIntegerNoNullsSameType<T>(options_, data_, out_values);
    }

    return Status::OK();
  }

  template <typename FUNCTOR>
  inline Status VisitObjects(FUNCTOR func) {
    if (options_.zero_copy_only) {
      return Status::Invalid("Object types need copies, but zero_copy_only was True");
    }
    RETURN_NOT_OK(AllocateOutput(NPY_OBJECT));
    auto out_values = reinterpret_cast<PyObject**>(PyArray_DATA(arr_));
    return func(options_, data_, out_values);
  }

  // UTF8 strings
  template <typename Type>
  typename std::enable_if<std::is_base_of<BinaryType, Type>::value, Status>::type Visit(
      const Type& type) {
    return VisitObjects(ConvertBinaryLike<Type>);
  }

  Status Visit(const NullType& type) { return VisitObjects(ConvertNulls); }

  // Fixed length binary strings
  Status Visit(const FixedSizeBinaryType& type) {
    return VisitObjects(ConvertFixedSizeBinary);
  }

  Status Visit(const DecimalType& type) { return VisitObjects(ConvertDecimals); }

  Status Visit(const Time32Type& type) { return VisitObjects(ConvertTimes<Time32Type>); }

  Status Visit(const Time64Type& type) { return VisitObjects(ConvertTimes<Time64Type>); }

  Status Visit(const StructType& type) { return VisitObjects(ConvertStruct); }

  // Boolean specialization
  Status Visit(const BooleanType& type) {
    if (options_.zero_copy_only) {
      return Status::Invalid("BooleanType needs copies, but zero_copy_only was True");
    } else if (data_.null_count() > 0) {
      return VisitObjects(ConvertBooleanWithNulls);
    } else {
      RETURN_NOT_OK(AllocateOutput(internal::arrow_traits<Type::BOOL>::npy_type));
      auto out_values = reinterpret_cast<uint8_t*>(PyArray_DATA(arr_));
      ConvertBooleanNoNulls(options_, data_, out_values);
    }
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    if (options_.zero_copy_only) {
      return Status::Invalid("ListType needs copies, but zero_copy_only was True");
    }
#define CONVERTVALUES_LISTSLIKE_CASE(ArrowType, ArrowEnum) \
  case Type::ArrowEnum:                                    \
    return ConvertListsLike<ArrowType>(options_, col_, out_values);

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
      CONVERTVALUES_LISTSLIKE_CASE(ListType, LIST)
      default: {
        std::stringstream ss;
        ss << "Not implemented type for lists: " << list_type->value_type()->ToString();
        return Status::NotImplemented(ss.str());
      }
    }
#undef CONVERTVALUES_LISTSLIKE_CASE
  }

  Status Visit(const DictionaryType& type) {
    auto block = std::make_shared<CategoricalBlock>(options_, nullptr, col_->length());
    RETURN_NOT_OK(block->Write(col_, 0, 0));

    PyAcquireGIL lock;
    result_ = PyDict_New();
    RETURN_IF_PYERROR();

    PyDict_SetItemString(result_, "indices", block->block_arr());
    PyDict_SetItemString(result_, "dictionary", block->dictionary());

    return Status::OK();
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
  PandasOptions options_;
  PyObject* py_ref_;
  PyArrayObject* arr_;
  PyObject* result_;
};

Status ConvertArrayToPandas(PandasOptions options, const std::shared_ptr<Array>& arr,
                            PyObject* py_ref, PyObject** out) {
  static std::string dummy_name = "dummy";
  auto field = std::make_shared<Field>(dummy_name, arr->type());
  auto col = std::make_shared<Column>(field, arr);
  return ConvertColumnToPandas(options, col, py_ref, out);
}

Status ConvertColumnToPandas(PandasOptions options, const std::shared_ptr<Column>& col,
                             PyObject* py_ref, PyObject** out) {
  ArrowDeserializer converter(options, col, py_ref);
  return converter.Convert(out);
}

Status ConvertTableToPandas(PandasOptions options, const std::shared_ptr<Table>& table,
                            int nthreads, MemoryPool* pool, PyObject** out) {
  DataFrameBlockCreator helper(options, table, pool);
  return helper.Convert(nthreads, out);
}

}  // namespace py
}  // namespace arrow
