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

#include "arrow/python/numpy_to_arrow.h"
#include "arrow/python/numpy_interop.h"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/visitor_inline.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/cast.h"

#include "arrow/python/common.h"
#include "arrow/python/config.h"
#include "arrow/python/helpers.h"
#include "arrow/python/iterators.h"
#include "arrow/python/numpy-internal.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/python_to_arrow.h"
#include "arrow/python/type_traits.h"
#include "arrow/python/util/datetime.h"

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

namespace py {

using internal::NumPyTypeSize;

// ----------------------------------------------------------------------
// Conversion utilities

namespace {

Status AllocateNullBitmap(MemoryPool* pool, int64_t length,
                          std::shared_ptr<ResizableBuffer>* out) {
  int64_t null_bytes = BitUtil::BytesForBits(length);
  std::shared_ptr<ResizableBuffer> null_bitmap;
  RETURN_NOT_OK(AllocateResizableBuffer(pool, null_bytes, &null_bitmap));

  // Padding zeroed by AllocateResizableBuffer
  memset(null_bitmap->mutable_data(), 0, static_cast<size_t>(null_bytes));
  *out = null_bitmap;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Conversion from NumPy-in-Pandas to Arrow null bitmap

template <int TYPE>
inline int64_t ValuesToBitmap(PyArrayObject* arr, uint8_t* bitmap) {
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

class NumPyNullsConverter {
 public:
  /// Convert the given array's null values to a null bitmap.
  /// The null bitmap is only allocated if null values are ever possible.
  static Status Convert(MemoryPool* pool, PyArrayObject* arr, bool from_pandas,
                        std::shared_ptr<ResizableBuffer>* out_null_bitmap_,
                        int64_t* out_null_count) {
    NumPyNullsConverter converter(pool, arr, from_pandas);
    RETURN_NOT_OK(VisitNumpyArrayInline(arr, &converter));
    *out_null_bitmap_ = converter.null_bitmap_;
    *out_null_count = converter.null_count_;
    return Status::OK();
  }

  template <int TYPE>
  Status Visit(PyArrayObject* arr) {
    typedef internal::npy_traits<TYPE> traits;

    const bool null_sentinels_possible =
        // Always treat Numpy's NaT as null
        TYPE == NPY_DATETIME ||
        // Observing pandas's null sentinels
        (from_pandas_ && traits::supports_nulls);

    if (null_sentinels_possible) {
      RETURN_NOT_OK(AllocateNullBitmap(pool_, PyArray_SIZE(arr), &null_bitmap_));
      null_count_ = ValuesToBitmap<TYPE>(arr, null_bitmap_->mutable_data());
    }
    return Status::OK();
  }

 protected:
  NumPyNullsConverter(MemoryPool* pool, PyArrayObject* arr, bool from_pandas)
      : pool_(pool),
        arr_(arr),
        from_pandas_(from_pandas),
        null_bitmap_data_(nullptr),
        null_count_(0) {}

  MemoryPool* pool_;
  PyArrayObject* arr_;
  bool from_pandas_;
  std::shared_ptr<ResizableBuffer> null_bitmap_;
  uint8_t* null_bitmap_data_;
  int64_t null_count_;
};

// Returns null count
int64_t MaskToBitmap(PyArrayObject* mask, int64_t length, uint8_t* bitmap) {
  int64_t null_count = 0;

  Ndarray1DIndexer<uint8_t> mask_values(mask);
  for (int i = 0; i < length; ++i) {
    if (mask_values[i]) {
      ++null_count;
      BitUtil::ClearBit(bitmap, i);
    } else {
      BitUtil::SetBit(bitmap, i);
    }
  }
  return null_count;
}

}  // namespace

// ----------------------------------------------------------------------
// Conversion from NumPy arrays (possibly originating from pandas) to Arrow
// format. Does not handle NPY_OBJECT dtype arrays; use ConvertPySequence for
// that

class NumPyConverter {
 public:
  NumPyConverter(MemoryPool* pool, PyObject* arr, PyObject* mo,
                 const std::shared_ptr<DataType>& type, bool from_pandas,
                 const compute::CastOptions& cast_options = compute::CastOptions())
      : pool_(pool),
        type_(type),
        arr_(reinterpret_cast<PyArrayObject*>(arr)),
        dtype_(PyArray_DESCR(arr_)),
        mask_(nullptr),
        from_pandas_(from_pandas),
        cast_options_(cast_options),
        null_bitmap_data_(nullptr),
        null_count_(0) {
    if (mo != nullptr && mo != Py_None) {
      mask_ = reinterpret_cast<PyArrayObject*>(mo);
    }
    length_ = static_cast<int64_t>(PyArray_SIZE(arr_));
    itemsize_ = static_cast<int>(PyArray_DESCR(arr_)->elsize);
    stride_ = static_cast<int64_t>(PyArray_STRIDES(arr_)[0]);
  }

  bool is_strided() const { return itemsize_ != stride_; }

  Status Convert();

  const ArrayVector& result() const { return out_arrays_; }

  template <typename T>
  typename std::enable_if<std::is_base_of<PrimitiveCType, T>::value ||
                              std::is_same<BooleanType, T>::value,
                          Status>::type
  Visit(const T& type) {
    return VisitNative<T>();
  }

  Status Visit(const HalfFloatType& type) { return VisitNative<UInt16Type>(); }

  Status Visit(const Date32Type& type) { return VisitNative<Date32Type>(); }
  Status Visit(const Date64Type& type) { return VisitNative<Date64Type>(); }
  Status Visit(const TimestampType& type) { return VisitNative<TimestampType>(); }
  Status Visit(const Time32Type& type) { return VisitNative<Int32Type>(); }
  Status Visit(const Time64Type& type) { return VisitNative<Int64Type>(); }

  Status Visit(const NullType& type) { return TypeNotImplemented(type.ToString()); }

  // NumPy ascii string arrays
  Status Visit(const BinaryType& type);

  // NumPy unicode arrays
  Status Visit(const StringType& type);

  Status Visit(const StructType& type);

  Status Visit(const FixedSizeBinaryType& type);

  Status Visit(const Decimal128Type& type) { return TypeNotImplemented(type.ToString()); }

  Status Visit(const DictionaryType& type) { return TypeNotImplemented(type.ToString()); }

  Status Visit(const NestedType& type) { return TypeNotImplemented(type.ToString()); }

 protected:
  Status InitNullBitmap() {
    RETURN_NOT_OK(AllocateNullBitmap(pool_, length_, &null_bitmap_));
    null_bitmap_data_ = null_bitmap_->mutable_data();
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
    out_arrays_.emplace_back(MakeArray(data));
    return Status::OK();
  }

  template <typename ArrowType>
  Status VisitNative() {
    if (mask_ != nullptr) {
      RETURN_NOT_OK(InitNullBitmap());
      null_count_ = MaskToBitmap(mask_, length_, null_bitmap_data_);
    } else {
      RETURN_NOT_OK(NumPyNullsConverter::Convert(pool_, arr_, from_pandas_, &null_bitmap_,
                                                 &null_count_));
    }

    std::shared_ptr<Buffer> data;
    RETURN_NOT_OK(ConvertData<ArrowType>(&data));

    auto arr_data = ArrayData::Make(type_, length_, {null_bitmap_, data}, null_count_, 0);
    return PushArray(arr_data);
  }

  Status TypeNotImplemented(std::string type_name) {
    std::stringstream ss;
    ss << "NumPyConverter doesn't implement <" << type_name << "> conversion. ";
    return Status::NotImplemented(ss.str());
  }

  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  PyArrayObject* arr_;
  PyArray_Descr* dtype_;
  PyArrayObject* mask_;
  int64_t length_;
  int64_t stride_;
  int itemsize_;

  bool from_pandas_;
  compute::CastOptions cast_options_;

  // Used in visitor pattern
  ArrayVector out_arrays_;

  std::shared_ptr<ResizableBuffer> null_bitmap_;
  uint8_t* null_bitmap_data_;
  int64_t null_count_;
};

Status NumPyConverter::Convert() {
  if (PyArray_NDIM(arr_) != 1) {
    return Status::Invalid("only handle 1-dimensional arrays");
  }

  DCHECK_NE(dtype_->type_num, NPY_OBJECT)
      << "This class does not handle NPY_OBJECT arrays";

  if (type_ == nullptr) {
    return Status::Invalid("Must pass data type for non-object arrays");
  }

  // Visit the type to perform conversion
  return VisitTypeInline(*type_, this);
}

namespace {

Status CastBuffer(const std::shared_ptr<DataType>& in_type,
                  const std::shared_ptr<Buffer>& input, const int64_t length,
                  const std::shared_ptr<Buffer>& valid_bitmap, const int64_t null_count,
                  const std::shared_ptr<DataType>& out_type,
                  const compute::CastOptions& cast_options, MemoryPool* pool,
                  std::shared_ptr<Buffer>* out) {
  // Must cast
  auto tmp_data = ArrayData::Make(in_type, length, {valid_bitmap, input}, null_count);

  std::shared_ptr<Array> tmp_array = MakeArray(tmp_data);
  std::shared_ptr<Array> casted_array;

  compute::FunctionContext context(pool);

  RETURN_NOT_OK(
      compute::Cast(&context, *tmp_array, out_type, cast_options, &casted_array));
  *out = casted_array->data()->buffers[1];
  return Status::OK();
}

template <typename FromType, typename ToType>
Status StaticCastBuffer(const Buffer& input, const int64_t length, MemoryPool* pool,
                        std::shared_ptr<Buffer>* out) {
  std::shared_ptr<Buffer> result;
  RETURN_NOT_OK(AllocateBuffer(pool, sizeof(ToType) * length, &result));

  auto in_values = reinterpret_cast<const FromType*>(input.data());
  auto out_values = reinterpret_cast<ToType*>(result->mutable_data());
  for (int64_t i = 0; i < length; ++i) {
    *out_values++ = static_cast<ToType>(*in_values++);
  }
  *out = result;
  return Status::OK();
}

template <typename T>
void CopyStridedBytewise(int8_t* input_data, int64_t length, int64_t stride,
                         T* output_data) {
  // Passing input_data as non-const is a concession to PyObject*
  for (int64_t i = 0; i < length; ++i) {
    memcpy(output_data + i, input_data, sizeof(T));
    input_data += stride;
  }
}

template <typename T>
void CopyStridedNatural(T* input_data, int64_t length, int64_t stride, T* output_data) {
  // Passing input_data as non-const is a concession to PyObject*
  int64_t j = 0;
  for (int64_t i = 0; i < length; ++i) {
    output_data[i] = input_data[j];
    j += stride;
  }
}

template <typename ArrowType>
Status CopyStridedArray(PyArrayObject* arr, const int64_t length, MemoryPool* pool,
                        std::shared_ptr<Buffer>* out) {
  using traits = internal::arrow_traits<ArrowType::type_id>;
  using T = typename traits::T;

  // Strided, must copy into new contiguous memory
  std::shared_ptr<Buffer> new_buffer;
  RETURN_NOT_OK(AllocateBuffer(pool, sizeof(T) * length, &new_buffer));

  const int64_t stride = PyArray_STRIDES(arr)[0];
  if (stride % sizeof(T) == 0) {
    const int64_t stride_elements = stride / sizeof(T);
    CopyStridedNatural(reinterpret_cast<T*>(PyArray_DATA(arr)), length, stride_elements,
                       reinterpret_cast<T*>(new_buffer->mutable_data()));
  } else {
    CopyStridedBytewise(reinterpret_cast<int8_t*>(PyArray_DATA(arr)), length, stride,
                        reinterpret_cast<T*>(new_buffer->mutable_data()));
  }

  *out = new_buffer;
  return Status::OK();
}

}  // namespace

template <typename ArrowType>
inline Status NumPyConverter::ConvertData(std::shared_ptr<Buffer>* data) {
  if (is_strided()) {
    RETURN_NOT_OK(CopyStridedArray<ArrowType>(arr_, length_, pool_, data));
  } else {
    // Can zero-copy
    *data = std::make_shared<NumPyBuffer>(reinterpret_cast<PyObject*>(arr_));
  }

  std::shared_ptr<DataType> input_type;
  RETURN_NOT_OK(NumPyDtypeToArrow(reinterpret_cast<PyObject*>(dtype_), &input_type));

  if (!input_type->Equals(*type_)) {
    RETURN_NOT_OK(CastBuffer(input_type, *data, length_, nullptr, 0, type_, cast_options_,
                             pool_, data));
  }

  return Status::OK();
}

template <>
inline Status NumPyConverter::ConvertData<BooleanType>(std::shared_ptr<Buffer>* data) {
  int64_t nbytes = BitUtil::BytesForBits(length_);
  std::shared_ptr<Buffer> buffer;
  RETURN_NOT_OK(AllocateBuffer(pool_, nbytes, &buffer));

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

template <>
inline Status NumPyConverter::ConvertData<Date32Type>(std::shared_ptr<Buffer>* data) {
  if (is_strided()) {
    RETURN_NOT_OK(CopyStridedArray<Date32Type>(arr_, length_, pool_, data));
  } else {
    // Can zero-copy
    *data = std::make_shared<NumPyBuffer>(reinterpret_cast<PyObject*>(arr_));
  }

  std::shared_ptr<DataType> input_type;

  auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(dtype_->c_metadata);
  if (dtype_->type_num == NPY_DATETIME) {
    // If we have inbound datetime64[D] data, this needs to be downcasted
    // separately here from int64_t to int32_t, because this data is not
    // supported in compute::Cast
    if (date_dtype->meta.base == NPY_FR_D) {
      // TODO(wesm): How pedantic do we really want to be about checking for int32
      // overflow here?
      Status s = StaticCastBuffer<int64_t, int32_t>(**data, length_, pool_, data);
      RETURN_NOT_OK(s);
    } else {
      RETURN_NOT_OK(NumPyDtypeToArrow(reinterpret_cast<PyObject*>(dtype_), &input_type));
      if (!input_type->Equals(*type_)) {
        // The null bitmap was already computed in VisitNative()
        RETURN_NOT_OK(CastBuffer(input_type, *data, length_, null_bitmap_, null_count_,
                                 type_, cast_options_, pool_, data));
      }
    }
  } else {
    RETURN_NOT_OK(NumPyDtypeToArrow(reinterpret_cast<PyObject*>(dtype_), &input_type));
    if (!input_type->Equals(*type_)) {
      RETURN_NOT_OK(CastBuffer(input_type, *data, length_, nullptr, 0, type_,
                               cast_options_, pool_, data));
    }
  }

  return Status::OK();
}

template <>
inline Status NumPyConverter::ConvertData<Date64Type>(std::shared_ptr<Buffer>* data) {
  if (is_strided()) {
    RETURN_NOT_OK(CopyStridedArray<Date64Type>(arr_, length_, pool_, data));
  } else {
    // Can zero-copy
    *data = std::make_shared<NumPyBuffer>(reinterpret_cast<PyObject*>(arr_));
  }

  constexpr int64_t kMillisecondsInDay = 86400000;

  std::shared_ptr<DataType> input_type;

  auto date_dtype = reinterpret_cast<PyArray_DatetimeDTypeMetaData*>(dtype_->c_metadata);
  if (dtype_->type_num == NPY_DATETIME) {
    // If we have inbound datetime64[D] data, this needs to be downcasted
    // separately here from int64_t to int32_t, because this data is not
    // supported in compute::Cast
    if (date_dtype->meta.base == NPY_FR_D) {
      std::shared_ptr<Buffer> result;
      RETURN_NOT_OK(AllocateBuffer(pool_, sizeof(int64_t) * length_, &result));

      auto in_values = reinterpret_cast<const int64_t*>((*data)->data());
      auto out_values = reinterpret_cast<int64_t*>(result->mutable_data());
      for (int64_t i = 0; i < length_; ++i) {
        *out_values++ = kMillisecondsInDay * (*in_values++);
      }
      *data = result;
    } else {
      RETURN_NOT_OK(NumPyDtypeToArrow(reinterpret_cast<PyObject*>(dtype_), &input_type));
      if (!input_type->Equals(*type_)) {
        // The null bitmap was already computed in VisitNative()
        RETURN_NOT_OK(CastBuffer(input_type, *data, length_, null_bitmap_, null_count_,
                                 type_, cast_options_, pool_, data));
      }
    }
  } else {
    RETURN_NOT_OK(NumPyDtypeToArrow(reinterpret_cast<PyObject*>(dtype_), &input_type));
    if (!input_type->Equals(*type_)) {
      RETURN_NOT_OK(CastBuffer(input_type, *data, length_, nullptr, 0, type_,
                               cast_options_, pool_, data));
    }
  }

  return Status::OK();
}

Status NumPyConverter::Visit(const BinaryType& type) {
  BinaryBuilder builder(pool_);

  auto data = reinterpret_cast<const uint8_t*>(PyArray_DATA(arr_));

  int item_length = 0;
  if (mask_ != nullptr) {
    Ndarray1DIndexer<uint8_t> mask_values(mask_);
    for (int64_t i = 0; i < length_; ++i) {
      if (mask_values[i]) {
        RETURN_NOT_OK(builder.AppendNull());
      } else {
        // This is annoying. NumPy allows strings to have nul-terminators, so
        // we must check for them here
        for (item_length = 0; item_length < itemsize_; ++item_length) {
          if (data[item_length] == 0) {
            break;
          }
        }
        RETURN_NOT_OK(builder.Append(data, item_length));
      }
      data += stride_;
    }
  } else {
    for (int64_t i = 0; i < length_; ++i) {
      for (item_length = 0; item_length < itemsize_; ++item_length) {
        // Look for nul-terminator
        if (data[item_length] == 0) {
          break;
        }
      }
      RETURN_NOT_OK(builder.Append(data, item_length));
      data += stride_;
    }
  }

  std::shared_ptr<Array> result;
  RETURN_NOT_OK(builder.Finish(&result));
  return PushArray(result->data());
}

Status NumPyConverter::Visit(const FixedSizeBinaryType& type) {
  auto byte_width = type.byte_width();

  if (itemsize_ != byte_width) {
    std::stringstream ss;
    ss << "Got bytestring of length " << itemsize_ << " (expected " << byte_width << ")";
    return Status::Invalid(ss.str());
  }

  FixedSizeBinaryBuilder builder(::arrow::fixed_size_binary(byte_width), pool_);
  auto data = reinterpret_cast<const uint8_t*>(PyArray_DATA(arr_));

  if (mask_ != nullptr) {
    Ndarray1DIndexer<uint8_t> mask_values(mask_);
    RETURN_NOT_OK(builder.AppendValues(data, length_, mask_values.data()));
  } else {
    RETURN_NOT_OK(builder.AppendValues(data, length_));
  }

  std::shared_ptr<Array> result;
  RETURN_NOT_OK(builder.Finish(&result));
  return PushArray(result->data());
}

namespace {

// NumPy unicode is UCS4/UTF32 always
constexpr int kNumPyUnicodeSize = 4;

Status AppendUTF32(const char* data, int itemsize, int byteorder,
                   StringBuilder* builder) {
  // The binary \x00\x00\x00\x00 indicates a nul terminator in NumPy unicode,
  // so we need to detect that here to truncate if necessary. Yep.
  int actual_length = 0;
  for (; actual_length < itemsize / kNumPyUnicodeSize; ++actual_length) {
    const char* code_point = data + actual_length * kNumPyUnicodeSize;
    if ((*code_point == '\0') && (*(code_point + 1) == '\0') &&
        (*(code_point + 2) == '\0') && (*(code_point + 3) == '\0')) {
      break;
    }
  }

  OwnedRef unicode_obj(PyUnicode_DecodeUTF32(data, actual_length * kNumPyUnicodeSize,
                                             nullptr, &byteorder));
  RETURN_IF_PYERROR();
  OwnedRef utf8_obj(PyUnicode_AsUTF8String(unicode_obj.obj()));
  if (utf8_obj.obj() == NULL) {
    PyErr_Clear();
    return Status::Invalid("failed converting UTF32 to UTF8");
  }

  const int32_t length = static_cast<int32_t>(PyBytes_GET_SIZE(utf8_obj.obj()));
  if (builder->value_data_length() + length > kBinaryMemoryLimit) {
    return Status::CapacityError("Encoded string length exceeds maximum size (2GB)");
  }
  return builder->Append(PyBytes_AS_STRING(utf8_obj.obj()), length);
}

}  // namespace

Status NumPyConverter::Visit(const StringType& type) {
  StringBuilder builder(pool_);

  auto data = reinterpret_cast<const char*>(PyArray_DATA(arr_));

  char numpy_byteorder = PyArray_DESCR(arr_)->byteorder;

  // For Python C API, -1 is little-endian, 1 is big-endian
  int byteorder = numpy_byteorder == '>' ? 1 : -1;

  PyAcquireGIL gil_lock;

  if (mask_ != nullptr) {
    Ndarray1DIndexer<uint8_t> mask_values(mask_);
    for (int64_t i = 0; i < length_; ++i) {
      if (mask_values[i]) {
        RETURN_NOT_OK(builder.AppendNull());
      } else {
        RETURN_NOT_OK(AppendUTF32(data, itemsize_, byteorder, &builder));
      }
      data += stride_;
    }
  } else {
    for (int64_t i = 0; i < length_; ++i) {
      RETURN_NOT_OK(AppendUTF32(data, itemsize_, byteorder, &builder));
      data += stride_;
    }
  }

  std::shared_ptr<Array> result;
  RETURN_NOT_OK(builder.Finish(&result));
  return PushArray(result->data());
}

Status NumPyConverter::Visit(const StructType& type) {
  std::vector<NumPyConverter> sub_converters;
  std::vector<OwnedRefNoGIL> sub_arrays;

  {
    PyAcquireGIL gil_lock;

    // Create converters for each struct type field
    if (dtype_->fields == NULL || !PyDict_Check(dtype_->fields)) {
      return Status::TypeError("Expected struct array");
    }

    for (auto field : type.children()) {
      PyObject* tup = PyDict_GetItemString(dtype_->fields, field->name().c_str());
      if (tup == NULL) {
        std::stringstream ss;
        ss << "Missing field '" << field->name() << "' in struct array";
        return Status::TypeError(ss.str());
      }
      PyArray_Descr* sub_dtype =
          reinterpret_cast<PyArray_Descr*>(PyTuple_GET_ITEM(tup, 0));
      DCHECK(PyArray_DescrCheck(sub_dtype));
      int offset = static_cast<int>(PyLong_AsLong(PyTuple_GET_ITEM(tup, 1)));
      RETURN_IF_PYERROR();
      Py_INCREF(sub_dtype); /* PyArray_GetField() steals ref */
      PyObject* sub_array = PyArray_GetField(arr_, sub_dtype, offset);
      RETURN_IF_PYERROR();
      sub_arrays.emplace_back(sub_array);
      sub_converters.emplace_back(pool_, sub_array, nullptr /* mask */, field->type(),
                                  from_pandas_);
    }
  }

  std::vector<ArrayVector> groups;
  int64_t null_count = 0;

  // Compute null bitmap and store it as a Boolean Array to include it
  // in the rechunking below
  {
    if (mask_ != nullptr) {
      RETURN_NOT_OK(InitNullBitmap());
      null_count = MaskToBitmap(mask_, length_, null_bitmap_data_);
    }
    groups.push_back({std::make_shared<BooleanArray>(length_, null_bitmap_)});
  }

  // Convert child data
  for (auto& converter : sub_converters) {
    RETURN_NOT_OK(converter.Convert());
    groups.push_back(converter.result());
    const auto& group = groups.back();
    int64_t n = 0;
    for (const auto& array : group) {
      n += array->length();
    }
  }
  // Ensure the different array groups are chunked consistently
  groups = ::arrow::internal::RechunkArraysConsistently(groups);
  for (const auto& group : groups) {
    int64_t n = 0;
    for (const auto& array : group) {
      n += array->length();
    }
  }

  // Make struct array chunks by combining groups
  size_t ngroups = groups.size();
  size_t nchunks = groups[0].size();
  for (size_t chunk = 0; chunk < nchunks; chunk++) {
    // First group has the null bitmaps as Boolean Arrays
    const auto& null_data = groups[0][chunk]->data();
    DCHECK_EQ(null_data->type->id(), Type::BOOL);
    DCHECK_EQ(null_data->buffers.size(), 2);
    const auto& null_buffer = null_data->buffers[1];
    // Careful: the rechunked null bitmap may have a non-zero offset
    // to its buffer, and it may not even start on a byte boundary
    int64_t null_offset = null_data->offset;
    std::shared_ptr<Buffer> fixed_null_buffer;

    if (!null_buffer) {
      fixed_null_buffer = null_buffer;
    } else if (null_offset % 8 == 0) {
      fixed_null_buffer =
          std::make_shared<Buffer>(null_buffer,
                                   // byte offset
                                   null_offset / 8,
                                   // byte size
                                   BitUtil::BytesForBits(null_data->length));
    } else {
      RETURN_NOT_OK(CopyBitmap(pool_, null_buffer->data(), null_offset, null_data->length,
                               &fixed_null_buffer));
    }

    // Create struct array chunk and populate it
    auto arr_data =
        ArrayData::Make(type_, null_data->length, null_count ? kUnknownNullCount : 0, 0);
    arr_data->buffers.push_back(fixed_null_buffer);
    // Append child chunks
    for (size_t i = 1; i < ngroups; i++) {
      arr_data->child_data.push_back(groups[i][chunk]->data());
    }
    RETURN_NOT_OK(PushArray(arr_data));
  }

  return Status::OK();
}

Status NdarrayToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo, bool from_pandas,
                      const std::shared_ptr<DataType>& type,
                      const compute::CastOptions& cast_options,
                      std::shared_ptr<ChunkedArray>* out) {
  if (!PyArray_Check(ao)) {
    return Status::Invalid("Input object was not a NumPy array");
  }

  PyArrayObject* arr = reinterpret_cast<PyArrayObject*>(ao);

  if (PyArray_DESCR(arr)->type_num == NPY_OBJECT) {
    PyConversionOptions py_options;
    py_options.type = type;
    py_options.from_pandas = from_pandas;
    return ConvertPySequence(ao, mo, py_options, out);
  }

  NumPyConverter converter(pool, ao, mo, type, from_pandas, cast_options);
  RETURN_NOT_OK(converter.Convert());
  const auto& output_arrays = converter.result();
  DCHECK_GT(output_arrays.size(), 0);
  *out = std::make_shared<ChunkedArray>(output_arrays);
  return Status::OK();
}

Status NdarrayToArrow(MemoryPool* pool, PyObject* ao, PyObject* mo, bool from_pandas,
                      const std::shared_ptr<DataType>& type,
                      std::shared_ptr<ChunkedArray>* out) {
  return NdarrayToArrow(pool, ao, mo, from_pandas, type, compute::CastOptions(), out);
}

}  // namespace py
}  // namespace arrow
