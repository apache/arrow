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

#include "arrow/python/platform.h"

#include <datetime.h>

#include <algorithm>
#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/python/builtin_convert.h"

#include "arrow/api.h"
#include "arrow/status.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#include "arrow/python/helpers.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/util/datetime.h"

namespace arrow {
namespace py {

Status InvalidConversion(PyObject* obj, const std::string& expected_types,
                         std::ostream* out) {
  OwnedRef type(PyObject_Type(obj));
  RETURN_IF_PYERROR();
  DCHECK_NE(type.obj(), nullptr);

  OwnedRef type_name(PyObject_GetAttrString(type.obj(), "__name__"));
  RETURN_IF_PYERROR();
  DCHECK_NE(type_name.obj(), nullptr);

  PyObjectStringify bytestring(type_name.obj());
  RETURN_IF_PYERROR();

  const char* bytes = bytestring.bytes;
  DCHECK_NE(bytes, nullptr) << "bytes from type(...).__name__ were null";

  Py_ssize_t size = bytestring.size;

  std::string cpp_type_name(bytes, size);

  (*out) << "Got Python object of type " << cpp_type_name
         << " but can only handle these types: " << expected_types;
  return Status::OK();
}

class ScalarVisitor {
 public:
  ScalarVisitor()
      : total_count_(0),
        none_count_(0),
        bool_count_(0),
        int_count_(0),
        date_count_(0),
        timestamp_count_(0),
        float_count_(0),
        binary_count_(0),
        unicode_count_(0) {}

  Status Visit(PyObject* obj) {
    ++total_count_;
    if (obj == Py_None) {
      ++none_count_;
    } else if (PyBool_Check(obj)) {
      ++bool_count_;
    } else if (PyFloat_Check(obj)) {
      ++float_count_;
    } else if (internal::IsPyInteger(obj)) {
      ++int_count_;
    } else if (PyDate_CheckExact(obj)) {
      ++date_count_;
    } else if (PyDateTime_CheckExact(obj)) {
      ++timestamp_count_;
    } else if (PyBytes_Check(obj)) {
      ++binary_count_;
    } else if (PyUnicode_Check(obj)) {
      ++unicode_count_;
    } else if (PyArray_CheckAnyScalarExact(obj)) {
      std::shared_ptr<DataType> type;
      RETURN_NOT_OK(NumPyDtypeToArrow(PyArray_DescrFromScalar(obj), &type));
      if (is_integer(type->id())) {
        ++int_count_;
      } else if (is_floating(type->id())) {
        ++float_count_;
      } else if (type->id() == Type::TIMESTAMP) {
        ++timestamp_count_;
      } else {
        std::ostringstream ss;
        ss << "Found a NumPy scalar with Arrow dtype that we cannot handle: ";
        ss << type->ToString();
        return Status::Invalid(ss.str());
      }
    } else {
      // TODO(wesm): accumulate error information somewhere
      static std::string supported_types =
          "bool, float, integer, date, datetime, bytes, unicode";
      std::stringstream ss;
      ss << "Error inferring Arrow data type for collection of Python objects. ";
      RETURN_NOT_OK(InvalidConversion(obj, supported_types, &ss));
      return Status::Invalid(ss.str());
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> GetType() {
    // TODO(wesm): handling mixed-type cases
    if (float_count_) {
      return float64();
    } else if (int_count_) {
      // TODO(wesm): tighter type later
      return int64();
    } else if (date_count_) {
      return date64();
    } else if (timestamp_count_) {
      return timestamp(TimeUnit::MICRO);
    } else if (bool_count_) {
      return boolean();
    } else if (binary_count_) {
      return binary();
    } else if (unicode_count_) {
      return utf8();
    } else {
      return null();
    }
  }

  int64_t total_count() const { return total_count_; }

 private:
  int64_t total_count_;
  int64_t none_count_;
  int64_t bool_count_;
  int64_t int_count_;
  int64_t date_count_;
  int64_t timestamp_count_;
  int64_t float_count_;
  int64_t binary_count_;
  int64_t unicode_count_;
  // Place to accumulate errors
  // std::vector<Status> errors_;
};

static constexpr int MAX_NESTING_LEVELS = 32;

// SeqVisitor is used to infer the type.
class SeqVisitor {
 public:
  SeqVisitor() : max_nesting_level_(0), max_observed_level_(0), nesting_histogram_() {
    std::fill(nesting_histogram_, nesting_histogram_ + MAX_NESTING_LEVELS, 0);
  }

  // co-recursive with VisitElem
  Status Visit(PyObject* obj, int level = 0) {
    max_nesting_level_ = std::max(max_nesting_level_, level);

    // Loop through a sequence
    if (!PySequence_Check(obj))
      return Status::TypeError("Object is not a sequence or iterable");

    Py_ssize_t size = PySequence_Size(obj);
    for (int64_t i = 0; i < size; ++i) {
      OwnedRef ref;
      if (PyArray_Check(obj)) {
        auto array = reinterpret_cast<PyArrayObject*>(obj);
        auto ptr = reinterpret_cast<const char*>(PyArray_GETPTR1(array, i));

        ref.reset(PyArray_GETITEM(array, ptr));
        RETURN_IF_PYERROR();

        RETURN_NOT_OK(VisitElem(ref, level));
      } else {
        ref.reset(PySequence_GetItem(obj, i));
        RETURN_IF_PYERROR();
        RETURN_NOT_OK(VisitElem(ref, level));
      }
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> GetType() {
    // If all the non-list inputs were null (or there were no inputs)
    if (scalars_.total_count() == 0) {
      if (max_nesting_level_ == 0) {
        // If its just a single empty list or list of nulls, return null.
        return null();
      } else {
        // Error, if we have nesting but no concrete base type.
        return nullptr;
      }
    } else {
      // Lists of Lists of [X]
      std::shared_ptr<DataType> result = scalars_.GetType();
      for (int i = 0; i < max_nesting_level_; ++i) {
        result = std::make_shared<ListType>(result);
      }
      return result;
    }
  }

  Status Validate() const {
    if (scalars_.total_count() > 0) {
      if (num_nesting_levels() > 1) {
        return Status::Invalid("Mixed nesting levels not supported");
        // If the nesting goes deeper than the deepest scalar
      } else if (max_observed_level_ < max_nesting_level_) {
        return Status::Invalid("Mixed nesting levels not supported");
      }
    }
    return Status::OK();
  }

  // Returns the number of nesting levels which have scalar elements.
  int num_nesting_levels() const {
    int result = 0;
    for (int i = 0; i < MAX_NESTING_LEVELS; ++i) {
      if (nesting_histogram_[i] > 0) {
        ++result;
      }
    }
    return result;
  }

 private:
  ScalarVisitor scalars_;

  // Track observed
  // Deapest nesting level (irregardless of scalars)
  int max_nesting_level_;
  int max_observed_level_;

  // Number of scalar elements at each nesting level.
  // (TOOD: We really only need to know if a scalar is present, not the count).
  int nesting_histogram_[MAX_NESTING_LEVELS];

  // Visits a specific element (inner part of the loop).
  Status VisitElem(const OwnedRef& item_ref, int level) {
    DCHECK_NE(item_ref.obj(), NULLPTR);
    if (PyList_Check(item_ref.obj())) {
      RETURN_NOT_OK(Visit(item_ref.obj(), level + 1));
    } else if (PyDict_Check(item_ref.obj())) {
      return Status::NotImplemented("No type inference for dicts");
    } else {
      // We permit nulls at any level of nesting, but they aren't treated like
      // other scalar values as far as the checking for mixed nesting structure
      if (item_ref.obj() != Py_None) {
        ++nesting_histogram_[level];
      }
      if (level > max_observed_level_) {
        max_observed_level_ = level;
      }
      return scalars_.Visit(item_ref.obj());
    }
    return Status::OK();
  }
};

// Convert *obj* to a sequence if necessary
// Fill *size* to its length.  If >= 0 on entry, *size* is an upper size
// bound that may lead to truncation.
Status ConvertToSequenceAndInferSize(PyObject* obj, PyObject** seq, int64_t* size) {
  if (PySequence_Check(obj)) {
    // obj is already a sequence
    int64_t real_size = static_cast<int64_t>(PySequence_Size(obj));
    if (*size < 0) {
      *size = real_size;
    } else {
      *size = std::min(real_size, *size);
    }
    Py_INCREF(obj);
    *seq = obj;
  } else if (*size < 0) {
    // unknown size, exhaust iterator
    *seq = PySequence_List(obj);
    RETURN_IF_PYERROR();
    *size = static_cast<int64_t>(PyList_GET_SIZE(*seq));
  } else {
    // size is known but iterator could be infinite
    Py_ssize_t i, n = *size;
    PyObject* iter = PyObject_GetIter(obj);
    RETURN_IF_PYERROR();
    OwnedRef iter_ref(iter);
    PyObject* lst = PyList_New(n);
    RETURN_IF_PYERROR();
    for (i = 0; i < n; i++) {
      PyObject* item = PyIter_Next(iter);
      if (!item) break;
      PyList_SET_ITEM(lst, i, item);
    }
    // Shrink list if len(iterator) < size
    if (i < n && PyList_SetSlice(lst, i, n, NULL)) {
      Py_DECREF(lst);
      return Status::UnknownError("failed to resize list");
    }
    *seq = lst;
    *size = std::min<int64_t>(i, *size);
  }
  return Status::OK();
}

// Non-exhaustive type inference
Status InferArrowType(PyObject* obj, std::shared_ptr<DataType>* out_type) {
  PyDateTime_IMPORT;
  SeqVisitor seq_visitor;
  RETURN_NOT_OK(seq_visitor.Visit(obj));
  RETURN_NOT_OK(seq_visitor.Validate());

  *out_type = seq_visitor.GetType();
  if (*out_type == nullptr) {
    return Status::TypeError("Unable to determine data type");
  }

  return Status::OK();
}

Status InferArrowTypeAndSize(PyObject* obj, int64_t* size,
                             std::shared_ptr<DataType>* out_type) {
  if (!PySequence_Check(obj)) {
    return Status::TypeError("Object is not a sequence");
  }
  *size = static_cast<int64_t>(PySequence_Size(obj));

  // For 0-length sequences, refuse to guess
  if (*size == 0) {
    *out_type = null();
    return Status::OK();
  }
  RETURN_NOT_OK(InferArrowType(obj, out_type));

  return Status::OK();
}

// Marshal Python sequence (list, tuple, etc.) to Arrow array
class SeqConverter {
 public:
  virtual Status Init(ArrayBuilder* builder) {
    builder_ = builder;
    return Status::OK();
  }

  // Append a single (non-sequence) Python datum to the underlying builder
  virtual Status AppendSingle(PyObject* obj) = 0;

  // Append the contents of a Python sequence to the underlying builder
  virtual Status AppendMultiple(PyObject* seq, int64_t size) = 0;

  virtual ~SeqConverter() = default;

 protected:
  ArrayBuilder* builder_;
};

template <typename BuilderType>
class TypedConverter : public SeqConverter {
 public:
  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    typed_builder_ = static_cast<BuilderType*>(builder);
    return Status::OK();
  }

 protected:
  BuilderType* typed_builder_;
};

// We use the CRTP trick here to devirtualize the AppendItem() and AppendNull()
// method calls.
template <typename BuilderType, class Derived>
class TypedConverterVisitor : public TypedConverter<BuilderType> {
 public:
  Status AppendSingle(PyObject* obj) override {
    if (obj == Py_None) {
      return static_cast<Derived*>(this)->AppendNull();
    } else {
      return static_cast<Derived*>(this)->AppendItem(obj);
    }
  }

  Status AppendMultiple(PyObject* obj, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->typed_builder_->Reserve(size));
    // Iterate over the items adding each one
    if (PySequence_Check(obj)) {
      for (int64_t i = 0; i < size; ++i) {
        OwnedRef ref(PySequence_GetItem(obj, i));
        RETURN_NOT_OK(static_cast<Derived*>(this)->AppendSingle(ref.obj()));
      }
    } else {
      return Status::TypeError("Object is not a sequence");
    }
    return Status::OK();
  }

  // Append a missing item (default implementation)
  Status AppendNull() { return this->typed_builder_->AppendNull(); }
};

class NullConverter : public TypedConverterVisitor<NullBuilder, NullConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    return Status::Invalid("NullConverter: passed non-None value");
  }
};

class BoolConverter : public TypedConverterVisitor<BooleanBuilder, BoolConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) { return typed_builder_->Append(obj == Py_True); }
};

class Int8Converter : public TypedConverterVisitor<Int8Builder, Int8Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<int64_t>(PyLong_AsLongLong(obj));

    if (ARROW_PREDICT_FALSE(val > std::numeric_limits<int8_t>::max() ||
                            val < std::numeric_limits<int8_t>::min())) {
      return Status::Invalid(
          "Cannot coerce values to array type that would "
          "lose data");
    }
    RETURN_IF_PYERROR();
    return typed_builder_->Append(static_cast<int8_t>(val));
  }
};

class Int16Converter : public TypedConverterVisitor<Int16Builder, Int16Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<int64_t>(PyLong_AsLongLong(obj));

    if (ARROW_PREDICT_FALSE(val > std::numeric_limits<int16_t>::max() ||
                            val < std::numeric_limits<int16_t>::min())) {
      return Status::Invalid(
          "Cannot coerce values to array type that would "
          "lose data");
    }
    RETURN_IF_PYERROR();
    return typed_builder_->Append(static_cast<int16_t>(val));
  }
};

class Int32Converter : public TypedConverterVisitor<Int32Builder, Int32Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<int64_t>(PyLong_AsLongLong(obj));

    if (ARROW_PREDICT_FALSE(val > std::numeric_limits<int32_t>::max() ||
                            val < std::numeric_limits<int32_t>::min())) {
      return Status::Invalid(
          "Cannot coerce values to array type that would "
          "lose data");
    }
    RETURN_IF_PYERROR();
    return typed_builder_->Append(static_cast<int32_t>(val));
  }
};

class Int64Converter : public TypedConverterVisitor<Int64Builder, Int64Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<int64_t>(PyLong_AsLongLong(obj));
    RETURN_IF_PYERROR();
    return typed_builder_->Append(val);
  }
};

class UInt8Converter : public TypedConverterVisitor<UInt8Builder, UInt8Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<uint64_t>(PyLong_AsLongLong(obj));
    RETURN_IF_PYERROR();

    if (ARROW_PREDICT_FALSE(val > std::numeric_limits<uint8_t>::max())) {
      return Status::Invalid(
          "Cannot coerce values to array type that would "
          "lose data");
    }
    return typed_builder_->Append(static_cast<uint8_t>(val));
  }
};

class UInt16Converter : public TypedConverterVisitor<UInt16Builder, UInt16Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<uint64_t>(PyLong_AsLongLong(obj));
    RETURN_IF_PYERROR();

    if (ARROW_PREDICT_FALSE(val > std::numeric_limits<uint16_t>::max())) {
      return Status::Invalid(
          "Cannot coerce values to array type that would "
          "lose data");
    }
    return typed_builder_->Append(static_cast<uint16_t>(val));
  }
};

class UInt32Converter : public TypedConverterVisitor<UInt32Builder, UInt32Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<uint64_t>(PyLong_AsLongLong(obj));
    RETURN_IF_PYERROR();

    if (ARROW_PREDICT_FALSE(val > std::numeric_limits<uint32_t>::max())) {
      return Status::Invalid(
          "Cannot coerce values to array type that would "
          "lose data");
    }
    return typed_builder_->Append(static_cast<uint32_t>(val));
  }
};

class UInt64Converter : public TypedConverterVisitor<UInt64Builder, UInt64Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    const auto val = static_cast<int64_t>(PyLong_AsUnsignedLongLong(obj));
    RETURN_IF_PYERROR();
    return typed_builder_->Append(val);
  }
};

class Date32Converter : public TypedConverterVisitor<Date32Builder, Date32Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    int32_t t;
    if (PyDate_Check(obj)) {
      auto pydate = reinterpret_cast<PyDateTime_Date*>(obj);
      t = static_cast<int32_t>(PyDate_to_s(pydate));
    } else {
      const auto casted_val = static_cast<int64_t>(PyLong_AsLongLong(obj));
      RETURN_IF_PYERROR();
      if (casted_val > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Integer as date32 larger than INT32_MAX");
      }
      t = static_cast<int32_t>(casted_val);
    }
    return typed_builder_->Append(t);
  }
};

class Date64Converter : public TypedConverterVisitor<Date64Builder, Date64Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    int64_t t;
    if (PyDate_Check(obj)) {
      auto pydate = reinterpret_cast<PyDateTime_Date*>(obj);
      t = PyDate_to_ms(pydate);
    } else {
      t = static_cast<int64_t>(PyLong_AsLongLong(obj));
      RETURN_IF_PYERROR();
    }
    return typed_builder_->Append(t);
  }
};

class TimestampConverter
    : public TypedConverterVisitor<TimestampBuilder, TimestampConverter> {
 public:
  explicit TimestampConverter(TimeUnit::type unit) : unit_(unit) {}

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    int64_t t;
    if (PyDateTime_Check(obj)) {
      auto pydatetime = reinterpret_cast<PyDateTime_DateTime*>(obj);

      switch (unit_) {
        case TimeUnit::SECOND:
          t = PyDateTime_to_s(pydatetime);
          break;
        case TimeUnit::MILLI:
          t = PyDateTime_to_ms(pydatetime);
          break;
        case TimeUnit::MICRO:
          t = PyDateTime_to_us(pydatetime);
          break;
        case TimeUnit::NANO:
          t = PyDateTime_to_ns(pydatetime);
          break;
        default:
          return Status::UnknownError("Invalid time unit");
      }
    } else if (PyArray_CheckAnyScalarExact(obj)) {
      // numpy.datetime64
      std::shared_ptr<DataType> type;
      RETURN_NOT_OK(NumPyDtypeToArrow(PyArray_DescrFromScalar(obj), &type));
      if (type->id() != Type::TIMESTAMP) {
        std::ostringstream ss;
        ss << "Expected np.datetime64 but got: ";
        ss << type->ToString();
        return Status::Invalid(ss.str());
      }
      const TimestampType& ttype = static_cast<const TimestampType&>(*type);
      if (unit_ != ttype.unit()) {
        return Status::NotImplemented(
            "Cannot convert NumPy datetime64 objects with differing unit");
      }

      t = reinterpret_cast<PyDatetimeScalarObject*>(obj)->obval;
    } else {
      t = static_cast<int64_t>(PyLong_AsLongLong(obj));
      RETURN_IF_PYERROR();
    }
    return typed_builder_->Append(t);
  }

 private:
  TimeUnit::type unit_;
};

class Float32Converter : public TypedConverterVisitor<FloatBuilder, Float32Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    float val = static_cast<float>(PyFloat_AsDouble(obj));
    RETURN_IF_PYERROR();
    return typed_builder_->Append(val);
  }
};

class DoubleConverter : public TypedConverterVisitor<DoubleBuilder, DoubleConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    double val = PyFloat_AsDouble(obj);
    RETURN_IF_PYERROR();
    return typed_builder_->Append(val);
  }
};

class BytesConverter : public TypedConverterVisitor<BinaryBuilder, BytesConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    PyObject* bytes_obj;
    const char* bytes;
    Py_ssize_t length;
    OwnedRef tmp;

    if (PyUnicode_Check(obj)) {
      tmp.reset(PyUnicode_AsUTF8String(obj));
      RETURN_IF_PYERROR();
      bytes_obj = tmp.obj();
    } else if (PyBytes_Check(obj)) {
      bytes_obj = obj;
    } else {
      std::stringstream ss;
      ss << "Error converting to Binary type: ";
      RETURN_NOT_OK(InvalidConversion(obj, "bytes", &ss));
      return Status::Invalid(ss.str());
    }
    // No error checking
    length = PyBytes_GET_SIZE(bytes_obj);
    bytes = PyBytes_AS_STRING(bytes_obj);
    return typed_builder_->Append(bytes, static_cast<int32_t>(length));
  }
};

class FixedWidthBytesConverter
    : public TypedConverterVisitor<FixedSizeBinaryBuilder, FixedWidthBytesConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    PyObject* bytes_obj;
    OwnedRef tmp;
    Py_ssize_t expected_length =
        std::dynamic_pointer_cast<FixedSizeBinaryType>(typed_builder_->type())
            ->byte_width();
    if (PyUnicode_Check(obj)) {
      tmp.reset(PyUnicode_AsUTF8String(obj));
      RETURN_IF_PYERROR();
      bytes_obj = tmp.obj();
    } else if (PyBytes_Check(obj)) {
      bytes_obj = obj;
    } else {
      std::stringstream ss;
      ss << "Error converting to FixedSizeBinary type: ";
      RETURN_NOT_OK(InvalidConversion(obj, "bytes", &ss));
      return Status::Invalid(ss.str());
    }
    // No error checking
    RETURN_NOT_OK(CheckPythonBytesAreFixedLength(bytes_obj, expected_length));
    return typed_builder_->Append(
        reinterpret_cast<const uint8_t*>(PyBytes_AS_STRING(bytes_obj)));
  }
};

class UTF8Converter : public TypedConverterVisitor<StringBuilder, UTF8Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    PyObject* bytes_obj;
    OwnedRef tmp;
    const char* bytes;
    Py_ssize_t length;

    if (PyBytes_Check(obj)) {
      tmp.reset(
          PyUnicode_FromStringAndSize(PyBytes_AS_STRING(obj), PyBytes_GET_SIZE(obj)));
      RETURN_IF_PYERROR();
      bytes_obj = obj;
    } else if (!PyUnicode_Check(obj)) {
      OwnedRef repr(PyObject_Repr(obj));
      PyObjectStringify stringified(repr.obj());
      std::stringstream ss;
      ss << "Non bytes/unicode value encountered: " << stringified.bytes;
      return Status::Invalid(ss.str());
    } else {
      tmp.reset(PyUnicode_AsUTF8String(obj));
      RETURN_IF_PYERROR();
      bytes_obj = tmp.obj();
    }

    // No error checking
    length = PyBytes_GET_SIZE(bytes_obj);
    bytes = PyBytes_AS_STRING(bytes_obj);
    return typed_builder_->Append(bytes, static_cast<int32_t>(length));
  }
};

class ListConverter : public TypedConverterVisitor<ListBuilder, ListConverter> {
 public:
  Status Init(ArrayBuilder* builder) override;

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    RETURN_NOT_OK(typed_builder_->Append());
    const auto list_size = static_cast<int64_t>(PySequence_Size(obj));
    return value_converter_->AppendMultiple(obj, list_size);
  }

 protected:
  std::unique_ptr<SeqConverter> value_converter_;
};

class StructConverter : public TypedConverterVisitor<StructBuilder, StructConverter> {
 public:
  Status Init(ArrayBuilder* builder) override;

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    RETURN_NOT_OK(typed_builder_->Append());
    // Note heterogenous sequences are not allowed
    if (ARROW_PREDICT_FALSE(source_kind_ == UNKNOWN)) {
      if (PyDict_Check(obj)) {
        source_kind_ = DICTS;
      } else if (PyTuple_Check(obj)) {
        source_kind_ = TUPLES;
      }
    }
    if (PyDict_Check(obj) && source_kind_ == DICTS) {
      return AppendDictItem(obj);
    } else if (PyTuple_Check(obj) && source_kind_ == TUPLES) {
      return AppendTupleItem(obj);
    } else {
      return Status::TypeError("Expected sequence of dicts or tuples for struct type");
    }
  }

  // Append a missing item
  Status AppendNull() {
    RETURN_NOT_OK(typed_builder_->AppendNull());
    // Need to also insert a missing item on all child builders
    // (compare with ListConverter)
    for (int i = 0; i < num_fields_; i++) {
      RETURN_NOT_OK(value_converters_[i]->AppendSingle(Py_None));
    }
    return Status::OK();
  }

 protected:
  Status AppendDictItem(PyObject* obj) {
    // NOTE we're ignoring any extraneous dict items
    for (int i = 0; i < num_fields_; i++) {
      PyObject* nameobj = PyList_GET_ITEM(field_name_list_.obj(), i);
      PyObject* valueobj = PyDict_GetItem(obj, nameobj);  // borrowed
      RETURN_IF_PYERROR();
      RETURN_NOT_OK(value_converters_[i]->AppendSingle(valueobj ? valueobj : Py_None));
    }
    return Status::OK();
  }

  Status AppendTupleItem(PyObject* obj) {
    if (PyTuple_GET_SIZE(obj) != num_fields_) {
      return Status::Invalid("Tuple size must be equal to number of struct fields");
    }
    for (int i = 0; i < num_fields_; i++) {
      PyObject* valueobj = PyTuple_GET_ITEM(obj, i);
      RETURN_NOT_OK(value_converters_[i]->AppendSingle(valueobj));
    }
    return Status::OK();
  }

  std::vector<std::unique_ptr<SeqConverter>> value_converters_;
  OwnedRef field_name_list_;
  int num_fields_;
  // Whether we're converting from a sequence of dicts or tuples
  enum { UNKNOWN, DICTS, TUPLES } source_kind_ = UNKNOWN;
};

class DecimalConverter
    : public TypedConverterVisitor<arrow::Decimal128Builder, DecimalConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    /// TODO(phillipc): Check for nan?
    Decimal128 value;
    const auto& type = static_cast<const DecimalType&>(*typed_builder_->type());
    RETURN_NOT_OK(internal::DecimalFromPythonDecimal(obj, type, &value));
    return typed_builder_->Append(value);
  }
};

// Dynamic constructor for sequence converters
std::unique_ptr<SeqConverter> GetConverter(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::NA:
      return std::unique_ptr<SeqConverter>(new NullConverter);
    case Type::BOOL:
      return std::unique_ptr<SeqConverter>(new BoolConverter);
    case Type::INT8:
      return std::unique_ptr<SeqConverter>(new Int8Converter);
    case Type::INT16:
      return std::unique_ptr<SeqConverter>(new Int16Converter);
    case Type::INT32:
      return std::unique_ptr<SeqConverter>(new Int32Converter);
    case Type::INT64:
      return std::unique_ptr<SeqConverter>(new Int64Converter);
    case Type::UINT8:
      return std::unique_ptr<SeqConverter>(new UInt8Converter);
    case Type::UINT16:
      return std::unique_ptr<SeqConverter>(new UInt16Converter);
    case Type::UINT32:
      return std::unique_ptr<SeqConverter>(new UInt32Converter);
    case Type::UINT64:
      return std::unique_ptr<SeqConverter>(new UInt64Converter);
    case Type::DATE32:
      return std::unique_ptr<SeqConverter>(new Date32Converter);
    case Type::DATE64:
      return std::unique_ptr<SeqConverter>(new Date64Converter);
    case Type::TIMESTAMP:
      return std::unique_ptr<SeqConverter>(
          new TimestampConverter(static_cast<const TimestampType&>(*type).unit()));
    case Type::FLOAT:
      return std::unique_ptr<SeqConverter>(new Float32Converter);
    case Type::DOUBLE:
      return std::unique_ptr<SeqConverter>(new DoubleConverter);
    case Type::BINARY:
      return std::unique_ptr<SeqConverter>(new BytesConverter);
    case Type::FIXED_SIZE_BINARY:
      return std::unique_ptr<SeqConverter>(new FixedWidthBytesConverter);
    case Type::STRING:
      return std::unique_ptr<SeqConverter>(new UTF8Converter);
    case Type::LIST:
      return std::unique_ptr<SeqConverter>(new ListConverter);
    case Type::STRUCT:
      return std::unique_ptr<SeqConverter>(new StructConverter);
    case Type::DECIMAL:
      return std::unique_ptr<SeqConverter>(new DecimalConverter);
    default:
      return nullptr;
  }
}

Status ListConverter::Init(ArrayBuilder* builder) {
  builder_ = builder;
  typed_builder_ = static_cast<ListBuilder*>(builder);

  value_converter_ =
      GetConverter(static_cast<ListType*>(builder->type().get())->value_type());
  if (value_converter_ == nullptr) {
    return Status::NotImplemented("value type not implemented");
  }

  return value_converter_->Init(typed_builder_->value_builder());
}

Status StructConverter::Init(ArrayBuilder* builder) {
  builder_ = builder;
  typed_builder_ = static_cast<StructBuilder*>(builder);
  StructType* struct_type = static_cast<StructType*>(builder->type().get());

  num_fields_ = typed_builder_->num_fields();
  DCHECK_EQ(num_fields_, struct_type->num_children());

  field_name_list_.reset(PyList_New(num_fields_));
  RETURN_IF_PYERROR();

  // Initialize the child converters and field names
  for (int i = 0; i < num_fields_; i++) {
    const std::string& field_name(struct_type->child(i)->name());
    std::shared_ptr<DataType> field_type(struct_type->child(i)->type());

    auto value_converter = GetConverter(field_type);
    if (value_converter == nullptr) {
      return Status::NotImplemented("value type not implemented");
    }
    RETURN_NOT_OK(value_converter->Init(typed_builder_->field_builder(i)));
    value_converters_.push_back(std::move(value_converter));

    // Store the field name as a PyObject, for dict matching
    PyObject* nameobj =
        PyUnicode_FromStringAndSize(field_name.c_str(), field_name.size());
    RETURN_IF_PYERROR();
    PyList_SET_ITEM(field_name_list_.obj(), i, nameobj);
  }

  return Status::OK();
}

Status AppendPySequence(PyObject* obj, int64_t size,
                        const std::shared_ptr<DataType>& type, ArrayBuilder* builder) {
  PyDateTime_IMPORT;
  auto converter = GetConverter(type);
  if (converter == nullptr) {
    std::stringstream ss;
    ss << "No type converter implemented for " << type->ToString();
    return Status::NotImplemented(ss.str());
  }
  RETURN_NOT_OK(converter->Init(builder));
  return converter->AppendMultiple(obj, size);
}

static Status ConvertPySequenceReal(PyObject* obj, int64_t size,
                                    const std::shared_ptr<DataType>* type,
                                    MemoryPool* pool, std::shared_ptr<Array>* out) {
  PyAcquireGIL lock;

  PyObject* seq;
  OwnedRef tmp_seq_nanny;

  std::shared_ptr<DataType> real_type;

  RETURN_NOT_OK(ConvertToSequenceAndInferSize(obj, &seq, &size));
  tmp_seq_nanny.reset(seq);
  if (type == nullptr) {
    RETURN_NOT_OK(InferArrowType(seq, &real_type));
  } else {
    real_type = *type;
  }
  DCHECK_GE(size, 0);

  // Handle NA / NullType case
  if (real_type->id() == Type::NA) {
    out->reset(new NullArray(size));
    return Status::OK();
  }

  // Give the sequence converter an array builder
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(pool, real_type, &builder));
  RETURN_NOT_OK(AppendPySequence(seq, size, real_type, builder.get()));
  return builder->Finish(out);
}

Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, -1, nullptr, pool, out);
}

Status ConvertPySequence(PyObject* obj, const std::shared_ptr<DataType>& type,
                         MemoryPool* pool, std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, -1, &type, pool, out);
}

Status ConvertPySequence(PyObject* obj, int64_t size, MemoryPool* pool,
                         std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, size, nullptr, pool, out);
}

Status ConvertPySequence(PyObject* obj, int64_t size,
                         const std::shared_ptr<DataType>& type, MemoryPool* pool,
                         std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, size, &type, pool, out);
}

Status CheckPythonBytesAreFixedLength(PyObject* obj, Py_ssize_t expected_length) {
  const Py_ssize_t length = PyBytes_GET_SIZE(obj);
  if (length != expected_length) {
    std::stringstream ss;
    ss << "Found byte string of length " << length << ", expected length is "
       << expected_length;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
