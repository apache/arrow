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
#include <sstream>
#include <string>

#include "arrow/python/builtin_convert.h"

#include "arrow/api.h"
#include "arrow/status.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#include "arrow/python/helpers.h"
#include "arrow/python/util/datetime.h"

namespace arrow {
namespace py {

static inline bool IsPyInteger(PyObject* obj) {
#if PYARROW_IS_PY2
  return PyLong_Check(obj) || PyInt_Check(obj);
#else
  return PyLong_Check(obj);
#endif
}

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
    } else if (IsPyInteger(obj)) {
      ++int_count_;
    } else if (PyDate_CheckExact(obj)) {
      ++date_count_;
    } else if (PyDateTime_CheckExact(obj)) {
      ++timestamp_count_;
    } else if (PyBytes_Check(obj)) {
      ++binary_count_;
    } else if (PyUnicode_Check(obj)) {
      ++unicode_count_;
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
  SeqVisitor() : max_nesting_level_(0) {
    memset(nesting_histogram_, 0, MAX_NESTING_LEVELS * sizeof(int));
  }

  // co-recursive with VisitElem
  Status Visit(PyObject* obj, int level = 0) {
    if (level > max_nesting_level_) {
      max_nesting_level_ = level;
    }
    // Loop through either a sequence or an iterator.
    if (PySequence_Check(obj)) {
      Py_ssize_t size = PySequence_Size(obj);
      for (int64_t i = 0; i < size; ++i) {
        OwnedRef ref;
        if (PyArray_Check(obj)) {
          auto array = reinterpret_cast<PyArrayObject*>(obj);
          auto ptr = reinterpret_cast<const char*>(PyArray_GETPTR1(array, i));
          ref.reset(PyArray_GETITEM(array, ptr));
          RETURN_NOT_OK(VisitElem(ref, level));
        } else {
          ref.reset(PySequence_GetItem(obj, i));
          RETURN_NOT_OK(VisitElem(ref, level));
        }
      }
    } else if (PyObject_HasAttrString(obj, "__iter__")) {
      OwnedRef iter = OwnedRef(PyObject_GetIter(obj));
      PyObject* item;
      while ((item = PyIter_Next(iter.obj()))) {
        OwnedRef ref = OwnedRef(item);
        RETURN_NOT_OK(VisitElem(ref, level));
      }
    } else {
      return Status::TypeError("Object is not a sequence or iterable");
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
      } else if (max_observed_level() < max_nesting_level_) {
        return Status::Invalid("Mixed nesting levels not supported");
      }
    }
    return Status::OK();
  }

  // Returns the deepest level which has scalar elements.
  int max_observed_level() const {
    int result = 0;
    for (int i = 0; i < MAX_NESTING_LEVELS; ++i) {
      if (nesting_histogram_[i] > 0) {
        result = i;
      }
    }
    return result;
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
  // Number of scalar elements at each nesting level.
  // (TOOD: We really only need to know if a scalar is present, not the count).
  int nesting_histogram_[MAX_NESTING_LEVELS];

  // Visits a specific element (inner part of the loop).
  Status VisitElem(const OwnedRef& item_ref, int level) {
    if (PyList_Check(item_ref.obj())) {
      RETURN_NOT_OK(Visit(item_ref.obj(), level + 1));
    } else if (PyDict_Check(item_ref.obj())) {
      return Status::NotImplemented("No type inference for dicts");
    } else {
      // We permit nulls at any level of nesting
      if (item_ref.obj() == Py_None) {
        // TODO
      } else {
        ++nesting_histogram_[level];
        return scalars_.Visit(item_ref.obj());
      }
    }
    return Status::OK();
  }
};

Status InferArrowSize(PyObject* obj, int64_t* size) {
  if (PySequence_Check(obj)) {
    *size = static_cast<int64_t>(PySequence_Size(obj));
  } else if (PyObject_HasAttrString(obj, "__iter__")) {
    PyObject* iter = PyObject_GetIter(obj);
    OwnedRef iter_ref(iter);
    *size = 0;
    PyObject* item;
    while ((item = PyIter_Next(iter))) {
      OwnedRef item_ref(item);
      *size += 1;
    }
  } else {
    return Status::TypeError("Object is not a sequence or iterable");
  }
  if (PyErr_Occurred()) {
    // Not a sequence
    PyErr_Clear();
    return Status::TypeError("Object is not a sequence or iterable");
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
  RETURN_NOT_OK(InferArrowSize(obj, size));

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

  virtual Status AppendData(PyObject* seq, int64_t size) = 0;

  virtual ~SeqConverter() {}

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

template <typename BuilderType, class Derived>
class TypedConverterVisitor : public TypedConverter<BuilderType> {
 public:
  Status AppendData(PyObject* obj, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->typed_builder_->Reserve(size));
    // Iterate over the items adding each one
    if (PySequence_Check(obj)) {
      for (int64_t i = 0; i < size; ++i) {
        OwnedRef ref(PySequence_GetItem(obj, i));
        RETURN_NOT_OK(static_cast<Derived*>(this)->AppendItem(ref));
      }
    } else if (PyObject_HasAttrString(obj, "__iter__")) {
      PyObject* iter = PyObject_GetIter(obj);
      OwnedRef iter_ref(iter);
      PyObject* item;
      int64_t i = 0;
      // To allow people with long generators to only convert a subset, stop
      // consuming at size.
      while ((item = PyIter_Next(iter)) && i < size) {
        OwnedRef ref(item);
        RETURN_NOT_OK(static_cast<Derived*>(this)->AppendItem(ref));
        ++i;
      }
      if (size != i) {
        RETURN_NOT_OK(this->typed_builder_->Resize(i));
      }
    } else {
      return Status::TypeError("Object is not a sequence or iterable");
    }
    return Status::OK();
  }

  virtual Status AppendItem(const OwnedRef& item) = 0;
};

class BoolConverter : public TypedConverterVisitor<BooleanBuilder, BoolConverter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else {
      if (item.obj() == Py_True) {
        return typed_builder_->Append(true);
      } else {
        return typed_builder_->Append(false);
      }
    }
  }
};

class Int64Converter : public TypedConverterVisitor<Int64Builder, Int64Converter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    int64_t val;
    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else {
      val = static_cast<int64_t>(PyLong_AsLongLong(item.obj()));
      RETURN_IF_PYERROR();
      return typed_builder_->Append(val);
    }
  }
};

class DateConverter : public TypedConverterVisitor<Date64Builder, DateConverter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else {
      PyDateTime_Date* pydate = reinterpret_cast<PyDateTime_Date*>(item.obj());
      return typed_builder_->Append(PyDate_to_ms(pydate));
    }
  }
};

class TimestampConverter
    : public TypedConverterVisitor<Date64Builder, TimestampConverter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else {
      PyDateTime_DateTime* pydatetime =
          reinterpret_cast<PyDateTime_DateTime*>(item.obj());
      return typed_builder_->Append(PyDateTime_to_us(pydatetime));
    }
  }
};

class DoubleConverter : public TypedConverterVisitor<DoubleBuilder, DoubleConverter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    double val;
    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else {
      val = PyFloat_AsDouble(item.obj());
      RETURN_IF_PYERROR();
      return typed_builder_->Append(val);
    }
  }
};

class BytesConverter : public TypedConverterVisitor<BinaryBuilder, BytesConverter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    PyObject* bytes_obj;
    const char* bytes;
    Py_ssize_t length;
    OwnedRef tmp;

    if (item.obj() == Py_None) {
      RETURN_NOT_OK(typed_builder_->AppendNull());
      return Status::OK();
    } else if (PyUnicode_Check(item.obj())) {
      tmp.reset(PyUnicode_AsUTF8String(item.obj()));
      RETURN_IF_PYERROR();
      bytes_obj = tmp.obj();
    } else if (PyBytes_Check(item.obj())) {
      bytes_obj = item.obj();
    } else {
      std::stringstream ss;
      ss << "Error converting to Binary type: ";
      RETURN_NOT_OK(InvalidConversion(item.obj(), "bytes", &ss));
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
  inline Status AppendItem(const OwnedRef& item) {
    PyObject* bytes_obj;
    OwnedRef tmp;
    Py_ssize_t expected_length =
        std::dynamic_pointer_cast<FixedSizeBinaryType>(typed_builder_->type())
            ->byte_width();
    if (item.obj() == Py_None) {
      RETURN_NOT_OK(typed_builder_->AppendNull());
      return Status::OK();
    } else if (PyUnicode_Check(item.obj())) {
      tmp.reset(PyUnicode_AsUTF8String(item.obj()));
      RETURN_IF_PYERROR();
      bytes_obj = tmp.obj();
    } else if (PyBytes_Check(item.obj())) {
      bytes_obj = item.obj();
    } else {
      std::stringstream ss;
      ss << "Error converting to FixedSizeBinary type: ";
      RETURN_NOT_OK(InvalidConversion(item.obj(), "bytes", &ss));
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
  inline Status AppendItem(const OwnedRef& item) {
    PyObject* bytes_obj;
    OwnedRef tmp;
    const char* bytes;
    Py_ssize_t length;

    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else if (!PyUnicode_Check(item.obj())) {
      return Status::Invalid("Non-unicode value encountered");
    }
    tmp.reset(PyUnicode_AsUTF8String(item.obj()));
    RETURN_IF_PYERROR();
    bytes_obj = tmp.obj();

    // No error checking
    length = PyBytes_GET_SIZE(bytes_obj);
    bytes = PyBytes_AS_STRING(bytes_obj);
    return typed_builder_->Append(bytes, static_cast<int32_t>(length));
  }
};

class ListConverter : public TypedConverterVisitor<ListBuilder, ListConverter> {
 public:
  Status Init(ArrayBuilder* builder) override;

  inline Status AppendItem(const OwnedRef& item) override {
    if (item.obj() == Py_None) {
      return typed_builder_->AppendNull();
    } else {
      RETURN_NOT_OK(typed_builder_->Append());
      PyObject* item_obj = item.obj();
      int64_t list_size = static_cast<int64_t>(PySequence_Size(item_obj));
      return value_converter_->AppendData(item_obj, list_size);
    }
  }

 protected:
  std::shared_ptr<SeqConverter> value_converter_;
};

#define DECIMAL_CONVERT_CASE(bit_width, item, builder)         \
  case bit_width: {                                            \
    arrow::decimal::Decimal##bit_width out;                    \
    std::string string_out;                                    \
    RETURN_NOT_OK(PythonDecimalToString((item), &string_out)); \
    RETURN_NOT_OK(FromString(string_out, &out));               \
    return ((builder)->Append(out));                           \
    break;                                                     \
  }

class DecimalConverter
    : public TypedConverterVisitor<arrow::DecimalBuilder, DecimalConverter> {
 public:
  inline Status AppendItem(const OwnedRef& item) {
    /// Can the compiler figure out that the case statement below isn't necessary
    /// once we're running?
    const int bit_width =
        std::dynamic_pointer_cast<arrow::DecimalType>(typed_builder_->type())
            ->bit_width();

    /// TODO(phillipc): Check for nan?
    if (item.obj() != Py_None) {
      switch (bit_width) {
        DECIMAL_CONVERT_CASE(32, item.obj(), typed_builder_)
        DECIMAL_CONVERT_CASE(64, item.obj(), typed_builder_)
        DECIMAL_CONVERT_CASE(128, item.obj(), typed_builder_)
        default:
          return Status::OK();
      }
      RETURN_IF_PYERROR();
    } else {
      return typed_builder_->AppendNull();
    }
  }
};

#undef DECIMAL_CONVERT_CASE

// Dynamic constructor for sequence converters
std::shared_ptr<SeqConverter> GetConverter(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::BOOL:
      return std::make_shared<BoolConverter>();
    case Type::INT64:
      return std::make_shared<Int64Converter>();
    case Type::DATE64:
      return std::make_shared<DateConverter>();
    case Type::TIMESTAMP:
      return std::make_shared<TimestampConverter>();
    case Type::DOUBLE:
      return std::make_shared<DoubleConverter>();
    case Type::BINARY:
      return std::make_shared<BytesConverter>();
    case Type::FIXED_SIZE_BINARY:
      return std::make_shared<FixedWidthBytesConverter>();
    case Type::STRING:
      return std::make_shared<UTF8Converter>();
    case Type::LIST:
      return std::make_shared<ListConverter>();
    case Type::DECIMAL: {
      return std::make_shared<DecimalConverter>();
    }
    case Type::STRUCT:
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

Status AppendPySequence(PyObject* obj, int64_t size,
                        const std::shared_ptr<DataType>& type, ArrayBuilder* builder) {
  PyDateTime_IMPORT;
  std::shared_ptr<SeqConverter> converter = GetConverter(type);
  if (converter == nullptr) {
    std::stringstream ss;
    ss << "No type converter implemented for " << type->ToString();
    return Status::NotImplemented(ss.str());
  }
  RETURN_NOT_OK(converter->Init(builder));
  return converter->AppendData(obj, size);
}

Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out) {
  std::shared_ptr<DataType> type;
  int64_t size;
  RETURN_NOT_OK(InferArrowTypeAndSize(obj, &size, &type));
  return ConvertPySequence(obj, pool, out, type, size);
}

Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out,
                         const std::shared_ptr<DataType>& type, int64_t size) {
  // Handle NA / NullType case
  if (type->id() == Type::NA) {
    out->reset(new NullArray(size));
    return Status::OK();
  }

  // Give the sequence converter an array builder
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(pool, type, &builder));
  RETURN_NOT_OK(AppendPySequence(obj, size, type, builder.get()));
  return builder->Finish(out);
}

Status ConvertPySequence(PyObject* obj, MemoryPool* pool, std::shared_ptr<Array>* out,
                         const std::shared_ptr<DataType>& type) {
  int64_t size;
  RETURN_NOT_OK(InferArrowSize(obj, &size));
  return ConvertPySequence(obj, pool, out, type, size);
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
