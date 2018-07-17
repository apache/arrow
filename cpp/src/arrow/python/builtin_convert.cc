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
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/python/builtin_convert.h"

#include "arrow/api.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#include "arrow/python/decimal.h"
#include "arrow/python/helpers.h"
#include "arrow/python/iterators.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/util/datetime.h"

namespace arrow {
namespace py {

Status InvalidConversion(PyObject* obj, const std::string& expected_types,
                         std::ostream* out) {
  (*out) << "Got Python object of type " << Py_TYPE(obj)->tp_name
         << " but can only handle these types: " << expected_types;
  // XXX streamline this?
  return Status::OK();
}

class TypeInferrer {
  // A type inference visitor for Python values

 public:
  TypeInferrer()
      : total_count_(0),
        none_count_(0),
        bool_count_(0),
        int_count_(0),
        date_count_(0),
        timestamp_second_count_(0),
        timestamp_milli_count_(0),
        timestamp_micro_count_(0),
        timestamp_nano_count_(0),
        float_count_(0),
        binary_count_(0),
        unicode_count_(0),
        decimal_count_(0),
        list_count_(0),
        struct_count_(0),
        max_decimal_metadata_(std::numeric_limits<int32_t>::min(),
                              std::numeric_limits<int32_t>::min()),
        decimal_type_() {
    Status status = internal::ImportDecimalType(&decimal_type_);
    DCHECK_OK(status);
  }

  // Infer value type from a sequence of values
  Status VisitSequence(PyObject* obj) {
    return internal::VisitSequence(obj, [this](PyObject* value) { return Visit(value); });
  }

  Status Visit(PyObject* obj) {
    ++total_count_;
    if (obj == Py_None || internal::PyFloat_IsNaN(obj)) {
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
      ++timestamp_micro_count_;
    } else if (internal::IsPyBinary(obj)) {
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
        const auto& type2 = checked_cast<TimestampType&>(*type);
        if (type2.unit() == TimeUnit::NANO) {
          ++timestamp_nano_count_;
        } else if (type2.unit() == TimeUnit::MICRO) {
          ++timestamp_micro_count_;
        } else if (type2.unit() == TimeUnit::MILLI) {
          ++timestamp_milli_count_;
        } else if (type2.unit() == TimeUnit::SECOND) {
          ++timestamp_second_count_;
        } else {
          throw std::runtime_error("Unknown unit of TimestampType");
        }
      } else {
        std::ostringstream ss;
        ss << "Found a NumPy scalar with Arrow dtype that we cannot handle: ";
        ss << type->ToString();
        return Status::Invalid(ss.str());
      }
    } else if (PyList_Check(obj) || PyArray_Check(obj)) {
      // TODO(ARROW-2514): This code path is used for non-object arrays, which
      // leads to wasteful creation and inspection of temporary Python objects.
      return VisitList(obj);
    } else if (PyDict_Check(obj)) {
      return VisitDict(obj);
    } else if (PyObject_IsInstance(obj, decimal_type_.obj())) {
      RETURN_NOT_OK(max_decimal_metadata_.Update(obj));
      ++decimal_count_;
    } else {
      // TODO(wesm): accumulate error information somewhere
      static std::string supported_types =
          "bool, float, integer, date, datetime, bytes, unicode, decimal";
      std::stringstream ss;
      ss << "Error inferring Arrow data type for collection of Python objects. ";
      RETURN_NOT_OK(InvalidConversion(obj, supported_types, &ss));
      return Status::Invalid(ss.str());
    }
    return Status::OK();
  }

  Status Validate() const {
    if (list_count_ > 0) {
      if (list_count_ + none_count_ != total_count_) {
        return Status::Invalid("cannot mix list and non-list, non-null values");
      }
      RETURN_NOT_OK(list_inferrer_->Validate());
    } else if (struct_count_ > 0) {
      if (struct_count_ + none_count_ != total_count_) {
        return Status::Invalid("cannot mix struct and non-struct, non-null values");
      }
      for (const auto& it : struct_inferrers_) {
        RETURN_NOT_OK(it.second.Validate());
      }
    }
    return Status::OK();
  }

  std::shared_ptr<DataType> GetType() const {
    // TODO(wesm): handling mixed-type cases
    if (list_count_) {
      auto value_type = list_inferrer_->GetType();
      DCHECK(value_type != nullptr);
      return list(value_type);
    } else if (struct_count_) {
      return GetStructType();
    } else if (decimal_count_) {
      return decimal(max_decimal_metadata_.precision(), max_decimal_metadata_.scale());
    } else if (float_count_) {
      return float64();
    } else if (int_count_) {
      return int64();
    } else if (date_count_) {
      return date64();
    } else if (timestamp_nano_count_) {
      return timestamp(TimeUnit::NANO);
    } else if (timestamp_micro_count_) {
      return timestamp(TimeUnit::MICRO);
    } else if (timestamp_milli_count_) {
      return timestamp(TimeUnit::MILLI);
    } else if (timestamp_second_count_) {
      return timestamp(TimeUnit::SECOND);
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

 protected:
  Status VisitList(PyObject* obj) {
    if (!list_inferrer_) {
      list_inferrer_.reset(new TypeInferrer);
    }
    ++list_count_;
    return list_inferrer_->VisitSequence(obj);
  }

  Status VisitDict(PyObject* obj) {
    PyObject* key_obj;
    PyObject* value_obj;
    Py_ssize_t pos = 0;

    while (PyDict_Next(obj, &pos, &key_obj, &value_obj)) {
      std::string key;
      if (PyUnicode_Check(key_obj)) {
        RETURN_NOT_OK(internal::PyUnicode_AsStdString(key_obj, &key));
      } else if (PyBytes_Check(key_obj)) {
        key = internal::PyBytes_AsStdString(key_obj);
      } else {
        std::stringstream ss;
        ss << "Expected dict key of type str or bytes, got '" << Py_TYPE(key_obj)->tp_name
           << "'";
        return Status::TypeError(ss.str());
      }
      // Get or create visitor for this key
      auto it = struct_inferrers_.find(key);
      if (it == struct_inferrers_.end()) {
        it = struct_inferrers_.insert(std::make_pair(key, TypeInferrer())).first;
      }
      TypeInferrer* visitor = &it->second;
      RETURN_NOT_OK(visitor->Visit(value_obj));
    }
    ++struct_count_;
    return Status::OK();
  }

  std::shared_ptr<DataType> GetStructType() const {
    std::vector<std::shared_ptr<Field>> fields;
    for (const auto& it : struct_inferrers_) {
      const auto struct_field = field(it.first, it.second.GetType());
      fields.emplace_back(struct_field);
    }
    return struct_(fields);
  }

 private:
  int64_t total_count_;
  int64_t none_count_;
  int64_t bool_count_;
  int64_t int_count_;
  int64_t date_count_;
  int64_t timestamp_second_count_;
  int64_t timestamp_milli_count_;
  int64_t timestamp_micro_count_;
  int64_t timestamp_nano_count_;
  int64_t float_count_;
  int64_t binary_count_;
  int64_t unicode_count_;
  int64_t decimal_count_;
  int64_t list_count_;
  std::unique_ptr<TypeInferrer> list_inferrer_;
  int64_t struct_count_;
  std::map<std::string, TypeInferrer> struct_inferrers_;

  internal::DecimalMetadata max_decimal_metadata_;

  // Place to accumulate errors
  // std::vector<Status> errors_;
  OwnedRefNoGIL decimal_type_;
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
  TypeInferrer inferrer;
  RETURN_NOT_OK(inferrer.VisitSequence(obj));
  RETURN_NOT_OK(inferrer.Validate());

  *out_type = inferrer.GetType();
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
    RETURN_NOT_OK(SeqConverter::Init(builder));
    DCHECK_NE(builder_, nullptr);
    typed_builder_ = checked_cast<BuilderType*>(builder);
    DCHECK_NE(typed_builder_, nullptr);
    return Status::OK();
  }

 protected:
  BuilderType* typed_builder_;
};

// We use the CRTP trick here to devirtualize the AppendItem(), AppendNull(), and IsNull()
// method calls.
template <typename BuilderType, class Derived>
class TypedConverterVisitor : public TypedConverter<BuilderType> {
 public:
  Status AppendSingle(PyObject* obj) override {
    auto self = checked_cast<Derived*>(this);
    return self->IsNull(obj) ? self->AppendNull() : self->AppendItem(obj);
  }

  Status AppendMultiple(PyObject* obj, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->typed_builder_->Reserve(size));
    // Iterate over the items adding each one
    auto self = checked_cast<Derived*>(this);
    auto visit = [self](PyObject* item) { return self->AppendSingle(item); };
    return internal::VisitSequence(obj, visit);
  }

  // Append a missing item (default implementation)
  Status AppendNull() { return this->typed_builder_->AppendNull(); }

  bool IsNull(PyObject* obj) const { return internal::PandasObjectIsNull(obj); }
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
  Status AppendItem(PyObject* obj) {
    return typed_builder_->Append(PyObject_IsTrue(obj) == 1);
  }
};

template <typename IntType, bool from_pandas = true>
class TypedIntConverter
    : public TypedConverterVisitor<NumericBuilder<IntType>, TypedIntConverter<IntType>> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    typename IntType::c_type value;
    RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    return this->typed_builder_->Append(value);
  }
};

template <typename IntType>
class TypedIntConverter<IntType, false>
    : public TypedConverterVisitor<NumericBuilder<IntType>,
                                   TypedIntConverter<IntType, false>> {
 public:
  Status AppendSingle(PyObject* obj) {
    return (obj == Py_None) ? this->AppendNull() : this->AppendItem(obj);
  }

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    typename IntType::c_type value;
    RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    return this->typed_builder_->Append(value);
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
      RETURN_NOT_OK(internal::CIntFromPython(obj, &t, "Integer too large for date32"));
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
      RETURN_NOT_OK(internal::CIntFromPython(obj, &t, "Integer too large for date64"));
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
      const TimestampType& ttype = checked_cast<const TimestampType&>(*type);
      if (unit_ != ttype.unit()) {
        return Status::NotImplemented(
            "Cannot convert NumPy datetime64 objects with differing unit");
      }

      t = reinterpret_cast<PyDatetimeScalarObject*>(obj)->obval;
    } else {
      RETURN_NOT_OK(internal::CIntFromPython(obj, &t));
    }
    return typed_builder_->Append(t);
  }

 private:
  TimeUnit::type unit_;
};

template <bool from_pandas = true>
class Float16Converter
    : public TypedConverterVisitor<HalfFloatBuilder, Float16Converter<from_pandas>> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    npy_half val;
    RETURN_NOT_OK(PyFloat_AsHalf(obj, &val));
    return this->typed_builder_->Append(val);
  }
};

template <>
class Float16Converter<false>
    : public TypedConverterVisitor<HalfFloatBuilder, Float16Converter<false>> {
 public:
  Status AppendSingle(PyObject* obj) override {
    return (obj == Py_None) ? this->AppendNull() : this->AppendItem(obj);
  }

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    npy_half val;
    RETURN_NOT_OK(PyFloat_AsHalf(obj, &val));
    return this->typed_builder_->Append(val);
  }
};

template <bool from_pandas = true>
class Float32Converter
    : public TypedConverterVisitor<FloatBuilder, Float32Converter<true>> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    float val = static_cast<float>(PyFloat_AsDouble(obj));
    RETURN_IF_PYERROR();
    return typed_builder_->Append(val);
  }
};

template <>
class Float32Converter<false>
    : public TypedConverterVisitor<FloatBuilder, Float32Converter<false>> {
 public:
  Status AppendSingle(PyObject* obj) override {
    return (obj == Py_None) ? this->AppendNull() : this->AppendItem(obj);
  }

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    float val = static_cast<float>(PyFloat_AsDouble(obj));
    RETURN_IF_PYERROR();
    return this->typed_builder_->Append(val);
  }
};

template <bool from_pandas = true>
class DoubleConverter
    : public TypedConverterVisitor<DoubleBuilder, DoubleConverter<true>> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    double val = PyFloat_AsDouble(obj);
    RETURN_IF_PYERROR();
    return typed_builder_->Append(val);
  }
};

template <>
class DoubleConverter<false>
    : public TypedConverterVisitor<DoubleBuilder, DoubleConverter<false>> {
 public:
  Status AppendSingle(PyObject* obj) override {
    return (obj == Py_None) ? this->AppendNull() : this->AppendItem(obj);
  }

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    double val = PyFloat_AsDouble(obj);
    RETURN_IF_PYERROR();
    return this->typed_builder_->Append(val);
  }
};

class BytesConverter : public TypedConverterVisitor<BinaryBuilder, BytesConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    return internal::BuilderAppend(typed_builder_, obj);
  }
};

class FixedWidthBytesConverter
    : public TypedConverterVisitor<FixedSizeBinaryBuilder, FixedWidthBytesConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    return internal::BuilderAppend(typed_builder_, obj);
  }
};

class UTF8Converter : public TypedConverterVisitor<StringBuilder, UTF8Converter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    return internal::BuilderAppend(typed_builder_, obj, true /* check_valid */);
  }
};

class ListConverter : public TypedConverterVisitor<ListBuilder, ListConverter> {
 public:
  explicit ListConverter(bool from_pandas) : from_pandas_(from_pandas) {}

  Status Init(ArrayBuilder* builder) override;

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    RETURN_NOT_OK(typed_builder_->Append());
    const auto list_size = static_cast<int64_t>(PySequence_Size(obj));
    if (ARROW_PREDICT_FALSE(list_size == -1)) {
      RETURN_IF_PYERROR();
    }
    return value_converter_->AppendMultiple(obj, list_size);
  }

 protected:
  std::unique_ptr<SeqConverter> value_converter_;
  bool from_pandas_;
};

class StructConverter : public TypedConverterVisitor<StructBuilder, StructConverter> {
 public:
  explicit StructConverter(bool from_pandas) : from_pandas_(from_pandas) {}

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
  bool from_pandas_;
};

class DecimalConverter
    : public TypedConverterVisitor<arrow::Decimal128Builder, DecimalConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    Decimal128 value;
    const auto& type = checked_cast<const DecimalType&>(*typed_builder_->type());
    RETURN_NOT_OK(internal::DecimalFromPythonDecimal(obj, type, &value));
    return typed_builder_->Append(value);
  }
};

#define INT_CONVERTER(ArrowType)                                                     \
  {                                                                                  \
    if (from_pandas) {                                                               \
      return std::unique_ptr<SeqConverter>(new TypedIntConverter<ArrowType, true>);  \
    } else {                                                                         \
      return std::unique_ptr<SeqConverter>(new TypedIntConverter<ArrowType, false>); \
    }                                                                                \
  }

// Dynamic constructor for sequence converters
std::unique_ptr<SeqConverter> GetConverter(const std::shared_ptr<DataType>& type,
                                           bool from_pandas) {
  switch (type->id()) {
    case Type::NA:
      return std::unique_ptr<SeqConverter>(new NullConverter);
    case Type::BOOL:
      return std::unique_ptr<SeqConverter>(new BoolConverter);
    case Type::INT8:
      INT_CONVERTER(Int8Type)
    case Type::INT16:
      INT_CONVERTER(Int16Type)
    case Type::INT32:
      INT_CONVERTER(Int32Type)
    case Type::INT64:
      INT_CONVERTER(Int64Type)
    case Type::UINT8:
      INT_CONVERTER(UInt8Type)
    case Type::UINT16:
      INT_CONVERTER(UInt16Type)
    case Type::UINT32:
      INT_CONVERTER(UInt32Type)
    case Type::UINT64:
      INT_CONVERTER(UInt64Type)
    case Type::DATE32:
      return std::unique_ptr<SeqConverter>(new Date32Converter);
    case Type::DATE64:
      return std::unique_ptr<SeqConverter>(new Date64Converter);
    case Type::TIMESTAMP:
      return std::unique_ptr<SeqConverter>(
          new TimestampConverter(checked_cast<const TimestampType&>(*type).unit()));
    case Type::HALF_FLOAT: {
      if (from_pandas) {
        return std::unique_ptr<SeqConverter>(new Float16Converter<true>);
      } else {
        return std::unique_ptr<SeqConverter>(new Float16Converter<false>);
      }
    }
    case Type::FLOAT: {
      if (from_pandas) {
        return std::unique_ptr<SeqConverter>(new Float32Converter<true>);
      } else {
        return std::unique_ptr<SeqConverter>(new Float32Converter<false>);
      }
    }
    case Type::DOUBLE: {
      if (from_pandas) {
        return std::unique_ptr<SeqConverter>(new DoubleConverter<true>);
      } else {
        return std::unique_ptr<SeqConverter>(new DoubleConverter<false>);
      }
    }
    case Type::BINARY:
      return std::unique_ptr<SeqConverter>(new BytesConverter);
    case Type::FIXED_SIZE_BINARY:
      return std::unique_ptr<SeqConverter>(new FixedWidthBytesConverter);
    case Type::STRING:
      return std::unique_ptr<SeqConverter>(new UTF8Converter);
    case Type::LIST:
      return std::unique_ptr<SeqConverter>(new ListConverter(from_pandas));
    case Type::STRUCT:
      return std::unique_ptr<SeqConverter>(new StructConverter(from_pandas));
    case Type::DECIMAL:
      return std::unique_ptr<SeqConverter>(new DecimalConverter);
    default:
      return nullptr;
  }
}

Status ListConverter::Init(ArrayBuilder* builder) {
  builder_ = builder;
  typed_builder_ = checked_cast<ListBuilder*>(builder);

  value_converter_ = GetConverter(
      checked_cast<const ListType&>(*builder->type()).value_type(), from_pandas_);
  if (value_converter_ == nullptr) {
    return Status::NotImplemented("value type not implemented");
  }

  return value_converter_->Init(typed_builder_->value_builder());
}

Status StructConverter::Init(ArrayBuilder* builder) {
  builder_ = builder;
  typed_builder_ = checked_cast<StructBuilder*>(builder);
  const auto& struct_type = checked_cast<const StructType&>(*builder->type());

  num_fields_ = typed_builder_->num_fields();
  DCHECK_EQ(num_fields_, struct_type.num_children());

  field_name_list_.reset(PyList_New(num_fields_));
  RETURN_IF_PYERROR();

  // Initialize the child converters and field names
  for (int i = 0; i < num_fields_; i++) {
    const std::string& field_name(struct_type.child(i)->name());
    std::shared_ptr<DataType> field_type(struct_type.child(i)->type());

    auto value_converter = GetConverter(field_type, from_pandas_);
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
                        const std::shared_ptr<DataType>& type, ArrayBuilder* builder,
                        bool from_pandas) {
  PyDateTime_IMPORT;
  auto converter = GetConverter(type, from_pandas);
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
                                    MemoryPool* pool, bool from_pandas,
                                    std::shared_ptr<Array>* out) {
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
  RETURN_NOT_OK(AppendPySequence(seq, size, real_type, builder.get(), from_pandas));
  return builder->Finish(out);
}

Status ConvertPySequence(PyObject* obj, MemoryPool* pool, bool from_pandas,
                         std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, -1, nullptr, pool, from_pandas, out);
}

Status ConvertPySequence(PyObject* obj, const std::shared_ptr<DataType>& type,
                         MemoryPool* pool, bool from_pandas,
                         std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, -1, &type, pool, from_pandas, out);
}

Status ConvertPySequence(PyObject* obj, int64_t size, MemoryPool* pool, bool from_pandas,
                         std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, size, nullptr, pool, from_pandas, out);
}

Status ConvertPySequence(PyObject* obj, int64_t size,
                         const std::shared_ptr<DataType>& type, MemoryPool* pool,
                         bool from_pandas, std::shared_ptr<Array>* out) {
  return ConvertPySequenceReal(obj, size, &type, pool, from_pandas, out);
}

}  // namespace py
}  // namespace arrow
