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
  // \param validate_interval the number of elements to observe before checking
  // whether the data is mixed type or has other problems. This helps avoid
  // excess computation for each element while also making sure we "bail out"
  // early with long sequences that may have problems up front
  // \param make_unions permit mixed-type data by creating union types (not yet
  // implemented)
  explicit TypeInferrer(int64_t validate_interval = 100, bool make_unions = false)
      : validate_interval_(validate_interval),
        make_unions_(make_unions),
        total_count_(0),
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

  Status Visit(PyObject* obj, bool* keep_going) {
    ++total_count_;

    if (total_count_ % validate_interval_ == 0) {
      RETURN_NOT_OK(Validate());
    }

    if (obj == Py_None || internal::PyFloat_IsNaN(obj)) {
      ++none_count_;
    } else if (PyBool_Check(obj)) {
      ++bool_count_;
      *keep_going = make_unions_ && false;
    } else if (PyFloat_Check(obj)) {
      ++float_count_;
      *keep_going = make_unions_ && false;
    } else if (internal::IsPyInteger(obj)) {
      ++int_count_;
      *keep_going = make_unions_ && false;
    } else if (PyDate_CheckExact(obj)) {
      ++date_count_;
      *keep_going = make_unions_ && false;
    } else if (PyTime_CheckExact(obj)) {
      ++time_count_;
      *keep_going = make_unions_ && false;
    } else if (PyDateTime_CheckExact(obj)) {
      ++timestamp_micro_count_;
      *keep_going = make_unions_ && false;
    } else if (internal::IsPyBinary(obj)) {
      ++binary_count_;
      *keep_going = make_unions_ && false;
    } else if (PyUnicode_Check(obj)) {
      ++unicode_count_;
      *keep_going = make_unions_ && false;
    } else if (PyArray_CheckAnyScalarExact(obj)) {
      RETURN_NOT_OK(VisitDType(PyArray_DescrFromScalar(obj)));
      *keep_going = make_unions_ && false;
    } else if (PyList_Check(obj)) {
      return VisitList(obj, keep_going);
    } else if (PyArray_Check(obj)) {
      return VisitNdarray(obj, keep_going);
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

  // Infer value type from a sequence of values
  Status VisitSequence(PyObject* obj) {
    return internal::VisitSequence(obj, [this](PyObject* value, bool* keep_going) {
      return Visit(value, keep_going);
    });
  }

  Status GetType(std::shared_ptr<DataType>* out) const {
    // TODO(wesm): handling forming unions
    if (make_unions_) {
      return Status::NotImplemented("Creating union types not yet supported");
    }

    RETURN_NOT_OK(Validate());

    if (list_count_) {
      std::shared_ptr<DataType> value_type;
      RETURN_NOT_OK(list_inferrer_->GetType(&value_type));
      *out = list(value_type);
    } else if (struct_count_) {
      RETURN_NOT_OK(GetStructType(out));
    } else if (decimal_count_) {
      *out = decimal(max_decimal_metadata_.precision(), max_decimal_metadata_.scale());
    } else if (float_count_) {
      *out = float64();
    } else if (int_count_) {
      *out = int64();
    } else if (date_count_) {
      *out = date64();
    } else if (time_count_) {
      *out = time64(TimeUunit::MICRO);
    } else if (timestamp_nano_count_) {
      *out = timestamp(TimeUnit::NANO);
    } else if (timestamp_micro_count_) {
      *out = timestamp(TimeUnit::MICRO);
    } else if (timestamp_milli_count_) {
      *out = timestamp(TimeUnit::MILLI);
    } else if (timestamp_second_count_) {
      *out = timestamp(TimeUnit::SECOND);
    } else if (bool_count_) {
      *out = boolean();
    } else if (binary_count_) {
      *out = binary();
    } else if (unicode_count_) {
      *out = utf8();
    } else {
      *out = null();
    }
    return Status::OK();
  }

  int64_t total_count() const { return total_count_; }

 protected:
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

  Status VisitDType(PyArray_Descr* dtype) {
    // This is a bit silly.  NumPyDtypeToArrow() infers a Arrow datatype
    // for us, but we go back to counting abstract type kinds.
    std::shared_ptr<DataType> type;
    RETURN_NOT_OK(NumPyDtypeToArrow(dtype, &type));
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
        return Status::Invalid("Unknown unit of TimestampType");
      }
    } else {
      std::ostringstream ss;
      ss << "Unsupported Numpy dtype: ";
      ss << type->ToString();
      return Status::Invalid(ss.str());
    }
    return Status::OK();
  }

  Status VisitList(PyObject* obj, bool* keep_going /* unused */) {
    if (!list_inferrer_) {
      list_inferrer_.reset(new TypeInferrer(validate_interval_, make_unions_));
    }
    ++list_count_;
    return list_inferrer_->VisitSequence(obj);
  }

  Status VisitNdarray(PyObject* obj, bool* keep_going) {
    PyArray_Descr* dtype = PyArray_DESCR(reinterpret_cast<PyArrayObject*>(obj));
    if (dtype->type_num == NPY_OBJECT) {
      return VisitList(obj, keep_going);
    }
    // Not an object array: infer child Arrow type from dtype
    if (!list_inferrer_) {
      list_inferrer_.reset(new TypeInferrer(validate_interval_, make_unions_));
    }
    ++list_count_;

    // Reached a concrete dype
    *keep_going = make_unions_ && false;
    return list_inferrer_->VisitDType(dtype);
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
        it = struct_inferrers_
                 .insert(
                     std::make_pair(key, TypeInferrer(validate_interval_, make_unions_)))
                 .first;
      }
      TypeInferrer* visitor = &it->second;

      // We ignore termination signals from child visitors
      bool keep_going = true;
      RETURN_NOT_OK(visitor->Visit(value_obj, &keep_going));
    }

    // We do not terminate visiting dicts (unless one of the struct_inferrers_
    // signaled to terminate) since we want the union of all observed keys
    ++struct_count_;
    return Status::OK();
  }

  Status GetStructType(std::shared_ptr<DataType>* out) const {
    std::vector<std::shared_ptr<Field>> fields;
    for (const auto& it : struct_inferrers_) {
      std::shared_ptr<DataType> field_type;
      RETURN_NOT_OK(it.second.GetType(&field_type));
      fields.emplace_back(field(it.first, field_type));
    }
    *out = struct_(fields);
    return Status::OK();
  }

 private:
  int64_t validate_interval_;
  bool make_unions_;
  int64_t total_count_;
  int64_t none_count_;
  int64_t bool_count_;
  int64_t int_count_;
  int64_t date_count_;
  int64_t time_count_;
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

// Non-exhaustive type inference
Status InferArrowType(PyObject* obj, std::shared_ptr<DataType>* out_type) {
  PyDateTime_IMPORT;
  TypeInferrer inferrer;
  RETURN_NOT_OK(inferrer.VisitSequence(obj));
  RETURN_NOT_OK(inferrer.GetType(out_type));
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

// ----------------------------------------------------------------------
// Sequence converter base and CRTP "middle" subclasses

// Marshal Python sequence (list, tuple, etc.) to Arrow array
class SeqConverter {
 public:
  virtual ~SeqConverter() = default;

  // Initialize the sequence converter with an ArrayBuilder created
  // externally. The reason for this interface is that we have
  // arrow::MakeBuilder which also creates child builders for nested types, so
  // we have to pass in the child builders to child SeqConverter in the case of
  // converting Python objects to Arrow nested types
  virtual Status Init(ArrayBuilder* builder) = 0;

  // Append a single (non-sequence) Python datum to the underlying builder,
  // virtual function
  virtual Status AppendSingleVirtual(PyObject* obj) = 0;

  // Append the contents of a Python sequence to the underlying builder,
  // virtual version
  virtual Status AppendMultiple(PyObject* seq, int64_t size) = 0;

  // Append the contents of a Python sequence to the underlying builder,
  // virtual version
  virtual Status AppendMultipleMasked(PyObject* seq, PyObject* mask, int64_t size) = 0;

  Status GetResult(std::vector<std::shared_ptr<Array>>* chunks) {
    chunks = chunks_;

    // Still some accumulated data in the builder
    if (builder_->length() > 0) {
      std::shared_ptr<Array> last_chunk;
      RETURN_NOT_OK(builder_->Finish(&last_chunk));
      chunks->emplace_back(std::move(last_chunk));
    }
    return Status::OK();
  }

 protected:
  ArrayBuilder* builder_;
  std::vector<std::shared_ptr<Array>> chunks_;
};

enum class NullConventionTypes : char { NONE_ONLY, PANDAS_SENTINELS };

template <int Kind>
struct NullConvention {};

template <>
struct NullConvention<NullConventionTypes::NONE_ONLY> {
  static inline bool IsNull(PyObject* obj) const { return obj == Py_None; }
};

template <>
struct NullConvention<NullConventionTypes::PANDAS_SENTINELS> {
  static inline bool IsNull(PyObject* obj) const {
    return internal::PandasObjectIsNull(obj);
  }
};

// ----------------------------------------------------------------------
// Helper templates to append PyObject* to builder for each target conversion
// type

template <typename Type, typename Enable = void>
struct Unbox {};

template <typename Type>
struct Unbox<Type, std::enable_if<std::is_base_of<Type, Integer>::value>::type> {
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  // Append a non-missing item
  Status Append(BuilderType* builder, PyObject* obj) {
    typename IntType::c_type value;
    RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    return builder->Append(value);
  }
}

template <>
struct Unbox<HalfFloatType> {
  Status Append(HalfFloatBuilder* builder, PyObject* obj) {
    npy_half val;
    RETURN_NOT_OK(PyFloat_AsHalf(obj, &val));
    return builder->Append(val);
  }
}

template <>
struct Unbox<FloatType> {
  Status Append(FloatBuilder* builder, PyObject* obj) {
    float val = static_cast<float>(PyFloat_AsDouble(obj));
    RETURN_IF_PYERROR();
    return builder->Append(val);
  }
}

template <>
struct Unbox<DoubleType> {
  Status Append(DoubleBuilder* builder, PyObject* obj) {
    if (PyFloat_Check(obj)) {
      double val = PyFloat_AsDouble(obj);
      RETURN_IF_PYERROR();
      return typed_builder_->Append(val);
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Double: ";
      RETURN_NOT_OK(InvalidConversion(obj, "float", &ss));
      return Status::Invalid(ss.str());
    }
  }
}

// We use CRTP to avoid virtual calls to the AppendItem(), AppendNull(), and
// IsNull() on the hot path
template <typename Type, class Derived,
          int NullConventionType = NullConventionTypes::PANDAS_SENTINELS>
class TypedConverter : public SeqConverter {
 public:
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    DCHECK_NE(builder_, nullptr);
    typed_builder_ = checked_cast<BuilderType*>(builder);
    return Status::OK();
  }

  bool CheckNull(PyObject* obj) const {
    return NullConvention<NullConventionType>::IsNull(obj);
  }

  // Append a missing item (default implementation)
  Status AppendNull() { return this->typed_builder_->AppendNull(); }

  // This is overridden in several subclasses, but if an Unbox implementation
  // is defined, it will be used here
  Status AppendItem(PyObject* obj) {
    return Unbox<Type, BuilderType>::Append(typed_builder_, obj);
  }

  Status AppendSingle(PyObject* obj) {
    auto self = checked_cast<Derived*>(this);
    return CheckNull(obj) ? self->AppendNull() : self->AppendItem(obj);
  }

  Status AppendSingleVirtual(PyObject* obj) override { return AppendSingle(obj); }

  Status AppendMultiple(PyObject* obj, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->typed_builder_->Reserve(size));
    // Iterate over the items adding each one
    auto self = checked_cast<Derived*>(this);
    return internal::VisitSequence(obj,
                                   [self](PyObject* item, bool* keep_going /* unused */) {
                                     return self->AppendSingle(item);
                                   });
  }

  Status AppendMultipleMasked(PyObject* obj, PyObject* mask, int64_t size) override {
    /// Ensure we've allocated enough space
    RETURN_NOT_OK(this->typed_builder_->Reserve(size));
    // Iterate over the items adding each one
    auto self = checked_cast<Derived*>(this);
    return internal::VisitSequenceMasked(
        obj, [self](PyObject* item, bool is_masked, bool* keep_going /* unused */) {
          if (is_masked) {
            return self->AppendNull();
          } else {
            // This will also apply the null-checking convention in the event
            // that the value is not masked
            return self->AppendSingle(item);
          }
        });
  }

 protected:
  BuilderType* typed_builder_;
};

// ----------------------------------------------------------------------
// Sequence converter for null type

class NullConverter : public TypedConverter<NullType, NullConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    return Status::Invalid("NullConverter: passed non-None value");
  }
};

// ----------------------------------------------------------------------
// Sequence converter for boolean type

class BoolConverter : public TypedConverter<BooleanType, BoolConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    if (obj == Py_True) {
      return typed_builder_->Append(true);
    } else if (obj == Py_False) {
      return typed_builder_->Append(false);
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Boolean: ";
      RETURN_NOT_OK(InvalidConversion(obj, "bool", &ss));
      return Status::Invalid(ss.str());
    }
  }
};

// ----------------------------------------------------------------------
// Sequence converter template for numeric (integer and floating point) types

template <typename Type, bool from_pandas = true>
class NumericConverter : public TypedConverter<Type, NumericConverter<from_pandas>> {};

template <typename Type>
class NumericConverter<Type, false>
    : public TypedConverter<Type, NumericConverter<Type, false>,
                            NullConventionTypes::NONE_ONLY> {};

// ----------------------------------------------------------------------
// Sequence converters for temporal types

class Date32Converter : public TypedConverter<Date32Type, Date32Converter> {
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

class Date64Converter : public TypedConverter<Date64Type, Date64Converter> {
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

class PyTimeConverter : public TypedConverter<Time64Type, PyTimeConverter> {
 public:
  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    // datetime.time stores microsecond resolution
    // Time64Builder builder(::arrow::time64(TimeUnit::MICRO), pool_);

    if (PyTime_Check(obj)) {
      RETURN_NOT_OK(builder.Append(PyTime_to_us(obj)));
    } else {
      std::stringstream ss;
      ss << "Error converting from Python objects to Time: ";
      RETURN_NOT_OK(InvalidConversion(obj, "datetime.time", &ss));
      return Status::Invalid(ss.str());
    }
  }
};

class TimestampConverter : public TypedConverter<TimestampType, TimestampConverter> {
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

// ----------------------------------------------------------------------
// Sequence converters for Binary, FixedSizeBinary, String

namespace detail {

template <typename BuilderType, typename AppendFunc>
Status AppendPyString(BuilderType* builder, const PyBytesView& view,
                      bool* is_full, AppendFunc&& append_func) {
  int32_t length;
  RETURN_NOT_OK(CastSize(view.size, &length));
  // Did we reach the builder size limit?
  if (ARROW_PREDICT_FALSE(builder->value_data_length() + length > kBinaryMemoryLimit)) {
    if (is_full) {
      *is_full = true;
      return Status::OK();
    } else {
      return Status::CapacityError("Maximum array size reached (2GB)");
    }
  }

  RETURN_NOT_OK(append_func(builder, view.bytes, length));
  if (is_full) {
    *is_full = false;
  }
  return Status::OK();
}

Status BuilderAppend(BinaryBuilder* builder, PyObject* obj, bool* is_full) {
  PyBytesView view;
  // XXX For some reason, we must accept unicode objects here
  RETURN_NOT_OK(view.FromString(obj));
  return AppendPyString(builder, view, is_full,
                        [&builder](const char* bytes, int32_t length) {
                          return builder->Append(bytes, length);
                        });
}

Status BuilderAppend(StringBuilder* builder, PyObject* obj, bool* is_full) {
  PyBytesView view;
  // XXX For some reason, we must accept unicode objects here
  RETURN_NOT_OK(view.FromString(obj, true /* check_valid */));
  return AppendPyString(builder, view, is_full,
                        [&builder](const char* bytes, int32_t length) {
                          return builder->Append(bytes, length);
                        });
}

Status BuilderAppend(FixedSizeBinaryBuilder* builder, PyObject* obj, bool* is_full) {
  PyBytesView view;
  // XXX For some reason, we must accept unicode objects here
  RETURN_NOT_OK(view.FromString(obj));
  const auto expected_length =
      checked_cast<const FixedSizeBinaryType&>(*builder->type()).byte_width();
  if (ARROW_PREDICT_FALSE(view.size != expected_length)) {
    std::stringstream ss;
    ss << "Got bytestring of length " << view.size << " (expected " << expected_length
       << ")";
    return Status::Invalid(ss.str());
  }

  return AppendPyBytes(builder, view, is_full,
                       [&builder](const char* bytes, int32_t length) {
                         return builder->Append(view.bytes);
                       });
}

}  // namespace detail

template <typename Type>
class BinaryLikeConverter :
      public TypedConverter<Type, BinaryLikeConverter<Type>> {
 public:
  Status AppendItem(PyObject* obj) {
    bool is_full = false;
    RETURN_NOT_OK(detail::BuilderAppend(typed_builder_, obj, &is_full));

    // Exceeded capacity of builder
    if (is_full) {
      std::shared_ptr<Array> chunk;
      RETURN_NOT_OK(typed_builder_->Finish(&chunk));
      chunks_.emplace_back(std::move(chunk));

      // Append the item now that the builder has been reset
      return detail::BuilderAppend(typed_builder_, obj, &is_full);
    }
    return Status::OK();
  }
};

    // if (internal::IsPyBinary(obj)) {
    //   *have_bytes = true;
    // }


class BytesConverter : public BinaryLikeConverter<BinaryType> {};

class FixedWidthBytesConverter : public BinaryLikeConverter<FixedSizeBinaryType> {};

class UTF8Converter : public BinaryLikeConverter<StringType> {};

class ListConverter : public TypedConverter<ListType, ListConverter> {
 public:
  explicit ListConverter(bool from_pandas) : from_pandas_(from_pandas) {}

  Status Init(ArrayBuilder* builder) {
    builder_ = builder;
    typed_builder_ = checked_cast<ListBuilder*>(builder);

    auto value_type = checked_cast<const ListType&>(*builder->type()).value_type();
    RETURN_NOT_OK(GetConverter(value_type, from_pandas_, &value_converter));

    value_type_ = value_type.get();
    return value_converter_->Init(typed_builder_->value_builder());
  }

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
  const DataType* value_type_;
  std::unique_ptr<SeqConverter> value_converter_;
  bool from_pandas_;
};

class StructConverter : public TypedConverter<StructType, StructConverter> {
 public:
  explicit StructConverter(bool from_pandas) : from_pandas_(from_pandas) {}

  Status Init(ArrayBuilder* builder) {
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

      std::unique_ptr<SeqConverter> value_converter;
      RETURN_NOT_OK(GetConverter(field_type, from_pandas_, &value_converter));
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
      RETURN_NOT_OK(value_converters_[i]->AppendSingleVirtual(Py_None));
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
      RETURN_NOT_OK(
          value_converters_[i]->AppendSingleVirtual(valueobj ? valueobj : Py_None));
    }
    return Status::OK();
  }

  Status AppendTupleItem(PyObject* obj) {
    if (PyTuple_GET_SIZE(obj) != num_fields_) {
      return Status::Invalid("Tuple size must be equal to number of struct fields");
    }
    for (int i = 0; i < num_fields_; i++) {
      PyObject* valueobj = PyTuple_GET_ITEM(obj, i);
      RETURN_NOT_OK(value_converters_[i]->AppendSingleVirtual(valueobj));
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

class DecimalConverter : public TypedConverter<arrow::Decimal128Type, DecimalConverter> {
 public:
  using BASE = TypedConverter<arrow::Decimal128Type, DecimalConverter>;
  Status Init(ArrayBuilder* builder) override {
    BASE::Init(builder);

    decimal_type_ = checked_cast<const DecimalType*>(typed_builder_->type().get());
    return internal::ImportDecimalType(&pydecimal_type_);
  }

  // Append a non-missing item
  Status AppendItem(PyObject* obj) {
    if (PyObject_IsInstance(obj, pydecimal_type_.obj()) == 1) {
      Decimal128 value;
      RETURN_NOT_OK(internal::DecimalFromPythonDecimal(obj, type, &value));
      return typed_builder_->Append(value);
    } else {
      // PyObject_IsInstance could error and set an exception
      RETURN_IF_PYERROR();
      std::stringstream ss;
      ss << "Error converting from Python objects to Decimal: ";
      RETURN_NOT_OK(InvalidConversion(obj, "decimal.Decimal", &ss));
      return Status::Invalid(ss.str());
    }
  }

 private:
  const DecimalType* decimal_type_;
  OwnedRefNoGIL pydecimal_type_;
};

#define NUMERIC_CONVERTER(TYPE)                                         \
  {                                                                     \
    if (from_pandas) {                                                  \
      *out = std::unique_ptr<SeqConverter>(new NumericConverter<TYPE, true>); \
    } else {                                                            \
      *out = std::unique_ptr<SeqConverter>(new NumericConverter<TYPE, false>); \
    }                                                                   \
    break;                                                              \
  }

// Dynamic constructor for sequence converters
Status GetConverter(const std::shared_ptr<DataType>& type, bool from_pandas,
                    std::unique_ptr<SeqConverter>* out) {
  switch (type->id()) {
    case Type::NA:
      *out = std::unique_ptr<SeqConverter>(new NullConverter);
      break;
    case Type::BOOL:
      *out = std::unique_ptr<SeqConverter>(new BoolConverter);
    case Type::INT8:
      NUMERIC_CONVERTER(Int8Type)
    case Type::INT16:
      NUMERIC_CONVERTER(Int16Type)
    case Type::INT32:
      NUMERIC_CONVERTER(Int32Type)
    case Type::INT64:
      NUMERIC_CONVERTER(Int64Type)
    case Type::UINT8:
      NUMERIC_CONVERTER(UInt8Type)
    case Type::UINT16:
      NUMERIC_CONVERTER(UInt16Type)
    case Type::UINT32:
      NUMERIC_CONVERTER(UInt32Type)
    case Type::UINT64:
      NUMERIC_CONVERTER(UInt64Type)
    case Type::DATE32:
      *out = std::unique_ptr<SeqConverter>(new Date32Converter);
      break;
    case Type::DATE64:
      *out = std::unique_ptr<SeqConverter>(new Date64Converter);
      break;
    case Type::TIME32:
      return Status::NotImplemented("No sequence converter for time32 available");
    case Type::TIME64:
      *out = std::unique_ptr<SeqConverter>(new PyTimeConverter);
      break;
    case Type::TIMESTAMP:
      *out = std::unique_ptr<SeqConverter>(
          new TimestampConverter(checked_cast<const TimestampType&>(*type).unit()));
      break;
    case Type::HALF_FLOAT:
      NUMERIC_CONVERTER(HalfFloatType);
    case Type::FLOAT:
      NUMERIC_CONVERTER(FloatType);
    case Type::DOUBLE:
      NUMERIC_CONVERTER(DoubleType);
    case Type::BINARY:
      *out = std::unique_ptr<SeqConverter>(new BytesConverter);
      break;
    case Type::FIXED_SIZE_BINARY:
      *out = std::unique_ptr<SeqConverter>(new FixedWidthBytesConverter);
      break;
    case Type::STRING:
      *out = std::unique_ptr<SeqConverter>(new UTF8Converter);
      break;
    case Type::LIST:
      *out = std::unique_ptr<SeqConverter>(new ListConverter(from_pandas));
      break;
    case Type::STRUCT:
      *out = std::unique_ptr<SeqConverter>(new StructConverter(from_pandas));
      break;
    case Type::DECIMAL:
      *out = std::unique_ptr<SeqConverter>(new DecimalConverter);
      break;
    default:
      std::stringstream ss;
      ss << "Sequence converter for type " << type->ToString() << " not implemented";
      return Status::NotImplemented(ss.str());
  }
}

// ----------------------------------------------------------------------

namespace {

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

bool PyObject_is_integer(PyObject* obj) {
  return !PyBool_Check(obj) && PyArray_IsIntegerScalar(obj);
}

// Status NumPyConverter::ConvertObjectsInferAndCast() {
//   size_t position = out_arrays_.size();
//   RETURN_NOT_OK(ConvertObjectsInfer());
//   DCHECK_EQ(position + 1, out_arrays_.size());
//   std::shared_ptr<Array> arr = out_arrays_[position];

//   // Perform cast
//   compute::FunctionContext context(pool_);
//   compute::CastOptions options;
//   options.allow_int_overflow = false;

//   std::shared_ptr<Array> casted;
//   RETURN_NOT_OK(compute::Cast(&context, *arr, type_, options, &casted));

//   // Replace with casted values
//   out_arrays_[position] = casted;

//   return Status::OK();
// }

}  // namespace

template <int ITEM_TYPE, typename ArrowType>
inline Status NumPyConverter::ConvertTypedLists(const std::shared_ptr<DataType>& type,
                                                ListBuilder* builder, PyObject* list) {
  auto foreach_item = [&](PyObject* object, bool mask) {
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
      return AppendPySequence(object, size, type, value_builder,
                              use_pandas_null_sentinels_);
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  };

  return LoopPySequenceWithMasks(list, mask_values, have_mask, foreach_item);
}

template <>
inline Status NumPyConverter::ConvertTypedLists<NPY_OBJECT, NullType>(
    const std::shared_ptr<DataType>& type, ListBuilder* builder, PyObject* list) {
  PyAcquireGIL lock;

  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask_ != nullptr) {
    mask_values.Init(mask_);
    have_mask = true;
  }

  auto value_builder = checked_cast<NullBuilder*>(builder->value_builder());

  auto foreach_item = [&](PyObject* object, bool mask) {
    if (mask || internal::PandasObjectIsNull(object)) {
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

  return LoopPySequenceWithMasks(list, mask_values, have_mask, foreach_item);
}

template <>
inline Status NumPyConverter::ConvertTypedLists<NPY_OBJECT, BinaryType>(
    const std::shared_ptr<DataType>& type, ListBuilder* builder, PyObject* list) {
  PyAcquireGIL lock;

  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask_ != nullptr) {
    mask_values.Init(mask_);
    have_mask = true;
  }

  auto value_builder = checked_cast<BinaryBuilder*>(builder->value_builder());

  auto foreach_item = [&](PyObject* object, bool mask) {
    if (mask || internal::PandasObjectIsNull(object)) {
      return builder->AppendNull();
    } else if (PyArray_Check(object)) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(object);
      RETURN_NOT_OK(builder->Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, NPY_OBJECT));

      int64_t offset = 0;
      RETURN_NOT_OK(
          AppendObjectBinaries(numpy_array, nullptr, 0, value_builder, &offset));
      if (offset < PyArray_SIZE(numpy_array)) {
        return Status::CapacityError("Array cell value exceeded 2GB");
      }
      return Status::OK();
    } else if (PyList_Check(object)) {
      int64_t size;
      std::shared_ptr<DataType> inferred_type;
      RETURN_NOT_OK(builder->Append(true));
      RETURN_NOT_OK(InferArrowTypeAndSize(object, &size, &inferred_type));
      if (inferred_type->id() != Type::NA && inferred_type->id() != Type::BINARY) {
        std::stringstream ss;
        ss << inferred_type->ToString() << " cannot be converted to BINARY.";
        return Status::TypeError(ss.str());
      }
      return AppendPySequence(object, size, type, value_builder,
                              use_pandas_null_sentinels_);
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  };

  return LoopPySequenceWithMasks(list, mask_values, have_mask, foreach_item);
}

template <>
inline Status NumPyConverter::ConvertTypedLists<NPY_OBJECT, StringType>(
    const std::shared_ptr<DataType>& type, ListBuilder* builder, PyObject* list) {
  PyAcquireGIL lock;
  // TODO: If there are bytes involed, convert to Binary representation
  bool have_bytes = false;

  Ndarray1DIndexer<uint8_t> mask_values;

  bool have_mask = false;
  if (mask_ != nullptr) {
    mask_values.Init(mask_);
    have_mask = true;
  }

  auto value_builder = checked_cast<StringBuilder*>(builder->value_builder());

  auto foreach_item = [&](PyObject* object, bool mask) {
    if (mask || internal::PandasObjectIsNull(object)) {
      return builder->AppendNull();
    } else if (PyArray_Check(object)) {
      auto numpy_array = reinterpret_cast<PyArrayObject*>(object);
      RETURN_NOT_OK(builder->Append(true));

      // TODO(uwe): Support more complex numpy array structures
      RETURN_NOT_OK(CheckFlatNumpyArray(numpy_array, NPY_OBJECT));

      int64_t offset = 0;
      // If a type was specified and it was utf8, then we set
      // check_valid to true. If any of the input cannot be
      // converted, then we will exit early here.
      bool check_valid = type_ != nullptr && type_->Equals(::arrow::utf8());
      RETURN_NOT_OK(AppendObjectStrings(numpy_array, nullptr, 0, check_valid,
                                        value_builder, &offset, &have_bytes));
      if (offset < PyArray_SIZE(numpy_array)) {
        return Status::CapacityError("Array cell value exceeded 2GB");
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
      return AppendPySequence(object, size, type, value_builder,
                              use_pandas_null_sentinels_);
    } else {
      return Status::TypeError("Unsupported Python type for list items");
    }
  };

  return LoopPySequenceWithMasks(list, mask_values, have_mask, foreach_item);
}

}  // namespace py

// ----------------------------------------------------------------------

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

Status ConvertPySequence(PyObject* sequence_source, PyObject* mask,
                         const PyConversionOptions& options,
                         std::shared_ptr<ChunkedArray>* out) {
  PyAcquireGIL lock;

  PyDateTime_IMPORT;

  PyObject* seq;
  OwnedRef tmp_seq_nanny;

  std::shared_ptr<DataType> real_type;

  int64_t size = options.size;
  RETURN_NOT_OK(ConvertToSequenceAndInferSize(sequence_source, &seq, &size));
  tmp_seq_nanny.reset(seq);
  if (options.type == nullptr) {
    RETURN_NOT_OK(InferArrowType(seq, &real_type));
  } else {
    real_type = options.type;
  }
  DCHECK_GE(size, 0);

  // Handle NA / NullType case
  if (real_type->id() == Type::NA) {
    out->reset(new NullArray(size));
    return Status::OK();
  }

  // Create the sequence converter, initialize with the builder
  std::unique_ptr<SeqConverter> converter;
  RETURN_NOT_OK(GetConverter(real_type, from_pandas, &converter));

  // Create ArrayBuilder for type, then pass into the SeqConverter
  // instance. The reason this is created here rather than in GetConverter is
  // because of nested types (child SeqConverter objects need the child
  // builders created by MakeBuilder)
  std::unique_ptr<ArrayBuilder> type_builder;
  RETURN_NOT_OK(MakeBuilder(options.pool, real_type, &type_builder));
  RETURN_NOT_OK(converter->Init(type_builder.get()));

  // Convert values
  if (mask == nullptr) {
    DCHECK(false) << "Cannot yet handle masks";
    // RETURN_NOT_OK(converter->AppendMultipleMasked(seq, mask, size));
  } else {
    RETURN_NOT_OK(converter->AppendMultiple(seq, size));
  }

  // Retrieve result. Conversion may yield one or more array values
  std::vector<std::shared_ptr<Array>> chunks;
  RETURN_NOT_OK(converter->GetResult(&chunks));
  return std::make_shared<ChunkedArray>(chunks);
}

}  // namespace arrow
}  // namespace arrow
