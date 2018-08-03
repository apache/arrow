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

#include "arrow/python/builtin_convert.h"
#include "arrow/python/numpy_interop.h"

#include <datetime.h>

#include <algorithm>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

#include "arrow/python/decimal.h"
#include "arrow/python/helpers.h"
#include "arrow/python/iterators.h"
#include "arrow/python/numpy_convert.h"
#include "arrow/python/type_traits.h"
#include "arrow/python/util/datetime.h"

namespace arrow {
namespace py {

namespace {

Status InvalidValue(PyObject* obj, const std::string& why) {
  std::stringstream ss;

  std::string obj_as_str;
  RETURN_NOT_OK(internal::PyObject_StdStringStr(obj, &obj_as_str));
  ss << "Could not convert " << obj_as_str << " with type " << Py_TYPE(obj)->tp_name
     << ": " << why;
  return Status::Invalid(ss.str());
}

}  // namespace

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
        time_count_(0),
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
    } else if (internal::PyFloatScalar_Check(obj)) {
      ++float_count_;
    } else if (internal::IsPyInteger(obj)) {
      ++int_count_;
    } else if (PyDateTime_Check(obj)) {
      ++timestamp_micro_count_;
      *keep_going = make_unions_ && false;
    } else if (PyDate_Check(obj)) {
      ++date_count_;
      *keep_going = make_unions_ && false;
    } else if (PyTime_Check(obj)) {
      ++time_count_;
      *keep_going = make_unions_ && false;
    } else if (internal::IsPyBinary(obj)) {
      ++binary_count_;
      *keep_going = make_unions_ && false;
    } else if (PyUnicode_Check(obj)) {
      ++unicode_count_;
      *keep_going = make_unions_ && false;
    } else if (PyArray_CheckAnyScalarExact(obj)) {
      RETURN_NOT_OK(VisitDType(PyArray_DescrFromScalar(obj), keep_going));
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
      return InvalidValue(obj,
                          "did not recognize Python value type when inferring "
                          "an Arrow data type");
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

    if (numpy_unifier_.current_dtype() != nullptr) {
      std::shared_ptr<DataType> type;
      RETURN_NOT_OK(NumPyDtypeToArrow(numpy_unifier_.current_dtype(), &type));
      *out = type;
    } else if (list_count_) {
      std::shared_ptr<DataType> value_type;
      RETURN_NOT_OK(list_inferrer_->GetType(&value_type));
      *out = list(value_type);
    } else if (struct_count_) {
      RETURN_NOT_OK(GetStructType(out));
    } else if (decimal_count_) {
      *out = decimal(max_decimal_metadata_.precision(), max_decimal_metadata_.scale());
    } else if (float_count_) {
      // Prioritize floats before integers
      *out = float64();
    } else if (int_count_) {
      *out = int64();
    } else if (date_count_) {
      *out = date32();
    } else if (time_count_) {
      *out = time64(TimeUnit::MICRO);
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

  Status VisitDType(PyArray_Descr* dtype, bool* keep_going) {
    // Continue visiting dtypes for now.
    // TODO(wesm): devise approach for unions
    *keep_going = true;
    return numpy_unifier_.Observe(dtype);
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
    return list_inferrer_->VisitDType(dtype, keep_going);
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

  // If we observe a strongly-typed value in e.g. a NumPy array, we can store
  // it here to skip the type counting logic above
  NumPyDtypeUnifier numpy_unifier_;

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

class SeqConverter;

// Forward-declare converter factory
Status GetConverter(const std::shared_ptr<DataType>& type, bool from_pandas,
                    bool strict_conversions, std::unique_ptr<SeqConverter>* out);

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

  virtual Status GetResult(std::vector<std::shared_ptr<Array>>* chunks) {
    *chunks = chunks_;

    // Still some accumulated data in the builder. If there are no chunks, we
    // always call Finish to deal with the edge case where a size-0 sequence
    // was converted with a specific output type, like array([], type=t)
    if (chunks_.size() == 0 || builder_->length() > 0) {
      std::shared_ptr<Array> last_chunk;
      RETURN_NOT_OK(builder_->Finish(&last_chunk));
      chunks->emplace_back(std::move(last_chunk));
    }
    return Status::OK();
  }

  ArrayBuilder* builder() const { return builder_; }

 protected:
  ArrayBuilder* builder_;
  bool unfinished_builder_;
  std::vector<std::shared_ptr<Array>> chunks_;
};

enum class NullCoding : char { NONE_ONLY, PANDAS_SENTINELS };

template <NullCoding kind>
struct NullChecker {};

template <>
struct NullChecker<NullCoding::NONE_ONLY> {
  static inline bool Check(PyObject* obj) { return obj == Py_None; }
};

template <>
struct NullChecker<NullCoding::PANDAS_SENTINELS> {
  static inline bool Check(PyObject* obj) { return internal::PandasObjectIsNull(obj); }
};

// ----------------------------------------------------------------------
// Helper templates to append PyObject* to builder for each target conversion
// type

template <typename Type, typename Enable = void>
struct Unbox {};

template <typename Type>
struct Unbox<Type, enable_if_integer<Type>> {
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  static inline Status Append(BuilderType* builder, PyObject* obj) {
    typename Type::c_type value;
    RETURN_NOT_OK(internal::CIntFromPython(obj, &value));
    return builder->Append(value);
  }
};

template <>
struct Unbox<HalfFloatType> {
  static inline Status Append(HalfFloatBuilder* builder, PyObject* obj) {
    npy_half val;
    RETURN_NOT_OK(PyFloat_AsHalf(obj, &val));
    return builder->Append(val);
  }
};

template <>
struct Unbox<FloatType> {
  static inline Status Append(FloatBuilder* builder, PyObject* obj) {
    float val = static_cast<float>(PyFloat_AsDouble(obj));
    RETURN_IF_PYERROR();
    return builder->Append(val);
  }
};

template <>
struct Unbox<DoubleType> {
  static inline Status Append(DoubleBuilder* builder, PyObject* obj) {
    if (internal::PyFloatScalar_Check(obj)) {
      double val = PyFloat_AsDouble(obj);
      RETURN_IF_PYERROR();
      return builder->Append(val);
    } else if (internal::PyIntScalar_Check(obj)) {
      double val = 0;
      RETURN_NOT_OK(IntegerScalarToDoubleSafe(obj, &val));
      return builder->Append(val);
    } else {
      return InvalidValue(obj, "tried to convert to double");
    }
  }
};

// We use CRTP to avoid virtual calls to the AppendItem(), AppendNull(), and
// IsNull() on the hot path
template <typename Type, class Derived,
          NullCoding null_coding = NullCoding::PANDAS_SENTINELS>
class TypedConverter : public SeqConverter {
 public:
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    DCHECK_NE(builder_, nullptr);
    typed_builder_ = checked_cast<BuilderType*>(builder);
    return Status::OK();
  }

  bool CheckNull(PyObject* obj) const { return NullChecker<null_coding>::Check(obj); }

  // Append a missing item (default implementation)
  Status AppendNull() { return this->typed_builder_->AppendNull(); }

  // This is overridden in several subclasses, but if an Unbox implementation
  // is defined, it will be used here
  Status AppendItem(PyObject* obj) { return Unbox<Type>::Append(typed_builder_, obj); }

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
        obj, mask, [self](PyObject* item, bool is_masked, bool* keep_going /* unused */) {
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
  Status AppendItem(PyObject* obj) {
    return InvalidValue(obj, "converting to null type");
  }
};

// ----------------------------------------------------------------------
// Sequence converter for boolean type

class BoolConverter : public TypedConverter<BooleanType, BoolConverter> {
 public:
  Status AppendItem(PyObject* obj) {
    if (obj == Py_True) {
      return typed_builder_->Append(true);
    } else if (obj == Py_False) {
      return typed_builder_->Append(false);
    } else {
      return InvalidValue(obj, "converting to boolean");
    }
  }
};

// ----------------------------------------------------------------------
// Sequence converter template for numeric (integer and floating point) types

template <typename Type, bool from_pandas = true>
class NumericConverter
    : public TypedConverter<Type, NumericConverter<Type, from_pandas>> {};

template <typename Type>
class NumericConverter<Type, false>
    : public TypedConverter<Type, NumericConverter<Type, false>, NullCoding::NONE_ONLY> {
};

// ----------------------------------------------------------------------
// Sequence converters for temporal types

class Date32Converter : public TypedConverter<Date32Type, Date32Converter> {
 public:
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

class TimeConverter : public TypedConverter<Time64Type, TimeConverter> {
 public:
  Status AppendItem(PyObject* obj) {
    if (PyTime_Check(obj)) {
      // datetime.time stores microsecond resolution
      return typed_builder_->Append(PyTime_to_us(obj));
    } else {
      return InvalidValue(obj, "converting to time64");
    }
  }
};

class TimestampConverter : public TypedConverter<TimestampType, TimestampConverter> {
 public:
  explicit TimestampConverter(TimeUnit::type unit) : unit_(unit) {}

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
Status AppendPyString(BuilderType* builder, const PyBytesView& view, bool* is_full,
                      AppendFunc&& append_func) {
  int32_t length;
  RETURN_NOT_OK(internal::CastSize(view.size, &length));
  // Did we reach the builder size limit?
  if (ARROW_PREDICT_FALSE(builder->value_data_length() + length > kBinaryMemoryLimit)) {
    if (is_full) {
      *is_full = true;
      return Status::OK();
    } else {
      return Status::CapacityError("Maximum array size reached (2GB)");
    }
  }

  RETURN_NOT_OK(append_func(view.bytes, length));
  if (is_full) {
    *is_full = false;
  }
  return Status::OK();
}

// if (internal::IsPyBinary(obj)) {
//   *have_bytes = true;
// }

Status BuilderAppend(BinaryBuilder* builder, PyObject* obj, bool* is_full) {
  PyBytesView view;
  // XXX For some reason, we must accept unicode objects here
  RETURN_NOT_OK(view.FromString(obj));
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
    ss << "expected to be length " << expected_length << " was " << view.size;
    return InvalidValue(obj, ss.str());
  }

  return AppendPyString(
      builder, view, is_full,
      [&builder](const char* bytes, int32_t length) { return builder->Append(bytes); });
}

}  // namespace detail

template <typename Type>
class BinaryLikeConverter : public TypedConverter<Type, BinaryLikeConverter<Type>> {
 public:
  Status AppendItem(PyObject* obj) {
    // XXX(wesm): for some reason accessing members of the templated base
    // requires using this-> here, look up why that is

    bool is_full = false;
    RETURN_NOT_OK(detail::BuilderAppend(this->typed_builder_, obj, &is_full));

    // Exceeded capacity of builder
    if (is_full) {
      std::shared_ptr<Array> chunk;
      RETURN_NOT_OK(this->typed_builder_->Finish(&chunk));
      this->chunks_.emplace_back(std::move(chunk));

      // Append the item now that the builder has been reset
      return detail::BuilderAppend(this->typed_builder_, obj, &is_full);
    }
    return Status::OK();
  }
};

class BytesConverter : public BinaryLikeConverter<BinaryType> {};

class FixedWidthBytesConverter : public BinaryLikeConverter<FixedSizeBinaryType> {};

// For String/UTF8, if strict_conversions enabled, we reject any non-UTF8,
// otherwise we allow but return results as BinaryArray
class StringConverter : public TypedConverter<StringType, StringConverter> {
 public:
  StringConverter(bool strict_conversions)
      : unicode_count_(0), binary_count_(0), strict_conversions_(strict_conversions) {}

  Status Append(PyObject* obj, bool* is_full) {
    PyBytesView view;

    if (strict_conversions_) {
      // Force output to be unicode / utf8 and validate that any binary values
      // are utf8
      bool is_utf8 = false;
      RETURN_NOT_OK(view.FromString(obj, &is_utf8));
      if (!is_utf8) {
        return InvalidValue(obj, "was not a utf8 string");
      }
      ++unicode_count_;
    } else {
      // Non-strict conversion; keep track of whether values are unicode or
      // bytes; if any bytes are observe, the result will be bytes
      if (internal::IsPyBinary(obj)) {
        RETURN_NOT_OK(view.FromBinary(obj));
        ++binary_count_;
      } else {
        RETURN_NOT_OK(view.FromString(obj));
        ++unicode_count_;
      }
    }

    return detail::AppendPyString(typed_builder_, view, is_full,
                                  [this](const char* bytes, int32_t length) {
                                    return this->typed_builder_->Append(bytes, length);
                                  });
  }

  Status AppendItem(PyObject* obj) {
    bool is_full = false;
    RETURN_NOT_OK(Append(obj, &is_full));

    // Exceeded capacity of builder
    if (is_full) {
      std::shared_ptr<Array> chunk;
      RETURN_NOT_OK(this->typed_builder_->Finish(&chunk));
      this->chunks_.emplace_back(std::move(chunk));

      // Append the item now that the builder has been reset
      RETURN_NOT_OK(Append(obj, &is_full));
    }
    return Status::OK();
  }

  virtual Status GetResult(std::vector<std::shared_ptr<Array>>* out) {
    RETURN_NOT_OK(SeqConverter::GetResult(out));

    // If we saw any non-unicode, cast results to BinaryArray
    if (binary_count_) {
      // We should have bailed out earlier
      DCHECK(!strict_conversions_);

      for (size_t i = 0; i < chunks_.size(); ++i) {
        auto binary_data = chunks_[i]->data()->Copy();
        binary_data->type = ::arrow::binary();
        (*out)[i] = std::make_shared<BinaryArray>(binary_data);
      }
    }
    return Status::OK();
  }

 private:
  int64_t unicode_count_;
  int64_t binary_count_;
  bool strict_conversions_;
};

// ----------------------------------------------------------------------
// Convert lists (NumPy arrays containing lists or ndarrays as values)

class ListConverter : public TypedConverter<ListType, ListConverter> {
 public:
  explicit ListConverter(bool from_pandas, bool strict_conversions)
      : from_pandas_(from_pandas), strict_conversions_(strict_conversions) {}

  Status Init(ArrayBuilder* builder) {
    builder_ = builder;
    typed_builder_ = checked_cast<ListBuilder*>(builder);

    value_type_ = checked_cast<const ListType&>(*builder->type()).value_type();
    RETURN_NOT_OK(
        GetConverter(value_type_, from_pandas_, strict_conversions_, &value_converter_));
    return value_converter_->Init(typed_builder_->value_builder());
  }

  template <int NUMPY_TYPE, typename Type>
  Status AppendNdarrayTypedItem(PyArrayObject* arr);
  Status AppendNdarrayItem(PyObject* arr);

  Status AppendItem(PyObject* obj) {
    RETURN_NOT_OK(typed_builder_->Append());
    if (PyArray_Check(obj)) {
      return AppendNdarrayItem(obj);
    }
    const auto list_size = static_cast<int64_t>(PySequence_Size(obj));
    if (ARROW_PREDICT_FALSE(list_size == -1)) {
      RETURN_IF_PYERROR();
    }
    return value_converter_->AppendMultiple(obj, list_size);
  }

  // virtual Status GetResult(std::vector<std::shared_ptr<Array>>* chunks) {
  //   // TODO: Handle chunked children
  //   return SeqConverter::GetResult(chunks);
  // }

 protected:
  std::shared_ptr<DataType> value_type_;
  std::unique_ptr<SeqConverter> value_converter_;
  bool from_pandas_;
  bool strict_conversions_;
};

template <int NUMPY_TYPE, typename Type>
Status ListConverter::AppendNdarrayTypedItem(PyArrayObject* arr) {
  using traits = internal::npy_traits<NUMPY_TYPE>;
  using T = typename traits::value_type;
  using ValueBuilderType = typename TypeTraits<Type>::BuilderType;

  const bool null_sentinels_possible = (from_pandas_ && traits::supports_nulls);

  auto child_builder = checked_cast<ValueBuilderType*>(value_converter_->builder());

  // TODO(wesm): Vector append when not strided
  Ndarray1DIndexer<T> values(arr);
  if (null_sentinels_possible) {
    for (int64_t i = 0; i < values.size(); ++i) {
      if (traits::isnull(values[i])) {
        RETURN_NOT_OK(child_builder->AppendNull());
      } else {
        RETURN_NOT_OK(child_builder->Append(values[i]));
      }
    }
  } else {
    for (int64_t i = 0; i < values.size(); ++i) {
      RETURN_NOT_OK(child_builder->Append(values[i]));
    }
  }
  return Status::OK();
}

// If the value type does not match the expected NumPy dtype, then fall through
// to a slower PySequence-based path
#define LIST_FAST_CASE(TYPE, NUMPY_TYPE, ArrowType)               \
  case Type::TYPE: {                                              \
    if (PyArray_DESCR(arr)->type_num != NUMPY_TYPE) {             \
      return value_converter_->AppendMultiple(obj, value_length); \
    }                                                             \
    return AppendNdarrayTypedItem<NUMPY_TYPE, ArrowType>(arr);    \
  }

// Use internal::VisitSequence, fast for NPY_OBJECT but slower otherwise
#define LIST_SLOW_CASE(TYPE)                                    \
  case Type::TYPE: {                                            \
    return value_converter_->AppendMultiple(obj, value_length); \
  }

Status ListConverter::AppendNdarrayItem(PyObject* obj) {
  PyArrayObject* arr = reinterpret_cast<PyArrayObject*>(obj);

  if (PyArray_NDIM(arr) != 1) {
    return Status::Invalid("Can only convert 1-dimensional array values");
  }

  const int64_t value_length = PyArray_SIZE(arr);

  switch (value_type_->id()) {
    LIST_SLOW_CASE(NA)
    LIST_FAST_CASE(UINT8, NPY_UINT8, UInt8Type)
    LIST_FAST_CASE(INT8, NPY_INT8, Int8Type)
    LIST_FAST_CASE(UINT16, NPY_UINT16, UInt16Type)
    LIST_FAST_CASE(INT16, NPY_INT16, Int16Type)
    LIST_FAST_CASE(UINT32, NPY_UINT32, UInt32Type)
    LIST_FAST_CASE(INT32, NPY_INT32, Int32Type)
    LIST_FAST_CASE(UINT64, NPY_UINT64, UInt64Type)
    LIST_FAST_CASE(INT64, NPY_INT64, Int64Type)
    LIST_SLOW_CASE(DATE32)
    LIST_SLOW_CASE(DATE64)
    LIST_SLOW_CASE(TIME64)
    LIST_FAST_CASE(TIMESTAMP, NPY_DATETIME, TimestampType)
    LIST_FAST_CASE(HALF_FLOAT, NPY_FLOAT16, HalfFloatType)
    LIST_FAST_CASE(FLOAT, NPY_FLOAT, FloatType)
    LIST_FAST_CASE(DOUBLE, NPY_DOUBLE, DoubleType)
    LIST_SLOW_CASE(BINARY)
    LIST_SLOW_CASE(FIXED_SIZE_BINARY)
    LIST_SLOW_CASE(STRING)
    case Type::LIST: {
      return value_converter_->AppendSingleVirtual(obj);
    }
    default: {
      std::stringstream ss;
      ss << "Unknown list item type: ";
      ss << value_type_->ToString();
      return Status::TypeError(ss.str());
    }
  }
}

// ----------------------------------------------------------------------
// Convert structs

class StructConverter : public TypedConverter<StructType, StructConverter> {
 public:
  explicit StructConverter(bool from_pandas, bool strict_conversions)
      : from_pandas_(from_pandas), strict_conversions_(strict_conversions) {}

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
      RETURN_NOT_OK(
          GetConverter(field_type, from_pandas_, strict_conversions_, &value_converter));
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
  bool strict_conversions_;
};

class DecimalConverter : public TypedConverter<arrow::Decimal128Type, DecimalConverter> {
 public:
  using BASE = TypedConverter<arrow::Decimal128Type, DecimalConverter>;
  Status Init(ArrayBuilder* builder) override {
    RETURN_NOT_OK(BASE::Init(builder));
    decimal_type_ = checked_cast<const DecimalType*>(typed_builder_->type().get());
    return internal::ImportDecimalType(&pydecimal_type_);
  }

  Status AppendItem(PyObject* obj) {
    if (PyObject_IsInstance(obj, pydecimal_type_.obj()) == 1) {
      Decimal128 value;
      RETURN_NOT_OK(internal::DecimalFromPythonDecimal(obj, *decimal_type_, &value));
      return typed_builder_->Append(value);
    } else {
      // PyObject_IsInstance could error and set an exception
      RETURN_IF_PYERROR();
      return InvalidValue(obj, "converting to Decimal128");
    }
  }

 private:
  const DecimalType* decimal_type_;
  OwnedRefNoGIL pydecimal_type_;
};

#define NUMERIC_CONVERTER(TYPE_ENUM, TYPE)                                     \
  case Type::TYPE_ENUM:                                                        \
    if (from_pandas) {                                                         \
      *out = std::unique_ptr<SeqConverter>(new NumericConverter<TYPE, true>);  \
    } else {                                                                   \
      *out = std::unique_ptr<SeqConverter>(new NumericConverter<TYPE, false>); \
    }                                                                          \
    break;

#define SIMPLE_CONVERTER_CASE(TYPE_ENUM, TYPE_CLASS)      \
  case Type::TYPE_ENUM:                                   \
    *out = std::unique_ptr<SeqConverter>(new TYPE_CLASS); \
    break;

// Dynamic constructor for sequence converters
Status GetConverter(const std::shared_ptr<DataType>& type, bool from_pandas,
                    bool strict_conversions, std::unique_ptr<SeqConverter>* out) {
  switch (type->id()) {
    SIMPLE_CONVERTER_CASE(NA, NullConverter);
    SIMPLE_CONVERTER_CASE(BOOL, BoolConverter);
    NUMERIC_CONVERTER(INT8, Int8Type);
    NUMERIC_CONVERTER(INT16, Int16Type);
    NUMERIC_CONVERTER(INT32, Int32Type);
    NUMERIC_CONVERTER(INT64, Int64Type);
    NUMERIC_CONVERTER(UINT8, UInt8Type);
    NUMERIC_CONVERTER(UINT16, UInt16Type);
    NUMERIC_CONVERTER(UINT32, UInt32Type);
    NUMERIC_CONVERTER(UINT64, UInt64Type);
    SIMPLE_CONVERTER_CASE(DATE32, Date32Converter);
    SIMPLE_CONVERTER_CASE(DATE64, Date64Converter);
    NUMERIC_CONVERTER(HALF_FLOAT, HalfFloatType);
    NUMERIC_CONVERTER(FLOAT, FloatType);
    NUMERIC_CONVERTER(DOUBLE, DoubleType);
    SIMPLE_CONVERTER_CASE(STRING, StringConverter(strict_conversions));
    SIMPLE_CONVERTER_CASE(BINARY, BytesConverter);
    SIMPLE_CONVERTER_CASE(FIXED_SIZE_BINARY, FixedWidthBytesConverter);
    case Type::TIMESTAMP: {
      *out = std::unique_ptr<SeqConverter>(
          new TimestampConverter(checked_cast<const TimestampType&>(*type).unit()));
      break;
    }
    case Type::TIME32: {
      return Status::NotImplemented("No sequence converter for time32 available");
    }
      SIMPLE_CONVERTER_CASE(TIME64, TimeConverter);
      SIMPLE_CONVERTER_CASE(DECIMAL, DecimalConverter);
    case Type::LIST:
      *out = std::unique_ptr<SeqConverter>(
          new ListConverter(from_pandas, strict_conversions));
      break;
    case Type::STRUCT:
      *out = std::unique_ptr<SeqConverter>(
          new StructConverter(from_pandas, strict_conversions));
      break;
    default:
      std::stringstream ss;
      ss << "Sequence converter for type " << type->ToString() << " not implemented";
      return Status::NotImplemented(ss.str());
  }
  return Status::OK();
}

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

  // In some cases, type inference may be "loose", like strings. If the user
  // passed pa.string(), then we will error if we encounter any non-UTF8
  // value. If not, then we will allow the result to be a BinaryArray
  bool strict_conversions = false;

  if (options.type == nullptr) {
    RETURN_NOT_OK(InferArrowType(seq, &real_type));
  } else {
    real_type = options.type;
    strict_conversions = true;
  }
  DCHECK_GE(size, 0);

  // Handle NA / NullType case
  if (real_type->id() == Type::NA) {
    ArrayVector chunks = {std::make_shared<NullArray>(size)};
    *out = std::make_shared<ChunkedArray>(chunks);
    return Status::OK();
  }

  // Create the sequence converter, initialize with the builder
  std::unique_ptr<SeqConverter> converter;
  RETURN_NOT_OK(
      GetConverter(real_type, options.from_pandas, strict_conversions, &converter));

  // Create ArrayBuilder for type, then pass into the SeqConverter
  // instance. The reason this is created here rather than in GetConverter is
  // because of nested types (child SeqConverter objects need the child
  // builders created by MakeBuilder)
  std::unique_ptr<ArrayBuilder> type_builder;
  RETURN_NOT_OK(MakeBuilder(options.pool, real_type, &type_builder));
  RETURN_NOT_OK(converter->Init(type_builder.get()));

  // Convert values
  if (mask != nullptr && mask != Py_None) {
    RETURN_NOT_OK(converter->AppendMultipleMasked(seq, mask, size));
  } else {
    RETURN_NOT_OK(converter->AppendMultiple(seq, size));
  }

  // Retrieve result. Conversion may yield one or more array values
  std::vector<std::shared_ptr<Array>> chunks;
  RETURN_NOT_OK(converter->GetResult(&chunks));

  // If the converted type does not match the user-supplied type, attempt to
  // cast

  // TODO TODO TODO
  //   // Perform cast
  //   compute::FunctionContext context(pool_);
  //   compute::CastOptions options;
  //   options.allow_int_overflow = false;

  //   std::shared_ptr<Array> casted;
  //   RETURN_NOT_OK(compute::Cast(&context, *arr, type_, options, &casted));

  //   // Replace with casted values
  //   out_arrays_[position] = casted;

  *out = std::make_shared<ChunkedArray>(chunks);
  return Status::OK();
}

Status ConvertPySequence(PyObject* obj, const PyConversionOptions& options,
                         std::shared_ptr<ChunkedArray>* out) {
  return ConvertPySequence(obj, nullptr, options, out);
}

Status ConvertPySequence(PyObject* obj, std::shared_ptr<Array>* out) {
  std::shared_ptr<ChunkedArray> result;
  RETURN_NOT_OK(ConvertPySequence(obj, nullptr, PyConversionOptions(), &result));
  if (result->num_chunks() != 1) {
    return Status::Invalid("Sequence conversion yielded multiple chunks");
  }
  *out = result->chunk(0);
  return Status::OK();
}

}  // namespace py
}  // namespace arrow
