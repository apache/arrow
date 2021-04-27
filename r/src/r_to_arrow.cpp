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

#include "./arrow_types.h"
#include "./arrow_vctrs.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_dict.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>
#include <arrow/type_traits.h>
#include <arrow/util/bitmap_writer.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/converter.h>
#include <arrow/util/logging.h>
#include <arrow/util/parallel.h>
#include <arrow/util/task_group.h>

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

using internal::Converter;
using internal::DictionaryConverter;
using internal::ListConverter;
using internal::PrimitiveConverter;
using internal::StructConverter;

using internal::MakeChunker;
using internal::MakeConverter;

namespace r {

std::mutex& get_r_mutex() {
  static std::mutex m;
  return m;
}

struct RConversionOptions {
  RConversionOptions() = default;

  std::shared_ptr<arrow::DataType> type;
  bool strict;
  int64_t size;
};

enum RVectorType {
  BOOLEAN,
  UINT8,
  INT32,
  FLOAT64,
  INT64,
  COMPLEX,
  STRING,
  DATAFRAME,
  DATE_INT,
  DATE_DBL,
  TIME,
  POSIXCT,
  POSIXLT,
  BINARY,
  LIST,
  FACTOR,

  OTHER
};

// this flattens out a logical type of what an R object is
// because TYPEOF() is not detailed enough
// we can't use arrow types though as there is no 1-1 mapping
RVectorType GetVectorType(SEXP x) {
  switch (TYPEOF(x)) {
    case LGLSXP:
      return BOOLEAN;
    case RAWSXP:
      return UINT8;
    case INTSXP:
      if (Rf_inherits(x, "factor")) {
        return FACTOR;
      } else if (Rf_inherits(x, "Date")) {
        return DATE_INT;
      }
      return INT32;
    case STRSXP:
      return STRING;
    case CPLXSXP:
      return COMPLEX;
    case REALSXP: {
      if (Rf_inherits(x, "Date")) {
        return DATE_DBL;
      } else if (Rf_inherits(x, "integer64")) {
        return INT64;
      } else if (Rf_inherits(x, "POSIXct")) {
        return POSIXCT;
      } else if (Rf_inherits(x, "difftime")) {
        return TIME;
      } else {
        return FLOAT64;
      }
    }
    case VECSXP: {
      if (Rf_inherits(x, "data.frame")) {
        return DATAFRAME;
      }

      if (Rf_inherits(x, "POSIXlt")) {
        return POSIXLT;
      }

      if (Rf_inherits(x, "arrow_binary")) {
        return BINARY;
      }

      return LIST;
    }
    default:
      break;
  }
  return OTHER;
}

template <typename T>
bool is_NA(T value);

template <>
bool is_NA<int>(int value) {
  return value == NA_INTEGER;
}

template <>
bool is_NA<double>(double value) {
  return ISNA(value);
}

template <>
bool is_NA<uint8_t>(uint8_t value) {
  return false;
}

template <>
bool is_NA<cpp11::r_bool>(cpp11::r_bool value) {
  return value == NA_LOGICAL;
}

template <>
bool is_NA<cpp11::r_string>(cpp11::r_string value) {
  return value == NA_STRING;
}

template <>
bool is_NA<SEXP>(SEXP value) {
  return Rf_isNull(value);
}

template <>
bool is_NA<int64_t>(int64_t value) {
  return value == NA_INT64;
}

template <typename T>
class RVectorIterator {
 public:
  using value_type = T;
  RVectorIterator(SEXP x, int64_t start)
      : ptr_x_(reinterpret_cast<const T*>(DATAPTR_RO(x)) + start) {}

  RVectorIterator& operator++() {
    ++ptr_x_;
    return *this;
  }

  const T operator*() const { return *ptr_x_; }

 private:
  const T* ptr_x_;
};

template <typename T>
class RVectorIterator_ALTREP {
 public:
  using value_type = T;
  using data_type =
      typename std::conditional<std::is_same<T, int64_t>::value, double, T>::type;
  using r_vector_type = cpp11::r_vector<data_type>;
  using r_vector_iterator = typename r_vector_type::const_iterator;

  RVectorIterator_ALTREP(SEXP x, int64_t start)
      : vector_(x), it_(vector_.begin() + start) {}

  RVectorIterator_ALTREP& operator++() {
    ++it_;
    return *this;
  }

  const T operator*() const { return GetValue(*it_); }

  static T GetValue(data_type x) { return x; }

 private:
  r_vector_type vector_;
  r_vector_iterator it_;
};

template <>
int64_t RVectorIterator_ALTREP<int64_t>::GetValue(double x) {
  int64_t value;
  memcpy(&value, &x, sizeof(int64_t));
  return value;
}

template <typename Iterator, typename AppendNull, typename AppendValue>
Status VisitVector(Iterator it, int64_t n, AppendNull&& append_null,
                   AppendValue&& append_value) {
  for (R_xlen_t i = 0; i < n; i++, ++it) {
    auto value = *it;

    if (is_NA<typename Iterator::value_type>(value)) {
      RETURN_NOT_OK(append_null());
    } else {
      RETURN_NOT_OK(append_value(value));
    }
  }

  return Status::OK();
}

class RConverter : public Converter<SEXP, RConversionOptions> {
 public:
  virtual Status Append(SEXP) { return Status::NotImplemented("Append"); }

  virtual Status Extend(SEXP values, int64_t size) {
    return Status::NotImplemented("Extend");
  }

  virtual Status ExtendMasked(SEXP values, SEXP mask, int64_t size) {
    return Status::NotImplemented("ExtendMasked");
  }

  virtual bool Parallel() { return false; }
};

template <typename T, typename Enable = void>
class RPrimitiveConverter;

template <typename T>
Result<T> CIntFromRScalarImpl(int64_t value) {
  if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
    return Status::Invalid("value outside of range");
  }
  return static_cast<T>(value);
}

template <>
Result<uint64_t> CIntFromRScalarImpl<uint64_t>(int64_t value) {
  if (value < 0) {
    return Status::Invalid("value outside of range");
  }
  return static_cast<uint64_t>(value);
}

// utility to convert R single values from (int, raw, double and int64) vectors
// to arrow integers and floating point
struct RConvert {
  // ---- convert to an arrow integer
  template <typename Type, typename From>
  static enable_if_integer<Type, Result<typename Type::c_type>> Convert(Type*,
                                                                        From from) {
    return CIntFromRScalarImpl<typename Type::c_type>(from);
  }

  // ---- convert R integer types to double
  template <typename Type, typename From>
  static enable_if_t<std::is_same<Type, const DoubleType>::value &&
                         !std::is_same<From, double>::value,
                     Result<typename Type::c_type>>
  Convert(Type*, From from) {
    constexpr int64_t kDoubleMax = 1LL << 53;
    constexpr int64_t kDoubleMin = -(1LL << 53);

    if (from < kDoubleMin || from > kDoubleMax) {
      return Status::Invalid("Integer value ", from, " is outside of the range exactly",
                             " representable by a IEEE 754 double precision value");
    }
    return static_cast<double>(from);
  }

  // ---- convert double to double
  template <typename Type, typename From>
  static enable_if_t<std::is_same<Type, const DoubleType>::value &&
                         std::is_same<From, double>::value,
                     Result<typename Type::c_type>>
  Convert(Type*, From from) {
    return from;
  }

  // ---- convert R integer types to float
  template <typename Type, typename From>
  static enable_if_t<std::is_same<Type, const FloatType>::value &&
                         !std::is_same<From, double>::value,
                     Result<typename Type::c_type>>
  Convert(Type*, From from) {
    constexpr int64_t kFloatMax = 1LL << 24;
    constexpr int64_t kFloatMin = -(1LL << 24);

    if (from < kFloatMin || from > kFloatMax) {
      return Status::Invalid("Integer value ", from, " is outside of the range exactly",
                             " representable by a IEEE 754 single precision value");
    }
    return static_cast<float>(from);
  }

  // ---- convert double to float
  template <typename Type, typename From>
  static enable_if_t<std::is_same<Type, const FloatType>::value &&
                         std::is_same<From, double>::value,
                     Result<typename Type::c_type>>
  Convert(Type*, From from) {
    return static_cast<float>(from);
  }

  // ---- convert to half float: not implemented
  template <typename Type, typename From>
  static enable_if_t<std::is_same<Type, const HalfFloatType>::value,
                     Result<typename Type::c_type>>
  Convert(Type*, From from) {
    return Status::Invalid("Cannot convert to Half Float");
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_null<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP, int64_t size) override {
    return this->primitive_builder_->AppendNulls(size);
  }

  bool Parallel() override { return true; }
};

// TODO: extend this to BooleanType, but this needs some work in RConvert
template <typename T>
class RPrimitiveConverter<
    T, enable_if_t<is_integer_type<T>::value || is_floating_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    auto rtype = GetVectorType(x);
    switch (rtype) {
      case UINT8:
        return ExtendDispatch<unsigned char>(x, size);
      case INT32:
        return ExtendDispatch<int>(x, size);
      case FLOAT64:
        return ExtendDispatch<double>(x, size);
      case INT64:
        return ExtendDispatch<int64_t>(x, size);

      default:
        break;
    }
    // TODO: mention T in the error
    return Status::Invalid("cannot convert");
  }

  bool Parallel() override { return true; }

 private:
  template <typename r_value_type>
  Status ExtendDispatch(SEXP x, int64_t size) {
    if (ALTREP(x)) {
      // `x` is an ALTREP R vector storing `r_value_type`
      // and that type matches exactly the type of the array this is building

      // constructing `vec` needs to protect `x` so we need locking
      std::lock_guard<std::mutex> lock(arrow::r::get_r_mutex());
      return Extend_impl(RVectorIterator_ALTREP<r_value_type>(x, 0), size);
    } else {
      // `x` is not an ALTREP vector so we have direct access to a range of values
      return Extend_impl(RVectorIterator<r_value_type>(x, 0), size);
    }
  }

  template <typename Iterator>
  Status Extend_impl(Iterator it, int64_t size) {
    using r_value_type = typename Iterator::value_type;
    RETURN_NOT_OK(this->primitive_builder_->Reserve(size));

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };

    if (std::is_same<typename T::c_type, r_value_type>::value) {
      auto append_value = [this](r_value_type value) {
        this->primitive_builder_->UnsafeAppend(value);
        return Status::OK();
      };
      return VisitVector(it, size, append_null, append_value);
    } else {
      auto append_value = [this](r_value_type value) {
        ARROW_ASSIGN_OR_RAISE(auto converted,
                              RConvert::Convert(this->primitive_type_, value));
        this->primitive_builder_->UnsafeAppend(converted);
        return Status::OK();
      };
      return VisitVector(it, size, append_null, append_value);
    }
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_boolean_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    auto rtype = GetVectorType(x);
    if (rtype != BOOLEAN) {
      return Status::Invalid("Expecting a logical vector");
    }

    if (ALTREP(x)) {
      std::lock_guard<std::mutex> lock(arrow::r::get_r_mutex());
      return Extend_impl(RVectorIterator_ALTREP<cpp11::r_bool>(x, 0), size);
    } else {
      return Extend_impl(RVectorIterator<cpp11::r_bool>(x, 0), size);
    }
  }

  bool Parallel() override { return true; }

 private:
  template <typename Iterator>
  Status Extend_impl(Iterator it, int64_t size) {
    RETURN_NOT_OK(this->Reserve(size));

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    auto append_value = [this](cpp11::r_bool value) {
      this->primitive_builder_->UnsafeAppend(value == 1);
      return Status::OK();
    };
    return VisitVector(it, size, append_null, append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_date_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    switch (GetVectorType(x)) {
      case DATE_INT:
        return AppendRange_Date_dispatch<int>(x, size);

      case DATE_DBL:
        return AppendRange_Date_dispatch<double>(x, size);

      case POSIXCT:
        return AppendRange_Posixct_dispatch(x, size);

      default:
        break;
    }

    return Status::Invalid("cannot convert to date type ");
  }

  bool Parallel() override { return true; }

 private:
  template <typename r_value_type>
  Status AppendRange_Date_dispatch(SEXP x, int64_t size) {
    if (ALTREP(x)) {
      std::lock_guard<std::mutex> lock(arrow::r::get_r_mutex());
      return AppendRange_Date(RVectorIterator_ALTREP<r_value_type>(x, 0), size);
    } else {
      return AppendRange_Date(RVectorIterator<r_value_type>(x, 0), size);
    }
  }

  template <typename Iterator>
  Status AppendRange_Date(Iterator it, int64_t size) {
    using r_value_type = typename Iterator::value_type;
    RETURN_NOT_OK(this->Reserve(size));

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    auto append_value = [this](r_value_type value) {
      this->primitive_builder_->UnsafeAppend(FromRDate(this->primitive_type_, value));
      return Status::OK();
    };
    return VisitVector(it, size, append_null, append_value);
  }

  Status AppendRange_Posixct_dispatch(SEXP x, int64_t size) {
    if (ALTREP(x)) {
      std::lock_guard<std::mutex> lock(arrow::r::get_r_mutex());
      return AppendRange_Posixct(RVectorIterator_ALTREP<double>(x, 0), size);
    } else {
      return AppendRange_Posixct(RVectorIterator<double>(x, 0), size);
    }
  }

  template <typename Iterator>
  Status AppendRange_Posixct(Iterator it, int64_t size) {
    using r_value_type = typename Iterator::value_type;
    RETURN_NOT_OK(this->Reserve(size));

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    auto append_value = [this](r_value_type value) {
      this->primitive_builder_->UnsafeAppend(FromPosixct(this->primitive_type_, value));
      return Status::OK();
    };
    return VisitVector(it, size, append_null, append_value);
  }

  static int FromRDate(const Date32Type*, int from) { return from; }

  static int64_t FromRDate(const Date64Type*, int from) {
    constexpr int64_t kMilliSecondsPerDay = 86400000;
    return from * kMilliSecondsPerDay;
  }

  static int FromPosixct(const Date32Type*, double from) {
    constexpr int64_t kSecondsPerDay = 86400;
    return from / kSecondsPerDay;
  }

  static int64_t FromPosixct(const Date64Type*, double from) { return from * 1000; }
};

int64_t get_TimeUnit_multiplier(TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      return 1;
    case TimeUnit::MILLI:
      return 1000;
    case TimeUnit::MICRO:
      return 1000000;
    case TimeUnit::NANO:
      return 1000000000;
    default:
      return 0;
  }
}

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_time_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    RETURN_NOT_OK(this->Reserve(size));
    auto rtype = GetVectorType(x);
    if (rtype != TIME) {
      return Status::Invalid("Invalid conversion to time");
    }

    // multiplier to get the number of seconds from the value stored in the R vector
    int difftime_multiplier;
    std::string unit(CHAR(STRING_ELT(Rf_getAttrib(x, symbols::units), 0)));
    if (unit == "secs") {
      difftime_multiplier = 1;
    } else if (unit == "mins") {
      difftime_multiplier = 60;
    } else if (unit == "hours") {
      difftime_multiplier = 3600;
    } else if (unit == "days") {
      difftime_multiplier = 86400;
    } else if (unit == "weeks") {
      difftime_multiplier = 604800;
    } else {
      return Status::Invalid("unknown difftime unit");
    }

    // then multiply the seconds by this to match the time unit
    auto multiplier =
        get_TimeUnit_multiplier(this->primitive_type_->unit()) * difftime_multiplier;

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    auto append_value = [this, multiplier](double value) {
      auto converted = static_cast<typename T::c_type>(value * multiplier);
      this->primitive_builder_->UnsafeAppend(converted);
      return Status::OK();
    };

    if (ALTREP(x)) {
      std::lock_guard<std::mutex> lock(arrow::r::get_r_mutex());
      return VisitVector(RVectorIterator_ALTREP<double>(x, 0), size, append_null,
                         append_value);
    } else {
      return VisitVector(RVectorIterator<double>(x, 0), size, append_null, append_value);
    }
  }

  bool Parallel() override { return true; }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_timestamp_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != POSIXCT) {
      return Status::Invalid("Invalid conversion to timestamp");
    }

    int64_t multiplier = get_TimeUnit_multiplier(this->primitive_type_->unit());

    auto append_value = [this, multiplier](double value) {
      auto converted = static_cast<typename T::c_type>(value * multiplier);
      this->primitive_builder_->UnsafeAppend(converted);
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };

    if (ALTREP(x)) {
      std::lock_guard<std::mutex> lock(arrow::r::get_r_mutex());
      return VisitVector(RVectorIterator_ALTREP<double>(x, 0), size, append_null,
                         append_value);
    } else {
      return VisitVector(RVectorIterator<double>(x, 0), size, append_null, append_value);
    }
  }

  bool Parallel() override { return true; }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_decimal_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    return Status::NotImplemented("Extend");
  }
};

Status check_binary(SEXP x, int64_t size) {
  RVectorType rtype = GetVectorType(x);
  switch (rtype) {
    case BINARY:
      break;
    case LIST: {
      // check this is a list of raw vectors
      const SEXP* p_x = VECTOR_PTR_RO(x);
      for (R_xlen_t i = 0; i < size; i++, ++p_x) {
        if (TYPEOF(*p_x) != RAWSXP) {
          return Status::Invalid("invalid R type to convert to binary");
        }
      }
      break;
    }
    default:
      return Status::Invalid("invalid R type to convert to binary");
  }
  return Status::OK();
}

template <typename T>
class RPrimitiveConverter<T, enable_if_binary<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  using OffsetType = typename T::offset_type;

  Status Extend(SEXP x, int64_t size) override {
    RETURN_NOT_OK(this->Reserve(size));
    RETURN_NOT_OK(check_binary(x, size));

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };

    auto append_value = [this](SEXP raw) {
      R_xlen_t n = XLENGTH(raw);
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(n));
      this->primitive_builder_->UnsafeAppend(RAW_RO(raw), static_cast<OffsetType>(n));
      return Status::OK();
    };
    return VisitVector(RVectorIterator<SEXP>(x, 0), size, append_null, append_value);
  }

  bool Parallel() override { return true; }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<std::is_same<T, FixedSizeBinaryType>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    RETURN_NOT_OK(this->Reserve(size));
    RETURN_NOT_OK(check_binary(x, size));

    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };

    auto append_value = [this](SEXP raw) {
      R_xlen_t n = XLENGTH(raw);

      if (n != this->primitive_builder_->byte_width()) {
        return Status::Invalid("invalid size");
      }
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(n));
      this->primitive_builder_->UnsafeAppend(RAW_RO(raw));
      return Status::OK();
    };
    return VisitVector(RVectorIterator<SEXP>(x, 0), size, append_null, append_value);
  }

  bool Parallel() override { return true; }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_string_like<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  using OffsetType = typename T::offset_type;

  // TODO: reconsider, but e.g. needed for arrow::r::utf8_strings()
  bool Parallel() override { return false; }

  Status Extend(SEXP x, int64_t size) override {
    int64_t start = 0;
    RVectorType rtype = GetVectorType(x);
    if (rtype != STRING) {
      return Status::Invalid("Expecting a character vector");
    }
    cpp11::strings s(arrow::r::utf8_strings(x));
    RETURN_NOT_OK(this->primitive_builder_->Reserve(s.size()));
    auto it = s.begin() + start;

    // we know all the R strings are utf8 already, so we can get
    // a definite size and then use UnsafeAppend*()
    int64_t total_length = 0;
    for (R_xlen_t i = 0; i < size; i++, ++it) {
      cpp11::r_string si = *it;
      total_length += cpp11::is_na(si) ? 0 : si.size();
    }
    RETURN_NOT_OK(this->primitive_builder_->ReserveData(total_length));

    // append
    it = s.begin() + start;
    for (R_xlen_t i = 0; i < size; i++, ++it) {
      cpp11::r_string si = *it;
      if (si == NA_STRING) {
        this->primitive_builder_->UnsafeAppendNull();
      } else {
        this->primitive_builder_->UnsafeAppend(CHAR(si), si.size());
      }
    }

    return Status::OK();
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_duration_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    // TODO: look in lubridate
    return Status::NotImplemented("Extend");
  }
};

template <typename T>
class RListConverter;

template <typename U, typename Enable = void>
class RDictionaryConverter;

template <typename U>
class RDictionaryConverter<U, enable_if_has_c_type<U>>
    : public DictionaryConverter<U, RConverter> {
 public:
  Status Extend(SEXP x, int64_t size) override {
    return Status::NotImplemented("Extend");
  }
};

template <typename ValueType>
class RDictionaryConverter<ValueType, enable_if_has_string_view<ValueType>>
    : public DictionaryConverter<ValueType, RConverter> {
 public:
  using BuilderType = DictionaryBuilder<ValueType>;

  // TODO: reconsider
  bool Parallel() override { return false; }

  Status Extend(SEXP x, int64_t size) override {
    // first we need to handle the levels
    cpp11::strings levels(Rf_getAttrib(x, R_LevelsSymbol));
    auto memo_array = arrow::r::vec_to_arrow(levels, utf8(), false);
    RETURN_NOT_OK(this->value_builder_->InsertMemoValues(*memo_array));

    // then we can proceed
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != FACTOR) {
      return Status::Invalid("invalid R type to convert to dictionary");
    }

    auto append_null = [this]() { return this->value_builder_->AppendNull(); };

    auto append_value = [this, levels](int value) {
      SEXP s = STRING_ELT(levels, value - 1);
      return this->value_builder_->Append(CHAR(s));
    };

    return VisitVector(RVectorIterator<int>(x, 0), size, append_null, append_value);
  }

  Result<std::shared_ptr<Array>> ToArray() override {
    ARROW_ASSIGN_OR_RAISE(auto result, this->builder_->Finish());

    auto result_type = checked_cast<DictionaryType*>(result->type().get());
    if (this->dict_type_->ordered() && !result_type->ordered()) {
      // TODO: we should not have to do that, there is probably something wrong
      //       in the DictionaryBuilder code
      result->data()->type =
          arrow::dictionary(result_type->index_type(), result_type->value_type(), true);
    }

    return std::make_shared<DictionaryArray>(result->data());
  }
};

template <typename T, typename Enable = void>
struct RConverterTrait;

template <typename T>
struct RConverterTrait<
    T, enable_if_t<!is_nested_type<T>::value && !is_interval_type<T>::value &&
                   !is_extension_type<T>::value>> {
  using type = RPrimitiveConverter<T>;
};

template <typename T>
struct RConverterTrait<T, enable_if_list_like<T>> {
  using type = RListConverter<T>;
};

template <typename T>
class RListConverter : public ListConverter<T, RConverter, RConverterTrait> {
 public:
  // TODO: reconsider
  bool Parallel() override { return false; }

  Status Extend(SEXP x, int64_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != LIST) {
      return Status::Invalid("Cannot convert to list type");
    }

    auto append_null = [this]() { return this->list_builder_->AppendNull(); };

    auto append_value = [this](SEXP value) {
      int n = vctrs::vec_size(value);

      RETURN_NOT_OK(this->list_builder_->ValidateOverflow(n));
      RETURN_NOT_OK(this->list_builder_->Append());
      return this->value_converter_.get()->Extend(value, n);
    };

    return VisitVector(RVectorIterator<SEXP>(x, 0), size, append_null, append_value);
  }
};

class RStructConverter;

template <>
struct RConverterTrait<StructType> {
  using type = RStructConverter;
};

class RStructConverter : public StructConverter<RConverter, RConverterTrait> {
 public:
  // TODO: reconsider
  bool Parallel() override { return false; }

  Status Extend(SEXP x, int64_t size) override {
    // check that x is compatible
    R_xlen_t n_columns = XLENGTH(x);

    if (!Rf_inherits(x, "data.frame") && !Rf_inherits(x, "POSIXlt")) {
      return Status::Invalid("Can only convert data frames to Struct type");
    }

    auto fields = this->struct_type_->fields();
    if (n_columns != static_cast<R_xlen_t>(fields.size())) {
      return Status::RError("Number of fields in struct (", fields.size(),
                            ") incompatible with number of columns in the data frame (",
                            n_columns, ")");
    }

    cpp11::strings x_names = Rf_getAttrib(x, R_NamesSymbol);

    RETURN_NOT_OK(cpp11::unwind_protect([&] {
      for (int i = 0; i < n_columns; i++) {
        const char* name_i = arrow::r::unsafe::utf8_string(x_names[i]);
        auto field_name = fields[i]->name();
        if (field_name != name_i) {
          return Status::RError(
              "Field name in position ", i, " (", field_name,
              ") does not match the name of the column of the data frame (", name_i, ")");
        }
      }

      return Status::OK();
    }));

    for (R_xlen_t i = 0; i < n_columns; i++) {
      SEXP x_i = VECTOR_ELT(x, i);
      if (vctrs::vec_size(x_i) < size) {
        return Status::RError("Degenerated data frame");
      }
    }

    RETURN_NOT_OK(this->Reserve(size));

    for (R_xlen_t i = 0; i < size; i++) {
      RETURN_NOT_OK(struct_builder_->Append());
    }

    for (R_xlen_t i = 0; i < n_columns; i++) {
      auto status = children_[i]->Extend(VECTOR_ELT(x, i), size);
      if (!status.ok()) {
        return Status::Invalid("Problem with column ", (i + 1), " (", fields[i]->name(),
                               "): ", status.ToString());
      }
    }

    return Status::OK();
  }

 protected:
  Status Init(MemoryPool* pool) override {
    return StructConverter<RConverter, RConverterTrait>::Init(pool);
  }
};

template <>
struct RConverterTrait<DictionaryType> {
  template <typename T>
  using dictionary_type = RDictionaryConverter<T>;
};

// ---- short circuit the Converter api entirely when we can do zero-copy

// in some situations we can just use the memory of the R object in an RBuffer
// instead of going through ArrayBuilder, etc ...
bool can_reuse_memory(SEXP x, const std::shared_ptr<arrow::DataType>& type) {
  // TODO: this probably should be disabled when x is an ALTREP object
  //       because MakeSimpleArray below will force materialization
  switch (type->id()) {
    case Type::INT32:
      return TYPEOF(x) == INTSXP && !OBJECT(x);
    case Type::DOUBLE:
      return TYPEOF(x) == REALSXP && !OBJECT(x);
    case Type::INT8:
      return TYPEOF(x) == RAWSXP && !OBJECT(x);
    case Type::INT64:
      return TYPEOF(x) == REALSXP && Rf_inherits(x, "integer64");
    default:
      break;
  }
  return false;
}

// this is only used on some special cases when the arrow Array can just use the memory of
// the R object, via an RBuffer, hence be zero copy
template <int RTYPE, typename RVector, typename Type>
std::shared_ptr<Array> MakeSimpleArray(SEXP x) {
  using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
  RVector vec(x);
  auto n = vec.size();
  auto p_vec_start = reinterpret_cast<const value_type*>(DATAPTR_RO(vec));
  auto p_vec_end = p_vec_start + n;
  std::vector<std::shared_ptr<Buffer>> buffers{nullptr,
                                               std::make_shared<RBuffer<RVector>>(vec)};

  int null_count = 0;

  auto first_na = std::find_if(p_vec_start, p_vec_end, is_NA<value_type>);
  if (first_na < p_vec_end) {
    auto null_bitmap =
        ValueOrStop(AllocateBuffer(BitUtil::BytesForBits(n), gc_memory_pool()));
    internal::FirstTimeBitmapWriter bitmap_writer(null_bitmap->mutable_data(), 0, n);

    // first loop to clear all the bits before the first NA
    auto j = std::distance(p_vec_start, first_na);
    int i = 0;
    for (; i < j; i++, bitmap_writer.Next()) {
      bitmap_writer.Set();
    }

    auto p_vec = first_na;
    // then finish
    for (; i < n; i++, bitmap_writer.Next(), ++p_vec) {
      if (is_NA<value_type>(*p_vec)) {
        bitmap_writer.Clear();
        null_count++;
      } else {
        bitmap_writer.Set();
      }
    }

    bitmap_writer.Finish();
    buffers[0] = std::move(null_bitmap);
  }

  auto data = ArrayData::Make(std::make_shared<Type>(), LENGTH(x), std::move(buffers),
                              null_count, 0 /*offset*/);

  // return the right Array class
  return std::make_shared<typename TypeTraits<Type>::ArrayType>(data);
}

std::shared_ptr<arrow::Array> vec_to_arrow__reuse_memory(SEXP x) {
  auto type = TYPEOF(x);

  if (type == INTSXP) {
    return MakeSimpleArray<INTSXP, cpp11::integers, Int32Type>(x);
  } else if (type == REALSXP && Rf_inherits(x, "integer64")) {
    return MakeSimpleArray<REALSXP, cpp11::doubles, Int64Type>(x);
  } else if (type == REALSXP) {
    return MakeSimpleArray<REALSXP, cpp11::doubles, DoubleType>(x);
  } else if (type == RAWSXP) {
    return MakeSimpleArray<RAWSXP, cpp11::raws, UInt8Type>(x);
  }

  cpp11::stop("Unreachable: you might need to fix can_reuse_memory()");
}

Status vector_to_Array(SEXP x, const std::shared_ptr<arrow::DataType>& type,
                       bool type_inferred,
                       std::shared_ptr<internal::TaskGroup>& task_group,
                       std::shared_ptr<arrow::Array>& out) {
  // short circuit if `x` is already an Array
  if (Rf_inherits(x, "Array")) {
    out = cpp11::as_cpp<std::shared_ptr<arrow::Array>>(x);
    return Status::OK();
  }

  RConversionOptions options;
  options.strict = !type_inferred;
  options.type = type;
  options.size = vctrs::vec_size(x);

  // maybe short circuit when zero-copy is possible
  if (can_reuse_memory(x, options.type)) {
    out = vec_to_arrow__reuse_memory(x);
    return Status::OK();
  }

  // otherwise go through the converter api
  auto converter = ValueOrStop(MakeConverter<RConverter, RConverterTrait>(
      options.type, options, gc_memory_pool()));

  RETURN_NOT_OK(converter->Extend(x, options.size));
  ARROW_ASSIGN_OR_RAISE(out, converter->ToArray());

  return Status::OK();
}

std::shared_ptr<arrow::Array> vec_to_arrow(SEXP x,
                                           const std::shared_ptr<arrow::DataType>& type,
                                           bool type_inferred) {
  // short circuit if `x` is already an Array
  if (Rf_inherits(x, "Array")) {
    return cpp11::as_cpp<std::shared_ptr<arrow::Array>>(x);
  }

  RConversionOptions options;
  options.strict = !type_inferred;
  options.type = type;
  options.size = vctrs::vec_size(x);

  // maybe short circuit when zero-copy is possible
  if (can_reuse_memory(x, options.type)) {
    return vec_to_arrow__reuse_memory(x);
  }

  // otherwise go through the converter api
  auto converter = ValueOrStop(MakeConverter<RConverter, RConverterTrait>(
      options.type, options, gc_memory_pool()));

  StopIfNotOk(converter->Extend(x, options.size));

  return ValueOrStop(converter->ToArray());
}

class RTasks {
 public:
  using Task = std::function<Status()>;

  RTasks()
      : parallel_tasks(arrow::internal::TaskGroup::MakeThreaded(
            arrow::internal::GetCpuThreadPool())){};

  Status Finish() {
    Status status = Status::OK();

    // run the delayed tasks now
    for (auto& task : delayed_serial_tasks) {
      status &= task();
      if (!status.ok()) break;
    }

    // then wait for the parallel tasks to finish
    status &= parallel_tasks->Finish();

    return status;
  }

  void Append(bool parallel, Task&& task) {
    if (parallel) {
      parallel_tasks->Append(task);
    } else {
      delayed_serial_tasks.push_back(std::move(task));
    }
  }

  std::shared_ptr<arrow::internal::TaskGroup> parallel_tasks;
  std::vector<std::function<Status()>> delayed_serial_tasks;
};

}  // namespace r
}  // namespace arrow

arrow::Status check_consistent_column_length(
    const std::vector<std::shared_ptr<arrow::ChunkedArray>>& columns) {
  if (columns.size()) {
    int64_t num_rows = columns[0]->length();

    for (const auto& column : columns) {
      if (column->length() != num_rows) {
        return arrow::Status::Invalid("All columns must have the same length");
      }
    }
  }

  return arrow::Status::OK();
}

// [[arrow::export]]
std::shared_ptr<arrow::Table> Table__from_dots(SEXP lst, SEXP schema_sxp) {
  bool infer_schema = !Rf_inherits(schema_sxp, "Schema");

  int num_fields;
  StopIfNotOk(arrow::r::count_fields(lst, &num_fields));

  // schema + metadata
  std::shared_ptr<arrow::Schema> schema;
  StopIfNotOk(arrow::r::InferSchemaFromDots(lst, schema_sxp, num_fields, schema));
  StopIfNotOk(arrow::r::AddMetadataFromDots(lst, num_fields, schema));

  if (!infer_schema && schema->num_fields() != num_fields) {
    cpp11::stop("incompatible. schema has %d fields, and %d columns are supplied",
                schema->num_fields(), num_fields);
  }

  // table
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns(num_fields);

  if (!infer_schema) {
    auto check_name = [&](int j, SEXP, cpp11::r_string name) {
      std::string cpp_name(name);
      if (schema->field(j)->name() != cpp_name) {
        cpp11::stop("field at index %d has name '%s' != '%s'", j + 1,
                    schema->field(j)->name().c_str(), cpp_name.c_str());
      }
    };
    arrow::r::TraverseDots(lst, num_fields, check_name);
  }

  arrow::r::RTasks tasks;

  arrow::Status status = arrow::Status::OK();

  auto flatten_lst = arrow::r::FlattenDots(lst, num_fields);
  std::vector<std::unique_ptr<arrow::r::RConverter>> converters(num_fields);

  // init converters
  for (int j = 0; j < num_fields && status.ok(); j++) {
    SEXP x = flatten_lst[j];

    if (Rf_inherits(x, "ChunkedArray")) {
      columns[j] = cpp11::as_cpp<std::shared_ptr<arrow::ChunkedArray>>(x);
    } else if (Rf_inherits(x, "Array")) {
      columns[j] = std::make_shared<arrow::ChunkedArray>(
          cpp11::as_cpp<std::shared_ptr<arrow::Array>>(x));
    } else {
      arrow::r::RConversionOptions options;
      options.strict = !infer_schema;
      options.type = schema->field(j)->type();
      options.size = vctrs::vec_size(x);

      // maybe short circuit when zero-copy is possible
      if (arrow::r::can_reuse_memory(x, options.type)) {
        columns[j] = std::make_shared<arrow::ChunkedArray>(
            arrow::r::vec_to_arrow__reuse_memory(x));
      } else {
        auto converter_result =
            arrow::MakeConverter<arrow::r::RConverter, arrow::r::RConverterTrait>(
                options.type, options, gc_memory_pool());
        if (!converter_result.ok()) {
          status = converter_result.status();
          break;
        }
        converters[j] = std::move(converter_result.ValueUnsafe());
      }
    }
  }
  StopIfNotOk(status);

  for (int j = 0; j < num_fields; j++) {
    auto& converter = converters[j];
    if (converter != nullptr) {
      auto task = [j, &converters, &flatten_lst, &columns] {
        auto& converter = converters[j];

        SEXP x = flatten_lst[j];
        RETURN_NOT_OK(converter->Extend(x, converter->options().size));
        ARROW_ASSIGN_OR_RAISE(auto array, converter->ToArray());
        columns[j] = std::make_shared<arrow::ChunkedArray>(array);
        return arrow::Status::OK();
      };

      tasks.Append(converter->Parallel(), std::move(task));
    }
  }

  status &= tasks.Finish();
  status &= check_consistent_column_length(columns);

  StopIfNotOk(status);

  return arrow::Table::Make(schema, columns);
}

// [[arrow::export]]
SEXP vec_to_arrow(SEXP x, SEXP s_type) {
  if (Rf_inherits(x, "Array")) return x;
  bool type_inferred = Rf_isNull(s_type);
  std::shared_ptr<arrow::DataType> type;

  if (type_inferred) {
    type = arrow::r::InferArrowType(x);
  } else {
    type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(s_type);
  }
  return cpp11::to_r6(arrow::r::vec_to_arrow(x, type, type_inferred));
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> DictionaryArray__FromArrays(
    const std::shared_ptr<arrow::DataType>& type,
    const std::shared_ptr<arrow::Array>& indices,
    const std::shared_ptr<arrow::Array>& dict) {
  return ValueOrStop(arrow::DictionaryArray::FromArrays(type, indices, dict));
}

#endif
