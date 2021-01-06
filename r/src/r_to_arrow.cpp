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
#include <arrow/type_traits.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/converter.h>

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
struct RVectorVisitor {
  using data_type =
      typename std::conditional<std::is_same<T, int64_t>::value, double, T>::type;

  template <typename AppendNull, typename AppendValue>
  static Status Visit(SEXP x, R_xlen_t start, R_xlen_t size, AppendNull&& append_null,
                      AppendValue&& append_value) {
    cpp11::r_vector<data_type> values(x);
    auto it = values.begin() + start;

    for (R_xlen_t i = 0; i < size; i++, ++it) {
      auto value = GetValue(*it);

      if (is_NA<T>(value)) {
        RETURN_NOT_OK(append_null());
      } else {
        RETURN_NOT_OK(append_value(value));
      }
    }

    return Status::OK();
  }

  static T GetValue(data_type x) { return x; }
};

template <>
int64_t RVectorVisitor<int64_t>::GetValue(double x) {
  return *reinterpret_cast<int64_t*>(&x);
}

class RConverter : public Converter<SEXP, RConversionOptions> {
 public:
  virtual Status Append(SEXP) { return Status::Invalid("not using Append()"); }

  virtual Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) {
    RVectorType rtype = GetVectorType(x);
    return Status::Invalid("No visitor for R type ", rtype);
  }
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
  Status AppendRange(SEXP, R_xlen_t start, R_xlen_t size) override {
    return this->primitive_builder_->AppendNulls(size);
  }
};

template <typename T>
class RPrimitiveConverter<
    T, enable_if_t<is_integer_type<T>::value || is_floating_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    auto rtype = GetVectorType(x);
    switch (rtype) {
      case UINT8:
        return AppendRangeImpl<unsigned char>(x, start, size);
      case INT32:
        return AppendRangeImpl<int>(x, start, size);
      case FLOAT64:
        return AppendRangeImpl<double>(x, start, size);
      case INT64:
        return AppendRangeImpl<int64_t>(x, start, size);

      default:
        break;
    }
    // TODO: mention T in the error
    return Status::Invalid("cannot convert");
  }

 private:
  template <typename r_value_type>
  Status AppendRangeImpl(SEXP x, R_xlen_t start, R_xlen_t size) {
    RETURN_NOT_OK(this->Reserve(size));

    auto append_value = [this](r_value_type value) {
      ARROW_ASSIGN_OR_RAISE(auto converted,
                            RConvert::Convert(this->primitive_type_, value));
      this->primitive_builder_->UnsafeAppend(converted);
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    return RVectorVisitor<r_value_type>::Visit(x, start, size, append_null, append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_boolean_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    auto rtype = GetVectorType(x);
    if (rtype != BOOLEAN) {
      return Status::Invalid("cannot convert");
    }
    RETURN_NOT_OK(this->Reserve(size));

    auto append_value = [this](cpp11::r_bool value) {
      this->primitive_builder_->UnsafeAppend(value == 1);
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    return RVectorVisitor<cpp11::r_bool>::Visit(x, start, size, append_null,
                                                append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_date_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    switch (GetVectorType(x)) {
      case DATE_INT:
        return AppendRange_Date<int>(x, start, size);

      case DATE_DBL:
        return AppendRange_Date<double>(x, start, size);

      case POSIXCT:
        return AppendRange_Posixct(x, start, size);

      default:
        break;
    }

    return Status::Invalid("cannot convert to date type ");
  }

 private:
  template <typename r_value_type>
  Status AppendRange_Date(SEXP x, R_xlen_t start, R_xlen_t size) {
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    auto append_value = [this](r_value_type value) {
      this->primitive_builder_->UnsafeAppend(FromRDate(this->primitive_type_, value));
      return Status::OK();
    };

    return RVectorVisitor<r_value_type>::Visit(x, start, size, append_null, append_value);
  }

  Status AppendRange_Posixct(SEXP x, R_xlen_t start, R_xlen_t size) {
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    auto append_value = [this](double value) {
      this->primitive_builder_->UnsafeAppend(FromPosixct(this->primitive_type_, value));
      return Status::OK();
    };

    return RVectorVisitor<double>::Visit(x, start, size, append_null, append_value);
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
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
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

    auto append_value = [this, multiplier](double value) {
      auto converted = static_cast<typename T::c_type>(value * multiplier);
      this->primitive_builder_->UnsafeAppend(converted);
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    return RVectorVisitor<double>::Visit(x, start, size, append_null, append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_timestamp_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
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
    return RVectorVisitor<double>::Visit(x, start, size, append_null, append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_decimal_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    return Status::NotImplemented("conversion from R to decimal");
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_binary<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  using OffsetType = typename T::offset_type;

  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != BINARY) {
      return Status::Invalid("invalid R type to convert to binary");
    }

    auto append_value = [this](SEXP raw) {
      R_xlen_t n = XLENGTH(raw);
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(n));
      this->primitive_builder_->UnsafeAppend(RAW_RO(raw), static_cast<OffsetType>(n));
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    return RVectorVisitor<SEXP>::Visit(x, start, size, append_null, append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<std::is_same<T, FixedSizeBinaryType>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    // TODO: handle STRSXP
    if (rtype != BINARY) {
      return Status::Invalid("invalid R type to convert to binary");
    }

    auto append_value = [this](SEXP raw) {
      R_xlen_t n = XLENGTH(raw);

      if (n != this->primitive_builder_->byte_width()) {
        return Status::Invalid("invalid size");
      }
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(n));
      this->primitive_builder_->UnsafeAppend(RAW_RO(raw));
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    return RVectorVisitor<SEXP>::Visit(x, start, size, append_null, append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_string_like<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  using OffsetType = typename T::offset_type;

  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != STRING) {
      return Status::Invalid("invalid R type to convert to string");
    }

    auto append_value = [this](cpp11::r_string s) {
      R_xlen_t n = XLENGTH(s);
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(n));
      this->primitive_builder_->UnsafeAppend(CHAR(s), static_cast<OffsetType>(n));
      return Status::OK();
    };
    auto append_null = [this]() {
      this->primitive_builder_->UnsafeAppendNull();
      return Status::OK();
    };
    return RVectorVisitor<cpp11::r_string>::Visit(x, start, size, append_null,
                                                  append_value);
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<is_duration_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    // TODO: look in lubridate
    return Status::NotImplemented("conversion to duration not yet implemented");
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
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    return Status::NotImplemented(
        "dictionaries only implemented with string value types");
  }
};

template <typename U>
class RDictionaryConverter<U, enable_if_has_string_view<U>>
    : public DictionaryConverter<U, RConverter> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != FACTOR) {
      return Status::Invalid("invalid R type to convert to dictionary");
    }

    cpp11::strings levels(Rf_getAttrib(x, R_LevelsSymbol));
    auto append_value = [this, levels](int value) {
      SEXP s = STRING_ELT(levels, value - 1);
      return this->value_builder_->Append(CHAR(s));
    };
    auto append_null = [this]() { return this->value_builder_->AppendNull(); };
    return RVectorVisitor<int>::Visit(x, start, size, append_null, append_value);
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
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    RETURN_NOT_OK(this->Reserve(size));

    RVectorType rtype = GetVectorType(x);
    if (rtype != LIST) {
      return Status::Invalid("Cannot convert to list type");
    }

    auto append_value = [this](SEXP value) {
      R_xlen_t n = XLENGTH(value);
      RETURN_NOT_OK(this->list_builder_->ValidateOverflow(n));
      return this->value_converter_.get()->AppendRange(value, 0, n);
    };
    auto append_null = [this]() { return this->list_builder_->AppendNull(); };
    return RVectorVisitor<SEXP>::Visit(x, start, size, append_null, append_value);
  }
};

class RStructConverter;

template <>
struct RConverterTrait<StructType> {
  using type = RStructConverter;
};

class RStructConverter : public StructConverter<RConverter, RConverterTrait> {
 public:
  Status AppendRange(SEXP x, R_xlen_t start, R_xlen_t size) override {
    // this specifically does not use this->Reserve() because
    // children_[i]->AppendRange() below will reserve the additional capacity
    RETURN_NOT_OK(this->builder_->Reserve(size));

    R_xlen_t n_columns = XLENGTH(x);
    if (!Rf_inherits(x, "data.frame")) {
      return Status::Invalid("Can only convert data frames to Struct type");
    }

    for (R_xlen_t i = 0; i < size; i++) {
      RETURN_NOT_OK(struct_builder_->Append());
    }

    for (R_xlen_t i = 0; i < n_columns; i++) {
      RETURN_NOT_OK(children_[i]->AppendRange(VECTOR_ELT(x, i), start, size));
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

std::shared_ptr<arrow::Array> vec_to_arrow(SEXP x, SEXP s_type) {
  RConversionOptions options;
  options.strict = !Rf_isNull(s_type);

  std::shared_ptr<arrow::DataType> type;
  if (options.strict) {
    options.type = cpp11::as_cpp<std::shared_ptr<arrow::DataType>>(s_type);
  } else {
    options.type = arrow::r::InferArrowType(x);
  }

  options.size = vctrs::short_vec_size(x);

  auto converter = ValueOrStop(MakeConverter<RConverter, RConverterTrait>(
      options.type, options, gc_memory_pool()));
  StopIfNotOk(converter->AppendRange(x, 0, options.size));

  return ValueOrStop(converter->ToArray());
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
SEXP vec_to_arrow(SEXP x, SEXP s_type) {
  if (Rf_inherits(x, "Array")) return x;
  return cpp11::to_r6(arrow::r::vec_to_arrow(x, s_type));
}

#endif
