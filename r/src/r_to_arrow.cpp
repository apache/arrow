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
  DATE,
  TIME,
  TIMESTAMP,
  BINARY,
  LIST,
  FACTOR,

  OTHER
};

RVectorType GetVectorType(SEXP x) {
  switch (TYPEOF(x)) {
    case LGLSXP:
      return BOOLEAN;
    case RAWSXP:
      return UINT8;
    case INTSXP:
      if (Rf_inherits(x, "factor")) {
        return FACTOR;
      }
      return INT32;
    case STRSXP:
      return STRING;
    case CPLXSXP:
      return COMPLEX;
    case REALSXP: {
      if (Rf_inherits(x, "Date")) {
        return DATE;
      } else if (Rf_inherits(x, "integer64")) {
        return INT64;
      } else if (Rf_inherits(x, "POSIXct")) {
        return TIMESTAMP;
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

struct RScalar {
  RVectorType rtype;
  void* data;
  bool null;
};

struct RBytesView {
  const char* bytes;
  R_xlen_t size;
  bool is_utf8;

  Status ParseString(RScalar* value) {
    SEXP s = *reinterpret_cast<SEXP*>(value->data);
    bytes = CHAR(s);
    size = XLENGTH(s);

    // TODO: test it
    is_utf8 = true;

    return Status::OK();
  }

  Status ParseRaw(RScalar* value) {
    SEXP raw;

    if (value->rtype == LIST || value->rtype == BINARY) {
      raw = *reinterpret_cast<SEXP*>(value->data);
      if (TYPEOF(raw) != RAWSXP) {
        return Status::Invalid("can only handle RAW vectors");
      }
    } else {
      return Status::NotImplemented("cannot parse binary with RBytesView::ParseRaw()");
    }

    bytes = reinterpret_cast<const char*>(RAW_RO(raw));
    size = XLENGTH(raw);
    is_utf8 = false;

    return Status::OK();
  }
};

class RValue {
 public:
  static bool IsNull(RScalar* obj) { return obj->null; }

  // TODO: generalise

  static Result<bool> Convert(const BooleanType*, const RConversionOptions&,
                              RScalar* value) {
    if (value->rtype == BOOLEAN) {
      return *reinterpret_cast<bool*>(value->data);
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to bool");
  }

  static Result<uint16_t> Convert(const HalfFloatType*, const RConversionOptions&,
                                  RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion");
  }

  static Result<float> Convert(const FloatType*, const RConversionOptions&,
                               RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to float");
  }

  static Result<double> Convert(const DoubleType*, const RConversionOptions&,
                                RScalar* value) {
    // TODO: handle conversion from other types
    if (value->rtype == FLOAT64) {
      return *reinterpret_cast<double*>(value->data);
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to double");
  }

  static Result<uint8_t> Convert(const UInt8Type*, const RConversionOptions&,
                                 RScalar* value) {
    // TODO: handle conversion from other types
    if (value->rtype == UINT8) {
      return *reinterpret_cast<uint8_t*>(value->data);
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to uint8");
  }

  static Result<int8_t> Convert(const Int8Type*, const RConversionOptions&,
                                RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to int8");
  }

  static Result<int16_t> Convert(const Int16Type*, const RConversionOptions&,
                                 RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to int16");
  }

  static Result<uint16_t> Convert(const UInt16Type*, const RConversionOptions&,
                                  RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to uint16");
  }

  static Result<int32_t> Convert(const Int32Type*, const RConversionOptions&,
                                 RScalar* value) {
    // TODO: handle conversion from other types
    if (value->rtype == INT32) {
      return *reinterpret_cast<int32_t*>(value->data);
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to int32");
  }

  static Result<uint32_t> Convert(const UInt32Type*, const RConversionOptions&,
                                  RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to uint32");
  }

  static Result<int64_t> Convert(const Int64Type*, const RConversionOptions&,
                                 RScalar* value) {
    // TODO: handle conversion from other types
    if (value->rtype == INT64) {
      return *reinterpret_cast<int64_t*>(value->data);
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to int64");
  }

  static Result<uint64_t> Convert(const UInt64Type*, const RConversionOptions&,
                                  RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to uint64");
  }

  static Result<int32_t> Convert(const Date32Type*, const RConversionOptions&,
                                 RScalar* value) {
    if (value->rtype == DATE) {
      return static_cast<int32_t>(*reinterpret_cast<double*>(value->data));
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to date32");
  }

  static Result<int64_t> Convert(const Date64Type*, const RConversionOptions&,
                                 RScalar* value) {
    constexpr static int64_t kMillisecondsPerDay = 86400000;

    if (value->rtype == DATE) {
      return static_cast<int64_t>(*reinterpret_cast<double*>(value->data) *
                                  kMillisecondsPerDay);
    }

    return Status::Invalid("invalid conversion to date64");
  }

  static Result<int32_t> Convert(const Time32Type*, const RConversionOptions&,
                                 RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to time32");
  }

  static Result<int64_t> Convert(const Time64Type*, const RConversionOptions&,
                                 RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to time64");
  }

  static Result<Decimal128> Convert(const Decimal128Type*, const RConversionOptions&,
                                    RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to decimal128");
  }

  static Result<Decimal256> Convert(const Decimal256Type*, const RConversionOptions&,
                                    RScalar* value) {
    // TODO: improve error
    return Status::Invalid("invalid conversion to decimal256");
  }

  template <typename T>
  static enable_if_string<T, Status> Convert(const T*, const RConversionOptions&,
                                             RScalar* value, RBytesView& view) {
    switch (value->rtype) {
      case STRING:
      case FACTOR:
        return view.ParseString(value);
      default:
        break;
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to string");
  }

  static Status Convert(const BaseBinaryType*, const RConversionOptions&, RScalar* value,
                        RBytesView& view) {
    switch (value->rtype) {
      case BINARY:
      case LIST:
        return view.ParseRaw(value);

      case STRING:
        return Status::NotImplemented("conversion string -> binary");

      default:
        break;
    }

    // TODO: improve error
    return Status::Invalid("invalid conversion to binary");
  }

  static Status Convert(const FixedSizeBinaryType* type, const RConversionOptions&,
                        RScalar* value, RBytesView& view) {
    ARROW_RETURN_NOT_OK(view.ParseRaw(value));
    if (view.size != type->byte_width()) {
      return Status::Invalid("invalid size");
    }
    return Status::OK();
  }
};

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
  return false;
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

template <RVectorType rtype, typename T, class VisitorFunc>
inline Status VisitRPrimitiveVector(SEXP x, R_xlen_t size, VisitorFunc&& func) {
  RScalar obj{rtype, nullptr, false};
  cpp11::r_vector<T> values(x);
  for (T value : values) {
    obj.data = reinterpret_cast<void*>(&value);
    obj.null = is_NA<T>(value);
    RETURN_NOT_OK(func(&obj));
  }
  return Status::OK();
}

template <class VisitorFunc>
inline Status VisitInt64Vector(SEXP x, R_xlen_t size, VisitorFunc&& func) {
  RScalar obj{INT64, nullptr, false};
  cpp11::doubles values(x);
  for (double value : values) {
    obj.data = reinterpret_cast<void*>(&value);
    obj.null = is_NA<int64_t>(*reinterpret_cast<int64_t*>(&value));
    RETURN_NOT_OK(func(&obj));
  }
  return Status::OK();
}

template <class VisitorFunc>
inline Status VisitFactor(SEXP x, R_xlen_t size, VisitorFunc&& func) {
  cpp11::strings levels(Rf_getAttrib(x, R_LevelsSymbol));
  SEXP* levels_ptr = const_cast<SEXP*>(STRING_PTR_RO(levels));

  RScalar obj{FACTOR, nullptr, false};
  cpp11::r_vector<int> values(x);

  for (int value : values) {
    if (is_NA<int>(value)) {
      obj.null = true;
    } else {
      obj.null = false;
      obj.data = reinterpret_cast<void*>(&levels_ptr[value - 1]);
    }
    RETURN_NOT_OK(func(&obj));
  }
  return Status::OK();
}

template <typename T>
inline Status VisitDataFrame(SEXP x, R_xlen_t size, T* converter);

template <typename T>
inline Status VisitVector(SEXP x, R_xlen_t size, T* converter) {
  if (converter->type()->id() == Type::STRUCT) {
    return VisitDataFrame(x, size, converter);
  }

  RVectorType rtype = GetVectorType(x);
  auto func = [&converter](RScalar* obj) { return converter->Append(obj); };
  using VisitorFunc = decltype(func);

  switch (rtype) {
    case BOOLEAN:
      return VisitRPrimitiveVector<BOOLEAN, cpp11::r_bool, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));
    case UINT8:
      return VisitRPrimitiveVector<UINT8, uint8_t, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));
    case INT32:
      return VisitRPrimitiveVector<INT32, int, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));
    case FLOAT64:
      return VisitRPrimitiveVector<FLOAT64, double, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));
    case DATE:
      return VisitRPrimitiveVector<DATE, double, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));

    case STRING:
      return VisitRPrimitiveVector<STRING, cpp11::r_string, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));

    case INT64:
      return VisitInt64Vector<VisitorFunc>(x, size, std::forward<VisitorFunc>(func));

    case BINARY:
      return VisitRPrimitiveVector<BINARY, SEXP, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));

    case LIST:
      return VisitRPrimitiveVector<LIST, SEXP, VisitorFunc>(
          x, size, std::forward<VisitorFunc>(func));

    case FACTOR:
      return VisitFactor<VisitorFunc>(x, size, std::forward<VisitorFunc>(func));

    default:
      break;
  }

  return Status::Invalid("No visitor for R type ", rtype);
}

template <typename T>
Status Extend(T* converter, SEXP x, R_xlen_t size) {
  RETURN_NOT_OK(converter->Reserve(size));
  return VisitVector(x, size, converter);
}

using RConverter = Converter<RScalar*, RConversionOptions>;

template <typename T, typename Enable = void>
class RPrimitiveConverter;

template <typename T>
class RPrimitiveConverter<T, enable_if_null<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Append(RScalar* value) override {
    return this->primitive_builder_->AppendNull();
  }
};

template <typename T>
class RPrimitiveConverter<
    T, enable_if_t<is_number_type<T>::value || is_boolean_type<T>::value ||
                   is_date_type<T>::value || is_time_type<T>::value ||
                   is_decimal_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Append(RScalar* value) {
    if (RValue::IsNull(value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_ASSIGN_OR_RAISE(
          auto converted, RValue::Convert(this->primitive_type_, this->options_, value));
      return this->primitive_builder_->Append(converted);
    }
    return Status::OK();
  }
};

template <typename T>
class RPrimitiveConverter<T, enable_if_binary<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  using OffsetType = typename T::offset_type;

  Status Append(RScalar* value) {
    if (RValue::IsNull(value)) {
      this->primitive_builder_->UnsafeAppendNull();
    } else {
      ARROW_RETURN_NOT_OK(
          RValue::Convert(this->primitive_type_, this->options_, value, view_));
      // Since we don't know the varying length input size in advance, we need to
      // reserve space in the value builder one by one. ReserveData raises CapacityError
      // if the value would not fit into the array.
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(view_.size));
      this->primitive_builder_->UnsafeAppend(view_.bytes,
                                             static_cast<OffsetType>(view_.size));
    }

    return Status::OK();
  }

 protected:
  RBytesView view_;
};

template <typename T>
class RPrimitiveConverter<T, enable_if_t<std::is_same<T, FixedSizeBinaryType>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Append(RScalar* value) {
    if (RValue::IsNull(value)) {
      this->primitive_builder_->UnsafeAppendNull();
    } else {
      ARROW_RETURN_NOT_OK(
          RValue::Convert(this->primitive_type_, this->options_, value, view_));
      // Since we don't know the varying length input size in advance, we need to
      // reserve space in the value builder one by one. ReserveData raises CapacityError
      // if the value would not fit into the array.
      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(view_.size));
      this->primitive_builder_->UnsafeAppend(view_.bytes);
    }

    return Status::OK();
  }

 protected:
  RBytesView view_;
};

template <typename T>
class RPrimitiveConverter<T, enable_if_string_like<T>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  using OffsetType = typename T::offset_type;

  Status Append(RScalar* value) {
    if (RValue::IsNull(value)) {
      return this->primitive_builder_->AppendNull();
    } else {
      ARROW_RETURN_NOT_OK(
          RValue::Convert(this->primitive_type_, this->options_, value, view_));

      if (!view_.is_utf8) {
        observed_binary_ = true;
      }

      ARROW_RETURN_NOT_OK(this->primitive_builder_->ReserveData(view_.size));
      this->primitive_builder_->UnsafeAppend(view_.bytes,
                                             static_cast<OffsetType>(view_.size));
    }
    return Status::OK();
  }

 protected:
  bool observed_binary_ = false;
  RBytesView view_;
};

template <typename T>
class RPrimitiveConverter<
    T, enable_if_t<is_timestamp_type<T>::value || is_duration_type<T>::value>>
    : public PrimitiveConverter<T, RConverter> {
 public:
  Status Append(RScalar* value) {
    return Status::NotImplemented(
        "conversion to timestamp or duration not yet implemented");
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
  Status Append(RScalar* value) override {
    return Status::NotImplemented(
        "dictionaries only implemented with string value types");
  }
};

template <typename U>
class RDictionaryConverter<U, enable_if_has_string_view<U>>
    : public DictionaryConverter<U, RConverter> {
 public:
  Status Append(RScalar* value) override {
    if (RValue::IsNull(value)) {
      return this->value_builder_->AppendNull();
    } else {
      ARROW_RETURN_NOT_OK(
          RValue::Convert(this->value_type_, this->options_, value, view_));
      return this->value_builder_->Append(view_.bytes, static_cast<int32_t>(view_.size));
    }
  }

 protected:
  RBytesView view_;
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
  Status Append(RScalar* value) {
    if (RValue::IsNull(value)) {
      return this->list_builder_->AppendNull();
    }

    // append one element to the list
    RETURN_NOT_OK(this->list_builder_->Append());

    // append the contents through the list value converter
    SEXP obj = *reinterpret_cast<SEXP*>(value->data);
    R_xlen_t size = XLENGTH(obj);
    RETURN_NOT_OK(this->list_builder_->ValidateOverflow(size));
    return Extend(this->value_converter_.get(), obj, size);
  }
};

class RStructConverter;

template <>
struct RConverterTrait<StructType> {
  using type = RStructConverter;
};

class RStructConverter : public StructConverter<RConverter, RConverterTrait> {
 public:
  Status Append(RScalar* value) override {
    return Status::NotImplemented("RStructConverter does not use Append()");
  }

  Status Reserve(int64_t additional_capacity) override {
    // in contrast with StructConverter, this does not Reserve()
    // on children, because it will be done as part of Visit() > Extend()
    return this->builder_->Reserve(additional_capacity);
  }

  Status Visit(SEXP x, R_xlen_t size) {
    // iterate over columns of x
    R_xlen_t n_columns = XLENGTH(x);
    if (!Rf_inherits(x, "data.frame")) {
      return Status::Invalid("Can only convert data frames to Struct type");
    }

    auto struct_builder = checked_cast<StructBuilder*>(this->builder().get());
    for (R_xlen_t i = 0; i < size; i++) {
      RETURN_NOT_OK(struct_builder->Append());
    }

    for (R_xlen_t i = 0; i < n_columns; i++) {
      RETURN_NOT_OK(Extend(this->children_[i].get(), VECTOR_ELT(x, i), size));
    }

    return Status::OK();
  }

 protected:
  Status Init(MemoryPool* pool) override {
    return StructConverter<RConverter, RConverterTrait>::Init(pool);
  }
};

template <typename T>
inline Status VisitDataFrame(SEXP x, R_xlen_t size, T* converter) {
  return static_cast<RStructConverter*>(converter)->Visit(x, size);
}

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
  StopIfNotOk(Extend(converter.get(), x, options.size));

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
