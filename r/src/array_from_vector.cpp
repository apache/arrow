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

#include <memory>

#include "./arrow_types.h"
#include "./arrow_vctrs.h"

#if defined(ARROW_R_WITH_ARROW)
#include <arrow/array/array_base.h>
#include <arrow/builder.h>
#include <arrow/chunked_array.h>
#include <arrow/util/bitmap_writer.h>
#include <arrow/visitor_inline.h>

using arrow::internal::checked_cast;

namespace arrow {
namespace r {

template <typename T>
inline bool is_na(T value) {
  return false;
}

template <>
inline bool is_na<int64_t>(int64_t value) {
  return value == NA_INT64;
}

template <>
inline bool is_na<double>(double value) {
  return ISNA(value);
}

template <>
inline bool is_na<int>(int value) {
  return value == NA_INTEGER;
}

struct VectorToArrayConverter {
  Status Visit(const arrow::NullType& type) {
    auto* null_builder = checked_cast<NullBuilder*>(builder);
    return null_builder->AppendNulls(XLENGTH(x));
  }

  Status Visit(const arrow::BooleanType& type) {
    ARROW_RETURN_IF(TYPEOF(x) != LGLSXP, Status::RError("Expecting a logical vector"));
    R_xlen_t n = XLENGTH(x);

    auto* bool_builder = checked_cast<BooleanBuilder*>(builder);
    auto* p = LOGICAL(x);

    RETURN_NOT_OK(bool_builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      auto value = p[i];
      if (value == NA_LOGICAL) {
        bool_builder->UnsafeAppendNull();
      } else {
        bool_builder->UnsafeAppend(value == 1);
      }
    }
    return Status::OK();
  }

  Status Visit(const arrow::Int32Type& type) {
    ARROW_RETURN_IF(TYPEOF(x) != INTSXP, Status::RError("Expecting an integer vector"));

    auto* int_builder = checked_cast<Int32Builder*>(builder);

    R_xlen_t n = XLENGTH(x);
    const auto* data = INTEGER(x);

    RETURN_NOT_OK(int_builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      const auto value = data[i];
      if (value == NA_INTEGER) {
        int_builder->UnsafeAppendNull();
      } else {
        int_builder->UnsafeAppend(value);
      }
    }

    return Status::OK();
  }

  Status Visit(const arrow::Int64Type& type) {
    ARROW_RETURN_IF(TYPEOF(x) != REALSXP, Status::RError("Expecting a numeric vector"));
    ARROW_RETURN_IF(Rf_inherits(x, "integer64"),
                    Status::RError("Expecting a vector that inherits integer64"));

    auto* int_builder = checked_cast<Int64Builder*>(builder);

    R_xlen_t n = XLENGTH(x);
    const auto* data = (REAL(x));

    RETURN_NOT_OK(int_builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      const auto value = arrow::util::SafeCopy<int64_t>(data[i]);
      if (value == NA_INT64) {
        int_builder->UnsafeAppendNull();
      } else {
        int_builder->UnsafeAppend(value);
      }
    }

    return Status::OK();
  }

  Status Visit(const arrow::DoubleType& type) {
    ARROW_RETURN_IF(TYPEOF(x) != REALSXP, Status::RError("Expecting a numeric vector"));

    auto* double_builder = checked_cast<DoubleBuilder*>(builder);

    R_xlen_t n = XLENGTH(x);
    const auto* data = (REAL(x));

    RETURN_NOT_OK(double_builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      const auto value = data[i];
      if (ISNA(value)) {
        double_builder->UnsafeAppendNull();
      } else {
        double_builder->UnsafeAppend(value);
      }
    }

    return Status::OK();
  }

  Status Visit(const arrow::BinaryType& type) {
    if (!(Rf_inherits(x, "vctrs_list_of") &&
          TYPEOF(Rf_getAttrib(x, symbols::ptype)) == RAWSXP)) {
      return Status::RError("Expecting a list of raw vectors");
    }
    return Status::OK();
  }

  Status Visit(const arrow::FixedSizeBinaryType& type) {
    if (!(Rf_inherits(x, "vctrs_list_of") &&
          TYPEOF(Rf_getAttrib(x, symbols::ptype)) == RAWSXP)) {
      return Status::RError("Expecting a list of raw vectors");
    }

    return Status::OK();
  }

  template <typename T>
  arrow::enable_if_base_binary<T, Status> Visit(const T& type) {
    using BuilderType = typename TypeTraits<T>::BuilderType;

    ARROW_RETURN_IF(TYPEOF(x) != STRSXP, Status::RError("Expecting a character vector"));

    auto* binary_builder = checked_cast<BuilderType*>(builder);

    R_xlen_t n = XLENGTH(x);
    RETURN_NOT_OK(builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP s = STRING_ELT(x, i);
      if (s == NA_STRING) {
        RETURN_NOT_OK(binary_builder->AppendNull());
        continue;
      } else {
        // Make sure we're ingesting UTF-8
        s = Rf_mkCharCE(Rf_translateCharUTF8(s), CE_UTF8);
      }

      RETURN_NOT_OK(binary_builder->Append(CHAR(s), LENGTH(s)));
    }

    return Status::OK();
  }

  template <typename T>
  arrow::enable_if_base_list<T, Status> Visit(const T& type) {
    using BuilderType = typename TypeTraits<T>::BuilderType;

    ARROW_RETURN_IF(TYPEOF(x) != VECSXP, Status::RError("Expecting a list vector"));

    auto* list_builder = checked_cast<BuilderType*>(builder);
    auto* value_builder = list_builder->value_builder();
    auto value_type = type.value_type();

    R_xlen_t n = XLENGTH(x);
    RETURN_NOT_OK(builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP vector = VECTOR_ELT(x, i);
      if (Rf_isNull(vector)) {
        RETURN_NOT_OK(list_builder->AppendNull());
        continue;
      }

      RETURN_NOT_OK(list_builder->Append());

      // Recurse.
      VectorToArrayConverter converter{vector, value_builder};
      Status status = arrow::VisitTypeInline(*value_type, &converter);
      if (!status.ok()) {
        return Status::RError("Cannot convert list element ", (i + 1),
                              " to an Array of type `", value_type->ToString(),
                              "` : ", status.message());
      }
    }

    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    ARROW_RETURN_IF(TYPEOF(x) != VECSXP, Status::RError("Expecting a list vector"));

    auto* fixed_size_list_builder = checked_cast<FixedSizeListBuilder*>(builder);
    auto* value_builder = fixed_size_list_builder->value_builder();
    auto value_type = type.value_type();
    int list_size = type.list_size();

    R_xlen_t n = XLENGTH(x);
    RETURN_NOT_OK(builder->Reserve(n));
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP vector = VECTOR_ELT(x, i);
      if (Rf_isNull(vector)) {
        RETURN_NOT_OK(fixed_size_list_builder->AppendNull());
        continue;
      }
      RETURN_NOT_OK(fixed_size_list_builder->Append());

      auto vect_type = arrow::r::InferArrowType(vector);
      if (!value_type->Equals(vect_type)) {
        return Status::RError("FixedSizeList vector expecting elements vector of type ",
                              value_type->ToString(), " but got ", vect_type->ToString());
      }
      int vector_size = vctrs::short_vec_size(vector);
      if (vector_size != list_size) {
        return Status::RError("FixedSizeList vector expecting elements vector of size ",
                              list_size, ", not ", vector_size);
      }

      // Recurse.
      VectorToArrayConverter converter{vector, value_builder};
      RETURN_NOT_OK(arrow::VisitTypeInline(*value_type, &converter));
    }

    return Status::OK();
  }

  template <typename T>
  arrow::enable_if_t<is_struct_type<T>::value, Status> Visit(const T& type) {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    ARROW_RETURN_IF(!Rf_inherits(x, "data.frame"),
                    Status::RError("Expecting a data frame"));

    auto* struct_builder = checked_cast<BuilderType*>(builder);

    int64_t n = vctrs::short_vec_size(x);
    RETURN_NOT_OK(struct_builder->Reserve(n));
    RETURN_NOT_OK(struct_builder->AppendValues(n, NULLPTR));

    int num_fields = struct_builder->num_fields();

    // Visit each column of the data frame using the associated
    // field builder
    for (R_xlen_t i = 0; i < num_fields; i++) {
      auto column_builder = struct_builder->field_builder(i);
      SEXP x_i = VECTOR_ELT(x, i);
      int64_t n_i = vctrs::short_vec_size(x_i);
      if (n_i != n) {
        SEXP name_i = STRING_ELT(Rf_getAttrib(x, R_NamesSymbol), i);
        return Status::RError("Degenerated data frame. Column '", CHAR(name_i),
                              "' has size ", n_i, " instead of the number of rows: ", n);
      }

      VectorToArrayConverter converter{x_i, column_builder};
      RETURN_NOT_OK(arrow::VisitTypeInline(*column_builder->type().get(), &converter));
    }

    return Status::OK();
  }

  template <typename T>
  arrow::enable_if_t<std::is_same<DictionaryType, T>::value, Status> Visit(
      const T& type) {
    // TODO: perhaps this replaces MakeFactorArrayImpl ?

    ARROW_RETURN_IF(!Rf_isFactor(x), Status::RError("Expecting a factor"));
    int64_t n = vctrs::short_vec_size(x);

    auto* dict_builder = checked_cast<StringDictionaryBuilder*>(builder);
    RETURN_NOT_OK(dict_builder->Reserve(n));

    SEXP levels = Rf_getAttrib(x, R_LevelsSymbol);
    auto memo = VectorToArrayConverter::Visit(levels, utf8());
    RETURN_NOT_OK(dict_builder->InsertMemoValues(*memo));

    int* p_values = INTEGER(x);
    for (int64_t i = 0; i < n; i++, ++p_values) {
      int v = *p_values;
      if (v == NA_INTEGER) {
        RETURN_NOT_OK(dict_builder->AppendNull());
      } else {
        RETURN_NOT_OK(dict_builder->Append(CHAR(STRING_ELT(levels, v - 1))));
      }
    }

    return Status::OK();
  }

  Status Visit(const arrow::DataType& type) {
    return Status::NotImplemented("Converting vector to arrow type ", type.ToString(),
                                  " not implemented");
  }

  static std::shared_ptr<Array> Visit(SEXP x, const std::shared_ptr<DataType>& type) {
    std::unique_ptr<ArrayBuilder> builder;
    StopIfNotOk(MakeBuilder(arrow::default_memory_pool(), type, &builder));

    VectorToArrayConverter converter{x, builder.get()};
    StopIfNotOk(arrow::VisitTypeInline(*type, &converter));

    std::shared_ptr<Array> result;
    StopIfNotOk(builder->Finish(&result));
    return result;
  }

  SEXP x;
  arrow::ArrayBuilder* builder;
};

template <typename Type>
std::shared_ptr<Array> MakeFactorArrayImpl(Rcpp::IntegerVector_ factor,
                                           const std::shared_ptr<arrow::DataType>& type) {
  using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
  auto n = factor.size();

  std::shared_ptr<Buffer> indices_buffer =
      ValueOrStop(AllocateBuffer(n * sizeof(value_type)));

  std::vector<std::shared_ptr<Buffer>> buffers{nullptr, indices_buffer};

  int64_t null_count = 0;
  R_xlen_t i = 0;
  auto p_factor = factor.begin();
  auto p_indices = reinterpret_cast<value_type*>(indices_buffer->mutable_data());
  for (; i < n; i++, ++p_indices, ++p_factor) {
    if (*p_factor == NA_INTEGER) break;
    *p_indices = *p_factor - 1;
  }

  if (i < n) {
    // there are NA's so we need a null buffer
    auto null_buffer = ValueOrStop(AllocateBuffer(BitUtil::BytesForBits(n)));
    internal::FirstTimeBitmapWriter null_bitmap_writer(null_buffer->mutable_data(), 0, n);

    // catch up
    for (R_xlen_t j = 0; j < i; j++, null_bitmap_writer.Next()) {
      null_bitmap_writer.Set();
    }

    // resume offset filling
    for (; i < n; i++, ++p_indices, ++p_factor, null_bitmap_writer.Next()) {
      if (*p_factor == NA_INTEGER) {
        null_bitmap_writer.Clear();
        null_count++;
      } else {
        null_bitmap_writer.Set();
        *p_indices = *p_factor - 1;
      }
    }

    null_bitmap_writer.Finish();
    buffers[0] = std::move(null_buffer);
  }

  auto array_indices_data =
      ArrayData::Make(std::make_shared<Type>(), n, std::move(buffers), null_count, 0);
  auto array_indices = MakeArray(array_indices_data);

  SEXP levels = Rf_getAttrib(factor, R_LevelsSymbol);
  auto dict = VectorToArrayConverter::Visit(levels, utf8());

  return ValueOrStop(DictionaryArray::FromArrays(type, array_indices, dict));
}

std::shared_ptr<Array> MakeFactorArray(Rcpp::IntegerVector_ factor,
                                       const std::shared_ptr<arrow::DataType>& type) {
  const auto& dict_type = checked_cast<const arrow::DictionaryType&>(*type);
  switch (dict_type.index_type()->id()) {
    case Type::INT8:
      return MakeFactorArrayImpl<arrow::Int8Type>(factor, type);
    case Type::INT16:
      return MakeFactorArrayImpl<arrow::Int16Type>(factor, type);
    case Type::INT32:
      return MakeFactorArrayImpl<arrow::Int32Type>(factor, type);
    case Type::INT64:
      return MakeFactorArrayImpl<arrow::Int64Type>(factor, type);
    default:
      Rcpp::stop(tfm::format("Cannot convert to dictionary with index_type %s",
                             dict_type.index_type()->ToString()));
  }
}

std::shared_ptr<Array> MakeStructArray(SEXP df, const std::shared_ptr<DataType>& type) {
  int n = type->num_fields();
  std::vector<std::shared_ptr<Array>> children(n);
  for (int i = 0; i < n; i++) {
    children[i] = Array__from_vector(VECTOR_ELT(df, i), type->field(i)->type(), true);
  }

  int64_t rows = n ? children[0]->length() : 0;
  return std::make_shared<StructArray>(type, rows, children);
}

template <typename T>
int64_t time_cast(T value);

template <>
inline int64_t time_cast<int>(int value) {
  return static_cast<int64_t>(value) * 1000;
}

template <>
inline int64_t time_cast<double>(double value) {
  return static_cast<int64_t>(value * 1000);
}

}  // namespace r
}  // namespace arrow

// ---------------- new api

namespace arrow {

namespace internal {

template <typename T, typename Target,
          typename std::enable_if<std::is_signed<Target>::value, Target>::type = 0>
Status int_cast(T x, Target* out) {
  if (static_cast<int64_t>(x) < std::numeric_limits<Target>::min() ||
      static_cast<int64_t>(x) > std::numeric_limits<Target>::max()) {
    return Status::Invalid("Value is too large to fit in C integer type");
  }
  *out = static_cast<Target>(x);
  return Status::OK();
}

template <typename T>
struct usigned_type;

template <typename T, typename Target,
          typename std::enable_if<std::is_unsigned<Target>::value, Target>::type = 0>
Status int_cast(T x, Target* out) {
  // we need to compare between unsigned integers
  uint64_t x64 = x;
  if (x64 < 0 || x64 > std::numeric_limits<Target>::max()) {
    return Status::Invalid("Value is too large to fit in C integer type");
  }
  *out = static_cast<Target>(x);
  return Status::OK();
}

template <typename Int>
Status double_cast(Int x, double* out) {
  *out = static_cast<double>(x);
  return Status::OK();
}

template <>
Status double_cast<int64_t>(int64_t x, double* out) {
  constexpr int64_t kDoubleMax = 1LL << 53;
  constexpr int64_t kDoubleMin = -(1LL << 53);

  if (x < kDoubleMin || x > kDoubleMax) {
    return Status::Invalid("integer value ", x, " is outside of the range exactly",
                           " representable by a IEEE 754 double precision value");
  }
  *out = static_cast<double>(x);
  return Status::OK();
}

// used for int and int64_t
template <typename T>
Status float_cast(T x, float* out) {
  constexpr int64_t kHalfFloatMax = 1LL << 24;
  constexpr int64_t kHalfFloatMin = -(1LL << 24);

  int64_t x64 = static_cast<int64_t>(x);
  if (x64 < kHalfFloatMin || x64 > kHalfFloatMax) {
    return Status::Invalid("integer value ", x, " is outside of the range exactly",
                           " representable by a IEEE 754 half precision value");
  }

  *out = static_cast<float>(x);
  return Status::OK();
}

template <>
Status float_cast<double>(double x, float* out) {
  // TODO: is there some sort of floating point overflow ?
  *out = static_cast<float>(x);
  return Status::OK();
}

}  // namespace internal

namespace r {

class VectorConverter;

Status GetConverter(const std::shared_ptr<DataType>& type,
                    std::unique_ptr<VectorConverter>* out);

class VectorConverter {
 public:
  virtual ~VectorConverter() = default;

  virtual Status Init(ArrayBuilder* builder) = 0;

  virtual Status Ingest(SEXP obj) = 0;

  virtual Status GetResult(std::shared_ptr<arrow::Array>* result) {
    return builder_->Finish(result);
  }

  ArrayBuilder* builder() const { return builder_; }

 protected:
  ArrayBuilder* builder_;
};

class NullVectorConverter : public VectorConverter {
 public:
  using BuilderType = NullBuilder;

  ~NullVectorConverter() {}

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    typed_builder_ = checked_cast<BuilderType*>(builder_);
    return Status::OK();
  }

  Status Ingest(SEXP obj) override {
    RETURN_NOT_OK(typed_builder_->AppendNulls(XLENGTH(obj)));
    return Status::OK();
  }

 protected:
  BuilderType* typed_builder_;
};

template <typename Type, typename Enable = void>
struct Unbox {};

// unboxer for int type
template <typename Type>
struct Unbox<Type, enable_if_integer<Type>> {
  using BuilderType = typename TypeTraits<Type>::BuilderType;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using CType = typename ArrayType::value_type;

  static inline Status Ingest(BuilderType* builder, SEXP obj) {
    switch (TYPEOF(obj)) {
      case INTSXP:
        return IngestRange<int>(builder, INTEGER(obj), XLENGTH(obj));
      case REALSXP:
        if (Rf_inherits(obj, "integer64")) {
          return IngestRange<int64_t>(builder, reinterpret_cast<int64_t*>(REAL(obj)),
                                      XLENGTH(obj));
        }
        return IngestRange(builder, REAL(obj), XLENGTH(obj));

      // TODO: handle raw and logical
      default:
        break;
    }

    return Status::Invalid(
        tfm::format("Cannot convert R vector of type %s to integer Arrow array",
                    Rcpp::type2name(obj)));
  }

  template <typename T>
  static inline Status IngestRange(BuilderType* builder, T* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (is_na<T>(*p)) {
        builder->UnsafeAppendNull();
      } else {
        CType value = 0;
        RETURN_NOT_OK(internal::int_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }
};

template <>
struct Unbox<DoubleType> {
  static inline Status Ingest(DoubleBuilder* builder, SEXP obj) {
    switch (TYPEOF(obj)) {
      // TODO: handle RAW
      case INTSXP:
        return IngestIntRange<int>(builder, INTEGER(obj), XLENGTH(obj), NA_INTEGER);
      case REALSXP:
        if (Rf_inherits(obj, "integer64")) {
          return IngestIntRange<int64_t>(builder, reinterpret_cast<int64_t*>(REAL(obj)),
                                         XLENGTH(obj), NA_INT64);
        }
        return IngestDoubleRange(builder, REAL(obj), XLENGTH(obj));
    }
    return Status::Invalid("Cannot convert R object to double type");
  }

  template <typename T>
  static inline Status IngestIntRange(DoubleBuilder* builder, T* p, R_xlen_t n, T na) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        double value = 0;
        RETURN_NOT_OK(internal::double_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }

  static inline Status IngestDoubleRange(DoubleBuilder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(*p);
      }
    }
    return Status::OK();
  }
};

template <>
struct Unbox<FloatType> {
  static inline Status Ingest(FloatBuilder* builder, SEXP obj) {
    switch (TYPEOF(obj)) {
      // TODO: handle RAW
      case INTSXP:
        return IngestIntRange<int>(builder, INTEGER(obj), XLENGTH(obj), NA_INTEGER);
      case REALSXP:
        if (Rf_inherits(obj, "integer64")) {
          return IngestIntRange<int64_t>(builder, reinterpret_cast<int64_t*>(REAL(obj)),
                                         XLENGTH(obj), NA_INT64);
        }
        return IngestDoubleRange(builder, REAL(obj), XLENGTH(obj));
    }
    return Status::Invalid("Cannot convert R object to double type");
  }

  template <typename T>
  static inline Status IngestIntRange(FloatBuilder* builder, T* p, R_xlen_t n, T na) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        float value = 0;
        RETURN_NOT_OK(internal::float_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }

  static inline Status IngestDoubleRange(FloatBuilder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        float value;
        RETURN_NOT_OK(internal::float_cast(*p, &value));
        builder->UnsafeAppend(value);
      }
    }
    return Status::OK();
  }
};

template <>
struct Unbox<BooleanType> {
  static inline Status Ingest(BooleanBuilder* builder, SEXP obj) {
    switch (TYPEOF(obj)) {
      case LGLSXP: {
        R_xlen_t n = XLENGTH(obj);
        RETURN_NOT_OK(builder->Resize(n));
        int* p = LOGICAL(obj);
        for (R_xlen_t i = 0; i < n; i++, ++p) {
          if (*p == NA_LOGICAL) {
            builder->UnsafeAppendNull();
          } else {
            builder->UnsafeAppend(*p == 1);
          }
        }
        return Status::OK();
      }

      default:
        break;
    }

    // TODO: include more information about the R object and the target type
    return Status::Invalid("Cannot convert R object to boolean type");
  }
};

template <>
struct Unbox<Date32Type> {
  static inline Status Ingest(Date32Builder* builder, SEXP obj) {
    switch (TYPEOF(obj)) {
      case INTSXP:
        if (Rf_inherits(obj, "Date")) {
          return IngestIntRange(builder, INTEGER(obj), XLENGTH(obj));
        }
        break;
      case REALSXP:
        if (Rf_inherits(obj, "Date")) {
          return IngestDoubleRange(builder, REAL(obj), XLENGTH(obj));
        }
        break;
      default:
        break;
    }
    return Status::Invalid("Cannot convert R object to date32 type");
  }

  static inline Status IngestIntRange(Date32Builder* builder, int* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(*p);
      }
    }
    return Status::OK();
  }

  static inline Status IngestDoubleRange(Date32Builder* builder, double* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(static_cast<int>(*p));
      }
    }
    return Status::OK();
  }
};

template <>
struct Unbox<Date64Type> {
  constexpr static int64_t kMillisecondsPerDay = 86400000;

  static inline Status Ingest(Date64Builder* builder, SEXP obj) {
    switch (TYPEOF(obj)) {
      case INTSXP:
        // number of days since epoch
        if (Rf_inherits(obj, "Date")) {
          return IngestDateInt32Range(builder, INTEGER(obj), XLENGTH(obj));
        }
        break;

      case REALSXP:
        // (fractional number of days since epoch)
        if (Rf_inherits(obj, "Date")) {
          return IngestDateDoubleRange<kMillisecondsPerDay>(builder, REAL(obj),
                                                            XLENGTH(obj));
        }

        // number of seconds since epoch
        if (Rf_inherits(obj, "POSIXct")) {
          return IngestDateDoubleRange<1000>(builder, REAL(obj), XLENGTH(obj));
        }
    }
    return Status::Invalid("Cannot convert R object to date64 type");
  }

  // ingest a integer vector that represents number of days since epoch
  static inline Status IngestDateInt32Range(Date64Builder* builder, int* p, R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));
    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (*p == NA_INTEGER) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(*p * kMillisecondsPerDay);
      }
    }
    return Status::OK();
  }

  // ingest a numeric vector that represents (fractional) number of days since epoch
  template <int64_t MULTIPLIER>
  static inline Status IngestDateDoubleRange(Date64Builder* builder, double* p,
                                             R_xlen_t n) {
    RETURN_NOT_OK(builder->Resize(n));

    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (ISNA(*p)) {
        builder->UnsafeAppendNull();
      } else {
        builder->UnsafeAppend(static_cast<int64_t>(*p * MULTIPLIER));
      }
    }
    return Status::OK();
  }
};

template <typename Type, class Derived>
class TypedVectorConverter : public VectorConverter {
 public:
  using BuilderType = typename TypeTraits<Type>::BuilderType;

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    typed_builder_ = checked_cast<BuilderType*>(builder_);
    return Status::OK();
  }

  Status Ingest(SEXP obj) override { return Unbox<Type>::Ingest(typed_builder_, obj); }

 protected:
  BuilderType* typed_builder_;
};

template <typename Type>
class NumericVectorConverter
    : public TypedVectorConverter<Type, NumericVectorConverter<Type>> {};

class BooleanVectorConverter
    : public TypedVectorConverter<BooleanType, BooleanVectorConverter> {};

class Date32Converter : public TypedVectorConverter<Date32Type, Date32Converter> {};
class Date64Converter : public TypedVectorConverter<Date64Type, Date64Converter> {};

inline int64_t get_time_multiplier(TimeUnit::type unit) {
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

template <typename Type>
class TimeConverter : public VectorConverter {
  using BuilderType = typename TypeTraits<Type>::BuilderType;

 public:
  explicit TimeConverter(TimeUnit::type unit)
      : unit_(unit), multiplier_(get_time_multiplier(unit)) {}

  Status Init(ArrayBuilder* builder) override {
    builder_ = builder;
    typed_builder_ = checked_cast<BuilderType*>(builder);
    return Status::OK();
  }

  Status Ingest(SEXP obj) override {
    if (valid_R_object(obj)) {
      int difftime_multiplier;
      RETURN_NOT_OK(GetDifftimeMultiplier(obj, &difftime_multiplier));
      return Ingest_POSIXct(REAL(obj), XLENGTH(obj), difftime_multiplier);
    }

    return Status::Invalid("Cannot convert R object to timestamp type");
  }

 protected:
  TimeUnit::type unit_;
  BuilderType* typed_builder_;
  int64_t multiplier_;

  Status Ingest_POSIXct(double* p, R_xlen_t n, int difftime_multiplier) {
    RETURN_NOT_OK(typed_builder_->Resize(n));

    for (R_xlen_t i = 0; i < n; i++, ++p) {
      if (ISNA(*p)) {
        typed_builder_->UnsafeAppendNull();
      } else {
        typed_builder_->UnsafeAppend(
            static_cast<int64_t>(*p * multiplier_ * difftime_multiplier));
      }
    }
    return Status::OK();
  }

  virtual bool valid_R_object(SEXP obj) = 0;

  // only used for Time32 and Time64
  virtual Status GetDifftimeMultiplier(SEXP obj, int* res) {
    std::string unit(CHAR(STRING_ELT(Rf_getAttrib(obj, symbols::units), 0)));
    if (unit == "secs") {
      *res = 1;
    } else if (unit == "mins") {
      *res = 60;
    } else if (unit == "hours") {
      *res = 3600;
    } else if (unit == "days") {
      *res = 86400;
    } else if (unit == "weeks") {
      *res = 604800;
    } else {
      return Status::Invalid("unknown difftime unit");
    }
    return Status::OK();
  }
};

class TimestampConverter : public TimeConverter<TimestampType> {
 public:
  explicit TimestampConverter(TimeUnit::type unit) : TimeConverter<TimestampType>(unit) {}

 protected:
  bool valid_R_object(SEXP obj) override {
    return TYPEOF(obj) == REALSXP && Rf_inherits(obj, "POSIXct");
  }

  Status GetDifftimeMultiplier(SEXP obj, int* res) override {
    *res = 1;
    return Status::OK();
  }
};

class Time32Converter : public TimeConverter<Time32Type> {
 public:
  explicit Time32Converter(TimeUnit::type unit) : TimeConverter<Time32Type>(unit) {}

 protected:
  bool valid_R_object(SEXP obj) override {
    return TYPEOF(obj) == REALSXP && Rf_inherits(obj, "difftime");
  }
};

class Time64Converter : public TimeConverter<Time64Type> {
 public:
  explicit Time64Converter(TimeUnit::type unit) : TimeConverter<Time64Type>(unit) {}

 protected:
  bool valid_R_object(SEXP obj) override {
    return TYPEOF(obj) == REALSXP && Rf_inherits(obj, "difftime");
  }
};

template <typename Builder>
class BinaryVectorConverter : public VectorConverter {
 public:
  ~BinaryVectorConverter() {}

  Status Init(ArrayBuilder* builder) {
    typed_builder_ = checked_cast<Builder*>(builder);
    return Status::OK();
  }

  Status Ingest(SEXP obj) {
    ARROW_RETURN_IF(TYPEOF(obj) != VECSXP, Status::RError("Expecting a list"));
    R_xlen_t n = XLENGTH(obj);

    // Reserve enough space before appending
    int64_t size = 0;
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP obj_i = VECTOR_ELT(obj, i);
      if (!Rf_isNull(obj_i)) {
        ARROW_RETURN_IF(TYPEOF(obj_i) != RAWSXP,
                        Status::RError("Expecting a raw vector"));
        size += XLENGTH(obj_i);
      }
    }
    RETURN_NOT_OK(typed_builder_->Reserve(size));

    // append
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP obj_i = VECTOR_ELT(obj, i);
      if (Rf_isNull(obj_i)) {
        RETURN_NOT_OK(typed_builder_->AppendNull());
      } else {
        RETURN_NOT_OK(typed_builder_->Append(RAW(obj_i), XLENGTH(obj_i)));
      }
    }
    return Status::OK();
  }

  Status GetResult(std::shared_ptr<arrow::Array>* result) {
    return typed_builder_->Finish(result);
  }

 private:
  Builder* typed_builder_;
};

class FixedSizeBinaryVectorConverter : public VectorConverter {
 public:
  ~FixedSizeBinaryVectorConverter() {}

  Status Init(ArrayBuilder* builder) {
    typed_builder_ = checked_cast<FixedSizeBinaryBuilder*>(builder);
    return Status::OK();
  }

  Status Ingest(SEXP obj) {
    ARROW_RETURN_IF(TYPEOF(obj) != VECSXP, Status::RError("Expecting a list"));
    R_xlen_t n = XLENGTH(obj);

    // Reserve enough space before appending
    int32_t byte_width = typed_builder_->byte_width();
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP obj_i = VECTOR_ELT(obj, i);
      if (!Rf_isNull(obj_i)) {
        ARROW_RETURN_IF(TYPEOF(obj_i) != RAWSXP,
                        Status::RError("Expecting a raw vector"));
        ARROW_RETURN_IF(XLENGTH(obj_i) != byte_width,
                        Status::RError("Expecting a raw vector of ", byte_width,
                                       " bytes, not ", XLENGTH(obj_i)));
      }
    }
    RETURN_NOT_OK(typed_builder_->Reserve(n * byte_width));

    // append
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP obj_i = VECTOR_ELT(obj, i);
      if (Rf_isNull(obj_i)) {
        RETURN_NOT_OK(typed_builder_->AppendNull());
      } else {
        RETURN_NOT_OK(typed_builder_->Append(RAW(obj_i)));
      }
    }
    return Status::OK();
  }

  Status GetResult(std::shared_ptr<arrow::Array>* result) {
    return typed_builder_->Finish(result);
  }

 private:
  FixedSizeBinaryBuilder* typed_builder_;
};

template <typename Builder>
class StringVectorConverter : public VectorConverter {
 public:
  ~StringVectorConverter() {}

  Status Init(ArrayBuilder* builder) {
    typed_builder_ = checked_cast<Builder*>(builder);
    return Status::OK();
  }

  Status Ingest(SEXP obj) {
    ARROW_RETURN_IF(TYPEOF(obj) != STRSXP,
                    Status::RError("Expecting a character vector"));
    R_xlen_t n = XLENGTH(obj);

    // Reserve enough space before appending
    int64_t size = 0;
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP string_i = STRING_ELT(obj, i);
      if (string_i != NA_STRING) {
        size += XLENGTH(Rf_mkCharCE(Rf_translateCharUTF8(string_i), CE_UTF8));
      }
    }
    RETURN_NOT_OK(typed_builder_->Reserve(size));

    // append
    for (R_xlen_t i = 0; i < n; i++) {
      SEXP string_i = STRING_ELT(obj, i);
      if (string_i == NA_STRING) {
        RETURN_NOT_OK(typed_builder_->AppendNull());
      } else {
        // Make sure we're ingesting UTF-8
        string_i = Rf_mkCharCE(Rf_translateCharUTF8(string_i), CE_UTF8);
        RETURN_NOT_OK(typed_builder_->Append(CHAR(string_i), XLENGTH(string_i)));
      }
    }
    return Status::OK();
  }

  Status GetResult(std::shared_ptr<arrow::Array>* result) {
    return typed_builder_->Finish(result);
  }

 private:
  Builder* typed_builder_;
};

#define NUMERIC_CONVERTER(TYPE_ENUM, TYPE)                                               \
  case Type::TYPE_ENUM:                                                                  \
    *out =                                                                               \
        std::unique_ptr<NumericVectorConverter<TYPE>>(new NumericVectorConverter<TYPE>); \
    return Status::OK()

#define SIMPLE_CONVERTER_CASE(TYPE_ENUM, TYPE) \
  case Type::TYPE_ENUM:                        \
    *out = std::unique_ptr<TYPE>(new TYPE);    \
    return Status::OK()

#define TIME_CONVERTER_CASE(TYPE_ENUM, DATA_TYPE, TYPE)                                \
  case Type::TYPE_ENUM:                                                                \
    *out =                                                                             \
        std::unique_ptr<TYPE>(new TYPE(checked_cast<DATA_TYPE*>(type.get())->unit())); \
    return Status::OK()

Status GetConverter(const std::shared_ptr<DataType>& type,
                    std::unique_ptr<VectorConverter>* out) {
  switch (type->id()) {
    SIMPLE_CONVERTER_CASE(BINARY, BinaryVectorConverter<arrow::BinaryBuilder>);
    SIMPLE_CONVERTER_CASE(LARGE_BINARY, BinaryVectorConverter<arrow::LargeBinaryBuilder>);
    SIMPLE_CONVERTER_CASE(FIXED_SIZE_BINARY, FixedSizeBinaryVectorConverter);
    SIMPLE_CONVERTER_CASE(BOOL, BooleanVectorConverter);
    SIMPLE_CONVERTER_CASE(STRING, StringVectorConverter<arrow::StringBuilder>);
    SIMPLE_CONVERTER_CASE(LARGE_STRING, StringVectorConverter<arrow::LargeStringBuilder>);
    NUMERIC_CONVERTER(INT8, Int8Type);
    NUMERIC_CONVERTER(INT16, Int16Type);
    NUMERIC_CONVERTER(INT32, Int32Type);
    NUMERIC_CONVERTER(INT64, Int64Type);
    NUMERIC_CONVERTER(UINT8, UInt8Type);
    NUMERIC_CONVERTER(UINT16, UInt16Type);
    NUMERIC_CONVERTER(UINT32, UInt32Type);
    NUMERIC_CONVERTER(UINT64, UInt64Type);

    // TODO: not sure how to handle half floats
    //       the python code uses npy_half
    // NUMERIC_CONVERTER(HALF_FLOAT, HalfFloatType);
    NUMERIC_CONVERTER(FLOAT, FloatType);
    NUMERIC_CONVERTER(DOUBLE, DoubleType);

    SIMPLE_CONVERTER_CASE(DATE32, Date32Converter);
    SIMPLE_CONVERTER_CASE(DATE64, Date64Converter);

    // TODO: probably after we merge ARROW-3628
    // case Type::DECIMAL:

    TIME_CONVERTER_CASE(TIME32, Time32Type, Time32Converter);
    TIME_CONVERTER_CASE(TIME64, Time64Type, Time64Converter);
    TIME_CONVERTER_CASE(TIMESTAMP, TimestampType, TimestampConverter);

    case Type::NA:
      *out = std::unique_ptr<NullVectorConverter>(new NullVectorConverter);
      return Status::OK();

    default:
      break;
  }
  return Status::NotImplemented("type not implemented");
}

static inline std::shared_ptr<arrow::DataType> IndexTypeForFactors(int n_factors) {
  if (n_factors < INT8_MAX) {
    return arrow::int8();
  } else if (n_factors < INT16_MAX) {
    return arrow::int16();
  } else {
    return arrow::int32();
  }
}

std::shared_ptr<arrow::DataType> InferArrowTypeFromFactor(SEXP factor) {
  SEXP factors = Rf_getAttrib(factor, R_LevelsSymbol);
  auto index_type = IndexTypeForFactors(Rf_length(factors));
  bool is_ordered = Rf_inherits(factor, "ordered");
  return dictionary(index_type, arrow::utf8(), is_ordered);
}

template <int VectorType>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector(SEXP x) {
  Rcpp::stop("Unknown vector type: ", VectorType);
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<ENVSXP>(SEXP x) {
  if (Rf_inherits(x, "Array")) {
    Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::Array>> array(x);
    return static_cast<std::shared_ptr<arrow::Array>>(array)->type();
  }

  Rcpp::stop("Unrecognized vector instance for type ENVSXP");
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<LGLSXP>(SEXP x) {
  return Rf_inherits(x, "vctrs_unspecified") ? null() : boolean();
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<INTSXP>(SEXP x) {
  if (Rf_isFactor(x)) {
    return InferArrowTypeFromFactor(x);
  } else if (Rf_inherits(x, "Date")) {
    return date32();
  } else if (Rf_inherits(x, "POSIXct")) {
    auto tzone_sexp = Rf_getAttrib(x, symbols::tzone);
    if (Rf_isNull(tzone_sexp)) {
      return timestamp(TimeUnit::MICRO);
    } else {
      return timestamp(TimeUnit::MICRO, CHAR(STRING_ELT(tzone_sexp, 0)));
    }
  }
  return int32();
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<REALSXP>(SEXP x) {
  if (Rf_inherits(x, "Date")) {
    return date32();
  }
  if (Rf_inherits(x, "POSIXct")) {
    auto tzone_sexp = Rf_getAttrib(x, symbols::tzone);
    if (Rf_isNull(tzone_sexp)) {
      return timestamp(TimeUnit::MICRO);
    } else {
      return timestamp(TimeUnit::MICRO, CHAR(STRING_ELT(tzone_sexp, 0)));
    }
  }
  if (Rf_inherits(x, "integer64")) {
    return int64();
  }
  if (Rf_inherits(x, "difftime")) {
    return time32(TimeUnit::SECOND);
  }
  return float64();
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<STRSXP>(SEXP x) {
  // See how big the character vector is
  R_xlen_t n = XLENGTH(x);
  int64_t size = 0;
  for (R_xlen_t i = 0; i < n; i++) {
    SEXP string_i = STRING_ELT(x, i);
    if (string_i != NA_STRING) {
      size += XLENGTH(Rf_mkCharCE(Rf_translateCharUTF8(string_i), CE_UTF8));
    }
    if (size > arrow::kBinaryMemoryLimit) {
      // Exceeds 2GB capacity of utf8 type, so use large
      return large_utf8();
    }
  }

  return utf8();
}

static inline std::shared_ptr<arrow::DataType> InferArrowTypeFromDataFrame(SEXP x) {
  R_xlen_t n = XLENGTH(x);
  SEXP names = Rf_getAttrib(x, R_NamesSymbol);
  std::vector<std::shared_ptr<arrow::Field>> fields(n);
  for (R_xlen_t i = 0; i < n; i++) {
    // Make sure we're ingesting UTF-8
    const auto* field_name =
        CHAR(Rf_mkCharCE(Rf_translateCharUTF8(STRING_ELT(names, i)), CE_UTF8));
    fields[i] = arrow::field(field_name, InferArrowType(VECTOR_ELT(x, i)));
  }
  return arrow::struct_(std::move(fields));
}

template <>
std::shared_ptr<arrow::DataType> InferArrowTypeFromVector<VECSXP>(SEXP x) {
  if (Rf_inherits(x, "data.frame") || Rf_inherits(x, "POSIXlt")) {
    return InferArrowTypeFromDataFrame(x);
  } else {
    // some known special cases
    if (Rf_inherits(x, "arrow_fixed_size_binary")) {
      SEXP byte_width = Rf_getAttrib(x, symbols::byte_width);
      if (Rf_isNull(byte_width) || TYPEOF(byte_width) != INTSXP ||
          XLENGTH(byte_width) != 1) {
        Rcpp::stop("malformed arrow_fixed_size_binary object");
      }
      return arrow::fixed_size_binary(INTEGER(byte_width)[0]);
    }

    if (Rf_inherits(x, "arrow_binary")) {
      return arrow::binary();
    }

    if (Rf_inherits(x, "arrow_large_binary")) {
      return arrow::large_binary();
    }

    SEXP ptype = Rf_getAttrib(x, symbols::ptype);
    if (Rf_isNull(ptype)) {
      if (XLENGTH(x) == 0) {
        Rcpp::stop(
            "Requires at least one element to infer the values' type of a list vector");
      }

      ptype = VECTOR_ELT(x, 0);
    }

    return arrow::list(InferArrowType(ptype));
  }
}

std::shared_ptr<arrow::DataType> InferArrowType(SEXP x) {
  switch (TYPEOF(x)) {
    case ENVSXP:
      return InferArrowTypeFromVector<ENVSXP>(x);
    case LGLSXP:
      return InferArrowTypeFromVector<LGLSXP>(x);
    case INTSXP:
      return InferArrowTypeFromVector<INTSXP>(x);
    case REALSXP:
      return InferArrowTypeFromVector<REALSXP>(x);
    case RAWSXP:
      return int8();
    case STRSXP:
      return InferArrowTypeFromVector<STRSXP>(x);
    case VECSXP:
      return InferArrowTypeFromVector<VECSXP>(x);
    default:
      break;
  }

  Rcpp::stop("Cannot infer type from vector");
}

// in some situations we can just use the memory of the R object in an RBuffer
// instead of going through ArrayBuilder, etc ...
bool can_reuse_memory(SEXP x, const std::shared_ptr<arrow::DataType>& type) {
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
template <int RTYPE, typename Type>
std::shared_ptr<Array> MakeSimpleArray(SEXP x) {
  using value_type = typename arrow::TypeTraits<Type>::ArrayType::value_type;
  Rcpp::Vector<RTYPE, Rcpp::NoProtectStorage> vec(x);
  auto n = vec.size();
  auto p_vec_start = reinterpret_cast<value_type*>(vec.begin());
  auto p_vec_end = p_vec_start + n;
  std::vector<std::shared_ptr<Buffer>> buffers{nullptr,
                                               std::make_shared<RBuffer<RTYPE>>(vec)};

  int null_count = 0;

  auto first_na = std::find_if(p_vec_start, p_vec_end, is_na<value_type>);
  if (first_na < p_vec_end) {
    auto null_bitmap = ValueOrStop(AllocateBuffer(BitUtil::BytesForBits(n)));
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
      if (is_na<value_type>(*p_vec)) {
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

std::shared_ptr<arrow::Array> Array__from_vector_reuse_memory(SEXP x) {
  auto type = TYPEOF(x);

  if (type == INTSXP) {
    return MakeSimpleArray<INTSXP, Int32Type>(x);
  } else if (type == REALSXP && Rf_inherits(x, "integer64")) {
    return MakeSimpleArray<REALSXP, Int64Type>(x);
  } else if (type == REALSXP) {
    return MakeSimpleArray<REALSXP, DoubleType>(x);
  } else if (type == RAWSXP) {
    return MakeSimpleArray<RAWSXP, UInt8Type>(x);
  }

  Rcpp::stop("Unreachable: you might need to fix can_reuse_memory()");
}

bool CheckCompatibleFactor(SEXP obj, const std::shared_ptr<arrow::DataType>& type) {
  if (!Rf_inherits(obj, "factor")) {
    return false;
  }

  const auto& dict_type = checked_cast<const arrow::DictionaryType&>(*type);
  return dict_type.value_type()->Equals(utf8());
}

arrow::Status CheckCompatibleStruct(SEXP obj,
                                    const std::shared_ptr<arrow::DataType>& type) {
  if (!Rf_inherits(obj, "data.frame")) {
    return Status::RError("Conversion to struct arrays requires a data.frame");
  }

  // check the number of columns
  int num_fields = type->num_fields();
  if (XLENGTH(obj) != num_fields) {
    return Status::RError("Number of fields in struct (", num_fields,
                          ") incompatible with number of columns in the data frame (",
                          XLENGTH(obj), ")");
  }

  // check the names of each column
  //
  // the columns themselves are not checked against the
  // types of the fields, because Array__from_vector will error
  // when not compatible.
  SEXP names = Rf_getAttrib(obj, R_NamesSymbol);
  SEXP name_i;
  for (int i = 0; i < num_fields; i++) {
    name_i = Rf_mkCharCE(Rf_translateCharUTF8(STRING_ELT(names, i)), CE_UTF8);
    if (type->field(i)->name() != CHAR(name_i)) {
      return Status::RError("Field name in position ", i, " (", type->field(i)->name(),
                            ") does not match the name of the column of the data frame (",
                            CHAR(name_i), ")");
    }
  }

  return Status::OK();
}

std::shared_ptr<arrow::Array> Array__from_vector(
    SEXP x, const std::shared_ptr<arrow::DataType>& type, bool type_inferred) {
  // short circuit if `x` is already an Array
  if (Rf_inherits(x, "Array")) {
    return Rcpp::ConstReferenceSmartPtrInputParameter<std::shared_ptr<arrow::Array>>(x);
  }

  // special case when we can just use the data from the R vector
  // directly. This still needs to handle the null bitmap
  if (arrow::r::can_reuse_memory(x, type)) {
    return arrow::r::Array__from_vector_reuse_memory(x);
  }

  // factors only when type has been inferred
  if (type->id() == Type::DICTIONARY) {
    if (type_inferred || arrow::r::CheckCompatibleFactor(x, type)) {
      // TODO: use VectorToArrayConverter instead, but it does not appear to work
      // correctly with ordered dictionary yet
      //
      // return VectorToArrayConverter::Visit(x, type);
      return arrow::r::MakeFactorArray(x, type);
    }

    Rcpp::stop("Object incompatible with dictionary type");
  }

  if (type->id() == Type::LIST || type->id() == Type::LARGE_LIST ||
      type->id() == Type::FIXED_SIZE_LIST) {
    return VectorToArrayConverter::Visit(x, type);
  }

  // struct types
  if (type->id() == Type::STRUCT) {
    if (!type_inferred) {
      StopIfNotOk(arrow::r::CheckCompatibleStruct(x, type));
    }
    // TODO: when the type has been infered, we could go through
    //       VectorToArrayConverter:
    //
    // else {
    //   return VectorToArrayConverter::Visit(df, type);
    // }

    return arrow::r::MakeStructArray(x, type);
  }

  // general conversion with converter and builder
  std::unique_ptr<arrow::r::VectorConverter> converter;
  StopIfNotOk(arrow::r::GetConverter(type, &converter));

  // Create ArrayBuilder for type
  std::unique_ptr<arrow::ArrayBuilder> type_builder;
  StopIfNotOk(arrow::MakeBuilder(arrow::default_memory_pool(), type, &type_builder));
  StopIfNotOk(converter->Init(type_builder.get()));

  // ingest R data and grab the result array
  StopIfNotOk(converter->Ingest(x));
  std::shared_ptr<arrow::Array> result;
  StopIfNotOk(converter->GetResult(&result));
  return result;
}

}  // namespace r
}  // namespace arrow

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Array__infer_type(SEXP x) {
  return arrow::r::InferArrowType(x);
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> Array__from_vector(SEXP x, SEXP s_type) {
  // the type might be NULL, in which case we need to infer it from the data
  // we keep track of whether it was inferred or supplied
  bool type_inferred = Rf_isNull(s_type);
  std::shared_ptr<arrow::DataType> type;
  if (type_inferred) {
    type = arrow::r::InferArrowType(x);
  } else {
    type = arrow::r::extract<arrow::DataType>(s_type);
  }

  return arrow::r::Array__from_vector(x, type, type_inferred);
}

// [[arrow::export]]
std::shared_ptr<arrow::ChunkedArray> ChunkedArray__from_list(Rcpp::List chunks,
                                                             SEXP s_type) {
  std::vector<std::shared_ptr<arrow::Array>> vec;

  // the type might be NULL, in which case we need to infer it from the data
  // we keep track of whether it was inferred or supplied
  bool type_inferred = Rf_isNull(s_type);
  R_xlen_t n = XLENGTH(chunks);

  std::shared_ptr<arrow::DataType> type;
  if (type_inferred) {
    if (n == 0) {
      Rcpp::stop("type must be specified for empty list");
    }
    type = arrow::r::InferArrowType(VECTOR_ELT(chunks, 0));
  } else {
    type = arrow::r::extract<arrow::DataType>(s_type);
  }

  if (n == 0) {
    std::shared_ptr<arrow::Array> array;
    std::unique_ptr<arrow::ArrayBuilder> type_builder;
    StopIfNotOk(arrow::MakeBuilder(arrow::default_memory_pool(), type, &type_builder));
    StopIfNotOk(type_builder->Finish(&array));
    vec.push_back(array);
  } else {
    // the first - might differ from the rest of the loop
    // because we might have inferred the type from the first element of the list
    //
    // this only really matters for dictionary arrays
    vec.push_back(
        arrow::r::Array__from_vector(VECTOR_ELT(chunks, 0), type, type_inferred));

    for (R_xlen_t i = 1; i < n; i++) {
      vec.push_back(arrow::r::Array__from_vector(VECTOR_ELT(chunks, i), type, false));
    }
  }

  return std::make_shared<arrow::ChunkedArray>(std::move(vec));
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> DictionaryArray__FromArrays(
    const std::shared_ptr<arrow::DataType>& type,
    const std::shared_ptr<arrow::Array>& indices,
    const std::shared_ptr<arrow::Array>& dict) {
  return ValueOrStop(arrow::DictionaryArray::FromArrays(type, indices, dict));
}

#endif
