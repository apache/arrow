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

#include "arrow/json/converter.h"

#include <memory>
#include <string_view>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/builder_time.h"
#include "arrow/json/parser.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::checked_cast;
using std::string_view;

namespace json {

template <typename... Args>
Status GenericConversionError(const DataType& type, Args&&... args) {
  return Status::Invalid("Failed to convert JSON to ", type, std::forward<Args>(args)...);
}

namespace {

const DictionaryArray& GetDictionaryArray(const std::shared_ptr<Array>& in) {
  DCHECK_EQ(in->type_id(), Type::DICTIONARY);
  auto dict_type = checked_cast<const DictionaryType*>(in->type().get());
  DCHECK_EQ(dict_type->index_type()->id(), Type::INT32);
  DCHECK_EQ(dict_type->value_type()->id(), Type::STRING);
  return checked_cast<const DictionaryArray&>(*in);
}

template <typename ValidVisitor, typename NullVisitor>
Status VisitDictionaryEntries(const DictionaryArray& dict_array,
                              ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
  const StringArray& dict = checked_cast<const StringArray&>(*dict_array.dictionary());
  const Int32Array& indices = checked_cast<const Int32Array&>(*dict_array.indices());
  for (int64_t i = 0; i < indices.length(); ++i) {
    if (indices.IsValid(i)) {
      RETURN_NOT_OK(visit_valid(dict.GetView(indices.GetView(i))));
    } else {
      RETURN_NOT_OK(visit_null());
    }
  }
  return Status::OK();
}

}  // namespace

// base class for types which accept and output non-nested types
class PrimitiveConverter : public Converter {
 public:
  PrimitiveConverter(MemoryPool* pool, std::shared_ptr<DataType> out_type)
      : Converter(pool, out_type) {}
};

class NullConverter : public PrimitiveConverter {
 public:
  using PrimitiveConverter::PrimitiveConverter;

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    if (in->type_id() != Type::NA) {
      return GenericConversionError(*out_type_, " from ", *in->type());
    }
    *out = in;
    return Status::OK();
  }
};

class BooleanConverter : public PrimitiveConverter {
 public:
  using PrimitiveConverter::PrimitiveConverter;

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    if (in->type_id() == Type::NA) {
      return MakeArrayOfNull(boolean(), in->length(), pool_).Value(out);
    }
    if (in->type_id() != Type::BOOL) {
      return GenericConversionError(*out_type_, " from ", *in->type());
    }
    *out = in;
    return Status::OK();
  }
};

template <typename T>
class NumericConverter : public PrimitiveConverter {
 public:
  using value_type = typename T::c_type;

  NumericConverter(MemoryPool* pool, const std::shared_ptr<DataType>& type)
      : PrimitiveConverter(pool, type), numeric_type_(checked_cast<const T&>(*type)) {}

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    if (in->type_id() == Type::NA) {
      return MakeArrayOfNull(out_type_, in->length(), pool_).Value(out);
    }
    const auto& dict_array = GetDictionaryArray(in);

    using Builder = typename TypeTraits<T>::BuilderType;
    Builder builder(out_type_, pool_);
    RETURN_NOT_OK(builder.Resize(dict_array.indices()->length()));

    auto visit_valid = [&](string_view repr) {
      value_type value;
      if (!arrow::internal::ParseValue(numeric_type_, repr.data(), repr.size(), &value)) {
        return GenericConversionError(*out_type_, ", couldn't parse:", repr);
      }

      builder.UnsafeAppend(value);
      return Status::OK();
    };

    auto visit_null = [&]() {
      builder.UnsafeAppendNull();
      return Status::OK();
    };

    RETURN_NOT_OK(VisitDictionaryEntries(dict_array, visit_valid, visit_null));
    return builder.Finish(out);
  }

  const T& numeric_type_;
};

template <typename T>
class DecimalConverter : public PrimitiveConverter {
 public:
  using value_type = typename TypeTraits<T>::BuilderType::ValueType;

  DecimalConverter(MemoryPool* pool, const std::shared_ptr<DataType>& type)
      : PrimitiveConverter(pool, type) {}

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    if (in->type_id() == Type::NA) {
      return MakeArrayOfNull(out_type_, in->length(), pool_).Value(out);
    }
    const auto& dict_array = GetDictionaryArray(in);

    using Builder = typename TypeTraits<T>::BuilderType;
    Builder builder(out_type_, pool_);
    RETURN_NOT_OK(builder.Resize(dict_array.indices()->length()));
    const auto& decimal_type(checked_cast<const DecimalType&>(*out_type_));
    int32_t out_precision = decimal_type.precision();
    int32_t out_scale = decimal_type.scale();

    auto visit_valid = [&](string_view repr) {
      value_type value;
      int32_t precision, scale;
      RETURN_NOT_OK(TypeTraits<T>::BuilderType::ValueType::FromString(
          repr, &value, &precision, &scale));
      if (precision > out_precision) {
        return GenericConversionError(*out_type_, ": ", repr, " requires precision ",
                                      precision);
      }
      if (scale != out_scale) {
        auto result = value.Rescale(scale, out_scale);
        if (ARROW_PREDICT_FALSE(!result.ok())) {
          return GenericConversionError(*out_type_, ": ", repr, " requires scale ",
                                        scale);
        } else {
          value = result.MoveValueUnsafe();
        }
      }
      builder.UnsafeAppend(value);
      return Status::OK();
    };

    auto visit_null = [&builder]() {
      builder.UnsafeAppendNull();
      return Status::OK();
    };

    RETURN_NOT_OK(VisitDictionaryEntries(dict_array, visit_valid, visit_null));
    return builder.Finish(out);
  }
};

template <typename DateTimeType>
class DateTimeConverter : public PrimitiveConverter {
 public:
  DateTimeConverter(MemoryPool* pool, const std::shared_ptr<DataType>& type)
      : PrimitiveConverter(pool, type), converter_(pool, repr_type()) {}

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    if (in->type_id() == Type::NA) {
      return MakeArrayOfNull(out_type_, in->length(), pool_).Value(out);
    }

    std::shared_ptr<Array> repr;
    RETURN_NOT_OK(converter_.Convert(in, &repr));

    auto out_data = repr->data()->Copy();
    out_data->type = out_type_;
    *out = MakeArray(out_data);

    return Status::OK();
  }

 private:
  using ReprType = typename CTypeTraits<typename DateTimeType::c_type>::ArrowType;
  static std::shared_ptr<DataType> repr_type() {
    return TypeTraits<ReprType>::type_singleton();
  }
  NumericConverter<ReprType> converter_;
};

template <typename T>
class BinaryConverter : public PrimitiveConverter {
 public:
  using PrimitiveConverter::PrimitiveConverter;

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    if (in->type_id() == Type::NA) {
      return MakeArrayOfNull(out_type_, in->length(), pool_).Value(out);
    }
    const auto& dict_array = GetDictionaryArray(in);

    using Builder = typename TypeTraits<T>::BuilderType;
    Builder builder(out_type_, pool_);
    RETURN_NOT_OK(builder.Resize(dict_array.indices()->length()));

    // TODO(bkietz) this can be computed during parsing at low cost
    int64_t data_length = 0;
    auto visit_lengths_valid = [&](string_view value) {
      data_length += value.size();
      return Status::OK();
    };

    auto visit_lengths_null = [&]() {
      // no-op
      return Status::OK();
    };

    RETURN_NOT_OK(
        VisitDictionaryEntries(dict_array, visit_lengths_valid, visit_lengths_null));
    RETURN_NOT_OK(builder.ReserveData(data_length));

    auto visit_valid = [&](string_view value) {
      builder.UnsafeAppend(value);
      return Status::OK();
    };

    auto visit_null = [&]() {
      builder.UnsafeAppendNull();
      return Status::OK();
    };

    RETURN_NOT_OK(VisitDictionaryEntries(dict_array, visit_valid, visit_null));
    return builder.Finish(out);
  }
};

Status MakeConverter(const std::shared_ptr<DataType>& out_type, MemoryPool* pool,
                     std::shared_ptr<Converter>* out) {
  switch (out_type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)              \
  case TYPE_ID:                                              \
    *out = std::make_shared<CONVERTER_TYPE>(pool, out_type); \
    break
    CONVERTER_CASE(Type::NA, NullConverter);
    CONVERTER_CASE(Type::BOOL, BooleanConverter);
    CONVERTER_CASE(Type::INT8, NumericConverter<Int8Type>);
    CONVERTER_CASE(Type::INT16, NumericConverter<Int16Type>);
    CONVERTER_CASE(Type::INT32, NumericConverter<Int32Type>);
    CONVERTER_CASE(Type::INT64, NumericConverter<Int64Type>);
    CONVERTER_CASE(Type::UINT8, NumericConverter<UInt8Type>);
    CONVERTER_CASE(Type::UINT16, NumericConverter<UInt16Type>);
    CONVERTER_CASE(Type::UINT32, NumericConverter<UInt32Type>);
    CONVERTER_CASE(Type::UINT64, NumericConverter<UInt64Type>);
    CONVERTER_CASE(Type::FLOAT, NumericConverter<FloatType>);
    CONVERTER_CASE(Type::DOUBLE, NumericConverter<DoubleType>);
    CONVERTER_CASE(Type::TIMESTAMP, NumericConverter<TimestampType>);
    CONVERTER_CASE(Type::TIME32, DateTimeConverter<Time32Type>);
    CONVERTER_CASE(Type::TIME64, DateTimeConverter<Time64Type>);
    CONVERTER_CASE(Type::DATE32, DateTimeConverter<Date32Type>);
    CONVERTER_CASE(Type::DATE64, DateTimeConverter<Date64Type>);
    CONVERTER_CASE(Type::BINARY, BinaryConverter<BinaryType>);
    CONVERTER_CASE(Type::STRING, BinaryConverter<StringType>);
    CONVERTER_CASE(Type::LARGE_BINARY, BinaryConverter<LargeBinaryType>);
    CONVERTER_CASE(Type::LARGE_STRING, BinaryConverter<LargeStringType>);
    CONVERTER_CASE(Type::DECIMAL128, DecimalConverter<Decimal128Type>);
    CONVERTER_CASE(Type::DECIMAL256, DecimalConverter<Decimal256Type>);
    default:
      return Status::NotImplemented("JSON conversion to ", *out_type,
                                    " is not supported");
#undef CONVERTER_CASE
  }
  return Status::OK();
}

const PromotionGraph* GetPromotionGraph() {
  static struct : PromotionGraph {
    std::shared_ptr<Field> Null(const std::string& name) const override {
      return field(name, null(), true, Kind::Tag(Kind::kNull));
    }

    std::shared_ptr<DataType> Infer(
        const std::shared_ptr<Field>& unexpected_field) const override {
      auto kind = Kind::FromTag(unexpected_field->metadata());
      switch (kind) {
        case Kind::kNull:
          return null();

        case Kind::kBoolean:
          return boolean();

        case Kind::kNumber:
          return int64();

        case Kind::kString:
          return timestamp(TimeUnit::SECOND);

        case Kind::kArray: {
          const auto& type = checked_cast<const ListType&>(*unexpected_field->type());
          auto value_field = type.value_field();
          return list(value_field->WithType(Infer(value_field)));
        }
        case Kind::kObject: {
          auto fields = unexpected_field->type()->fields();
          for (auto& field : fields) {
            field = field->WithType(Infer(field));
          }
          return struct_(std::move(fields));
        }
        default:
          return nullptr;
      }
    }

    std::shared_ptr<DataType> Promote(
        const std::shared_ptr<DataType>& failed,
        const std::shared_ptr<Field>& unexpected_field) const override {
      switch (failed->id()) {
        case Type::NA:
          return Infer(unexpected_field);

        case Type::TIMESTAMP:
          return utf8();

        case Type::INT64:
          return float64();

        default:
          return nullptr;
      }
    }
  } impl;

  return &impl;
}

}  // namespace json
}  // namespace arrow
