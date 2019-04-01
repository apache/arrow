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

#include <utility>

#include "arrow/array.h"
#include "arrow/json/parser.h"
#include "arrow/type.h"
#include "arrow/util/parsing.h"
#include "arrow/util/stl.h"
#include "arrow/util/task-group.h"

namespace arrow {
namespace json {

using internal::make_unique;
using util::string_view;

template <typename... Args>
Status GenericConversionError(const DataType& type, Args&&... args) {
  return Status::Invalid("Failed of conversion of JSON to ", type,
                         std::forward<Args>(args)...);
}

template <typename Vis>
Status VisitDictionaryEntries(const DictionaryArray* dict_array, Vis&& vis) {
  const StringArray& dict = static_cast<const StringArray&>(*dict_array->dictionary());
  const Int32Array& indices = static_cast<const Int32Array&>(*dict_array->indices());
  for (int64_t i = 0; i != indices.length(); ++i) {
    bool is_valid = indices.IsValid(i);
    RETURN_NOT_OK(vis(is_valid, is_valid ? dict.GetView(indices.GetView(i)) : ""));
  }
  return Status::OK();
}

// base class for types which accept and output non-nested types
class PrimitiveConverter : public Converter {
 public:
  virtual std::shared_ptr<DataType> out_type() const { return out_type_; }

  bool dont_promote_;

  PrimitiveConverter(MemoryPool* pool, std::shared_ptr<DataType> out_type)
      : Converter(pool), out_type_(out_type) {}
  std::shared_ptr<DataType> out_type_;
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
  using value_type = typename internal::StringConverter<T>::value_type;

  NumericConverter(MemoryPool* pool, const std::shared_ptr<DataType>& type)
      : PrimitiveConverter(pool, type), convert_one_(type) {}

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    auto dict_array = static_cast<const DictionaryArray*>(in.get());

    using Builder = typename TypeTraits<T>::BuilderType;
    Builder builder(out_type_, pool_);
    RETURN_NOT_OK(builder.Resize(dict_array->indices()->length()));

    auto visit = [&](bool is_valid, string_view repr) {
      if (!is_valid) {
        builder.UnsafeAppendNull();
        return Status::OK();
      }

      value_type value;
      if (!convert_one_(repr.data(), repr.size(), &value)) {
        return GenericConversionError(*out_type_, ", couldn't parse:", repr);
      }

      builder.UnsafeAppend(value);
      return Status::OK();
    };

    RETURN_NOT_OK(VisitDictionaryEntries(dict_array, visit));
    return builder.Finish(out);
  }

  internal::StringConverter<T> convert_one_;
};

template <typename DateTimeType>
class DateTimeConverter : public PrimitiveConverter {
 public:
  DateTimeConverter(MemoryPool* pool, const std::shared_ptr<DataType>& type)
      : PrimitiveConverter(pool, type), converter_(pool, type) {}

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> repr;
    RETURN_NOT_OK(converter_.Convert(in, &repr));

    auto out_data = repr->data()->Copy();
    out_data->type = out_type_;
    *out = MakeArray(out_data);

    return Status::OK();
  }

 private:
  using ReprType = typename CTypeTraits<typename DateTimeType::c_type>::ArrowType;
  NumericConverter<ReprType> converter_;
};

template <typename T>
class BinaryConverter : public PrimitiveConverter {
 public:
  using PrimitiveConverter::PrimitiveConverter;

  Status Convert(const std::shared_ptr<Array>& in, std::shared_ptr<Array>* out) override {
    auto dict_array = static_cast<const DictionaryArray*>(in.get());

    using Builder = typename TypeTraits<T>::BuilderType;
    Builder builder(out_type_, pool_);
    RETURN_NOT_OK(builder.Resize(dict_array->indices()->length()));

    // TODO(bkietz) this can be computed during parsing at low cost
    int64_t data_length = 0;
    auto visit_lengths = [&](bool is_valid, string_view value) {
      if (is_valid) {
        data_length += value.size();
      }
      return Status::OK();
    };
    RETURN_NOT_OK(VisitDictionaryEntries(dict_array, visit_lengths));
    RETURN_NOT_OK(builder.ReserveData(data_length));

    auto visit = [&](bool is_valid, string_view value) {
      if (is_valid) {
        builder.UnsafeAppend(value);
      } else {
        builder.UnsafeAppendNull();
      }
      return Status::OK();
    };
    RETURN_NOT_OK(VisitDictionaryEntries(dict_array, visit));
    return builder.Finish(out);
  }
};

Status MakeConverter(MemoryPool* pool, const std::shared_ptr<DataType>& out_type,
                     std::unique_ptr<Converter>* out) {
  switch (out_type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)         \
  case TYPE_ID:                                         \
    *out = make_unique<CONVERTER_TYPE>(pool, out_type); \
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
    default:
      return Status::NotImplemented("JSON conversion to ", *out_type,
                                    " is not supported");
#undef CONVERTER_CASE
  }
  static_cast<PrimitiveConverter*>(out->get())->dont_promote_ = false;
  return Status::OK();
}

Status Promote(std::unique_ptr<Converter> failed, std::unique_ptr<Converter>* promoted) {
  auto failed_scalar = static_cast<PrimitiveConverter*>(failed.get());
  if (failed_scalar->dont_promote_) {
    *promoted = nullptr;
  } else if (failed_scalar->out_type()->id() == Type::INT64) {
    *promoted = make_unique<NumericConverter<DoubleType>>(failed->pool(), float64());
  } else if (failed_scalar->out_type()->id() == Type::TIMESTAMP) {
    *promoted = make_unique<BinaryConverter<StringType>>(failed->pool(), utf8());
  }
  return Status::OK();
}

}  // namespace json
}  // namespace arrow
