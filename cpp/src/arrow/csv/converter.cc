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

#include "arrow/csv/converter.h"

#include <cstring>
#include <limits>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_dict.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/csv/parser.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/trie.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"  // IWYU pragma: keep

namespace arrow {
namespace csv {

using internal::checked_cast;
using internal::Trie;
using internal::TrieBuilder;

namespace {

Status GenericConversionError(const std::shared_ptr<DataType>& type, const uint8_t* data,
                              uint32_t size) {
  return Status::Invalid("CSV conversion error to ", type->ToString(),
                         ": invalid value '",
                         std::string(reinterpret_cast<const char*>(data), size), "'");
}

inline bool IsWhitespace(uint8_t c) {
  if (ARROW_PREDICT_TRUE(c > ' ')) {
    return false;
  }
  return c == ' ' || c == '\t';
}

// Updates data_inout and size_inout to not include leading/trailing whitespace
// characters.
inline void TrimWhiteSpace(const uint8_t** data_inout, uint32_t* size_inout) {
  const uint8_t*& data = *data_inout;
  uint32_t& size = *size_inout;
  // Skip trailing whitespace
  if (ARROW_PREDICT_TRUE(size > 0) && ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
    const uint8_t* p = data + size - 1;
    while (size > 0 && IsWhitespace(*p)) {
      --size;
      --p;
    }
  }
  // Skip leading whitespace
  if (ARROW_PREDICT_TRUE(size > 0) && ARROW_PREDICT_FALSE(IsWhitespace(data[0]))) {
    while (size > 0 && IsWhitespace(*data)) {
      --size;
      ++data;
    }
  }
}

Status InitializeTrie(const std::vector<std::string>& inputs, Trie* trie) {
  TrieBuilder builder;
  for (const auto& s : inputs) {
    RETURN_NOT_OK(builder.Append(s, true /* allow_duplicates */));
  }
  *trie = builder.Finish();
  return Status::OK();
}

// Presize a builder based on parser contents
template <typename BuilderType>
enable_if_t<!is_base_binary_type<typename BuilderType::TypeClass>::value, Status>
PresizeBuilder(const BlockParser& parser, BuilderType* builder) {
  return builder->Resize(parser.num_rows());
}

// Same, for variable-sized binary builders
template <typename T>
Status PresizeBuilder(const BlockParser& parser, BaseBinaryBuilder<T>* builder) {
  RETURN_NOT_OK(builder->Resize(parser.num_rows()));
  return builder->ReserveData(parser.num_bytes());
}

/////////////////////////////////////////////////////////////////////////
// Per-type value decoders

struct ValueDecoder {
  explicit ValueDecoder(const std::shared_ptr<DataType>& type,
                        const ConvertOptions& options)
      : type_(type), options_(options) {}

  Status Initialize() {
    // TODO no need to build a separate Trie for each instance
    return InitializeTrie(options_.null_values, &null_trie_);
  }

  bool IsNull(const uint8_t* data, uint32_t size, bool quoted) {
    if (quoted) {
      return false;
    }
    return null_trie_.Find(
               util::string_view(reinterpret_cast<const char*>(data), size)) >= 0;
  }

 protected:
  Trie null_trie_;
  const std::shared_ptr<DataType> type_;
  const ConvertOptions& options_;
};

//
// Value decoder for fixed-size binary
//

struct FixedSizeBinaryValueDecoder : public ValueDecoder {
  using value_type = const uint8_t*;

  explicit FixedSizeBinaryValueDecoder(const std::shared_ptr<DataType>& type,
                                       const ConvertOptions& options)
      : ValueDecoder(type, options),
        byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()) {}

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    if (ARROW_PREDICT_FALSE(size != byte_width_)) {
      return Status::Invalid("CSV conversion error to ", type_->ToString(), ": got a ",
                             size, "-byte long string");
    }
    *out = data;
    return Status::OK();
  }

 protected:
  const uint32_t byte_width_;
};

//
// Value decoder for variable-size binary
//

template <bool CheckUTF8>
struct BinaryValueDecoder : public ValueDecoder {
  using value_type = util::string_view;

  using ValueDecoder::ValueDecoder;

  Status Initialize() {
    util::InitializeUTF8();
    return ValueDecoder::Initialize();
  }

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    if (CheckUTF8 && ARROW_PREDICT_FALSE(!util::ValidateUTF8(data, size))) {
      return Status::Invalid("CSV conversion error to ", type_->ToString(),
                             ": invalid UTF8 data");
    }
    *out = {reinterpret_cast<const char*>(data), size};
    return Status::OK();
  }

  bool IsNull(const uint8_t* data, uint32_t size, bool quoted) {
    return options_.strings_can_be_null &&
           (!quoted || options_.quoted_strings_can_be_null) &&
           ValueDecoder::IsNull(data, size, false /* quoted */);
  }
};

//
// Value decoder for integers, floats and temporals
//

template <typename T>
struct NumericValueDecoder : public ValueDecoder {
  using value_type = typename T::c_type;

  explicit NumericValueDecoder(const std::shared_ptr<DataType>& type,
                               const ConvertOptions& options)
      : ValueDecoder(type, options), concrete_type_(checked_cast<const T&>(*type)) {}

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    // XXX should quoted values be allowed at all?
    TrimWhiteSpace(&data, &size);
    if (ARROW_PREDICT_FALSE(!internal::ParseValue<T>(
            concrete_type_, reinterpret_cast<const char*>(data), size, out))) {
      return GenericConversionError(type_, data, size);
    }
    return Status::OK();
  }

 protected:
  const T& concrete_type_;
};

//
// Value decoder for booleans
//

struct BooleanValueDecoder : public ValueDecoder {
  using value_type = bool;

  using ValueDecoder::ValueDecoder;

  Status Initialize() {
    // TODO no need to build separate Tries for each instance
    RETURN_NOT_OK(InitializeTrie(options_.true_values, &true_trie_));
    RETURN_NOT_OK(InitializeTrie(options_.false_values, &false_trie_));
    return ValueDecoder::Initialize();
  }

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    // XXX should quoted values be allowed at all?
    if (false_trie_.Find(util::string_view(reinterpret_cast<const char*>(data), size)) >=
        0) {
      *out = false;
      return Status::OK();
    }
    if (ARROW_PREDICT_TRUE(true_trie_.Find(util::string_view(
                               reinterpret_cast<const char*>(data), size)) >= 0)) {
      *out = true;
      return Status::OK();
    }
    return GenericConversionError(type_, data, size);
  }

 protected:
  Trie true_trie_;
  Trie false_trie_;
};

//
// Value decoder for decimals
//

struct DecimalValueDecoder : public ValueDecoder {
  using value_type = Decimal128;

  explicit DecimalValueDecoder(const std::shared_ptr<DataType>& type,
                               const ConvertOptions& options)
      : ValueDecoder(type, options),
        decimal_type_(internal::checked_cast<const DecimalType&>(*type_)),
        type_precision_(decimal_type_.precision()),
        type_scale_(decimal_type_.scale()) {}

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    TrimWhiteSpace(&data, &size);
    Decimal128 decimal;
    int32_t precision, scale;
    util::string_view view(reinterpret_cast<const char*>(data), size);
    RETURN_NOT_OK(Decimal128::FromString(view, &decimal, &precision, &scale));
    if (precision > type_precision_) {
      return Status::Invalid("Error converting '", view, "' to ", type_->ToString(),
                             ": precision not supported by type.");
    }
    if (scale != type_scale_) {
      ARROW_ASSIGN_OR_RAISE(*out, decimal.Rescale(scale, type_scale_));
    } else {
      *out = std::move(decimal);
    }
    return Status::OK();
  }

 protected:
  const DecimalType& decimal_type_;
  const int32_t type_precision_;
  const int32_t type_scale_;
};

//
// Value decoders for timestamps
//

struct InlineISO8601ValueDecoder : public ValueDecoder {
  using value_type = int64_t;

  explicit InlineISO8601ValueDecoder(const std::shared_ptr<DataType>& type,
                                     const ConvertOptions& options)
      : ValueDecoder(type, options),
        unit_(checked_cast<const TimestampType&>(*type_).unit()) {}

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    if (ARROW_PREDICT_FALSE(!internal::ParseTimestampISO8601(
            reinterpret_cast<const char*>(data), size, unit_, out))) {
      return GenericConversionError(type_, data, size);
    }
    return Status::OK();
  }

 protected:
  TimeUnit::type unit_;
};

struct SingleParserTimestampValueDecoder : public ValueDecoder {
  using value_type = int64_t;

  explicit SingleParserTimestampValueDecoder(const std::shared_ptr<DataType>& type,
                                             const ConvertOptions& options)
      : ValueDecoder(type, options),
        unit_(checked_cast<const TimestampType&>(*type_).unit()),
        parser_(*options_.timestamp_parsers[0]) {}

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    if (ARROW_PREDICT_FALSE(
            !parser_(reinterpret_cast<const char*>(data), size, unit_, out))) {
      return GenericConversionError(type_, data, size);
    }
    return Status::OK();
  }

 protected:
  TimeUnit::type unit_;
  const TimestampParser& parser_;
};

struct MultipleParsersTimestampValueDecoder : public ValueDecoder {
  using value_type = int64_t;

  explicit MultipleParsersTimestampValueDecoder(const std::shared_ptr<DataType>& type,
                                                const ConvertOptions& options)
      : ValueDecoder(type, options),
        unit_(checked_cast<const TimestampType&>(*type_).unit()),
        parsers_(GetParsers(options_)) {}

  Status Decode(const uint8_t* data, uint32_t size, bool quoted, value_type* out) {
    for (const auto& parser : parsers_) {
      if (parser->operator()(reinterpret_cast<const char*>(data), size, unit_, out)) {
        return Status::OK();
      }
    }
    return GenericConversionError(type_, data, size);
  }

 protected:
  using ParserVector = std::vector<const TimestampParser*>;

  static ParserVector GetParsers(const ConvertOptions& options) {
    ParserVector parsers(options.timestamp_parsers.size());
    for (size_t i = 0; i < options.timestamp_parsers.size(); ++i) {
      parsers[i] = options.timestamp_parsers[i].get();
    }
    return parsers;
  }

  TimeUnit::type unit_;
  std::vector<const TimestampParser*> parsers_;
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter hierarchy

class ConcreteConverter : public Converter {
 public:
  using Converter::Converter;
};

class ConcreteDictionaryConverter : public DictionaryConverter {
 public:
  using DictionaryConverter::DictionaryConverter;
};

//
// Concrete Converter for nulls
//

class NullConverter : public ConcreteConverter {
 public:
  NullConverter(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
                MemoryPool* pool)
      : ConcreteConverter(type, options, pool), decoder_(type_, options_) {}

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    NullBuilder builder(pool_);

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (ARROW_PREDICT_TRUE(decoder_.IsNull(data, size, quoted))) {
        return builder.AppendNull();
      } else {
        return GenericConversionError(type_, data, size);
      }
    };
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }

 protected:
  Status Initialize() override { return decoder_.Initialize(); }

  ValueDecoder decoder_;
};

//
// Concrete Converter for primitives
//

template <typename T, typename ValueDecoderType>
class PrimitiveConverter : public ConcreteConverter {
 public:
  PrimitiveConverter(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
                     MemoryPool* pool)
      : ConcreteConverter(type, options, pool), decoder_(type_, options_) {}

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using value_type = typename ValueDecoderType::value_type;

    BuilderType builder(type_, pool_);
    RETURN_NOT_OK(PresizeBuilder(parser, &builder));

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (decoder_.IsNull(data, size, quoted /* quoted */)) {
        return builder.AppendNull();
      }
      value_type value{};
      RETURN_NOT_OK(decoder_.Decode(data, size, quoted, &value));
      builder.UnsafeAppend(value);
      return Status::OK();
    };
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }

 protected:
  Status Initialize() override { return decoder_.Initialize(); }

  ValueDecoderType decoder_;
};

//
// Concrete Converter for dictionaries
//

template <typename T, typename ValueDecoderType>
class TypedDictionaryConverter : public ConcreteDictionaryConverter {
 public:
  TypedDictionaryConverter(const std::shared_ptr<DataType>& value_type,
                           const ConvertOptions& options, MemoryPool* pool)
      : ConcreteDictionaryConverter(value_type, options, pool),
        decoder_(value_type, options_) {}

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    // We use a fixed index width so that all column chunks get the same index type
    using BuilderType = Dictionary32Builder<T>;
    using value_type = typename ValueDecoderType::value_type;

    BuilderType builder(value_type_, pool_);
    RETURN_NOT_OK(PresizeBuilder(parser, &builder));

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (decoder_.IsNull(data, size, quoted /* quoted */)) {
        return builder.AppendNull();
      }
      if (ARROW_PREDICT_FALSE(builder.dictionary_length() > max_cardinality_)) {
        return Status::IndexError("Dictionary length exceeded max cardinality");
      }
      value_type value{};
      RETURN_NOT_OK(decoder_.Decode(data, size, quoted, &value));
      return builder.Append(value);
    };
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }

  void SetMaxCardinality(int32_t max_length) override { max_cardinality_ = max_length; }

 protected:
  Status Initialize() override {
    util::InitializeUTF8();
    return decoder_.Initialize();
  }

  ValueDecoderType decoder_;
  int32_t max_cardinality_ = std::numeric_limits<int32_t>::max();
};

//
// Concrete Converter factory for timestamps
//

template <template <typename, typename> class ConverterType>
std::shared_ptr<Converter> MakeTimestampConverter(const std::shared_ptr<DataType>& type,
                                                  const ConvertOptions& options,
                                                  MemoryPool* pool) {
  if (options.timestamp_parsers.size() == 0) {
    // Default to ISO-8601
    return std::make_shared<ConverterType<TimestampType, InlineISO8601ValueDecoder>>(
        type, options, pool);
  } else if (options.timestamp_parsers.size() == 1) {
    // Single user-supplied converter
    return std::make_shared<
        ConverterType<TimestampType, SingleParserTimestampValueDecoder>>(type, options,
                                                                         pool);
  } else {
    // Multiple converters, must iterate for each value
    return std::make_shared<
        ConverterType<TimestampType, MultipleParsersTimestampValueDecoder>>(type, options,
                                                                            pool);
  }
}

}  // namespace

/////////////////////////////////////////////////////////////////////////
// Base Converter class implementation

Converter::Converter(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
                     MemoryPool* pool)
    : options_(options), pool_(pool), type_(type) {}

DictionaryConverter::DictionaryConverter(const std::shared_ptr<DataType>& value_type,
                                         const ConvertOptions& options, MemoryPool* pool)
    : Converter(dictionary(int32(), value_type), options, pool),
      value_type_(value_type) {}

Result<std::shared_ptr<Converter>> Converter::Make(const std::shared_ptr<DataType>& type,
                                                   const ConvertOptions& options,
                                                   MemoryPool* pool) {
  std::shared_ptr<Converter> ptr;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)         \
  case TYPE_ID:                                         \
    ptr.reset(new CONVERTER_TYPE(type, options, pool)); \
    break;

#define NUMERIC_CONVERTER_CASE(TYPE_ID, TYPE_CLASS) \
  CONVERTER_CASE(TYPE_ID,                           \
                 (PrimitiveConverter<TYPE_CLASS, NumericValueDecoder<TYPE_CLASS>>))

    CONVERTER_CASE(Type::NA, NullConverter)
    NUMERIC_CONVERTER_CASE(Type::INT8, Int8Type)
    NUMERIC_CONVERTER_CASE(Type::INT16, Int16Type)
    NUMERIC_CONVERTER_CASE(Type::INT32, Int32Type)
    NUMERIC_CONVERTER_CASE(Type::INT64, Int64Type)
    NUMERIC_CONVERTER_CASE(Type::UINT8, UInt8Type)
    NUMERIC_CONVERTER_CASE(Type::UINT16, UInt16Type)
    NUMERIC_CONVERTER_CASE(Type::UINT32, UInt32Type)
    NUMERIC_CONVERTER_CASE(Type::UINT64, UInt64Type)
    NUMERIC_CONVERTER_CASE(Type::FLOAT, FloatType)
    NUMERIC_CONVERTER_CASE(Type::DOUBLE, DoubleType)
    NUMERIC_CONVERTER_CASE(Type::DATE32, Date32Type)
    NUMERIC_CONVERTER_CASE(Type::DATE64, Date64Type)
    NUMERIC_CONVERTER_CASE(Type::TIME32, Time32Type)
    NUMERIC_CONVERTER_CASE(Type::TIME64, Time64Type)
    CONVERTER_CASE(Type::BOOL, (PrimitiveConverter<BooleanType, BooleanValueDecoder>))
    CONVERTER_CASE(Type::BINARY,
                   (PrimitiveConverter<BinaryType, BinaryValueDecoder<false>>))
    CONVERTER_CASE(Type::LARGE_BINARY,
                   (PrimitiveConverter<LargeBinaryType, BinaryValueDecoder<false>>))
    CONVERTER_CASE(Type::FIXED_SIZE_BINARY,
                   (PrimitiveConverter<FixedSizeBinaryType, FixedSizeBinaryValueDecoder>))
    CONVERTER_CASE(Type::DECIMAL,
                   (PrimitiveConverter<Decimal128Type, DecimalValueDecoder>))

    case Type::TIMESTAMP:
      ptr = MakeTimestampConverter<PrimitiveConverter>(type, options, pool);
      break;

    case Type::STRING:
      if (options.check_utf8) {
        ptr = std::make_shared<PrimitiveConverter<StringType, BinaryValueDecoder<true>>>(
            type, options, pool);
      } else {
        ptr = std::make_shared<PrimitiveConverter<StringType, BinaryValueDecoder<false>>>(
            type, options, pool);
      }
      break;

    case Type::LARGE_STRING:
      if (options.check_utf8) {
        ptr = std::make_shared<
            PrimitiveConverter<LargeStringType, BinaryValueDecoder<true>>>(type, options,
                                                                           pool);
      } else {
        ptr = std::make_shared<
            PrimitiveConverter<LargeStringType, BinaryValueDecoder<false>>>(type, options,
                                                                            pool);
      }
      break;

    case Type::DICTIONARY: {
      const auto& dict_type = checked_cast<const DictionaryType&>(*type);
      if (dict_type.index_type()->id() != Type::INT32) {
        return Status::NotImplemented(
            "CSV conversion to dictionary only supported for int32 indices, "
            "got ",
            type->ToString());
      }
      return DictionaryConverter::Make(dict_type.value_type(), options, pool);
    }

    default: {
      return Status::NotImplemented("CSV conversion to ", type->ToString(),
                                    " is not supported");
    }

#undef CONVERTER_CASE
#undef NUMERIC_CONVERTER_CASE
  }
  RETURN_NOT_OK(ptr->Initialize());
  return ptr;
}

Result<std::shared_ptr<DictionaryConverter>> DictionaryConverter::Make(
    const std::shared_ptr<DataType>& type, const ConvertOptions& options,
    MemoryPool* pool) {
  std::shared_ptr<DictionaryConverter> ptr;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, TYPE, VALUE_DECODER_TYPE)                             \
  case TYPE_ID:                                                                       \
    ptr.reset(                                                                        \
        new TypedDictionaryConverter<TYPE, VALUE_DECODER_TYPE>(type, options, pool)); \
    break;

    // XXX Are 32-bit types useful?
    CONVERTER_CASE(Type::INT32, Int32Type, NumericValueDecoder<Int32Type>)
    CONVERTER_CASE(Type::INT64, Int64Type, NumericValueDecoder<Int64Type>)
    CONVERTER_CASE(Type::UINT32, UInt32Type, NumericValueDecoder<UInt32Type>)
    CONVERTER_CASE(Type::UINT64, UInt64Type, NumericValueDecoder<UInt64Type>)
    CONVERTER_CASE(Type::FLOAT, FloatType, NumericValueDecoder<FloatType>)
    CONVERTER_CASE(Type::DOUBLE, DoubleType, NumericValueDecoder<DoubleType>)
    CONVERTER_CASE(Type::DECIMAL, Decimal128Type, DecimalValueDecoder)
    CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryType,
                   FixedSizeBinaryValueDecoder)
    CONVERTER_CASE(Type::BINARY, BinaryType, BinaryValueDecoder<false>)
    CONVERTER_CASE(Type::LARGE_BINARY, LargeBinaryType, BinaryValueDecoder<false>)

    case Type::STRING:
      if (options.check_utf8) {
        ptr = std::make_shared<
            TypedDictionaryConverter<StringType, BinaryValueDecoder<true>>>(type, options,
                                                                            pool);
      } else {
        ptr = std::make_shared<
            TypedDictionaryConverter<StringType, BinaryValueDecoder<false>>>(
            type, options, pool);
      }
      break;

    case Type::LARGE_STRING:
      if (options.check_utf8) {
        ptr = std::make_shared<
            TypedDictionaryConverter<LargeStringType, BinaryValueDecoder<true>>>(
            type, options, pool);
      } else {
        ptr = std::make_shared<
            TypedDictionaryConverter<LargeStringType, BinaryValueDecoder<false>>>(
            type, options, pool);
      }
      break;

    default: {
      return Status::NotImplemented("CSV dictionary conversion to ", type->ToString(),
                                    " is not supported");
    }

#undef CONVERTER_CASE
  }
  RETURN_NOT_OK(ptr->Initialize());
  return ptr;
}

}  // namespace csv
}  // namespace arrow
