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

#include "arrow/builder.h"
#include "arrow/csv/parser.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
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

class ConcreteConverterMixin {
 protected:
  Status InitializeNullTrie(const ConvertOptions& options);

  bool IsNull(const uint8_t* data, uint32_t size, bool quoted) {
    if (quoted) {
      return false;
    }
    return null_trie_.Find(
               util::string_view(reinterpret_cast<const char*>(data), size)) >= 0;
  }

  Trie null_trie_;
};

Status ConcreteConverterMixin::InitializeNullTrie(const ConvertOptions& options) {
  // TODO no need to build a separate Trie for each Converter instance
  return InitializeTrie(options.null_values, &null_trie_);
}

class ConcreteConverter : public Converter, public ConcreteConverterMixin {
 public:
  using Converter::Converter;

 protected:
  Status Initialize() override { return InitializeNullTrie(options_); }
};

class ConcreteDictionaryConverter : public DictionaryConverter,
                                    public ConcreteConverterMixin {
 public:
  using DictionaryConverter::DictionaryConverter;

 protected:
  Status Initialize() override { return InitializeNullTrie(options_); }
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for null values

class NullConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    NullBuilder builder(pool_);

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (ARROW_PREDICT_TRUE(IsNull(data, size, quoted))) {
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
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for var-sized binary strings

template <typename T, bool CheckUTF8>
class BinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    BuilderType builder(pool_);

    auto visit_non_null = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (CheckUTF8 && ARROW_PREDICT_FALSE(!util::ValidateUTF8(data, size))) {
        return Status::Invalid("CSV conversion error to ", type_->ToString(),
                               ": invalid UTF8 data");
      }
      builder.UnsafeAppend(data, size);
      return Status::OK();
    };

    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(builder.ReserveData(parser.num_bytes()));

    if (options_.strings_can_be_null) {
      auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
        if (IsNull(data, size, false /* quoted */)) {
          builder.UnsafeAppendNull();
          return Status::OK();
        } else {
          return visit_non_null(data, size, quoted);
        }
      };
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
    } else {
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit_non_null));
    }

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }

 protected:
  Status Initialize() override {
    util::InitializeUTF8();
    return ConcreteConverter::Initialize();
  }
};

template <typename T, bool CheckUTF8>
class DictionaryBinaryConverter : public ConcreteDictionaryConverter {
 public:
  using ConcreteDictionaryConverter::ConcreteDictionaryConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    // We use a fixed index width so that all column chunks get the same index type
    using BuilderType = Dictionary32Builder<T>;
    BuilderType builder(value_type_, pool_);

    auto visit_non_null = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (CheckUTF8 && ARROW_PREDICT_FALSE(!util::ValidateUTF8(data, size))) {
        return Status::Invalid("CSV conversion error to ", value_type_->ToString(),
                               ": invalid UTF8 data");
      }
      RETURN_NOT_OK(
          builder.Append(util::string_view(reinterpret_cast<const char*>(data), size)));
      if (ARROW_PREDICT_FALSE(builder.dictionary_length() > max_cardinality_)) {
        return Status::IndexError("Dictionary length exceeded max cardinality");
      }
      return Status::OK();
    };

    RETURN_NOT_OK(builder.Resize(parser.num_rows()));

    if (options_.strings_can_be_null) {
      auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
        if (IsNull(data, size, false /* quoted */)) {
          return builder.AppendNull();
        } else {
          return visit_non_null(data, size, quoted);
        }
      };
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
    } else {
      RETURN_NOT_OK(parser.VisitColumn(col_index, visit_non_null));
    }

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }

  void SetMaxCardinality(int32_t max_length) override { max_cardinality_ = max_length; }

 protected:
  Status Initialize() override {
    util::InitializeUTF8();
    return ConcreteDictionaryConverter::Initialize();
  }

  int32_t max_cardinality_ = std::numeric_limits<int32_t>::max();
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for fixed-sized binary strings

class FixedSizeBinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    FixedSizeBinaryBuilder builder(type_, pool_);
    const uint32_t byte_width = static_cast<uint32_t>(builder.byte_width());

    // TODO do we accept nulls here?

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (ARROW_PREDICT_FALSE(size != byte_width)) {
        return Status::Invalid("CSV conversion error to ", type_->ToString(), ": got a ",
                               size, "-byte long string");
      }
      return builder.Append(data);
    };
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for booleans

class BooleanConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    BooleanBuilder builder(type_, pool_);

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      // XXX should quoted values be allowed at all?
      if (IsNull(data, size, quoted)) {
        builder.UnsafeAppendNull();
        return Status::OK();
      }
      if (false_trie_.Find(
              util::string_view(reinterpret_cast<const char*>(data), size)) >= 0) {
        builder.UnsafeAppend(false);
        return Status::OK();
      }
      if (true_trie_.Find(util::string_view(reinterpret_cast<const char*>(data), size)) >=
          0) {
        builder.UnsafeAppend(true);
        return Status::OK();
      }
      return GenericConversionError(type_, data, size);
    };
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }

 protected:
  Status Initialize() override {
    // TODO no need to build separate Tries for each BooleanConverter instance
    RETURN_NOT_OK(InitializeTrie(options_.true_values, &true_trie_));
    RETURN_NOT_OK(InitializeTrie(options_.false_values, &false_trie_));
    return ConcreteConverter::Initialize();
  }

  Trie true_trie_;
  Trie false_trie_;
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for numbers

template <typename T>
class NumericConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    using BuilderType = typename TypeTraits<T>::BuilderType;
    using value_type = typename T::c_type;

    BuilderType builder(type_, pool_);

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      // XXX should quoted values be allowed at all?
      value_type value = 0;
      if (IsNull(data, size, quoted)) {
        builder.UnsafeAppendNull();
        return Status::OK();
      }
      if (!std::is_same<BooleanType, T>::value) {
        TrimWhiteSpace(&data, &size);
      }
      if (ARROW_PREDICT_FALSE(!internal::ParseValue<T>(
              reinterpret_cast<const char*>(data), size, &value))) {
        return GenericConversionError(type_, data, size);
      }
      builder.UnsafeAppend(value);
      return Status::OK();
    };
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for timestamps

namespace {

struct InlineISO8601 {
  TimeUnit::type unit;

  explicit InlineISO8601(TimeUnit::type unit) : unit(unit) {}

  bool operator()(const char* s, size_t length, int64_t* out) const {
    return internal::ParseTimestampISO8601(s, length, unit, out);
  }
};

struct SingleTimestampParser {
  const TimestampParser& parser;
  TimeUnit::type unit;

  SingleTimestampParser(const TimestampParser& parser, TimeUnit::type unit)
      : parser(parser), unit(unit) {}

  bool operator()(const char* s, size_t length, int64_t* out) const {
    return this->parser(s, length, this->unit, out);
  }
};

struct MultipleTimestampParsers {
  std::vector<const TimestampParser*> parsers;
  TimeUnit::type unit;

  MultipleTimestampParsers(const std::vector<std::shared_ptr<TimestampParser>>& parsers,
                           TimeUnit::type unit)
      : unit(unit) {
    for (const auto& parser : parsers) {
      this->parsers.push_back(parser.get());
    }
  }

  bool operator()(const char* s, size_t length, int64_t* out) const {
    for (const auto& parser : this->parsers) {
      if (parser->operator()(s, length, this->unit, out)) {
        return true;
      }
    }
    return false;
  }
};

}  // namespace

class TimestampConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  template <typename ConvertValue>
  Status ConvertValuesWith(const BlockParser& parser, int32_t col_index,
                           const ConvertValue& converter, TimestampBuilder* builder) {
    using value_type = TimestampType::c_type;
    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      value_type value = 0;
      if (IsNull(data, size, quoted)) {
        builder->UnsafeAppendNull();
        return Status::OK();
      }

      if (ARROW_PREDICT_FALSE(
              !converter(reinterpret_cast<const char*>(data), size, &value))) {
        return GenericConversionError(type_, data, size);
      }
      builder->UnsafeAppend(value);
      return Status::OK();
    };
    return parser.VisitColumn(col_index, visit);
  }

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    TimestampBuilder builder(type_, pool_);
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));

    TimeUnit::type unit = checked_cast<const TimestampType&>(*type_).unit();
    if (options_.timestamp_parsers.size() == 0) {
      // Default to ISO-8601
      InlineISO8601 converter(unit);
      RETURN_NOT_OK(ConvertValuesWith(parser, col_index, converter, &builder));
    } else if (options_.timestamp_parsers.size() == 1) {
      // Single user-supplied converter
      SingleTimestampParser converter(*options_.timestamp_parsers[0], unit);
      RETURN_NOT_OK(ConvertValuesWith(parser, col_index, converter, &builder));
    } else {
      // Multiple converters, must iterate for each value
      MultipleTimestampParsers converter(options_.timestamp_parsers, unit);
      RETURN_NOT_OK(ConvertValuesWith(parser, col_index, converter, &builder));
    }

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for Decimals

class DecimalConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Result<std::shared_ptr<Array>> Convert(const BlockParser& parser,
                                         int32_t col_index) override {
    Decimal128Builder builder(type_, pool_);

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      if (IsNull(data, size, quoted)) {
        builder.UnsafeAppendNull();
        return Status::OK();
      }
      TrimWhiteSpace(&data, &size);
      Decimal128 decimal;
      int32_t precision, scale;
      util::string_view view(reinterpret_cast<const char*>(data), size);
      RETURN_NOT_OK(Decimal128::FromString(view, &decimal, &precision, &scale));
      DecimalType& type = *internal::checked_cast<DecimalType*>(type_.get());
      if (precision > type.precision()) {
        return Status::Invalid("Error converting ", view, " to ", type_->ToString(),
                               " precision not supported by type.");
      }
      if (scale != type.scale()) {
        Decimal128 scaled;
        ARROW_ASSIGN_OR_RAISE(scaled, decimal.Rescale(scale, type.scale()));
        builder.UnsafeAppend(scaled);
      } else {
        builder.UnsafeAppend(decimal);
      }
      return Status::OK();
    };
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));

    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder.Finish(&res));
    return res;
  }
};

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
  Converter* ptr;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)    \
  case TYPE_ID:                                    \
    ptr = new CONVERTER_TYPE(type, options, pool); \
    break;

    CONVERTER_CASE(Type::NA, NullConverter)
    CONVERTER_CASE(Type::INT8, NumericConverter<Int8Type>)
    CONVERTER_CASE(Type::INT16, NumericConverter<Int16Type>)
    CONVERTER_CASE(Type::INT32, NumericConverter<Int32Type>)
    CONVERTER_CASE(Type::INT64, NumericConverter<Int64Type>)
    CONVERTER_CASE(Type::UINT8, NumericConverter<UInt8Type>)
    CONVERTER_CASE(Type::UINT16, NumericConverter<UInt16Type>)
    CONVERTER_CASE(Type::UINT32, NumericConverter<UInt32Type>)
    CONVERTER_CASE(Type::UINT64, NumericConverter<UInt64Type>)
    CONVERTER_CASE(Type::FLOAT, NumericConverter<FloatType>)
    CONVERTER_CASE(Type::DOUBLE, NumericConverter<DoubleType>)
    CONVERTER_CASE(Type::BOOL, BooleanConverter)
    CONVERTER_CASE(Type::TIMESTAMP, TimestampConverter)
    CONVERTER_CASE(Type::BINARY, (BinaryConverter<BinaryType, false>))
    CONVERTER_CASE(Type::LARGE_BINARY, (BinaryConverter<LargeBinaryType, false>))
    CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter)
    CONVERTER_CASE(Type::DECIMAL, DecimalConverter)

    case Type::STRING:
      if (options.check_utf8) {
        ptr = new BinaryConverter<StringType, true>(type, options, pool);
      } else {
        ptr = new BinaryConverter<StringType, false>(type, options, pool);
      }
      break;

    case Type::LARGE_STRING:
      if (options.check_utf8) {
        ptr = new BinaryConverter<LargeStringType, true>(type, options, pool);
      } else {
        ptr = new BinaryConverter<LargeStringType, false>(type, options, pool);
      }
      break;

    default: {
      return Status::NotImplemented("CSV conversion to ", type->ToString(),
                                    " is not supported");
    }

#undef CONVERTER_CASE
  }
  std::shared_ptr<Converter> result(ptr);
  RETURN_NOT_OK(result->Initialize());
  return result;
}

Result<std::shared_ptr<DictionaryConverter>> DictionaryConverter::Make(
    const std::shared_ptr<DataType>& type, const ConvertOptions& options,
    MemoryPool* pool) {
  DictionaryConverter* ptr;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)    \
  case TYPE_ID:                                    \
    ptr = new CONVERTER_TYPE(type, options, pool); \
    break;

    CONVERTER_CASE(Type::BINARY, (DictionaryBinaryConverter<BinaryType, false>))

    case Type::STRING:
      if (options.check_utf8) {
        ptr = new DictionaryBinaryConverter<StringType, true>(type, options, pool);
      } else {
        ptr = new DictionaryBinaryConverter<StringType, false>(type, options, pool);
      }
      break;

    default: {
      return Status::NotImplemented("CSV dictionary conversion to ", type->ToString(),
                                    " is not supported");
    }

#undef CONVERTER_CASE
  }
  std::shared_ptr<DictionaryConverter> result(ptr);
  RETURN_NOT_OK(result->Initialize());
  return result;
}

}  // namespace csv
}  // namespace arrow
