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
#include "arrow/util/parsing.h"  // IWYU pragma: keep
#include "arrow/util/trie.h"
#include "arrow/util/utf8.h"

namespace arrow {
namespace csv {

using internal::StringConverter;
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

Status InitializeTrie(const std::vector<std::string>& inputs, Trie* trie) {
  TrieBuilder builder;
  for (const auto& s : inputs) {
    RETURN_NOT_OK(builder.Append(s, true /* allow_duplicates */));
  }
  *trie = builder.Finish();
  return Status::OK();
}

class ConcreteConverter : public Converter {
 public:
  using Converter::Converter;

 protected:
  Status Initialize() override;
  inline bool IsNull(const uint8_t* data, uint32_t size, bool quoted);

  Trie null_trie_;
};

Status ConcreteConverter::Initialize() {
  // TODO no need to build a separate Trie for each Converter instance
  return InitializeTrie(options_.null_values, &null_trie_);
}

bool ConcreteConverter::IsNull(const uint8_t* data, uint32_t size, bool quoted) {
  if (quoted) {
    return false;
  }
  return null_trie_.Find(util::string_view(reinterpret_cast<const char*>(data), size)) >=
         0;
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for null values

class NullConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override;
};

Status NullConverter::Convert(const BlockParser& parser, int32_t col_index,
                              std::shared_ptr<Array>* out) {
  NullBuilder builder(pool_);

  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    if (ARROW_PREDICT_TRUE(IsNull(data, size, quoted))) {
      return builder.AppendNull();
    } else {
      return GenericConversionError(type_, data, size);
    }
  };
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for var-sized binary strings

template <typename T, bool CheckUTF8>
class VarSizeBinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override {
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

    RETURN_NOT_OK(builder.Finish(out));

    return Status::OK();
  }

 protected:
  Status Initialize() override {
    util::InitializeUTF8();
    return ConcreteConverter::Initialize();
  }
};

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for fixed-sized binary strings

class FixedSizeBinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override;
};

Status FixedSizeBinaryConverter::Convert(const BlockParser& parser, int32_t col_index,
                                         std::shared_ptr<Array>* out) {
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
  RETURN_NOT_OK(builder.Finish(out));

  return Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for booleans

class BooleanConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override;

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

Status BooleanConverter::Convert(const BlockParser& parser, int32_t col_index,
                                 std::shared_ptr<Array>* out) {
  BooleanBuilder builder(type_, pool_);

  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    // XXX should quoted values be allowed at all?
    if (IsNull(data, size, quoted)) {
      builder.UnsafeAppendNull();
      return Status::OK();
    }
    if (false_trie_.Find(util::string_view(reinterpret_cast<const char*>(data), size)) >=
        0) {
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
  RETURN_NOT_OK(builder.Finish(out));

  return Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for numbers

template <typename T>
class NumericConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override;
};

template <typename T>
Status NumericConverter<T>::Convert(const BlockParser& parser, int32_t col_index,
                                    std::shared_ptr<Array>* out) {
  using BuilderType = typename TypeTraits<T>::BuilderType;
  using value_type = typename StringConverter<T>::value_type;

  BuilderType builder(type_, pool_);
  StringConverter<T> converter;

  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    // XXX should quoted values be allowed at all?
    value_type value;
    if (IsNull(data, size, quoted)) {
      builder.UnsafeAppendNull();
      return Status::OK();
    }
    if (!std::is_same<BooleanType, T>::value) {
      // Skip trailing whitespace
      if (ARROW_PREDICT_TRUE(size > 0) &&
          ARROW_PREDICT_FALSE(IsWhitespace(data[size - 1]))) {
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
    if (ARROW_PREDICT_FALSE(
            !converter(reinterpret_cast<const char*>(data), size, &value))) {
      return GenericConversionError(type_, data, size);
    }
    builder.UnsafeAppend(value);
    return Status::OK();
  };
  RETURN_NOT_OK(builder.Resize(parser.num_rows()));
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return Status::OK();
}

/////////////////////////////////////////////////////////////////////////
// Concrete Converter for timestamps

class TimestampConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override {
    using value_type = TimestampType::c_type;

    TimestampBuilder builder(type_, pool_);
    StringConverter<TimestampType> converter(type_);

    auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
      value_type value = 0;
      if (IsNull(data, size, quoted)) {
        builder.UnsafeAppendNull();
        return Status::OK();
      }
      if (ARROW_PREDICT_FALSE(
              !converter(reinterpret_cast<const char*>(data), size, &value))) {
        return GenericConversionError(type_, data, size);
      }
      builder.UnsafeAppend(value);
      return Status::OK();
    };
    RETURN_NOT_OK(builder.Resize(parser.num_rows()));
    RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
    RETURN_NOT_OK(builder.Finish(out));

    return Status::OK();
  }
};

}  // namespace

/////////////////////////////////////////////////////////////////////////
// Base Converter class implementation

Converter::Converter(const std::shared_ptr<DataType>& type, const ConvertOptions& options,
                     MemoryPool* pool)
    : options_(options), pool_(pool), type_(type) {}

Status Converter::Make(const std::shared_ptr<DataType>& type,
                       const ConvertOptions& options, MemoryPool* pool,
                       std::shared_ptr<Converter>* out) {
  Converter* result;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)       \
  case TYPE_ID:                                       \
    result = new CONVERTER_TYPE(type, options, pool); \
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
    CONVERTER_CASE(Type::BINARY, (VarSizeBinaryConverter<BinaryType, false>))
    CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter)

    case Type::STRING:
      if (options.check_utf8) {
        result = new VarSizeBinaryConverter<StringType, true>(type, options, pool);
      } else {
        result = new VarSizeBinaryConverter<StringType, false>(type, options, pool);
      }
      break;

    default: {
      return Status::NotImplemented("CSV conversion to ", type->ToString(),
                                    " is not supported");
    }

#undef CONVERTER_CASE
  }
  out->reset(result);
  return result->Initialize();
}

Status Converter::Make(const std::shared_ptr<DataType>& type,
                       const ConvertOptions& options, std::shared_ptr<Converter>* out) {
  return Make(type, options, default_memory_pool(), out);
}

}  // namespace csv
}  // namespace arrow
