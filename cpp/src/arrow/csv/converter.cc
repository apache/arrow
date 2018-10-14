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

#include "arrow/builder.h"
#include "arrow/csv/parser.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/parsing.h"  // IWYU pragma: keep

namespace arrow {
namespace csv {

using internal::StringConverter;

namespace {

Status GenericConversionError(const std::shared_ptr<DataType>& type, const uint8_t* data,
                              uint32_t size) {
  std::stringstream ss;
  ss << "CSV conversion error to " << type->ToString() << ": invalid value '"
     << std::string(reinterpret_cast<const char*>(data), size) << "'";
  return Status::Invalid(ss.str());
}

class ConcreteConverter : public Converter {
 public:
  using Converter::Converter;

 protected:
  inline bool IsNull(const uint8_t* data, uint32_t size, bool quoted);
};

// Recognize various spellings of null values.  The list of possible spellings
// is taken from Pandas read_csv() documentation.
bool ConcreteConverter::IsNull(const uint8_t* data, uint32_t size, bool quoted) {
  if (quoted) {
    return false;
  }
  if (size == 0) {
    return true;
  }
  // No 1-character null value exists
  if (size == 1) {
    return false;
  }

  // XXX if the CSV parser guaranteed enough excess bytes at the end of the
  // parsed area, we wouldn't need to always check size before comparing characters.

  auto chars = reinterpret_cast<const char*>(data);
  auto first = chars[0];
  auto second = chars[1];
  switch (first) {
    case 'N': {
      // "NA", "N/A", "NaN", "NULL"
      if (size == 2) {
        return second == 'A';
      }
      auto third = chars[2];
      if (size == 3) {
        return (second == '/' && third == 'A') || (second == 'a' && third == 'N');
      }
      if (size == 4) {
        return (second == 'U' && third == 'L' && chars[3] == 'L');
      }
      return false;
    }
    case 'n': {
      // "n/a", "nan", "null"
      if (size == 2) {
        return false;
      }
      auto third = chars[2];
      if (size == 3) {
        return (second == '/' && third == 'a') || (second == 'a' && third == 'n');
      }
      if (size == 4) {
        return (second == 'u' && third == 'l' && chars[3] == 'l');
      }
      return false;
    }
    case '1': {
      // '1.#IND', '1.#QNAN'
      if (size == 6) {
        // '#' is the most unlikely char here, check it first
        return (chars[2] == '#' && chars[1] == '.' && chars[3] == 'I' &&
                chars[4] == 'N' && chars[5] == 'D');
      }
      if (size == 7) {
        return (chars[2] == '#' && chars[1] == '.' && chars[3] == 'Q' &&
                chars[4] == 'N' && chars[5] == 'A' && chars[6] == 'N');
      }
      return false;
    }
    case '-': {
      switch (second) {
        case 'N':
          // "-NaN"
          return (size == 4 && chars[2] == 'a' && chars[3] == 'N');
        case 'n':
          // "-nan"
          return (size == 4 && chars[2] == 'a' && chars[3] == 'n');
        case '1':
          // "-1.#IND", "-1.#QNAN"
          if (size == 7) {
            return (chars[3] == '#' && chars[2] == '.' && chars[4] == 'I' &&
                    chars[5] == 'N' && chars[6] == 'D');
          }
          if (size == 8) {
            return (chars[3] == '#' && chars[2] == '.' && chars[4] == 'Q' &&
                    chars[5] == 'N' && chars[6] == 'A' && chars[7] == 'N');
          }
          return false;
        default:
          return false;
      }
    }
    case '#': {
      // "#N/A", "#N/A N/A", "#NA"
      if (size < 3 || chars[1] != 'N') {
        return false;
      }
      auto third = chars[2];
      if (size == 3) {
        return third == 'A';
      }
      if (size == 4) {
        return third == '/' && chars[3] == 'A';
      }
      if (size == 8) {
        return std::memcmp(data + 2, "/A N/A", 5) == 0;
      }
      return false;
    }
    default:
      return false;
  }
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

template <typename T>
class VarSizeBinaryConverter : public ConcreteConverter {
 public:
  using ConcreteConverter::ConcreteConverter;

  Status Convert(const BlockParser& parser, int32_t col_index,
                 std::shared_ptr<Array>* out) override;
};

template <typename T>
Status VarSizeBinaryConverter<T>::Convert(const BlockParser& parser, int32_t col_index,
                                          std::shared_ptr<Array>* out) {
  using BuilderType = typename TypeTraits<T>::BuilderType;
  BuilderType builder(pool_);

  // TODO handle nulls

  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    return builder.Append(data, size);
  };
  RETURN_NOT_OK(builder.Resize(parser.num_rows()));
  RETURN_NOT_OK(builder.ReserveData(parser.num_bytes()));
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return Status::OK();
}

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

  // TODO handle nulls

  auto visit = [&](const uint8_t* data, uint32_t size, bool quoted) -> Status {
    if (ARROW_PREDICT_FALSE(size != byte_width)) {
      std::stringstream ss;
      ss << "CSV conversion error to " << type_->ToString() << ": got a " << size
         << "-byte long string";
      return Status::Invalid(ss.str());
    }
    return builder.Append(data);
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
    value_type value;
    if (IsNull(data, size, quoted)) {
      return builder.AppendNull();
    }
    if (ARROW_PREDICT_FALSE(
            !converter(reinterpret_cast<const char*>(data), size, &value))) {
      return GenericConversionError(type_, data, size);
    }
    return builder.Append(value);
  };
  RETURN_NOT_OK(builder.Resize(parser.num_rows()));
  RETURN_NOT_OK(parser.VisitColumn(col_index, visit));
  RETURN_NOT_OK(builder.Finish(out));

  return Status::OK();
}

}  // namespace

/////////////////////////////////////////////////////////////////////////
// Base Converter class implementation

Converter::Converter(const std::shared_ptr<DataType>& type, ConvertOptions options,
                     MemoryPool* pool)
    : options_(options), pool_(pool), type_(type) {}

Status Converter::Make(const std::shared_ptr<DataType>& type, ConvertOptions options,
                       MemoryPool* pool, std::shared_ptr<Converter>* out) {
  Converter* result;

  switch (type->id()) {
#define CONVERTER_CASE(TYPE_ID, CONVERTER_TYPE)       \
  case TYPE_ID:                                       \
    result = new CONVERTER_TYPE(type, options, pool); \
    break;

    CONVERTER_CASE(Type::NA, NullConverter)
    CONVERTER_CASE(Type::BINARY, VarSizeBinaryConverter<BinaryType>)
    CONVERTER_CASE(Type::STRING, VarSizeBinaryConverter<StringType>)
    CONVERTER_CASE(Type::FIXED_SIZE_BINARY, FixedSizeBinaryConverter)
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
    CONVERTER_CASE(Type::BOOL, NumericConverter<BooleanType>)

    default: {
      std::stringstream ss;
      ss << "CSV conversion to " << type->ToString() << " is not supported";
      return Status::NotImplemented(ss.str());
    }

#undef CONVERTER_CASE
  }
  out->reset(result);
  return Status::OK();
}

Status Converter::Make(const std::shared_ptr<DataType>& type, ConvertOptions options,
                       std::shared_ptr<Converter>* out) {
  return Make(type, options, default_memory_pool(), out);
}

}  // namespace csv
}  // namespace arrow
