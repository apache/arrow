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

#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>

#include "parquet/types.h"

namespace parquet {

std::string FormatStatValue(Type::type parquet_type, const char* val) {
  std::stringstream result;
  switch (parquet_type) {
    case Type::BOOLEAN:
      result << reinterpret_cast<const bool*>(val)[0];
      break;
    case Type::INT32:
      result << reinterpret_cast<const int32_t*>(val)[0];
      break;
    case Type::INT64:
      result << reinterpret_cast<const int64_t*>(val)[0];
      break;
    case Type::DOUBLE:
      result << reinterpret_cast<const double*>(val)[0];
      break;
    case Type::FLOAT:
      result << reinterpret_cast<const float*>(val)[0];
      break;
    case Type::INT96: {
      for (int i = 0; i < 3; i++) {
        result << reinterpret_cast<const int32_t*>(val)[i] << " ";
      }
      break;
    }
    case Type::BYTE_ARRAY: {
      result << val << " ";
      break;
    }
    case Type::FIXED_LEN_BYTE_ARRAY: {
      result << val << " ";
      break;
    }
    default:
      break;
  }
  return result.str();
}

std::string EncodingToString(Encoding::type t) {
  switch (t) {
    case Encoding::PLAIN:
      return "PLAIN";
    case Encoding::PLAIN_DICTIONARY:
      return "PLAIN_DICTIONARY";
    case Encoding::RLE:
      return "RLE";
    case Encoding::BIT_PACKED:
      return "BIT_PACKED";
    case Encoding::DELTA_BINARY_PACKED:
      return "DELTA_BINARY_PACKED";
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
      return "DELTA_LENGTH_BYTE_ARRAY";
    case Encoding::DELTA_BYTE_ARRAY:
      return "DELTA_BYTE_ARRAY";
    case Encoding::RLE_DICTIONARY:
      return "RLE_DICTIONARY";
    default:
      return "UNKNOWN";
  }
}

std::string CompressionToString(Compression::type t) {
  switch (t) {
    case Compression::UNCOMPRESSED:
      return "UNCOMPRESSED";
    case Compression::SNAPPY:
      return "SNAPPY";
    case Compression::GZIP:
      return "GZIP";
    case Compression::LZO:
      return "LZO";
    case Compression::LZ4:
      return "LZ4";
    case Compression::ZSTD:
      return "ZSTD";
    default:
      return "UNKNOWN";
  }
}

std::string TypeToString(Type::type t) {
  switch (t) {
    case Type::BOOLEAN:
      return "BOOLEAN";
    case Type::INT32:
      return "INT32";
    case Type::INT64:
      return "INT64";
    case Type::INT96:
      return "INT96";
    case Type::FLOAT:
      return "FLOAT";
    case Type::DOUBLE:
      return "DOUBLE";
    case Type::BYTE_ARRAY:
      return "BYTE_ARRAY";
    case Type::FIXED_LEN_BYTE_ARRAY:
      return "FIXED_LEN_BYTE_ARRAY";
    default:
      return "UNKNOWN";
  }
}

std::string LogicalTypeToString(LogicalType::type t) {
  switch (t) {
    case LogicalType::NONE:
      return "NONE";
    case LogicalType::UTF8:
      return "UTF8";
    case LogicalType::MAP_KEY_VALUE:
      return "MAP_KEY_VALUE";
    case LogicalType::LIST:
      return "LIST";
    case LogicalType::ENUM:
      return "ENUM";
    case LogicalType::DECIMAL:
      return "DECIMAL";
    case LogicalType::DATE:
      return "DATE";
    case LogicalType::TIME_MILLIS:
      return "TIME_MILLIS";
    case LogicalType::TIME_MICROS:
      return "TIME_MICROS";
    case LogicalType::TIMESTAMP_MILLIS:
      return "TIMESTAMP_MILLIS";
    case LogicalType::TIMESTAMP_MICROS:
      return "TIMESTAMP_MICROS";
    case LogicalType::UINT_8:
      return "UINT_8";
    case LogicalType::UINT_16:
      return "UINT_16";
    case LogicalType::UINT_32:
      return "UINT_32";
    case LogicalType::UINT_64:
      return "UINT_64";
    case LogicalType::INT_8:
      return "INT_8";
    case LogicalType::INT_16:
      return "INT_16";
    case LogicalType::INT_32:
      return "INT_32";
    case LogicalType::INT_64:
      return "INT_64";
    case LogicalType::JSON:
      return "JSON";
    case LogicalType::BSON:
      return "BSON";
    case LogicalType::INTERVAL:
      return "INTERVAL";
    default:
      return "UNKNOWN";
  }
}

int GetTypeByteSize(Type::type parquet_type) {
  switch (parquet_type) {
    case Type::BOOLEAN:
      return type_traits<BooleanType::type_num>::value_byte_size;
    case Type::INT32:
      return type_traits<Int32Type::type_num>::value_byte_size;
    case Type::INT64:
      return type_traits<Int64Type::type_num>::value_byte_size;
    case Type::INT96:
      return type_traits<Int96Type::type_num>::value_byte_size;
    case Type::DOUBLE:
      return type_traits<DoubleType::type_num>::value_byte_size;
    case Type::FLOAT:
      return type_traits<FloatType::type_num>::value_byte_size;
    case Type::BYTE_ARRAY:
      return type_traits<ByteArrayType::type_num>::value_byte_size;
    case Type::FIXED_LEN_BYTE_ARRAY:
      return type_traits<FLBAType::type_num>::value_byte_size;
    default:
      return 0;
  }
  return 0;
}

// Return the Sort Order of the Parquet Physical Types
SortOrder::type DefaultSortOrder(Type::type primitive) {
  switch (primitive) {
    case Type::BOOLEAN:
    case Type::INT32:
    case Type::INT64:
    case Type::FLOAT:
    case Type::DOUBLE:
      return SortOrder::SIGNED;
    case Type::BYTE_ARRAY:
    case Type::FIXED_LEN_BYTE_ARRAY:
    case Type::INT96:  // only used for timestamp, which uses unsigned values
      return SortOrder::UNSIGNED;
  }
  return SortOrder::UNKNOWN;
}

// Return the SortOrder of the Parquet Types using Logical or Physical Types
SortOrder::type GetSortOrder(LogicalType::type converted, Type::type primitive) {
  if (converted == LogicalType::NONE) return DefaultSortOrder(primitive);
  switch (converted) {
    case LogicalType::INT_8:
    case LogicalType::INT_16:
    case LogicalType::INT_32:
    case LogicalType::INT_64:
    case LogicalType::DATE:
    case LogicalType::TIME_MICROS:
    case LogicalType::TIME_MILLIS:
    case LogicalType::TIMESTAMP_MICROS:
    case LogicalType::TIMESTAMP_MILLIS:
      return SortOrder::SIGNED;
    case LogicalType::UINT_8:
    case LogicalType::UINT_16:
    case LogicalType::UINT_32:
    case LogicalType::UINT_64:
    case LogicalType::ENUM:
    case LogicalType::UTF8:
    case LogicalType::BSON:
    case LogicalType::JSON:
      return SortOrder::UNSIGNED;
    case LogicalType::DECIMAL:
    case LogicalType::LIST:
    case LogicalType::MAP:
    case LogicalType::MAP_KEY_VALUE:
    case LogicalType::INTERVAL:
    case LogicalType::NONE:  // required instead of default
    case LogicalType::NA:    // required instead of default
      return SortOrder::UNKNOWN;
  }
  return SortOrder::UNKNOWN;
}

}  // namespace parquet
