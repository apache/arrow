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

std::string encoding_to_string(Encoding::type t) {
  switch (t) {
    case Encoding::PLAIN:
      return "PLAIN";
      break;
    case Encoding::PLAIN_DICTIONARY:
      return "PLAIN_DICTIONARY";
      break;
    case Encoding::RLE:
      return "RLE";
      break;
    case Encoding::BIT_PACKED:
      return "BIT_PACKED";
      break;
    case Encoding::DELTA_BINARY_PACKED:
      return "DELTA_BINARY_PACKED";
      break;
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
      return "DELTA_LENGTH_BYTE_ARRAY";
      break;
    case Encoding::DELTA_BYTE_ARRAY:
      return "DELTA_BYTE_ARRAY";
      break;
    case Encoding::RLE_DICTIONARY:
      return "RLE_DICTIONARY";
      break;
    default:
      return "UNKNOWN";
      break;
  }
}

std::string compression_to_string(Compression::type t) {
  switch (t) {
    case Compression::UNCOMPRESSED:
      return "UNCOMPRESSED";
      break;
    case Compression::SNAPPY:
      return "SNAPPY";
      break;
    case Compression::GZIP:
      return "GZIP";
      break;
    case Compression::LZO:
      return "LZO";
      break;
    default:
      return "UNKNOWN";
      break;
  }
}

std::string type_to_string(Type::type t) {
  switch (t) {
    case Type::BOOLEAN:
      return "BOOLEAN";
      break;
    case Type::INT32:
      return "INT32";
      break;
    case Type::INT64:
      return "INT64";
      break;
    case Type::INT96:
      return "INT96";
      break;
    case Type::FLOAT:
      return "FLOAT";
      break;
    case Type::DOUBLE:
      return "DOUBLE";
      break;
    case Type::BYTE_ARRAY:
      return "BYTE_ARRAY";
      break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      return "FIXED_LEN_BYTE_ARRAY";
      break;
    default:
      return "UNKNOWN";
      break;
  }
}

std::string logical_type_to_string(LogicalType::type t) {
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
}  // namespace parquet
