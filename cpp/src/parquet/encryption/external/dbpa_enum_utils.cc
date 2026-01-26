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

#include "parquet/encryption/external/dbpa_enum_utils.h"

#include <stdexcept>
#include <unordered_map>

namespace parquet::encryption::external {

// Static map definitions for the class members

// ------------------------------------------------------------
// parquet::Type -> dbps::external::Type
// Parquet types are coming from parquet/types.h
// ------------------------------------------------------------
const std::unordered_map<parquet::Type::type, dbps::external::Type::type>
    DBPAEnumUtils::parquet_to_external_type_map = {
        {parquet::Type::BOOLEAN, dbps::external::Type::BOOLEAN},
        {parquet::Type::INT32, dbps::external::Type::INT32},
        {parquet::Type::INT64, dbps::external::Type::INT64},
        {parquet::Type::INT96, dbps::external::Type::INT96},
        {parquet::Type::FLOAT, dbps::external::Type::FLOAT},
        {parquet::Type::DOUBLE, dbps::external::Type::DOUBLE},
        {parquet::Type::BYTE_ARRAY, dbps::external::Type::BYTE_ARRAY},
        {parquet::Type::FIXED_LEN_BYTE_ARRAY, dbps::external::Type::FIXED_LEN_BYTE_ARRAY},
        {parquet::Type::UNDEFINED, dbps::external::Type::UNDEFINED}};

// ------------------------------------------------------------
// arrow::Compression -> dbps::external::CompressionCodec
// values for arrow::Compression are coming from arrow/type_fwd.h
// Note: Some Arrow compression types don't have direct DBPS equivalents
// ------------------------------------------------------------
const std::unordered_map<::arrow::Compression::type,
                         dbps::external::CompressionCodec::type>
    DBPAEnumUtils::arrow_to_external_compression_map = {
        {::arrow::Compression::UNCOMPRESSED,
         dbps::external::CompressionCodec::UNCOMPRESSED},
        {::arrow::Compression::SNAPPY, dbps::external::CompressionCodec::SNAPPY},
        {::arrow::Compression::GZIP, dbps::external::CompressionCodec::GZIP},
        {::arrow::Compression::LZO, dbps::external::CompressionCodec::LZO},
        {::arrow::Compression::BROTLI, dbps::external::CompressionCodec::BROTLI},
        {::arrow::Compression::LZ4, dbps::external::CompressionCodec::LZ4},
        {::arrow::Compression::ZSTD, dbps::external::CompressionCodec::ZSTD},
        {::arrow::Compression::LZ4_FRAME, dbps::external::CompressionCodec::LZ4_FRAME},
        {::arrow::Compression::BZ2, dbps::external::CompressionCodec::BZ2},
        {::arrow::Compression::LZ4_HADOOP, dbps::external::CompressionCodec::LZ4_HADOOP}};

// ------------------------------------------------------------
// function which returns parquet::Type::type to dbps::external::Type::type
// ------------------------------------------------------------
dbps::external::Type::type DBPAEnumUtils::ParquetTypeToDBPA(
    parquet::Type::type parquet_type) {
  // Look up the mapping in the static map
  auto it = parquet_to_external_type_map.find(parquet_type);
  if (it != parquet_to_external_type_map.end()) {
    return it->second;
  }

  throw std::invalid_argument("Invalid parquet::Type value");
}

// ------------------------------------------------------------
// function which returns arrow::Compression::type to
// dbps::external::CompressionCodec::type
// ------------------------------------------------------------
dbps::external::CompressionCodec::type DBPAEnumUtils::ArrowCompressionToDBPA(
    ::arrow::Compression::type arrow_compression) {
  // Look up the mapping in the static map
  auto it = arrow_to_external_compression_map.find(arrow_compression);
  if (it != arrow_to_external_compression_map.end()) {
    return it->second;
  }

  throw std::invalid_argument("Invalid arrow::Compression value");
}

}  // namespace parquet::encryption::external
