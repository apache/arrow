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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "arrow/util/compression.h"
#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/metadata.h"
#include "parquet/types.h"

namespace parquet::encryption {

class EncodingPropertiesBuilder;

class EncodingProperties {
 public:
  static std::unique_ptr<EncodingProperties> MakeFromMetadata(
      const ColumnDescriptor* column_descriptor,
      const WriterProperties* writer_properties, const Page& column_page);

  // Builder pattern
  static EncodingPropertiesBuilder Builder();

  // Setters for column-level properties
  void set_column_path(const std::string& column_path);
  void set_physical_type(
      parquet::Type::type physical_type,
      const std::optional<std::int64_t>& fixed_length_bytes = std::nullopt);
  void set_compression_codec(::arrow::Compression::type compression_codec);

  void validate();

  std::map<std::string, std::string> ToPropertiesMap() const;

  // Lightweight accessor to avoid building maps when only page type is needed
  parquet::PageType::type GetPageType() const { return page_type_; }

 private:
  // Private constructor for builder
  explicit EncodingProperties(const EncodingPropertiesBuilder& builder);

  EncodingProperties(std::optional<std::string> column_path,
                     std::optional<parquet::Type::type> physical_type,
                     std::optional<::arrow::Compression::type> compression_codec,
                     std::int64_t fixed_length_bytes, parquet::PageType::type page_type,
                     parquet::Encoding::type page_encoding, int64_t data_page_num_values,
                     parquet::Encoding::type page_v1_definition_level_encoding,
                     parquet::Encoding::type page_v1_repetition_level_encoding,
                     int32_t page_v2_definition_levels_byte_length,
                     int32_t page_v2_repetition_levels_byte_length,
                     int32_t page_v2_num_nulls, bool page_v2_is_compressed);

  // Allow the builder to access private constructor
  friend class EncodingPropertiesBuilder;

  //--------------------------------
  // from column metadata. does not change across chunks nor data pages.
  std::optional<std::string> column_path_;
  std::optional<parquet::Type::type>
      physical_type_;  // BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY,
                       // FIXED_LEN_BYTE_ARRAY, etc
  std::optional<::arrow::Compression::type> compression_codec_;

  std::optional<std::int64_t> fixed_length_bytes_;  // for FIXED_LEN_BYTE_ARRAY

  //--------------------------------
  // page type. - applies across all types of pages. non optional.
  parquet::PageType::type page_type_;  // V1, V2, DICTIONARY_PAGE

  //--------------------------------
  // from data page. changes across chunks and data pages.
  // page-level properties can be seen in parquet/column_page.h
  std::optional<parquet::Encoding::type> page_encoding_;

  // common between V1 and V2 data pages.
  std::optional<int64_t> data_page_num_values_;
  std::optional<int16_t> data_page_max_definition_level_;
  std::optional<int16_t> data_page_max_repetition_level_;

  //--------------------------------
  // V1 data page properties.
  std::optional<parquet::Encoding::type> page_v1_definition_level_encoding_;
  std::optional<parquet::Encoding::type> page_v1_repetition_level_encoding_;

  //--------------------------------
  // V2 data page properties.
  std::optional<int32_t>
      page_v2_definition_levels_byte_length_;  // note that typing is different from V1
  std::optional<int32_t>
      page_v2_repetition_levels_byte_length_;  // note that typing is different from V1
  std::optional<int32_t> page_v2_num_nulls_;
  std::optional<bool>
      page_v2_is_compressed_;  // this does not exist in V1 nor dictionary pages.

  //--------------------------------
  // Dictionary page properties.

  // there are not specific properties for dictionary pages,
  // other than the page encoding (captured above).

  //--------------------------------
};  // class EncodingProperties

class EncodingPropertiesBuilder {
 public:
  EncodingPropertiesBuilder() = default;

  // Column-level properties (required)
  EncodingPropertiesBuilder& ColumnPath(const std::string& column_path);
  EncodingPropertiesBuilder& PhysicalType(parquet::Type::type physical_type);
  EncodingPropertiesBuilder& CompressionCodec(
      ::arrow::Compression::type compression_codec);
  EncodingPropertiesBuilder& PageType(parquet::PageType::type page_type);

  // Column-level optional fields
  EncodingPropertiesBuilder& FixedLengthBytes(std::int64_t fixed_length_bytes);

  // Data page properties
  EncodingPropertiesBuilder& PageEncoding(parquet::Encoding::type page_encoding);
  EncodingPropertiesBuilder& DataPageNumValues(int64_t data_page_num_values);

  // V1 data page properties
  EncodingPropertiesBuilder& PageV1DefinitionLevelEncoding(
      parquet::Encoding::type encoding);
  EncodingPropertiesBuilder& PageV1RepetitionLevelEncoding(
      parquet::Encoding::type encoding);

  // Data page common properties (apply to V1 and V2)
  EncodingPropertiesBuilder& DataPageMaxDefinitionLevel(int16_t level);
  EncodingPropertiesBuilder& DataPageMaxRepetitionLevel(int16_t level);

  // V2 data page properties
  EncodingPropertiesBuilder& PageV2DefinitionLevelsByteLength(int32_t byte_length);
  EncodingPropertiesBuilder& PageV2RepetitionLevelsByteLength(int32_t byte_length);
  EncodingPropertiesBuilder& PageV2NumNulls(int32_t num_nulls);
  EncodingPropertiesBuilder& PageV2IsCompressed(bool is_compressed);

  // Build the final object
  std::unique_ptr<EncodingProperties> Build();

 private:
  friend class EncodingProperties;

  // Required fields
  std::optional<std::string> column_path_;
  std::optional<parquet::Type::type> physical_type_;
  std::optional<::arrow::Compression::type> compression_codec_;
  std::optional<parquet::PageType::type> page_type_;

  // column metadata
  std::optional<std::int64_t> fixed_length_bytes_;

  // data page properties
  std::optional<parquet::Encoding::type> page_encoding_;
  std::optional<int64_t> data_page_num_values_;
  std::optional<int16_t> data_page_max_definition_level_;
  std::optional<int16_t> data_page_max_repetition_level_;

  // V1 data page properties
  std::optional<parquet::Encoding::type> page_v1_definition_level_encoding_;
  std::optional<parquet::Encoding::type> page_v1_repetition_level_encoding_;

  // V2 data page properties
  std::optional<int32_t> page_v2_definition_levels_byte_length_;
  std::optional<int32_t> page_v2_repetition_levels_byte_length_;
  std::optional<int32_t> page_v2_num_nulls_;
  std::optional<bool> page_v2_is_compressed_;
};  // class EncodingPropertiesBuilder

//--------------------------------
// Enum to string helpers
// Kept inline in the header for convenient reuse and zero-link overhead.
inline std::string EnumToString(parquet::Type::type t) {
  switch (t) {
    case parquet::Type::BOOLEAN:
      return "BOOLEAN";
    case parquet::Type::INT32:
      return "INT32";
    case parquet::Type::INT64:
      return "INT64";
    case parquet::Type::INT96:
      return "INT96";
    case parquet::Type::FLOAT:
      return "FLOAT";
    case parquet::Type::DOUBLE:
      return "DOUBLE";
    case parquet::Type::BYTE_ARRAY:
      return "BYTE_ARRAY";
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return "FIXED_LEN_BYTE_ARRAY";
    case parquet::Type::UNDEFINED:
      return "UNDEFINED";
    default:
      throw std::invalid_argument(std::string("Unknown parquet Type:: ") +
                                  std::to_string(t));
  }
}

inline std::string EnumToString(::arrow::Compression::type t) {
  // Use uppercase names for consistency with other enums
  switch (t) {
    case ::arrow::Compression::UNCOMPRESSED:
      return "UNCOMPRESSED";
    case ::arrow::Compression::SNAPPY:
      return "SNAPPY";
    case ::arrow::Compression::GZIP:
      return "GZIP";
    case ::arrow::Compression::BROTLI:
      return "BROTLI";
    case ::arrow::Compression::ZSTD:
      return "ZSTD";
    case ::arrow::Compression::LZ4:
      return "LZ4";
    case ::arrow::Compression::LZ4_FRAME:
      return "LZ4_FRAME";
    case ::arrow::Compression::LZO:
      return "LZO";
    case ::arrow::Compression::BZ2:
      return "BZ2";
    case ::arrow::Compression::LZ4_HADOOP:
      return "LZ4_HADOOP";
    default:
      throw std::invalid_argument(std::string("Unknown arrow Compression::type:: ") +
                                  std::to_string(t));
  }
}

inline std::string EnumToString(parquet::Encoding::type t) {
  switch (t) {
    case parquet::Encoding::PLAIN:
      return "PLAIN";
    case parquet::Encoding::PLAIN_DICTIONARY:
      return "PLAIN_DICTIONARY";
    case parquet::Encoding::RLE:
      return "RLE";
    case parquet::Encoding::BIT_PACKED:
      return "BIT_PACKED";
    case parquet::Encoding::DELTA_BINARY_PACKED:
      return "DELTA_BINARY_PACKED";
    case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
      return "DELTA_LENGTH_BYTE_ARRAY";
    case parquet::Encoding::DELTA_BYTE_ARRAY:
      return "DELTA_BYTE_ARRAY";
    case parquet::Encoding::RLE_DICTIONARY:
      return "RLE_DICTIONARY";
    case parquet::Encoding::BYTE_STREAM_SPLIT:
      return "BYTE_STREAM_SPLIT";
    case parquet::Encoding::UNDEFINED:
      return "UNDEFINED";
    case parquet::Encoding::UNKNOWN:
      return "UNKNOWN";
    default:
      throw std::invalid_argument(std::string("Unknown parquet Encoding::type:: ") +
                                  std::to_string(t));
  }
}

inline std::string EnumToString(parquet::PageType::type t) {
  switch (t) {
    case parquet::PageType::DATA_PAGE:
      return "DATA_PAGE_V1";
    case parquet::PageType::DATA_PAGE_V2:
      return "DATA_PAGE_V2";
    case parquet::PageType::DICTIONARY_PAGE:
      return "DICTIONARY_PAGE";
    case parquet::PageType::INDEX_PAGE:
      return "INDEX_PAGE";
    case parquet::PageType::UNDEFINED:
      return "UNDEFINED";
    default:
      throw std::invalid_argument(std::string("Unknown parquet PageType::type:: ") +
                                  std::to_string(t));
  }
}

}  // namespace parquet::encryption
