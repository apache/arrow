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

#include <stdexcept>

#include "parquet/column_page.h"
#include "parquet/encryption/encoding_properties.h"
#include "parquet/metadata.h"

namespace parquet::encryption {

using parquet::ColumnDescriptor;
using parquet::WriterProperties;

// Private constructor for builder
EncodingProperties::EncodingProperties(const EncodingPropertiesBuilder& builder)
    : column_path_(builder.column_path_),
      physical_type_(builder.physical_type_),
      compression_codec_(builder.compression_codec_),
      fixed_length_bytes_(builder.fixed_length_bytes_),
      page_type_(builder.page_type_.value()),
      page_encoding_(builder.page_encoding_),
      data_page_num_values_(builder.data_page_num_values_),
      data_page_max_definition_level_(builder.data_page_max_definition_level_),
      data_page_max_repetition_level_(builder.data_page_max_repetition_level_),
      page_v1_definition_level_encoding_(builder.page_v1_definition_level_encoding_),
      page_v1_repetition_level_encoding_(builder.page_v1_repetition_level_encoding_),
      page_v2_definition_levels_byte_length_(
          builder.page_v2_definition_levels_byte_length_),
      page_v2_repetition_levels_byte_length_(
          builder.page_v2_repetition_levels_byte_length_),
      page_v2_num_nulls_(builder.page_v2_num_nulls_),
      page_v2_is_compressed_(builder.page_v2_is_compressed_) {}

// Builder static method
EncodingPropertiesBuilder EncodingProperties::Builder() {
  return EncodingPropertiesBuilder();
}

// private method to validate the properties
void EncodingProperties::validate() {
  // Validate required fields
  if ((!column_path_.has_value()) || column_path_.value().empty()) {
    throw std::invalid_argument("ColumnPath is required");
  }

  // page encoding is required for all page types (data and dictionary)
  if (!page_encoding_.has_value()) {
    throw std::invalid_argument("PageEncoding is required");
  }

  // Validate page-level properties.
  // First, let's validate common properties across data pages.
  // DATA_PAGE is equivalent to DATA_PAGE_V1.
  if ((page_type_ == parquet::PageType::DATA_PAGE) ||
      (page_type_ == parquet::PageType::DATA_PAGE_V2)) {
    if (!data_page_num_values_.has_value()) {
      throw std::invalid_argument("DataPageNumValues is required");
    }

    if (physical_type_ == parquet::Type::type::FIXED_LEN_BYTE_ARRAY) {
      if (!fixed_length_bytes_.has_value()) {
        throw std::invalid_argument(
            "FixedLengthBytes is required for column page with "
            "FIXED_LEN_BYTE_ARRAY data type");
      }
    }

    if (fixed_length_bytes_.has_value()) {
      if (physical_type_ != parquet::Type::type::FIXED_LEN_BYTE_ARRAY) {
        throw std::invalid_argument(
            "FixedLengthBytes is only allowed for FIXED_LEN_BYTE_ARRAY "
            "physical type");
      }
    }

    // max levels may be required  for decoding both data page types
    if (!data_page_max_definition_level_.has_value()) {
      throw std::invalid_argument("DataPageMaxDefinitionLevel is required");
    }
    if (!data_page_max_repetition_level_.has_value()) {
      throw std::invalid_argument("DataPageMaxRepetitionLevel is required");
    }
  }

  // then, let's validate properties specific to each page type.
  // DATA_PAGE is equivalent to DATA_PAGE_V1.
  if (page_type_ == parquet::PageType::DATA_PAGE) {
    if (!page_v1_definition_level_encoding_.has_value()) {
      throw std::invalid_argument("PageV1DefinitionLevelEncoding is required");
    }
    if (!page_v1_repetition_level_encoding_.has_value()) {
      throw std::invalid_argument("PageV1RepetitionLevelEncoding is required");
    }
  } else if (page_type_ == parquet::PageType::DATA_PAGE_V2) {
    if (!page_v2_num_nulls_.has_value()) {
      throw std::invalid_argument("PageV2NumNulls is required");
    }
    if (!page_v2_definition_levels_byte_length_.has_value()) {
      throw std::invalid_argument("PageV2DefinitionLevelsByteLength is required");
    }
    if (!page_v2_repetition_levels_byte_length_.has_value()) {
      throw std::invalid_argument("PageV2RepetitionLevelsByteLength is required");
    }
    if (!page_v2_is_compressed_.has_value()) {
      throw std::invalid_argument("PageV2IsCompressed is required");
    }
  } else if (page_type_ == parquet::PageType::DICTIONARY_PAGE) {
    // no validations required for DICTIONARY_PAGE
    // (the requirement for 'encoding' is satisfied by the page_encoding check above)
  }
}  // validate()

std::unique_ptr<EncodingProperties> EncodingProperties::MakeFromMetadata(
    const ColumnDescriptor* column_descriptor, const WriterProperties* writer_properties,
    const Page& column_page) {
  EncodingPropertiesBuilder builder;

  builder.ColumnPath(column_descriptor->path()->ToDotString());
  builder.PhysicalType(column_descriptor->physical_type());
  builder.CompressionCodec(writer_properties->compression(column_descriptor->path()));
  builder.PageType(column_page.type());

  // If the physical type is FIXED_LEN_BYTE_ARRAY, propagate the byte width
  // from the column descriptor so downstream users (e.g., external agents)
  // have access to the length. Validation also requires this for data pages.
  if (column_descriptor->physical_type() == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    builder.FixedLengthBytes(column_descriptor->type_length());
  }

  bool is_data_page = (column_page.type() == parquet::PageType::DATA_PAGE) ||
                      (column_page.type() == parquet::PageType::DATA_PAGE_V2);

  // properties common to V1 and V2 data pages.
  if (is_data_page) {
    DataPage data_page = static_cast<const DataPage&>(column_page);
    builder.PageEncoding(data_page.encoding());
    builder.DataPageNumValues(data_page.num_values());
    builder.DataPageMaxDefinitionLevel(column_descriptor->max_definition_level());
    builder.DataPageMaxRepetitionLevel(column_descriptor->max_repetition_level());
  }

  // properties specific to each type of page
  if (column_page.type() == parquet::PageType::DATA_PAGE) {
    DataPageV1 data_page_v1 = static_cast<const DataPageV1&>(column_page);
    builder.PageV1DefinitionLevelEncoding(data_page_v1.definition_level_encoding());
    builder.PageV1RepetitionLevelEncoding(data_page_v1.repetition_level_encoding());
  } else if (column_page.type() == parquet::PageType::DATA_PAGE_V2) {
    DataPageV2 data_page_v2 = static_cast<const DataPageV2&>(column_page);
    builder.PageV2DefinitionLevelsByteLength(
        data_page_v2.definition_levels_byte_length());
    builder.PageV2RepetitionLevelsByteLength(
        data_page_v2.repetition_levels_byte_length());
    builder.PageV2NumNulls(data_page_v2.num_nulls());
    builder.PageV2IsCompressed(data_page_v2.is_compressed());
  } else if (column_page.type() == parquet::PageType::DICTIONARY_PAGE) {
    DictionaryPage dict_page = static_cast<const DictionaryPage&>(column_page);
    builder.PageEncoding(dict_page.encoding());
  } else {
    throw std::invalid_argument(std::string("Unknown Page Type:: ") +
                                EnumToString(column_page.type()));
  }

  return builder.Build();
}

std::map<std::string, std::string> EncodingProperties::ToPropertiesMap() const {
  std::map<std::string, std::string> result;

  result["column_path"] = std::string(column_path_.value());
  result["physical_type"] = EnumToString(physical_type_.value());
  result["compression_codec"] = EnumToString(compression_codec_.value());
  result["page_type"] = EnumToString(page_type_);
  result["page_encoding"] = EnumToString(page_encoding_.value());

  if (fixed_length_bytes_.has_value()) {
    result["fixed_length_bytes"] = std::to_string(fixed_length_bytes_.value());
  }

  if (page_type_ == parquet::PageType::DATA_PAGE ||
      page_type_ == parquet::PageType::DATA_PAGE_V2) {
    result["data_page_max_definition_level"] =
        std::to_string(data_page_max_definition_level_.value());
    result["data_page_max_repetition_level"] =
        std::to_string(data_page_max_repetition_level_.value());
  }

  if (page_type_ == parquet::PageType::DATA_PAGE) {  // DATA_PAGE_V1
    result["data_page_num_values"] = std::to_string(data_page_num_values_.value());
    result["page_v1_definition_level_encoding"] =
        EnumToString(page_v1_definition_level_encoding_.value());
    result["page_v1_repetition_level_encoding"] =
        EnumToString(page_v1_repetition_level_encoding_.value());
  } else if (page_type_ == parquet::PageType::DATA_PAGE_V2) {
    result["data_page_num_values"] = std::to_string(data_page_num_values_.value());
    result["page_v2_definition_levels_byte_length"] =
        std::to_string(page_v2_definition_levels_byte_length_.value());
    result["page_v2_repetition_levels_byte_length"] =
        std::to_string(page_v2_repetition_levels_byte_length_.value());
    result["page_v2_num_nulls"] = std::to_string(page_v2_num_nulls_.value());
    result["page_v2_is_compressed"] = (page_v2_is_compressed_.value() ? "true" : "false");
  } else if (page_type_ == parquet::PageType::DICTIONARY_PAGE) {
    // no other properties are set for DICTIONARY_PAGE
  }

  return result;
}

//--------------------------------
// Builder method implementations

std::unique_ptr<EncodingProperties> EncodingPropertiesBuilder::Build() {
  // while we will perform validation upon construction,
  // we know that these properties are required.
  // validating here simplifies our code.

  if (!page_type_) {
    throw std::invalid_argument(
        "EncodingPropertiesBuilder::Build - PageType is required");
  }

  return std::unique_ptr<EncodingProperties>(new EncodingProperties(*this));
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::ColumnPath(
    const std::string& column_path) {
  column_path_ = column_path;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PhysicalType(
    parquet::Type::type physical_type) {
  physical_type_ = physical_type;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::CompressionCodec(
    ::arrow::Compression::type compression_codec) {
  compression_codec_ = compression_codec;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::FixedLengthBytes(
    std::int64_t fixed_length_bytes) {
  fixed_length_bytes_ = fixed_length_bytes;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageType(
    parquet::PageType::type page_type) {
  page_type_ = page_type;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageEncoding(
    parquet::Encoding::type page_encoding) {
  page_encoding_ = page_encoding;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::DataPageNumValues(
    int64_t data_page_num_values) {
  data_page_num_values_ = data_page_num_values;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageV1DefinitionLevelEncoding(
    parquet::Encoding::type encoding) {
  page_v1_definition_level_encoding_ = encoding;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageV1RepetitionLevelEncoding(
    parquet::Encoding::type encoding) {
  page_v1_repetition_level_encoding_ = encoding;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::DataPageMaxDefinitionLevel(
    int16_t level) {
  data_page_max_definition_level_ = level;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::DataPageMaxRepetitionLevel(
    int16_t level) {
  data_page_max_repetition_level_ = level;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageV2DefinitionLevelsByteLength(
    int32_t byte_length) {
  page_v2_definition_levels_byte_length_ = byte_length;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageV2RepetitionLevelsByteLength(
    int32_t byte_length) {
  page_v2_repetition_levels_byte_length_ = byte_length;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageV2NumNulls(int32_t num_nulls) {
  page_v2_num_nulls_ = num_nulls;
  return *this;
}

EncodingPropertiesBuilder& EncodingPropertiesBuilder::PageV2IsCompressed(
    bool is_compressed) {
  page_v2_is_compressed_ = is_compressed;
  return *this;
}

//--------------------------------
// Setters for column-level properties
// used to fill-in values provided in the encryptor/decryptor constructor.
void EncodingProperties::set_column_path(const std::string& column_path) {
  column_path_ = column_path;
}

void EncodingProperties::set_compression_codec(
    ::arrow::Compression::type compression_codec) {
  compression_codec_ = compression_codec;
}

void EncodingProperties::set_physical_type(
    parquet::Type::type physical_type,
    const std::optional<std::int64_t>& fixed_length_bytes) {
  physical_type_ = physical_type;
  if (fixed_length_bytes.has_value()) {
    fixed_length_bytes_ = fixed_length_bytes;
  }
}

}  // namespace parquet::encryption
