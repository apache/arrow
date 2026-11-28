// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "generated/parquet_types.h"

namespace parquet::internal {

struct DirectStatistics {
  std::string_view max, min, max_value, min_value;
  int64_t null_count = 0, distinct_count = 0, nan_count = 0;
  bool has_max = false, has_min = false, has_max_value = false, has_min_value = false;
  bool has_null_count = false, has_distinct_count = false, has_nan_count = false;
  bool is_max_value_exact = false, is_min_value_exact = false;
  bool has_max_value_exact = false, has_min_value_exact = false;
};

struct DirectKeyValue {
  std::string_view key;
  std::string_view value;
  bool has_value = false;
};

// Compact representation of SchemaElement used while opening a footer.  The
// generated Thrift object has storage for optional annotations. This representation
// is consumed directly by UnflattenDirect; generated schema elements are only
// materialized by the serialization/compatibility path.
struct DirectSchemaElement {
  format::Type::type type = static_cast<format::Type::type>(0);
  int32_t type_length = 0;
  format::FieldRepetitionType::type repetition_type =
      format::FieldRepetitionType::REQUIRED;
  std::string_view name;
  int32_t num_children = 0;
  format::ConvertedType::type converted_type = format::ConvertedType::UTF8;
  int32_t scale = 0;
  int32_t precision = 0;
  int32_t field_id = 0;
  format::LogicalType logical_type;
  bool has_type = false;
  bool has_type_length = false;
  bool has_repetition_type = false;
  bool has_num_children = false;
  bool has_converted_type = false;
  bool has_scale = false;
  bool has_precision = false;
  bool has_field_id = false;
  bool has_logical_type = false;
};

struct DirectSizeStatistics {
  int64_t unencoded_byte_array_data_bytes = 0;
  std::vector<int64_t> repetition_level_histogram, definition_level_histogram;
  bool has_unencoded_byte_array_data_bytes = false;
  bool has_repetition_level_histogram = false, has_definition_level_histogram = false;
};

struct DirectColumnMetadata {
  format::Type::type type = static_cast<format::Type::type>(0);
  std::vector<format::Encoding::type> encodings;
  std::vector<std::string_view> path_in_schema;
  format::CompressionCodec::type codec = format::CompressionCodec::UNCOMPRESSED;
  int64_t num_values = 0;
  int64_t total_uncompressed_size = 0;
  int64_t total_compressed_size = 0;
  std::vector<DirectKeyValue> key_value_metadata;
  int64_t data_page_offset = 0;
  int64_t index_page_offset = 0;
  int64_t dictionary_page_offset = 0;
  bool has_index_page_offset = false;
  bool has_dictionary_page_offset = false;
  DirectStatistics statistics;
  bool has_statistics = false;
  std::vector<format::PageEncodingStats> encoding_stats;
  uint64_t encoding_stats_mask = 0;
  bool has_encoding_stats_mask = false;
  int64_t bloom_filter_offset = 0;
  int32_t bloom_filter_length = 0;
  bool has_bloom_filter_offset = false;
  bool has_bloom_filter_length = false;
  DirectSizeStatistics size_statistics;
  bool has_size_statistics = false;
  format::GeospatialStatistics geospatial_statistics;
  bool has_geospatial_statistics = false;
  mutable std::unique_ptr<format::ColumnMetaData> materialized;
};

struct DirectColumnChunk {
  std::string_view file_path;
  bool has_file_path = false;
  int64_t file_offset = 0;
  bool has_file_offset = false;
  DirectColumnMetadata metadata;
  bool has_metadata = false;
  int64_t offset_index_offset = 0;
  int32_t offset_index_length = 0;
  int64_t column_index_offset = 0;
  int32_t column_index_length = 0;
  bool has_offset_index_offset = false;
  bool has_offset_index_length = false;
  bool has_column_index_offset = false;
  bool has_column_index_length = false;
  format::ColumnCryptoMetaData crypto_metadata;
  bool has_crypto_metadata = false;
  std::string_view encrypted_column_metadata;
  bool has_encrypted_column_metadata = false;
};

struct DirectRowGroup {
  std::vector<DirectColumnChunk> columns;
  int64_t total_byte_size = 0;
  int64_t num_rows = 0;
  std::vector<format::SortingColumn> sorting_columns;
  int64_t file_offset = 0;
  int64_t total_compressed_size = 0;
  int16_t ordinal = 0;
  bool has_sorting_columns = false;
  bool has_file_offset = false;
  bool has_total_compressed_size = false;
  bool has_ordinal = false;
};

struct DirectFileMetaData {
  int32_t version = 0;
  std::vector<DirectSchemaElement> schema;
  int64_t num_rows = 0;
  std::vector<DirectRowGroup> row_groups;
  std::vector<DirectKeyValue> key_value_metadata;
  std::string_view created_by;
  std::vector<format::ColumnOrder> column_orders;
  format::EncryptionAlgorithm encryption_algorithm;
  std::string_view footer_signing_key_metadata;
  bool has_key_value_metadata = false;
  bool has_created_by = false;
  bool has_column_orders = false;
  bool has_encryption_algorithm = false;
  bool has_footer_signing_key_metadata = false;
  // Keeps borrowed schema/string views valid for the lifetime of direct metadata.
  std::shared_ptr<std::string> owned_buffer;
};

struct FastMetadataSummary {
  int32_t version = 0;
  int64_t num_rows = 0;
  int32_t num_schema_elements = 0;
  int32_t num_columns = 0;
  int32_t num_row_groups = 0;
  std::string_view created_by;
  bool has_created_by = false;
};

}  // namespace parquet::internal
