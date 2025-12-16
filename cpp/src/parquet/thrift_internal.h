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

#include "parquet/windows_compatibility.h"

#include <cstdint>
#include <limits>

#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "arrow/util/key_value_metadata.h"  // IWYU pragma: keep
#include "arrow/util/logging.h"

#include "parquet/encryption/internal_file_decryptor.h"
#include "parquet/encryption/internal_file_encryptor.h"
#include "parquet/exception.h"
#include "parquet/geospatial/statistics.h"
#include "parquet/metadata.h"
#include "parquet/page_index.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/size_statistics.h"
#include "parquet/statistics.h"
#include "parquet/types.h"

#include "generated/parquet_types.h"  // IWYU pragma: export

namespace parquet {

// ----------------------------------------------------------------------
// Convert Thrift enums to Parquet enums

// Unsafe enum converters (input is not checked for validity)

constexpr Type::type FromThriftUnsafe(format::Type::type type) {
  return static_cast<Type::type>(type);
}

constexpr ConvertedType::type FromThriftUnsafe(format::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<ConvertedType::type>(static_cast<int>(type) + 1);
}

constexpr Repetition::type FromThriftUnsafe(format::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

static inline Encoding::type FromThriftUnsafe(format::Encoding::type type) {
  return static_cast<Encoding::type>(type);
}

constexpr PageType::type FromThriftUnsafe(format::PageType::type type) {
  return static_cast<PageType::type>(type);
}

constexpr Compression::type FromThriftUnsafe(format::CompressionCodec::type type) {
  switch (type) {
    case format::CompressionCodec::UNCOMPRESSED:
      return Compression::UNCOMPRESSED;
    case format::CompressionCodec::SNAPPY:
      return Compression::SNAPPY;
    case format::CompressionCodec::GZIP:
      return Compression::GZIP;
    case format::CompressionCodec::LZO:
      return Compression::LZO;
    case format::CompressionCodec::BROTLI:
      return Compression::BROTLI;
    case format::CompressionCodec::LZ4:
      return Compression::LZ4_HADOOP;
    case format::CompressionCodec::LZ4_RAW:
      return Compression::LZ4;
    case format::CompressionCodec::ZSTD:
      return Compression::ZSTD;
    default:
      ARROW_DCHECK(false) << "Cannot reach here";
      return Compression::UNCOMPRESSED;
  }
}

constexpr BoundaryOrder::type FromThriftUnsafe(format::BoundaryOrder::type type) {
  return static_cast<BoundaryOrder::type>(type);
}

constexpr GeometryLogicalType::EdgeInterpolationAlgorithm FromThriftUnsafe(
    format::EdgeInterpolationAlgorithm::type type) {
  switch (type) {
    case format::EdgeInterpolationAlgorithm::SPHERICAL:
      return GeometryLogicalType::EdgeInterpolationAlgorithm::SPHERICAL;
    case format::EdgeInterpolationAlgorithm::VINCENTY:
      return GeometryLogicalType::EdgeInterpolationAlgorithm::VINCENTY;
    case format::EdgeInterpolationAlgorithm::THOMAS:
      return GeometryLogicalType::EdgeInterpolationAlgorithm::THOMAS;
    case format::EdgeInterpolationAlgorithm::ANDOYER:
      return GeometryLogicalType::EdgeInterpolationAlgorithm::ANDOYER;
    case format::EdgeInterpolationAlgorithm::KARNEY:
      return GeometryLogicalType::EdgeInterpolationAlgorithm::KARNEY;
    default:
      ARROW_DCHECK(false) << "Cannot reach here";
      return GeometryLogicalType::EdgeInterpolationAlgorithm::UNKNOWN;
  }
}

namespace internal {

template <typename T>
struct ThriftEnumTypeTraits {};

template <>
struct ThriftEnumTypeTraits<::parquet::format::Type::type> {
  using ParquetEnum = Type;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::ConvertedType::type> {
  using ParquetEnum = ConvertedType;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::FieldRepetitionType::type> {
  using ParquetEnum = Repetition;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::Encoding::type> {
  using ParquetEnum = Encoding;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::PageType::type> {
  using ParquetEnum = PageType;
};

template <>
struct ThriftEnumTypeTraits<::parquet::format::BoundaryOrder::type> {
  using ParquetEnum = BoundaryOrder;
};

// If the parquet file is corrupted it is possible the enum value decoded
// will not be in the range of defined values, which is undefined behaviour.
// This facility prevents this by loading the value as the underlying type
// and checking to make sure it is in range.

template <typename EnumType,
          typename EnumTypeRaw = typename std::underlying_type<EnumType>::type>
inline static EnumTypeRaw LoadEnumRaw(const EnumType* in) {
  EnumTypeRaw raw_value;
  // Use memcpy(), as a regular cast would be undefined behaviour on invalid values
  memcpy(&raw_value, in, sizeof(EnumType));
  return raw_value;
}

template <typename ApiType>
struct SafeLoader {
  using ApiTypeEnum = typename ApiType::type;
  using ApiTypeRawEnum = typename std::underlying_type<ApiTypeEnum>::type;

  template <typename ThriftType>
  inline static ApiTypeRawEnum LoadRaw(const ThriftType* in) {
    static_assert(sizeof(ApiTypeEnum) == sizeof(ThriftType),
                  "parquet type should always be the same size as thrift type");
    return static_cast<ApiTypeRawEnum>(LoadEnumRaw(in));
  }

  template <typename ThriftType>
  inline static ApiTypeEnum Load(const ThriftType* in) {
    const auto raw_value = LoadRaw(in);
    if constexpr (std::is_unsigned_v<ApiTypeRawEnum>) {
      if (ARROW_PREDICT_FALSE(raw_value >=
                              static_cast<ApiTypeRawEnum>(ApiType::UNDEFINED))) {
        return ApiType::UNDEFINED;
      }
    } else {
      if (ARROW_PREDICT_FALSE(raw_value >=
                                  static_cast<ApiTypeRawEnum>(ApiType::UNDEFINED) ||
                              raw_value < 0)) {
        return ApiType::UNDEFINED;
      }
    }
    return FromThriftUnsafe(static_cast<ThriftType>(raw_value));
  }
};

}  // namespace internal

// Safe enum loader: will check for invalid enum value before converting

template <typename ThriftType,
          typename ParquetEnum =
              typename internal::ThriftEnumTypeTraits<ThriftType>::ParquetEnum>
inline typename ParquetEnum::type LoadEnumSafe(const ThriftType* in) {
  return internal::SafeLoader<ParquetEnum>::Load(in);
}

inline typename Compression::type LoadEnumSafe(const format::CompressionCodec::type* in) {
  const auto raw_value = internal::LoadEnumRaw(in);
  // Check bounds manually, as Compression::type doesn't have the same values
  // as format::CompressionCodec.
  const auto min_value =
      static_cast<decltype(raw_value)>(format::CompressionCodec::UNCOMPRESSED);
  const auto max_value =
      static_cast<decltype(raw_value)>(format::CompressionCodec::LZ4_RAW);
  if (raw_value < min_value || raw_value > max_value) {
    return Compression::UNCOMPRESSED;
  }
  return FromThriftUnsafe(*in);
}

inline typename LogicalType::EdgeInterpolationAlgorithm LoadEnumSafe(
    const format::EdgeInterpolationAlgorithm::type* in) {
  const auto raw_value = internal::LoadEnumRaw(in);
  if (ARROW_PREDICT_FALSE(raw_value < format::EdgeInterpolationAlgorithm::SPHERICAL ||
                          raw_value > format::EdgeInterpolationAlgorithm::KARNEY)) {
    return LogicalType::EdgeInterpolationAlgorithm::UNKNOWN;
  }
  return FromThriftUnsafe(*in);
}

// Safe non-enum converters

static inline AadMetadata FromThrift(format::AesGcmV1 aesGcmV1) {
  return AadMetadata{aesGcmV1.aad_prefix, aesGcmV1.aad_file_unique,
                     aesGcmV1.supply_aad_prefix};
}

static inline AadMetadata FromThrift(format::AesGcmCtrV1 aesGcmCtrV1) {
  return AadMetadata{aesGcmCtrV1.aad_prefix, aesGcmCtrV1.aad_file_unique,
                     aesGcmCtrV1.supply_aad_prefix};
}

static inline EncodedStatistics FromThrift(const format::Statistics& stats) {
  EncodedStatistics out;

  // Use the new V2 min-max statistics over the former one if it is filled
  if (stats.__isset.max_value || stats.__isset.min_value) {
    // TODO: check if the column_order is TYPE_DEFINED_ORDER.
    if (stats.__isset.max_value) {
      out.set_max(stats.max_value);
      if (stats.__isset.is_max_value_exact) {
        out.is_max_value_exact = stats.is_max_value_exact;
      }
    }
    if (stats.__isset.min_value) {
      out.set_min(stats.min_value);
      if (stats.__isset.is_min_value_exact) {
        out.is_min_value_exact = stats.is_min_value_exact;
      }
    }
  } else if (stats.__isset.max || stats.__isset.min) {
    // TODO: check created_by to see if it is corrupted for some types.
    // TODO: check if the sort_order is SIGNED.
    if (stats.__isset.max) {
      out.set_max(stats.max);
    }
    if (stats.__isset.min) {
      out.set_min(stats.min);
    }
  }
  if (stats.__isset.null_count) {
    out.set_null_count(stats.null_count);
  }
  if (stats.__isset.distinct_count) {
    out.set_distinct_count(stats.distinct_count);
  }

  return out;
}

static inline geospatial::EncodedGeoStatistics FromThrift(
    const format::GeospatialStatistics& geo_stats) {
  geospatial::EncodedGeoStatistics out;

  out.geospatial_types = geo_stats.geospatial_types;

  if (geo_stats.__isset.bbox) {
    out.xmin = geo_stats.bbox.xmin;
    out.xmax = geo_stats.bbox.xmax;
    out.ymin = geo_stats.bbox.ymin;
    out.ymax = geo_stats.bbox.ymax;
    out.xy_bounds_present = true;

    if (geo_stats.bbox.__isset.zmin && geo_stats.bbox.__isset.zmax) {
      out.zmin = geo_stats.bbox.zmin;
      out.zmax = geo_stats.bbox.zmax;
      out.z_bounds_present = true;
    }

    if (geo_stats.bbox.__isset.mmin && geo_stats.bbox.__isset.mmax) {
      out.mmin = geo_stats.bbox.mmin;
      out.mmax = geo_stats.bbox.mmax;
      out.m_bounds_present = true;
    }
  }

  return out;
}

static inline format::EdgeInterpolationAlgorithm::type ToThrift(
    LogicalType::EdgeInterpolationAlgorithm algorithm) {
  switch (algorithm) {
    case LogicalType::EdgeInterpolationAlgorithm::SPHERICAL:
      return format::EdgeInterpolationAlgorithm::SPHERICAL;
    case LogicalType::EdgeInterpolationAlgorithm::VINCENTY:
      return format::EdgeInterpolationAlgorithm::VINCENTY;
    case LogicalType::EdgeInterpolationAlgorithm::THOMAS:
      return format::EdgeInterpolationAlgorithm::THOMAS;
    case LogicalType::EdgeInterpolationAlgorithm::ANDOYER:
      return format::EdgeInterpolationAlgorithm::ANDOYER;
    case LogicalType::EdgeInterpolationAlgorithm::KARNEY:
      return format::EdgeInterpolationAlgorithm::KARNEY;
    default:
      throw ParquetException("Unknown value for geometry algorithm: ",
                             static_cast<int>(algorithm));
  }
}

static inline EncryptionAlgorithm FromThrift(format::EncryptionAlgorithm encryption) {
  EncryptionAlgorithm encryption_algorithm;

  if (encryption.__isset.AES_GCM_V1) {
    encryption_algorithm.algorithm = ParquetCipher::AES_GCM_V1;
    encryption_algorithm.aad = FromThrift(encryption.AES_GCM_V1);
  } else if (encryption.__isset.AES_GCM_CTR_V1) {
    encryption_algorithm.algorithm = ParquetCipher::AES_GCM_CTR_V1;
    encryption_algorithm.aad = FromThrift(encryption.AES_GCM_CTR_V1);
  } else {
    throw ParquetException("Unsupported algorithm");
  }
  return encryption_algorithm;
}

static inline SortingColumn FromThrift(format::SortingColumn thrift_sorting_column) {
  SortingColumn sorting_column;
  sorting_column.column_idx = thrift_sorting_column.column_idx;
  sorting_column.nulls_first = thrift_sorting_column.nulls_first;
  sorting_column.descending = thrift_sorting_column.descending;
  return sorting_column;
}

static inline SizeStatistics FromThrift(const format::SizeStatistics& size_stats) {
  return SizeStatistics{
      size_stats.definition_level_histogram, size_stats.repetition_level_histogram,
      size_stats.__isset.unencoded_byte_array_data_bytes
          ? std::make_optional(size_stats.unencoded_byte_array_data_bytes)
          : std::nullopt};
}

// ----------------------------------------------------------------------
// Convert Thrift enums from Parquet enums

constexpr format::Type::type ToThrift(Type::type type) {
  return static_cast<format::Type::type>(type);
}

constexpr format::ConvertedType::type ToThrift(ConvertedType::type type) {
  // item 0 is NONE
  ARROW_DCHECK_NE(type, ConvertedType::NONE);
  // it is forbidden to emit "NA" (PARQUET-1990)
  ARROW_DCHECK_NE(type, ConvertedType::NA);
  ARROW_DCHECK_NE(type, ConvertedType::UNDEFINED);
  return static_cast<format::ConvertedType::type>(static_cast<int>(type) - 1);
}

constexpr format::FieldRepetitionType::type ToThrift(Repetition::type type) {
  return static_cast<format::FieldRepetitionType::type>(type);
}

constexpr format::Encoding::type ToThrift(Encoding::type type) {
  return static_cast<format::Encoding::type>(type);
}

constexpr format::CompressionCodec::type ToThrift(Compression::type type) {
  switch (type) {
    case Compression::UNCOMPRESSED:
      return format::CompressionCodec::UNCOMPRESSED;
    case Compression::SNAPPY:
      return format::CompressionCodec::SNAPPY;
    case Compression::GZIP:
      return format::CompressionCodec::GZIP;
    case Compression::LZO:
      return format::CompressionCodec::LZO;
    case Compression::BROTLI:
      return format::CompressionCodec::BROTLI;
    case Compression::LZ4:
      return format::CompressionCodec::LZ4_RAW;
    case Compression::LZ4_HADOOP:
      // Deprecated "LZ4" Parquet compression has Hadoop-specific framing
      return format::CompressionCodec::LZ4;
    case Compression::ZSTD:
      return format::CompressionCodec::ZSTD;
    default:
      ARROW_DCHECK(false) << "Cannot reach here";
      return format::CompressionCodec::UNCOMPRESSED;
  }
}

constexpr format::BoundaryOrder::type ToThrift(BoundaryOrder::type type) {
  switch (type) {
    case BoundaryOrder::Unordered:
    case BoundaryOrder::Ascending:
    case BoundaryOrder::Descending:
      return static_cast<format::BoundaryOrder::type>(type);
    default:
      ARROW_DCHECK(false) << "Cannot reach here";
      return format::BoundaryOrder::UNORDERED;
  }
}

constexpr format::PageType::type ToThrift(PageType::type type) {
  switch (type) {
    case PageType::DATA_PAGE:
    case PageType::INDEX_PAGE:
    case PageType::DICTIONARY_PAGE:
    case PageType::DATA_PAGE_V2:
      return static_cast<format::PageType::type>(type);
    default:
      ARROW_DCHECK(false) << "Cannot reach here";
      return format::PageType::DATA_PAGE;
  }
}

static inline format::SortingColumn ToThrift(SortingColumn sorting_column) {
  format::SortingColumn thrift_sorting_column;
  thrift_sorting_column.column_idx = sorting_column.column_idx;
  thrift_sorting_column.descending = sorting_column.descending;
  thrift_sorting_column.nulls_first = sorting_column.nulls_first;
  return thrift_sorting_column;
}

static inline format::GeospatialStatistics ToThrift(
    const geospatial::EncodedGeoStatistics& encoded_geo_stats) {
  format::GeospatialStatistics geospatial_statistics;

  geospatial_statistics.__set_geospatial_types(encoded_geo_stats.geospatial_types);

  if (encoded_geo_stats.xy_bounds_present) {
    format::BoundingBox bbox;
    bbox.__set_xmin(encoded_geo_stats.xmin);
    bbox.__set_xmax(encoded_geo_stats.xmax);
    bbox.__set_ymin(encoded_geo_stats.ymin);
    bbox.__set_ymax(encoded_geo_stats.ymax);

    if (encoded_geo_stats.z_bounds_present) {
      bbox.__set_zmin(encoded_geo_stats.zmin);
      bbox.__set_zmax(encoded_geo_stats.zmax);
    }

    if (encoded_geo_stats.m_bounds_present) {
      bbox.__set_mmin(encoded_geo_stats.mmin);
      bbox.__set_mmax(encoded_geo_stats.mmax);
    }

    geospatial_statistics.__set_bbox(std::move(bbox));
  }

  return geospatial_statistics;
}

static inline format::Statistics ToThrift(const EncodedStatistics& stats) {
  format::Statistics statistics;
  if (stats.has_min) {
    statistics.__set_min_value(stats.min());
    if (stats.is_min_value_exact.has_value()) {
      statistics.__set_is_min_value_exact(stats.is_min_value_exact.value());
    }
    // If the order is SIGNED, then the old min value must be set too.
    // This for backward compatibility
    if (stats.is_signed()) {
      statistics.__set_min(stats.min());
    }
  }
  if (stats.has_max) {
    statistics.__set_max_value(stats.max());
    if (stats.is_max_value_exact.has_value()) {
      statistics.__set_is_max_value_exact(stats.is_max_value_exact.value());
    }
    // If the order is SIGNED, then the old max value must be set too.
    // This for backward compatibility
    if (stats.is_signed()) {
      statistics.__set_max(stats.max());
    }
  }
  if (stats.has_null_count) {
    statistics.__set_null_count(stats.null_count);
  }
  if (stats.has_distinct_count) {
    statistics.__set_distinct_count(stats.distinct_count);
  }

  return statistics;
}

static inline format::AesGcmV1 ToAesGcmV1Thrift(AadMetadata aad) {
  format::AesGcmV1 aesGcmV1;
  // aad_file_unique is always set
  aesGcmV1.__set_aad_file_unique(aad.aad_file_unique);
  aesGcmV1.__set_supply_aad_prefix(aad.supply_aad_prefix);
  if (!aad.aad_prefix.empty()) {
    aesGcmV1.__set_aad_prefix(aad.aad_prefix);
  }
  return aesGcmV1;
}

static inline format::AesGcmCtrV1 ToAesGcmCtrV1Thrift(AadMetadata aad) {
  format::AesGcmCtrV1 aesGcmCtrV1;
  // aad_file_unique is always set
  aesGcmCtrV1.__set_aad_file_unique(aad.aad_file_unique);
  aesGcmCtrV1.__set_supply_aad_prefix(aad.supply_aad_prefix);
  if (!aad.aad_prefix.empty()) {
    aesGcmCtrV1.__set_aad_prefix(aad.aad_prefix);
  }
  return aesGcmCtrV1;
}

static inline format::EncryptionAlgorithm ToThrift(EncryptionAlgorithm encryption) {
  format::EncryptionAlgorithm encryption_algorithm;
  if (encryption.algorithm == ParquetCipher::AES_GCM_V1) {
    encryption_algorithm.__set_AES_GCM_V1(ToAesGcmV1Thrift(encryption.aad));
  } else {
    encryption_algorithm.__set_AES_GCM_CTR_V1(ToAesGcmCtrV1Thrift(encryption.aad));
  }
  return encryption_algorithm;
}

static inline format::SizeStatistics ToThrift(const SizeStatistics& size_stats) {
  format::SizeStatistics size_statistics;
  size_statistics.__set_definition_level_histogram(size_stats.definition_level_histogram);
  size_statistics.__set_repetition_level_histogram(size_stats.repetition_level_histogram);
  if (size_stats.unencoded_byte_array_data_bytes.has_value()) {
    size_statistics.__set_unencoded_byte_array_data_bytes(
        size_stats.unencoded_byte_array_data_bytes.value());
  }
  return size_statistics;
}

// Get KeyValueMetadata from parquet Thrift RowGroup or ColumnChunk metadata.
//
// Returns nullptr if the metadata is not set.
template <typename Metadata>
std::shared_ptr<KeyValueMetadata> FromThriftKeyValueMetadata(const Metadata& source) {
  std::shared_ptr<KeyValueMetadata> metadata = nullptr;
  if (source.__isset.key_value_metadata) {
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(source.key_value_metadata.size());
    values.reserve(source.key_value_metadata.size());
    for (const auto& it : source.key_value_metadata) {
      keys.push_back(it.key);
      values.push_back(it.value);
    }
    metadata = std::make_shared<KeyValueMetadata>(std::move(keys), std::move(values));
  }
  return metadata;
}

template <typename Metadata>
void ToThriftKeyValueMetadata(const KeyValueMetadata& source, Metadata* metadata) {
  std::vector<format::KeyValue> key_value_metadata;
  key_value_metadata.reserve(static_cast<size_t>(source.size()));
  for (int64_t i = 0; i < source.size(); ++i) {
    format::KeyValue kv_pair;
    kv_pair.__set_key(source.key(i));
    kv_pair.__set_value(source.value(i));
    key_value_metadata.emplace_back(std::move(kv_pair));
  }
  metadata->__set_key_value_metadata(std::move(key_value_metadata));
}

static inline format::ColumnMetaData ToThrift(const ColumnChunkMetaData& cc_metadata) {
  format::ColumnMetaData column_meta_data;
  column_meta_data.__set_type(ToThrift(cc_metadata.type()));
  std::vector<format::Encoding::type> thrift_encodings;
  thrift_encodings.reserve(cc_metadata.encodings().size());
  for (const auto& encoding : cc_metadata.encodings()) {
    thrift_encodings.push_back(ToThrift(encoding));
  }
  column_meta_data.__set_encodings(std::move(thrift_encodings));
  column_meta_data.__set_path_in_schema(cc_metadata.path_in_schema()->ToDotVector());
  column_meta_data.__set_codec(ToThrift(cc_metadata.compression()));
  column_meta_data.__set_num_values(cc_metadata.num_values());
  column_meta_data.__set_total_uncompressed_size(cc_metadata.total_uncompressed_size());
  column_meta_data.__set_total_compressed_size(cc_metadata.total_compressed_size());
  if (auto& key_value_metadata = cc_metadata.key_value_metadata();
      key_value_metadata != nullptr) {
    ToThriftKeyValueMetadata(*key_value_metadata, &column_meta_data);
  }
  column_meta_data.__set_data_page_offset(cc_metadata.data_page_offset());
  if (cc_metadata.has_index_page()) {
    column_meta_data.__set_index_page_offset(cc_metadata.index_page_offset());
  }
  if (cc_metadata.has_dictionary_page()) {
    column_meta_data.__set_dictionary_page_offset(cc_metadata.dictionary_page_offset());
  }
  if (cc_metadata.is_stats_set()) {
    column_meta_data.__set_statistics(ToThrift(*cc_metadata.encoded_statistics()));
  }
  std::vector<format::PageEncodingStats> thrift_encoding_stats;
  thrift_encoding_stats.reserve(cc_metadata.encoding_stats().size());
  for (const auto& encoding_stat : cc_metadata.encoding_stats()) {
    format::PageEncodingStats enc_stat;
    enc_stat.__set_page_type(ToThrift(encoding_stat.page_type));
    enc_stat.__set_encoding(ToThrift(encoding_stat.encoding));
    enc_stat.__set_count(encoding_stat.count);
    thrift_encoding_stats.push_back(enc_stat);
  }
  column_meta_data.__set_encoding_stats(std::move(thrift_encoding_stats));
  if (auto size_stats = cc_metadata.size_statistics(); size_stats != nullptr) {
    column_meta_data.__set_size_statistics(ToThrift(*size_stats));
  }
  if (auto geo_stats = cc_metadata.geo_statistics(); geo_stats != nullptr) {
    if (auto encoded_geo_stats = geo_stats->Encode(); encoded_geo_stats.has_value()) {
      column_meta_data.__set_geospatial_statistics(ToThrift(*encoded_geo_stats));
    }
  }
  return column_meta_data;
}

static inline format::PageLocation ToThrift(const PageLocation& page_location) {
  format::PageLocation thrift_page_location;
  thrift_page_location.__set_offset(page_location.offset);
  thrift_page_location.__set_compressed_page_size(page_location.compressed_page_size);
  thrift_page_location.__set_first_row_index(page_location.first_row_index);
  return thrift_page_location;
}

static inline format::ColumnIndex ToThrift(const ColumnIndex& column_index) {
  format::ColumnIndex thrift_column_index;
  thrift_column_index.__set_null_pages(column_index.null_pages());
  thrift_column_index.__set_min_values(column_index.encoded_min_values());
  thrift_column_index.__set_max_values(column_index.encoded_max_values());
  thrift_column_index.__set_boundary_order(ToThrift(column_index.boundary_order()));
  if (column_index.has_null_counts()) {
    thrift_column_index.__set_null_counts(column_index.null_counts());
  }
  if (column_index.has_definition_level_histograms()) {
    thrift_column_index.__set_definition_level_histograms(
        column_index.definition_level_histograms());
  }
  if (column_index.has_repetition_level_histograms()) {
    thrift_column_index.__set_repetition_level_histograms(
        column_index.repetition_level_histograms());
  }
  return thrift_column_index;
}

static inline format::OffsetIndex ToThrift(const OffsetIndex& offset_index) {
  format::OffsetIndex thrift_offset_index;
  std::vector<format::PageLocation> thrift_page_locations;
  thrift_page_locations.reserve(offset_index.page_locations().size());
  for (const auto& page_location : offset_index.page_locations()) {
    thrift_page_locations.push_back(ToThrift(page_location));
  }
  thrift_offset_index.__set_page_locations(std::move(thrift_page_locations));
  if (!offset_index.unencoded_byte_array_data_bytes().empty()) {
    thrift_offset_index.__set_unencoded_byte_array_data_bytes(
        offset_index.unencoded_byte_array_data_bytes());
  }
  return thrift_offset_index;
}

// ----------------------------------------------------------------------
// Thrift struct serialization / deserialization utilities

using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;

class ThriftDeserializer {
 public:
  explicit ThriftDeserializer(const ReaderProperties& properties)
      : ThriftDeserializer(properties.thrift_string_size_limit(),
                           properties.thrift_container_size_limit()) {}

  ThriftDeserializer(int32_t string_size_limit, int32_t container_size_limit)
      : string_size_limit_(string_size_limit),
        container_size_limit_(container_size_limit) {}

  // Deserialize a thrift message from buf/len.  buf/len must at least contain
  // all the bytes needed to store the thrift message.  On return, len will be
  // set to the actual length of the header.
  template <class T>
  void DeserializeMessage(const uint8_t* buf, uint32_t* len, T* deserialized_msg,
                          Decryptor* decryptor = NULLPTR) {
    if (decryptor == NULLPTR) {
      // thrift message is not encrypted
      DeserializeUnencryptedMessage(buf, len, deserialized_msg);
    } else {
      // thrift message is encrypted
      uint32_t clen;
      clen = *len;
      if (clen > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        std::stringstream ss;
        ss << "Cannot decrypt buffer with length " << clen << ", which overflows int32\n";
        throw ParquetException(ss.str());
      }
      // decrypt
      auto decrypted_buffer = AllocateBuffer(
          decryptor->pool(), decryptor->PlaintextLength(static_cast<int32_t>(clen)));
      ::arrow::util::span<const uint8_t> cipher_buf(buf, clen);
      uint32_t decrypted_buffer_len =
          decryptor->Decrypt(cipher_buf, decrypted_buffer->mutable_span_as<uint8_t>());
      if (decrypted_buffer_len <= 0) {
        throw ParquetException("Couldn't decrypt buffer\n");
      }
      *len = decryptor->CiphertextLength(static_cast<int32_t>(decrypted_buffer_len));
      DeserializeUnencryptedMessage(decrypted_buffer->data(), &decrypted_buffer_len,
                                    deserialized_msg);
    }
  }

 private:
  // On Thrift 0.14.0+, we want to use TConfiguration to raise the max message size
  // limit (ARROW-13655).  If we wanted to protect against huge messages, we could
  // do it ourselves since we know the message size up front.
  std::shared_ptr<ThriftBuffer> CreateReadOnlyMemoryBuffer(uint8_t* buf, uint32_t len) {
#if PARQUET_THRIFT_VERSION_MAJOR > 0 || PARQUET_THRIFT_VERSION_MINOR >= 14
    auto conf = std::make_shared<apache::thrift::TConfiguration>();
    conf->setMaxMessageSize(std::numeric_limits<int>::max());
    return std::make_shared<ThriftBuffer>(buf, len, ThriftBuffer::OBSERVE, conf);
#else
    return std::make_shared<ThriftBuffer>(buf, len);
#endif
  }

  template <class T>
  void DeserializeUnencryptedMessage(const uint8_t* buf, uint32_t* len,
                                     T* deserialized_msg) {
    // Deserialize msg bytes into c++ thrift msg using memory transport.
    auto tmem_transport = CreateReadOnlyMemoryBuffer(const_cast<uint8_t*>(buf), *len);
    auto tproto = apache::thrift::protocol::TCompactProtocolT<ThriftBuffer>(
        tmem_transport, string_size_limit_, container_size_limit_);
    try {
      deserialized_msg
          ->template read<apache::thrift::protocol::TCompactProtocolT<ThriftBuffer>>(
              &tproto);
    } catch (std::exception& e) {
      std::stringstream ss;
      ss << "Couldn't deserialize thrift: " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
    uint32_t bytes_left = tmem_transport->available_read();
    *len = *len - bytes_left;
  }

  const int32_t string_size_limit_;
  const int32_t container_size_limit_;
};

/// Utility class to serialize thrift objects to a binary format.  This object
/// should be reused if possible to reuse the underlying memory.
/// Note: thrift will encode NULLs into the serialized buffer so it is not valid
/// to treat it as a string.
class ThriftSerializer {
 public:
  explicit ThriftSerializer(int initial_buffer_size = 1024)
      : mem_buffer_(new ThriftBuffer(initial_buffer_size)) {
    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  }

  /// Serialize obj into a memory buffer.  The result is returned in buffer/len.  The
  /// memory returned is owned by this object and will be invalid when another object
  /// is serialized.
  template <class T>
  void SerializeToBuffer(const T* obj, uint32_t* len, uint8_t** buffer) {
    SerializeObject(obj);
    mem_buffer_->getBuffer(buffer, len);
  }

  template <class T>
  void SerializeToString(const T* obj, std::string* result) {
    SerializeObject(obj);
    *result = mem_buffer_->getBufferAsString();
  }

  template <class T>
  int64_t Serialize(const T* obj, ArrowOutputStream* out,
                    Encryptor* encryptor = NULLPTR) {
    uint8_t* out_buffer;
    uint32_t out_length;
    SerializeToBuffer(obj, &out_length, &out_buffer);

    // obj is not encrypted
    if (encryptor == NULLPTR) {
      PARQUET_THROW_NOT_OK(out->Write(out_buffer, out_length));
      return static_cast<int64_t>(out_length);
    } else {  // obj is encrypted
      return SerializeEncryptedObj(out, out_buffer, out_length, encryptor);
    }
  }

 private:
  template <class T>
  void SerializeObject(const T* obj) {
    try {
      mem_buffer_->resetBuffer();
      obj->write(protocol_.get());
    } catch (std::exception& e) {
      std::stringstream ss;
      ss << "Couldn't serialize thrift: " << e.what() << "\n";
      throw ParquetException(ss.str());
    }
  }

  int64_t SerializeEncryptedObj(ArrowOutputStream* out, const uint8_t* out_buffer,
                                uint32_t out_length, Encryptor* encryptor) {
    auto cipher_buffer =
        AllocateBuffer(encryptor->pool(), encryptor->CiphertextLength(out_length));
    ::arrow::util::span<const uint8_t> out_span(out_buffer, out_length);
    int32_t cipher_buffer_len =
        encryptor->Encrypt(out_span, cipher_buffer->mutable_span_as<uint8_t>());

    PARQUET_THROW_NOT_OK(out->Write(cipher_buffer->data(), cipher_buffer_len));
    return static_cast<int64_t>(cipher_buffer_len);
  }

  std::shared_ptr<ThriftBuffer> mem_buffer_;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
};

}  // namespace parquet
