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

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "parquet/exception.h"
#include "parquet/metadata.h"
#include "parquet/schema-internal.h"
#include "parquet/schema.h"
#include "parquet/thrift.h"
#include "parquet/util/memory.h"

#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>

namespace parquet {

const ApplicationVersion& ApplicationVersion::PARQUET_251_FIXED_VERSION() {
  static ApplicationVersion version("parquet-mr", 1, 8, 0);
  return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_816_FIXED_VERSION() {
  static ApplicationVersion version("parquet-mr", 1, 2, 9);
  return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_CPP_FIXED_STATS_VERSION() {
  static ApplicationVersion version("parquet-cpp", 1, 3, 0);
  return version;
}

const ApplicationVersion& ApplicationVersion::PARQUET_MR_FIXED_STATS_VERSION() {
  static ApplicationVersion version("parquet-mr", 1, 10, 0);
  return version;
}

std::string ParquetVersionToString(ParquetVersion::type ver) {
  switch (ver) {
    case ParquetVersion::PARQUET_1_0:
      return "1.0";
    case ParquetVersion::PARQUET_2_0:
      return "2.0";
  }

  // This should be unreachable
  return "UNKNOWN";
}

template <typename DType>
static std::shared_ptr<RowGroupStatistics> MakeTypedColumnStats(
    const format::ColumnMetaData& metadata, const ColumnDescriptor* descr) {
  // If ColumnOrder is defined, return max_value and min_value
  if (descr->column_order().get_order() == ColumnOrder::TYPE_DEFINED_ORDER) {
    return std::make_shared<TypedRowGroupStatistics<DType>>(
        descr, metadata.statistics.min_value, metadata.statistics.max_value,
        metadata.num_values - metadata.statistics.null_count,
        metadata.statistics.null_count, metadata.statistics.distinct_count,
        metadata.statistics.__isset.max_value || metadata.statistics.__isset.min_value);
  }
  // Default behavior
  return std::make_shared<TypedRowGroupStatistics<DType>>(
      descr, metadata.statistics.min, metadata.statistics.max,
      metadata.num_values - metadata.statistics.null_count,
      metadata.statistics.null_count, metadata.statistics.distinct_count,
      metadata.statistics.__isset.max || metadata.statistics.__isset.min);
}

std::shared_ptr<RowGroupStatistics> MakeColumnStats(
    const format::ColumnMetaData& meta_data, const ColumnDescriptor* descr) {
  switch (static_cast<Type::type>(meta_data.type)) {
    case Type::BOOLEAN:
      return MakeTypedColumnStats<BooleanType>(meta_data, descr);
    case Type::INT32:
      return MakeTypedColumnStats<Int32Type>(meta_data, descr);
    case Type::INT64:
      return MakeTypedColumnStats<Int64Type>(meta_data, descr);
    case Type::INT96:
      return MakeTypedColumnStats<Int96Type>(meta_data, descr);
    case Type::DOUBLE:
      return MakeTypedColumnStats<DoubleType>(meta_data, descr);
    case Type::FLOAT:
      return MakeTypedColumnStats<FloatType>(meta_data, descr);
    case Type::BYTE_ARRAY:
      return MakeTypedColumnStats<ByteArrayType>(meta_data, descr);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return MakeTypedColumnStats<FLBAType>(meta_data, descr);
  }
  throw ParquetException("Can't decode page statistics for selected column type");
}

// MetaData Accessor
// ColumnChunk metadata
class ColumnChunkMetaData::ColumnChunkMetaDataImpl {
 public:
  explicit ColumnChunkMetaDataImpl(const format::ColumnChunk* column,
                                   const ColumnDescriptor* descr,
                                   const ApplicationVersion* writer_version)
      : column_(column), descr_(descr), writer_version_(writer_version) {
    const format::ColumnMetaData& meta_data = column->meta_data;
    for (auto encoding : meta_data.encodings) {
      encodings_.push_back(FromThrift(encoding));
    }
    possible_stats_ = nullptr;
  }

  // column chunk
  inline int64_t file_offset() const { return column_->file_offset; }
  inline const std::string& file_path() const { return column_->file_path; }

  // column metadata
  inline Type::type type() const { return FromThrift(column_->meta_data.type); }

  inline int64_t num_values() const { return column_->meta_data.num_values; }

  std::shared_ptr<schema::ColumnPath> path_in_schema() {
    return std::make_shared<schema::ColumnPath>(column_->meta_data.path_in_schema);
  }

  // Check if statistics are set and are valid
  // 1) Must be set in the metadata
  // 2) Statistics must not be corrupted
  // 3) parquet-mr and parquet-cpp write statistics by SIGNED order comparison.
  //    The statistics are corrupted if the type requires UNSIGNED order comparison.
  //    Eg: UTF8
  inline bool is_stats_set() const {
    DCHECK(writer_version_ != nullptr);
    // If the column statistics don't exist or column sort order is unknown
    // we cannot use the column stats
    if (!column_->meta_data.__isset.statistics ||
        descr_->sort_order() == SortOrder::UNKNOWN) {
      return false;
    }
    if (possible_stats_ == nullptr) {
      possible_stats_ = MakeColumnStats(column_->meta_data, descr_);
    }
    EncodedStatistics encodedStatistics = possible_stats_->Encode();
    return writer_version_->HasCorrectStatistics(type(), encodedStatistics,
                                                 descr_->sort_order());
  }

  inline std::shared_ptr<RowGroupStatistics> statistics() const {
    return is_stats_set() ? possible_stats_ : nullptr;
  }

  inline Compression::type compression() const {
    return FromThrift(column_->meta_data.codec);
  }

  const std::vector<Encoding::type>& encodings() const { return encodings_; }

  inline bool has_dictionary_page() const {
    return column_->meta_data.__isset.dictionary_page_offset;
  }

  inline int64_t dictionary_page_offset() const {
    return column_->meta_data.dictionary_page_offset;
  }

  inline int64_t data_page_offset() const { return column_->meta_data.data_page_offset; }

  inline bool has_index_page() const {
    return column_->meta_data.__isset.index_page_offset;
  }

  inline int64_t index_page_offset() const {
    return column_->meta_data.index_page_offset;
  }

  inline int64_t total_compressed_size() const {
    return column_->meta_data.total_compressed_size;
  }

  inline int64_t total_uncompressed_size() const {
    return column_->meta_data.total_uncompressed_size;
  }

 private:
  mutable std::shared_ptr<RowGroupStatistics> possible_stats_;
  std::vector<Encoding::type> encodings_;
  const format::ColumnChunk* column_;
  const ColumnDescriptor* descr_;
  const ApplicationVersion* writer_version_;
};

std::unique_ptr<ColumnChunkMetaData> ColumnChunkMetaData::Make(
    const void* metadata, const ColumnDescriptor* descr,
    const ApplicationVersion* writer_version) {
  return std::unique_ptr<ColumnChunkMetaData>(
      new ColumnChunkMetaData(metadata, descr, writer_version));
}

ColumnChunkMetaData::ColumnChunkMetaData(const void* metadata,
                                         const ColumnDescriptor* descr,
                                         const ApplicationVersion* writer_version)
    : impl_{std::unique_ptr<ColumnChunkMetaDataImpl>(new ColumnChunkMetaDataImpl(
          reinterpret_cast<const format::ColumnChunk*>(metadata), descr,
          writer_version))} {}
ColumnChunkMetaData::~ColumnChunkMetaData() {}

// column chunk
int64_t ColumnChunkMetaData::file_offset() const { return impl_->file_offset(); }

const std::string& ColumnChunkMetaData::file_path() const { return impl_->file_path(); }

// column metadata
Type::type ColumnChunkMetaData::type() const { return impl_->type(); }

int64_t ColumnChunkMetaData::num_values() const { return impl_->num_values(); }

std::shared_ptr<schema::ColumnPath> ColumnChunkMetaData::path_in_schema() const {
  return impl_->path_in_schema();
}

std::shared_ptr<RowGroupStatistics> ColumnChunkMetaData::statistics() const {
  return impl_->statistics();
}

bool ColumnChunkMetaData::is_stats_set() const { return impl_->is_stats_set(); }

bool ColumnChunkMetaData::has_dictionary_page() const {
  return impl_->has_dictionary_page();
}

int64_t ColumnChunkMetaData::dictionary_page_offset() const {
  return impl_->dictionary_page_offset();
}

int64_t ColumnChunkMetaData::data_page_offset() const {
  return impl_->data_page_offset();
}

bool ColumnChunkMetaData::has_index_page() const { return impl_->has_index_page(); }

int64_t ColumnChunkMetaData::index_page_offset() const {
  return impl_->index_page_offset();
}

Compression::type ColumnChunkMetaData::compression() const {
  return impl_->compression();
}

const std::vector<Encoding::type>& ColumnChunkMetaData::encodings() const {
  return impl_->encodings();
}

int64_t ColumnChunkMetaData::total_uncompressed_size() const {
  return impl_->total_uncompressed_size();
}

int64_t ColumnChunkMetaData::total_compressed_size() const {
  return impl_->total_compressed_size();
}

// row-group metadata
class RowGroupMetaData::RowGroupMetaDataImpl {
 public:
  explicit RowGroupMetaDataImpl(const format::RowGroup* row_group,
                                const SchemaDescriptor* schema,
                                const ApplicationVersion* writer_version)
      : row_group_(row_group), schema_(schema), writer_version_(writer_version) {}

  inline int num_columns() const { return static_cast<int>(row_group_->columns.size()); }

  inline int64_t num_rows() const { return row_group_->num_rows; }

  inline int64_t total_byte_size() const { return row_group_->total_byte_size; }

  inline const SchemaDescriptor* schema() const { return schema_; }

  std::unique_ptr<ColumnChunkMetaData> ColumnChunk(int i) {
    if (!(i < num_columns())) {
      std::stringstream ss;
      ss << "The file only has " << num_columns()
         << " columns, requested metadata for column: " << i;
      throw ParquetException(ss.str());
    }
    return ColumnChunkMetaData::Make(&row_group_->columns[i], schema_->Column(i),
                                     writer_version_);
  }

 private:
  const format::RowGroup* row_group_;
  const SchemaDescriptor* schema_;
  const ApplicationVersion* writer_version_;
};

std::unique_ptr<RowGroupMetaData> RowGroupMetaData::Make(
    const void* metadata, const SchemaDescriptor* schema,
    const ApplicationVersion* writer_version) {
  return std::unique_ptr<RowGroupMetaData>(
      new RowGroupMetaData(metadata, schema, writer_version));
}

RowGroupMetaData::RowGroupMetaData(const void* metadata, const SchemaDescriptor* schema,
                                   const ApplicationVersion* writer_version)
    : impl_{std::unique_ptr<RowGroupMetaDataImpl>(new RowGroupMetaDataImpl(
          reinterpret_cast<const format::RowGroup*>(metadata), schema, writer_version))} {
}
RowGroupMetaData::~RowGroupMetaData() {}

int RowGroupMetaData::num_columns() const { return impl_->num_columns(); }

int64_t RowGroupMetaData::num_rows() const { return impl_->num_rows(); }

int64_t RowGroupMetaData::total_byte_size() const { return impl_->total_byte_size(); }

const SchemaDescriptor* RowGroupMetaData::schema() const { return impl_->schema(); }

std::unique_ptr<ColumnChunkMetaData> RowGroupMetaData::ColumnChunk(int i) const {
  return impl_->ColumnChunk(i);
}

// file metadata
class FileMetaData::FileMetaDataImpl {
 public:
  FileMetaDataImpl() : metadata_len_(0) {}

  explicit FileMetaDataImpl(const void* metadata, uint32_t* metadata_len)
      : metadata_len_(0) {
    metadata_.reset(new format::FileMetaData);
    DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(metadata), metadata_len,
                         metadata_.get());
    metadata_len_ = *metadata_len;

    if (metadata_->__isset.created_by) {
      writer_version_ = ApplicationVersion(metadata_->created_by);
    } else {
      writer_version_ = ApplicationVersion("unknown 0.0.0");
    }

    InitSchema();
    InitColumnOrders();
    InitKeyValueMetadata();
  }

  inline uint32_t size() const { return metadata_len_; }
  inline int num_columns() const { return schema_.num_columns(); }
  inline int64_t num_rows() const { return metadata_->num_rows; }
  inline int num_row_groups() const {
    return static_cast<int>(metadata_->row_groups.size());
  }
  inline int32_t version() const { return metadata_->version; }
  inline const std::string& created_by() const { return metadata_->created_by; }
  inline int num_schema_elements() const {
    return static_cast<int>(metadata_->schema.size());
  }

  const ApplicationVersion& writer_version() const { return writer_version_; }

  void WriteTo(OutputStream* dst) const {
    ThriftSerializer serializer;
    serializer.Serialize(metadata_.get(), dst);
  }

  std::unique_ptr<RowGroupMetaData> RowGroup(int i) {
    if (!(i < num_row_groups())) {
      std::stringstream ss;
      ss << "The file only has " << num_row_groups()
         << " row groups, requested metadata for row group: " << i;
      throw ParquetException(ss.str());
    }
    return RowGroupMetaData::Make(&metadata_->row_groups[i], &schema_, &writer_version_);
  }

  const SchemaDescriptor* schema() const { return &schema_; }

  std::shared_ptr<const KeyValueMetadata> key_value_metadata() const {
    return key_value_metadata_;
  }

 private:
  friend FileMetaDataBuilder;
  uint32_t metadata_len_;
  std::unique_ptr<format::FileMetaData> metadata_;
  void InitSchema() {
    schema::FlatSchemaConverter converter(&metadata_->schema[0],
                                          static_cast<int>(metadata_->schema.size()));
    schema_.Init(converter.Convert());
  }
  void InitColumnOrders() {
    // update ColumnOrder
    std::vector<parquet::ColumnOrder> column_orders;
    if (metadata_->__isset.column_orders) {
      for (auto column_order : metadata_->column_orders) {
        if (column_order.__isset.TYPE_ORDER) {
          column_orders.push_back(ColumnOrder::type_defined_);
        } else {
          column_orders.push_back(ColumnOrder::undefined_);
        }
      }
    } else {
      column_orders.resize(schema_.num_columns(), ColumnOrder::undefined_);
    }

    schema_.updateColumnOrders(column_orders);
  }
  SchemaDescriptor schema_;
  ApplicationVersion writer_version_;

  void InitKeyValueMetadata() {
    std::shared_ptr<KeyValueMetadata> metadata = nullptr;
    if (metadata_->__isset.key_value_metadata) {
      metadata = std::make_shared<KeyValueMetadata>();
      for (const auto& it : metadata_->key_value_metadata) {
        metadata->Append(it.key, it.value);
      }
    }
    key_value_metadata_ = metadata;
  }

  std::shared_ptr<const KeyValueMetadata> key_value_metadata_;
};

std::shared_ptr<FileMetaData> FileMetaData::Make(const void* metadata,
                                                 uint32_t* metadata_len) {
  // This FileMetaData ctor is private, not compatible with std::make_shared
  return std::shared_ptr<FileMetaData>(new FileMetaData(metadata, metadata_len));
}

FileMetaData::FileMetaData(const void* metadata, uint32_t* metadata_len)
    : impl_{std::unique_ptr<FileMetaDataImpl>(
          new FileMetaDataImpl(metadata, metadata_len))} {}

FileMetaData::FileMetaData()
    : impl_{std::unique_ptr<FileMetaDataImpl>(new FileMetaDataImpl())} {}

FileMetaData::~FileMetaData() {}

std::unique_ptr<RowGroupMetaData> FileMetaData::RowGroup(int i) const {
  return impl_->RowGroup(i);
}

uint32_t FileMetaData::size() const { return impl_->size(); }

int FileMetaData::num_columns() const { return impl_->num_columns(); }

int64_t FileMetaData::num_rows() const { return impl_->num_rows(); }

int FileMetaData::num_row_groups() const { return impl_->num_row_groups(); }

ParquetVersion::type FileMetaData::version() const {
  switch (impl_->version()) {
    case 1:
      return ParquetVersion::PARQUET_1_0;
    case 2:
      return ParquetVersion::PARQUET_2_0;
    default:
      // Improperly set version, assuming Parquet 1.0
      break;
  }
  return ParquetVersion::PARQUET_1_0;
}

const ApplicationVersion& FileMetaData::writer_version() const {
  return impl_->writer_version();
}

const std::string& FileMetaData::created_by() const { return impl_->created_by(); }

int FileMetaData::num_schema_elements() const { return impl_->num_schema_elements(); }

const SchemaDescriptor* FileMetaData::schema() const { return impl_->schema(); }

std::shared_ptr<const KeyValueMetadata> FileMetaData::key_value_metadata() const {
  return impl_->key_value_metadata();
}

void FileMetaData::WriteTo(OutputStream* dst) const { return impl_->WriteTo(dst); }

ApplicationVersion::ApplicationVersion(const std::string& application, int major,
                                       int minor, int patch)
    : application_(application), version{major, minor, patch, "", "", ""} {}

ApplicationVersion::ApplicationVersion(const std::string& created_by) {
  boost::regex app_regex{ApplicationVersion::APPLICATION_FORMAT};
  boost::regex ver_regex{ApplicationVersion::VERSION_FORMAT};
  boost::smatch app_matches;
  boost::smatch ver_matches;

  std::string created_by_lower = created_by;
  std::transform(created_by_lower.begin(), created_by_lower.end(),
                 created_by_lower.begin(), ::tolower);

  bool app_success = boost::regex_match(created_by_lower, app_matches, app_regex);
  bool ver_success = false;
  std::string version_str;

  if (app_success && app_matches.size() >= 4) {
    // first match is the entire string. sub-matches start from second.
    application_ = app_matches[1];
    version_str = app_matches[3];
    build_ = app_matches[4];
    ver_success = boost::regex_match(version_str, ver_matches, ver_regex);
  } else {
    application_ = "unknown";
  }

  if (ver_success && ver_matches.size() >= 7) {
    version.major = atoi(ver_matches[1].str().c_str());
    version.minor = atoi(ver_matches[2].str().c_str());
    version.patch = atoi(ver_matches[3].str().c_str());
    version.unknown = ver_matches[4].str();
    version.pre_release = ver_matches[5].str();
    version.build_info = ver_matches[6].str();
  } else {
    version.major = 0;
    version.minor = 0;
    version.patch = 0;
  }
}

bool ApplicationVersion::VersionLt(const ApplicationVersion& other_version) const {
  if (application_ != other_version.application_) return false;

  if (version.major < other_version.version.major) return true;
  if (version.major > other_version.version.major) return false;
  DCHECK_EQ(version.major, other_version.version.major);
  if (version.minor < other_version.version.minor) return true;
  if (version.minor > other_version.version.minor) return false;
  DCHECK_EQ(version.minor, other_version.version.minor);
  return version.patch < other_version.version.patch;
}

bool ApplicationVersion::VersionEq(const ApplicationVersion& other_version) const {
  return application_ == other_version.application_ &&
         version.major == other_version.version.major &&
         version.minor == other_version.version.minor &&
         version.patch == other_version.version.patch;
}

// Reference:
// parquet-mr/parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java
// PARQUET-686 has more disussion on statistics
bool ApplicationVersion::HasCorrectStatistics(Type::type col_type,
                                              EncodedStatistics& statistics,
                                              SortOrder::type sort_order) const {
  // parquet-cpp version 1.3.0 and parquet-mr 1.10.0 onwards stats are computed
  // correctly for all types
  if ((application_ == "parquet-cpp" && VersionLt(PARQUET_CPP_FIXED_STATS_VERSION())) ||
      (application_ == "parquet-mr" && VersionLt(PARQUET_MR_FIXED_STATS_VERSION()))) {
    // Only SIGNED are valid unless max and min are the same
    // (in which case the sort order does not matter)
    bool max_equals_min = statistics.has_min && statistics.has_max
                              ? statistics.min() == statistics.max()
                              : false;
    if (SortOrder::SIGNED != sort_order && !max_equals_min) {
      return false;
    }

    // Statistics of other types are OK
    if (col_type != Type::FIXED_LEN_BYTE_ARRAY && col_type != Type::BYTE_ARRAY) {
      return true;
    }
  }
  // created_by is not populated, which could have been caused by
  // parquet-mr during the same time as PARQUET-251, see PARQUET-297
  if (application_ == "unknown") {
    return true;
  }

  // Unknown sort order has incorrect stats
  if (SortOrder::UNKNOWN == sort_order) {
    return false;
  }

  // PARQUET-251
  if (VersionLt(PARQUET_251_FIXED_VERSION())) {
    return false;
  }

  return true;
}

// MetaData Builders
// row-group metadata
class ColumnChunkMetaDataBuilder::ColumnChunkMetaDataBuilderImpl {
 public:
  explicit ColumnChunkMetaDataBuilderImpl(const std::shared_ptr<WriterProperties>& props,
                                          const ColumnDescriptor* column)
      : owned_column_chunk_(new format::ColumnChunk),
        properties_(props),
        column_(column) {
    Init(owned_column_chunk_.get());
  }

  explicit ColumnChunkMetaDataBuilderImpl(const std::shared_ptr<WriterProperties>& props,
                                          const ColumnDescriptor* column,
                                          format::ColumnChunk* column_chunk)
      : properties_(props), column_(column) {
    Init(column_chunk);
  }

  const void* contents() const { return column_chunk_; }

  // column chunk
  void set_file_path(const std::string& val) { column_chunk_->__set_file_path(val); }

  // column metadata
  void SetStatistics(bool is_signed, const EncodedStatistics& val) {
    format::Statistics stats;
    stats.null_count = val.null_count;
    stats.distinct_count = val.distinct_count;
    stats.max_value = val.max();
    stats.min_value = val.min();
    stats.__isset.min_value = val.has_min;
    stats.__isset.max_value = val.has_max;
    stats.__isset.null_count = val.has_null_count;
    stats.__isset.distinct_count = val.has_distinct_count;
    // If the order is SIGNED, then the old min/max values must be set too.
    // This for backward compatibility
    if (is_signed) {
      stats.max = val.max();
      stats.min = val.min();
      stats.__isset.min = val.has_min;
      stats.__isset.max = val.has_max;
    }

    column_chunk_->meta_data.__set_statistics(stats);
  }

  void Finish(int64_t num_values, int64_t dictionary_page_offset,
              int64_t index_page_offset, int64_t data_page_offset,
              int64_t compressed_size, int64_t uncompressed_size, bool has_dictionary,
              bool dictionary_fallback) {
    if (dictionary_page_offset > 0) {
      column_chunk_->meta_data.__set_dictionary_page_offset(dictionary_page_offset);
      column_chunk_->__set_file_offset(dictionary_page_offset + compressed_size);
    } else {
      column_chunk_->__set_file_offset(data_page_offset + compressed_size);
    }
    column_chunk_->__isset.meta_data = true;
    column_chunk_->meta_data.__set_num_values(num_values);
    if (index_page_offset >= 0) {
      column_chunk_->meta_data.__set_index_page_offset(index_page_offset);
    }
    column_chunk_->meta_data.__set_data_page_offset(data_page_offset);
    column_chunk_->meta_data.__set_total_uncompressed_size(uncompressed_size);
    column_chunk_->meta_data.__set_total_compressed_size(compressed_size);
    std::vector<format::Encoding::type> thrift_encodings;
    if (has_dictionary) {
      thrift_encodings.push_back(ToThrift(properties_->dictionary_index_encoding()));
      if (properties_->version() == ParquetVersion::PARQUET_1_0) {
        thrift_encodings.push_back(ToThrift(Encoding::PLAIN));
      } else {
        thrift_encodings.push_back(ToThrift(properties_->dictionary_page_encoding()));
      }
    } else {  // Dictionary not enabled
      thrift_encodings.push_back(ToThrift(properties_->encoding(column_->path())));
    }
    thrift_encodings.push_back(ToThrift(Encoding::RLE));
    // Only PLAIN encoding is supported for fallback in V1
    // TODO(majetideepak): Use user specified encoding for V2
    if (dictionary_fallback) {
      thrift_encodings.push_back(ToThrift(Encoding::PLAIN));
    }
    column_chunk_->meta_data.__set_encodings(thrift_encodings);
  }

  void WriteTo(OutputStream* sink) {
    ThriftSerializer serializer;
    serializer.Serialize(column_chunk_, sink);
  }

  const ColumnDescriptor* descr() const { return column_; }

 private:
  void Init(format::ColumnChunk* column_chunk) {
    column_chunk_ = column_chunk;
    column_chunk_->meta_data.__set_type(ToThrift(column_->physical_type()));
    column_chunk_->meta_data.__set_path_in_schema(column_->path()->ToDotVector());
    column_chunk_->meta_data.__set_codec(
        ToThrift(properties_->compression(column_->path())));
  }

  format::ColumnChunk* column_chunk_;
  std::unique_ptr<format::ColumnChunk> owned_column_chunk_;
  const std::shared_ptr<WriterProperties> properties_;
  const ColumnDescriptor* column_;
};

std::unique_ptr<ColumnChunkMetaDataBuilder> ColumnChunkMetaDataBuilder::Make(
    const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column,
    void* contents) {
  return std::unique_ptr<ColumnChunkMetaDataBuilder>(
      new ColumnChunkMetaDataBuilder(props, column, contents));
}

std::unique_ptr<ColumnChunkMetaDataBuilder> ColumnChunkMetaDataBuilder::Make(
    const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column) {
  return std::unique_ptr<ColumnChunkMetaDataBuilder>(
      new ColumnChunkMetaDataBuilder(props, column));
}

ColumnChunkMetaDataBuilder::ColumnChunkMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column)
    : impl_{std::unique_ptr<ColumnChunkMetaDataBuilderImpl>(
          new ColumnChunkMetaDataBuilderImpl(props, column))} {}

ColumnChunkMetaDataBuilder::ColumnChunkMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column,
    void* contents)
    : impl_{std::unique_ptr<ColumnChunkMetaDataBuilderImpl>(
          new ColumnChunkMetaDataBuilderImpl(
              props, column, reinterpret_cast<format::ColumnChunk*>(contents)))} {}

ColumnChunkMetaDataBuilder::~ColumnChunkMetaDataBuilder() {}

const void* ColumnChunkMetaDataBuilder::contents() const { return impl_->contents(); }

void ColumnChunkMetaDataBuilder::set_file_path(const std::string& path) {
  impl_->set_file_path(path);
}

void ColumnChunkMetaDataBuilder::Finish(int64_t num_values,
                                        int64_t dictionary_page_offset,
                                        int64_t index_page_offset,
                                        int64_t data_page_offset, int64_t compressed_size,
                                        int64_t uncompressed_size, bool has_dictionary,
                                        bool dictionary_fallback) {
  impl_->Finish(num_values, dictionary_page_offset, index_page_offset, data_page_offset,
                compressed_size, uncompressed_size, has_dictionary, dictionary_fallback);
}

void ColumnChunkMetaDataBuilder::WriteTo(OutputStream* sink) { impl_->WriteTo(sink); }

const ColumnDescriptor* ColumnChunkMetaDataBuilder::descr() const {
  return impl_->descr();
}

void ColumnChunkMetaDataBuilder::SetStatistics(bool is_signed,
                                               const EncodedStatistics& result) {
  impl_->SetStatistics(is_signed, result);
}

class RowGroupMetaDataBuilder::RowGroupMetaDataBuilderImpl {
 public:
  explicit RowGroupMetaDataBuilderImpl(const std::shared_ptr<WriterProperties>& props,
                                       const SchemaDescriptor* schema, void* contents)
      : properties_(props), schema_(schema), current_column_(0) {
    row_group_ = reinterpret_cast<format::RowGroup*>(contents);
    InitializeColumns(schema->num_columns());
  }

  ColumnChunkMetaDataBuilder* NextColumnChunk() {
    if (!(current_column_ < num_columns())) {
      std::stringstream ss;
      ss << "The schema only has " << num_columns()
         << " columns, requested metadata for column: " << current_column_;
      throw ParquetException(ss.str());
    }
    auto column = schema_->Column(current_column_);
    auto column_builder = ColumnChunkMetaDataBuilder::Make(
        properties_, column, &row_group_->columns[current_column_++]);
    auto column_builder_ptr = column_builder.get();
    column_builders_.push_back(std::move(column_builder));
    return column_builder_ptr;
  }

  int current_column() { return current_column_; }

  void Finish(int64_t total_bytes_written) {
    if (!(current_column_ == schema_->num_columns())) {
      std::stringstream ss;
      ss << "Only " << current_column_ - 1 << " out of " << schema_->num_columns()
         << " columns are initialized";
      throw ParquetException(ss.str());
    }
    int64_t total_byte_size = 0;

    for (int i = 0; i < schema_->num_columns(); i++) {
      if (!(row_group_->columns[i].file_offset >= 0)) {
        std::stringstream ss;
        ss << "Column " << i << " is not complete.";
        throw ParquetException(ss.str());
      }
      total_byte_size += row_group_->columns[i].meta_data.total_compressed_size;
    }
    DCHECK(total_bytes_written == total_byte_size)
        << "Total bytes in this RowGroup does not match with compressed sizes of columns";

    row_group_->__set_total_byte_size(total_byte_size);
  }

  void set_num_rows(int64_t num_rows) { row_group_->num_rows = num_rows; }

  int num_columns() { return static_cast<int>(row_group_->columns.size()); }

  int64_t num_rows() { return row_group_->num_rows; }

 private:
  void InitializeColumns(int ncols) { row_group_->columns.resize(ncols); }

  format::RowGroup* row_group_;
  const std::shared_ptr<WriterProperties> properties_;
  const SchemaDescriptor* schema_;
  std::vector<std::unique_ptr<ColumnChunkMetaDataBuilder>> column_builders_;
  int current_column_;
};

std::unique_ptr<RowGroupMetaDataBuilder> RowGroupMetaDataBuilder::Make(
    const std::shared_ptr<WriterProperties>& props, const SchemaDescriptor* schema_,
    void* contents) {
  return std::unique_ptr<RowGroupMetaDataBuilder>(
      new RowGroupMetaDataBuilder(props, schema_, contents));
}

RowGroupMetaDataBuilder::RowGroupMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const SchemaDescriptor* schema_,
    void* contents)
    : impl_{std::unique_ptr<RowGroupMetaDataBuilderImpl>(
          new RowGroupMetaDataBuilderImpl(props, schema_, contents))} {}

RowGroupMetaDataBuilder::~RowGroupMetaDataBuilder() {}

ColumnChunkMetaDataBuilder* RowGroupMetaDataBuilder::NextColumnChunk() {
  return impl_->NextColumnChunk();
}

int RowGroupMetaDataBuilder::current_column() const { return impl_->current_column(); }

int RowGroupMetaDataBuilder::num_columns() { return impl_->num_columns(); }

int64_t RowGroupMetaDataBuilder::num_rows() { return impl_->num_rows(); }

void RowGroupMetaDataBuilder::set_num_rows(int64_t num_rows) {
  impl_->set_num_rows(num_rows);
}

void RowGroupMetaDataBuilder::Finish(int64_t total_bytes_written) {
  impl_->Finish(total_bytes_written);
}

// file metadata
// TODO(PARQUET-595) Support key_value_metadata
class FileMetaDataBuilder::FileMetaDataBuilderImpl {
 public:
  explicit FileMetaDataBuilderImpl(
      const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props,
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata)
      : properties_(props), schema_(schema), key_value_metadata_(key_value_metadata) {
    metadata_.reset(new format::FileMetaData());
  }

  RowGroupMetaDataBuilder* AppendRowGroup() {
    row_groups_.emplace_back();
    current_row_group_builder_ =
        RowGroupMetaDataBuilder::Make(properties_, schema_, &row_groups_.back());
    return current_row_group_builder_.get();
  }

  std::unique_ptr<FileMetaData> Finish() {
    int64_t total_rows = 0;
    for (auto row_group : row_groups_) {
      total_rows += row_group.num_rows;
    }
    metadata_->__set_num_rows(total_rows);
    metadata_->__set_row_groups(row_groups_);

    if (key_value_metadata_) {
      metadata_->key_value_metadata.clear();
      metadata_->key_value_metadata.reserve(key_value_metadata_->size());
      for (int64_t i = 0; i < key_value_metadata_->size(); ++i) {
        format::KeyValue kv_pair;
        kv_pair.__set_key(key_value_metadata_->key(i));
        kv_pair.__set_value(key_value_metadata_->value(i));
        metadata_->key_value_metadata.push_back(kv_pair);
      }
      metadata_->__isset.key_value_metadata = true;
    }

    int32_t file_version = 0;
    switch (properties_->version()) {
      case ParquetVersion::PARQUET_1_0:
        file_version = 1;
        break;
      case ParquetVersion::PARQUET_2_0:
        file_version = 2;
        break;
      default:
        break;
    }
    metadata_->__set_version(file_version);
    metadata_->__set_created_by(properties_->created_by());

    // Users cannot set the `ColumnOrder` since we donot not have user defined sort order
    // in the spec yet.
    // We always default to `TYPE_DEFINED_ORDER`. We can expose it in
    // the API once we have user defined sort orders in the Parquet format.
    // TypeDefinedOrder implies choose SortOrder based on LogicalType/PhysicalType
    format::TypeDefinedOrder type_defined_order;
    format::ColumnOrder column_order;
    column_order.__set_TYPE_ORDER(type_defined_order);
    column_order.__isset.TYPE_ORDER = true;
    metadata_->column_orders.resize(schema_->num_columns(), column_order);
    metadata_->__isset.column_orders = true;

    parquet::schema::SchemaFlattener flattener(
        static_cast<parquet::schema::GroupNode*>(schema_->schema_root().get()),
        &metadata_->schema);
    flattener.Flatten();
    auto file_meta_data = std::unique_ptr<FileMetaData>(new FileMetaData());
    file_meta_data->impl_->metadata_ = std::move(metadata_);
    file_meta_data->impl_->InitSchema();
    return file_meta_data;
  }

 protected:
  std::unique_ptr<format::FileMetaData> metadata_;

 private:
  const std::shared_ptr<WriterProperties> properties_;
  std::vector<format::RowGroup> row_groups_;

  std::unique_ptr<RowGroupMetaDataBuilder> current_row_group_builder_;
  const SchemaDescriptor* schema_;
  std::shared_ptr<const KeyValueMetadata> key_value_metadata_;
};

std::unique_ptr<FileMetaDataBuilder> FileMetaDataBuilder::Make(
    const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata) {
  return std::unique_ptr<FileMetaDataBuilder>(
      new FileMetaDataBuilder(schema, props, key_value_metadata));
}

FileMetaDataBuilder::FileMetaDataBuilder(
    const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props,
    const std::shared_ptr<const KeyValueMetadata>& key_value_metadata)
    : impl_{std::unique_ptr<FileMetaDataBuilderImpl>(
          new FileMetaDataBuilderImpl(schema, props, key_value_metadata))} {}

FileMetaDataBuilder::~FileMetaDataBuilder() {}

RowGroupMetaDataBuilder* FileMetaDataBuilder::AppendRowGroup() {
  return impl_->AppendRowGroup();
}

std::unique_ptr<FileMetaData> FileMetaDataBuilder::Finish() { return impl_->Finish(); }

}  // namespace parquet
