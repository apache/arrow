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

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "arrow/io/caching.h"
#include "arrow/type.h"
#include "arrow/util/compression.h"
#include "parquet/encryption.h"
#include "parquet/exception.h"
#include "parquet/parquet_version.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

/// Determines use of Parquet Format version >= 2.0.0 logical types. For
/// example, when writing from Arrow data structures, PARQUET_2_0 will enable
/// use of INT_* and UINT_* converted types as well as nanosecond timestamps
/// stored physically as INT64. Since some Parquet implementations do not
/// support the logical types added in the 2.0.0 format version, if you want to
/// maximize compatibility of your files you may want to use PARQUET_1_0.
///
/// Note that the 2.x format version series also introduced new serialized
/// data page metadata and on disk data page layout. To enable this, use
/// ParquetDataPageVersion.
struct ParquetVersion {
  enum type { PARQUET_1_0, PARQUET_2_0 };
};

/// Controls serialization format of data pages.  parquet-format v2.0.0
/// introduced a new data page metadata type DataPageV2 and serialized page
/// structure (for example, encoded levels are no longer compressed). Prior to
/// the completion of PARQUET-457 in 2020, this library did not implement
/// DataPageV2 correctly, so if you use the V2 data page format, you may have
/// forward compatibility issues (older versions of the library will be unable
/// to read the files). Note that some Parquet implementations do not implement
/// DataPageV2 at all.
enum class ParquetDataPageVersion { V1, V2 };

/// Align the default buffer size to a small multiple of a page size.
constexpr int64_t kDefaultBufferSize = 4096 * 4;

class PARQUET_EXPORT ReaderProperties {
 public:
  explicit ReaderProperties(MemoryPool* pool = ::arrow::default_memory_pool())
      : pool_(pool) {}

  MemoryPool* memory_pool() const { return pool_; }

  std::shared_ptr<ArrowInputStream> GetStream(std::shared_ptr<ArrowInputFile> source,
                                              int64_t start, int64_t num_bytes);

  /// Buffered stream reading allows the user to control the memory usage of
  /// parquet readers. This ensure that all `RandomAccessFile::ReadAt` calls are
  /// wrapped in a buffered reader that uses a fix sized buffer (of size
  /// `buffer_size()`) instead of the full size of the ReadAt.
  ///
  /// The primary reason for this control knobs is for resource control and not
  /// performance.
  bool is_buffered_stream_enabled() const { return buffered_stream_enabled_; }
  void enable_buffered_stream() { buffered_stream_enabled_ = true; }
  void disable_buffered_stream() { buffered_stream_enabled_ = false; }

  int64_t buffer_size() const { return buffer_size_; }
  void set_buffer_size(int64_t size) { buffer_size_ = size; }

  void file_decryption_properties(std::shared_ptr<FileDecryptionProperties> decryption) {
    file_decryption_properties_ = std::move(decryption);
  }

  const std::shared_ptr<FileDecryptionProperties>& file_decryption_properties() const {
    return file_decryption_properties_;
  }

 private:
  MemoryPool* pool_;
  int64_t buffer_size_ = kDefaultBufferSize;
  bool buffered_stream_enabled_ = false;
  std::shared_ptr<FileDecryptionProperties> file_decryption_properties_;
};

ReaderProperties PARQUET_EXPORT default_reader_properties();

static constexpr int64_t kDefaultDataPageSize = 1024 * 1024;
static constexpr bool DEFAULT_IS_DICTIONARY_ENABLED = true;
static constexpr int64_t DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT = kDefaultDataPageSize;
static constexpr int64_t DEFAULT_WRITE_BATCH_SIZE = 1024;
static constexpr int64_t DEFAULT_MAX_ROW_GROUP_LENGTH = 64 * 1024 * 1024;
static constexpr bool DEFAULT_ARE_STATISTICS_ENABLED = true;
static constexpr int64_t DEFAULT_MAX_STATISTICS_SIZE = 4096;
static constexpr Encoding::type DEFAULT_ENCODING = Encoding::PLAIN;
static const char DEFAULT_CREATED_BY[] = CREATED_BY_VERSION;
static constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;

class PARQUET_EXPORT ColumnProperties {
 public:
  ColumnProperties(Encoding::type encoding = DEFAULT_ENCODING,
                   Compression::type codec = DEFAULT_COMPRESSION_TYPE,
                   bool dictionary_enabled = DEFAULT_IS_DICTIONARY_ENABLED,
                   bool statistics_enabled = DEFAULT_ARE_STATISTICS_ENABLED,
                   size_t max_stats_size = DEFAULT_MAX_STATISTICS_SIZE)
      : encoding_(encoding),
        codec_(codec),
        dictionary_enabled_(dictionary_enabled),
        statistics_enabled_(statistics_enabled),
        max_stats_size_(max_stats_size),
        compression_level_(Codec::UseDefaultCompressionLevel()) {}

  void set_encoding(Encoding::type encoding) { encoding_ = encoding; }

  void set_compression(Compression::type codec) { codec_ = codec; }

  void set_dictionary_enabled(bool dictionary_enabled) {
    dictionary_enabled_ = dictionary_enabled;
  }

  void set_statistics_enabled(bool statistics_enabled) {
    statistics_enabled_ = statistics_enabled;
  }

  void set_max_statistics_size(size_t max_stats_size) {
    max_stats_size_ = max_stats_size;
  }

  void set_compression_level(int compression_level) {
    compression_level_ = compression_level;
  }

  Encoding::type encoding() const { return encoding_; }

  Compression::type compression() const { return codec_; }

  bool dictionary_enabled() const { return dictionary_enabled_; }

  bool statistics_enabled() const { return statistics_enabled_; }

  size_t max_statistics_size() const { return max_stats_size_; }

  int compression_level() const { return compression_level_; }

 private:
  Encoding::type encoding_;
  Compression::type codec_;
  bool dictionary_enabled_;
  bool statistics_enabled_;
  size_t max_stats_size_;
  int compression_level_;
};

class PARQUET_EXPORT WriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : pool_(::arrow::default_memory_pool()),
          dictionary_pagesize_limit_(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT),
          write_batch_size_(DEFAULT_WRITE_BATCH_SIZE),
          max_row_group_length_(DEFAULT_MAX_ROW_GROUP_LENGTH),
          pagesize_(kDefaultDataPageSize),
          version_(ParquetVersion::PARQUET_1_0),
          data_page_version_(ParquetDataPageVersion::V1),
          created_by_(DEFAULT_CREATED_BY) {}
    virtual ~Builder() {}

    Builder* memory_pool(MemoryPool* pool) {
      pool_ = pool;
      return this;
    }

    Builder* enable_dictionary() {
      default_column_properties_.set_dictionary_enabled(true);
      return this;
    }

    Builder* disable_dictionary() {
      default_column_properties_.set_dictionary_enabled(false);
      return this;
    }

    Builder* enable_dictionary(const std::string& path) {
      dictionary_enabled_[path] = true;
      return this;
    }

    Builder* enable_dictionary(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->enable_dictionary(path->ToDotString());
    }

    Builder* disable_dictionary(const std::string& path) {
      dictionary_enabled_[path] = false;
      return this;
    }

    Builder* disable_dictionary(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->disable_dictionary(path->ToDotString());
    }

    Builder* dictionary_pagesize_limit(int64_t dictionary_psize_limit) {
      dictionary_pagesize_limit_ = dictionary_psize_limit;
      return this;
    }

    Builder* write_batch_size(int64_t write_batch_size) {
      write_batch_size_ = write_batch_size;
      return this;
    }

    Builder* max_row_group_length(int64_t max_row_group_length) {
      max_row_group_length_ = max_row_group_length;
      return this;
    }

    Builder* data_pagesize(int64_t pg_size) {
      pagesize_ = pg_size;
      return this;
    }

    Builder* data_page_version(ParquetDataPageVersion data_page_version) {
      data_page_version_ = data_page_version;
      return this;
    }

    Builder* version(ParquetVersion::type version) {
      version_ = version;
      return this;
    }

    Builder* created_by(const std::string& created_by) {
      created_by_ = created_by;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(Encoding::type encoding_type) {
      if (encoding_type == Encoding::PLAIN_DICTIONARY ||
          encoding_type == Encoding::RLE_DICTIONARY) {
        throw ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      default_column_properties_.set_encoding(encoding_type);
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(const std::string& path, Encoding::type encoding_type) {
      if (encoding_type == Encoding::PLAIN_DICTIONARY ||
          encoding_type == Encoding::RLE_DICTIONARY) {
        throw ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      encodings_[path] = encoding_type;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(const std::shared_ptr<schema::ColumnPath>& path,
                      Encoding::type encoding_type) {
      return this->encoding(path->ToDotString(), encoding_type);
    }

    Builder* compression(Compression::type codec) {
      default_column_properties_.set_compression(codec);
      return this;
    }

    Builder* max_statistics_size(size_t max_stats_sz) {
      default_column_properties_.set_max_statistics_size(max_stats_sz);
      return this;
    }

    Builder* compression(const std::string& path, Compression::type codec) {
      codecs_[path] = codec;
      return this;
    }

    Builder* compression(const std::shared_ptr<schema::ColumnPath>& path,
                         Compression::type codec) {
      return this->compression(path->ToDotString(), codec);
    }

    /// \brief Specify the default compression level for the compressor in
    /// every column.  In case a column does not have an explicitly specified
    /// compression level, the default one would be used.
    ///
    /// The provided compression level is compressor specific. The user would
    /// have to familiarize oneself with the available levels for the selected
    /// compressor.  If the compressor does not allow for selecting different
    /// compression levels, calling this function would not have any effect.
    /// Parquet and Arrow do not validate the passed compression level.  If no
    /// level is selected by the user or if the special
    /// std::numeric_limits<int>::min() value is passed, then Arrow selects the
    /// compression level.
    Builder* compression_level(int compression_level) {
      default_column_properties_.set_compression_level(compression_level);
      return this;
    }

    /// \brief Specify a compression level for the compressor for the column
    /// described by path.
    ///
    /// The provided compression level is compressor specific. The user would
    /// have to familiarize oneself with the available levels for the selected
    /// compressor.  If the compressor does not allow for selecting different
    /// compression levels, calling this function would not have any effect.
    /// Parquet and Arrow do not validate the passed compression level.  If no
    /// level is selected by the user or if the special
    /// std::numeric_limits<int>::min() value is passed, then Arrow selects the
    /// compression level.
    Builder* compression_level(const std::string& path, int compression_level) {
      codecs_compression_level_[path] = compression_level;
      return this;
    }

    /// \brief Specify a compression level for the compressor for the column
    /// described by path.
    ///
    /// The provided compression level is compressor specific. The user would
    /// have to familiarize oneself with the available levels for the selected
    /// compressor.  If the compressor does not allow for selecting different
    /// compression levels, calling this function would not have any effect.
    /// Parquet and Arrow do not validate the passed compression level.  If no
    /// level is selected by the user or if the special
    /// std::numeric_limits<int>::min() value is passed, then Arrow selects the
    /// compression level.
    Builder* compression_level(const std::shared_ptr<schema::ColumnPath>& path,
                               int compression_level) {
      return this->compression_level(path->ToDotString(), compression_level);
    }

    Builder* encryption(
        std::shared_ptr<FileEncryptionProperties> file_encryption_properties) {
      file_encryption_properties_ = std::move(file_encryption_properties);
      return this;
    }

    Builder* enable_statistics() {
      default_column_properties_.set_statistics_enabled(true);
      return this;
    }

    Builder* disable_statistics() {
      default_column_properties_.set_statistics_enabled(false);
      return this;
    }

    Builder* enable_statistics(const std::string& path) {
      statistics_enabled_[path] = true;
      return this;
    }

    Builder* enable_statistics(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->enable_statistics(path->ToDotString());
    }

    Builder* disable_statistics(const std::string& path) {
      statistics_enabled_[path] = false;
      return this;
    }

    Builder* disable_statistics(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->disable_statistics(path->ToDotString());
    }

    std::shared_ptr<WriterProperties> build() {
      std::unordered_map<std::string, ColumnProperties> column_properties;
      auto get = [&](const std::string& key) -> ColumnProperties& {
        auto it = column_properties.find(key);
        if (it == column_properties.end())
          return column_properties[key] = default_column_properties_;
        else
          return it->second;
      };

      for (const auto& item : encodings_) get(item.first).set_encoding(item.second);
      for (const auto& item : codecs_) get(item.first).set_compression(item.second);
      for (const auto& item : codecs_compression_level_)
        get(item.first).set_compression_level(item.second);
      for (const auto& item : dictionary_enabled_)
        get(item.first).set_dictionary_enabled(item.second);
      for (const auto& item : statistics_enabled_)
        get(item.first).set_statistics_enabled(item.second);

      return std::shared_ptr<WriterProperties>(new WriterProperties(
          pool_, dictionary_pagesize_limit_, write_batch_size_, max_row_group_length_,
          pagesize_, version_, created_by_, std::move(file_encryption_properties_),
          default_column_properties_, column_properties, data_page_version_));
    }

   private:
    MemoryPool* pool_;
    int64_t dictionary_pagesize_limit_;
    int64_t write_batch_size_;
    int64_t max_row_group_length_;
    int64_t pagesize_;
    ParquetVersion::type version_;
    ParquetDataPageVersion data_page_version_;
    std::string created_by_;

    std::shared_ptr<FileEncryptionProperties> file_encryption_properties_;

    // Settings used for each column unless overridden in any of the maps below
    ColumnProperties default_column_properties_;
    std::unordered_map<std::string, Encoding::type> encodings_;
    std::unordered_map<std::string, Compression::type> codecs_;
    std::unordered_map<std::string, int32_t> codecs_compression_level_;
    std::unordered_map<std::string, bool> dictionary_enabled_;
    std::unordered_map<std::string, bool> statistics_enabled_;
  };

  inline MemoryPool* memory_pool() const { return pool_; }

  inline int64_t dictionary_pagesize_limit() const { return dictionary_pagesize_limit_; }

  inline int64_t write_batch_size() const { return write_batch_size_; }

  inline int64_t max_row_group_length() const { return max_row_group_length_; }

  inline int64_t data_pagesize() const { return pagesize_; }

  inline ParquetDataPageVersion data_page_version() const {
    return parquet_data_page_version_;
  }

  inline ParquetVersion::type version() const { return parquet_version_; }

  inline std::string created_by() const { return parquet_created_by_; }

  inline Encoding::type dictionary_index_encoding() const {
    if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
      return Encoding::PLAIN_DICTIONARY;
    } else {
      return Encoding::RLE_DICTIONARY;
    }
  }

  inline Encoding::type dictionary_page_encoding() const {
    if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
      return Encoding::PLAIN_DICTIONARY;
    } else {
      return Encoding::PLAIN;
    }
  }

  const ColumnProperties& column_properties(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = column_properties_.find(path->ToDotString());
    if (it != column_properties_.end()) return it->second;
    return default_column_properties_;
  }

  Encoding::type encoding(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).encoding();
  }

  Compression::type compression(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).compression();
  }

  int compression_level(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).compression_level();
  }

  bool dictionary_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).dictionary_enabled();
  }

  bool statistics_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).statistics_enabled();
  }

  size_t max_statistics_size(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).max_statistics_size();
  }

  inline FileEncryptionProperties* file_encryption_properties() const {
    return file_encryption_properties_.get();
  }

  std::shared_ptr<ColumnEncryptionProperties> column_encryption_properties(
      const std::string& path) const {
    if (file_encryption_properties_) {
      return file_encryption_properties_->column_encryption_properties(path);
    } else {
      return NULLPTR;
    }
  }

 private:
  explicit WriterProperties(
      MemoryPool* pool, int64_t dictionary_pagesize_limit, int64_t write_batch_size,
      int64_t max_row_group_length, int64_t pagesize, ParquetVersion::type version,
      const std::string& created_by,
      std::shared_ptr<FileEncryptionProperties> file_encryption_properties,
      const ColumnProperties& default_column_properties,
      const std::unordered_map<std::string, ColumnProperties>& column_properties,
      ParquetDataPageVersion data_page_version)
      : pool_(pool),
        dictionary_pagesize_limit_(dictionary_pagesize_limit),
        write_batch_size_(write_batch_size),
        max_row_group_length_(max_row_group_length),
        pagesize_(pagesize),
        parquet_data_page_version_(data_page_version),
        parquet_version_(version),
        parquet_created_by_(created_by),
        file_encryption_properties_(file_encryption_properties),
        default_column_properties_(default_column_properties),
        column_properties_(column_properties) {}

  MemoryPool* pool_;
  int64_t dictionary_pagesize_limit_;
  int64_t write_batch_size_;
  int64_t max_row_group_length_;
  int64_t pagesize_;
  ParquetDataPageVersion parquet_data_page_version_;
  ParquetVersion::type parquet_version_;
  std::string parquet_created_by_;

  std::shared_ptr<FileEncryptionProperties> file_encryption_properties_;

  ColumnProperties default_column_properties_;
  std::unordered_map<std::string, ColumnProperties> column_properties_;
};

PARQUET_EXPORT const std::shared_ptr<WriterProperties>& default_writer_properties();

// ----------------------------------------------------------------------
// Properties specific to Apache Arrow columnar read and write

static constexpr bool kArrowDefaultUseThreads = false;

// Default number of rows to read when using ::arrow::RecordBatchReader
static constexpr int64_t kArrowDefaultBatchSize = 64 * 1024;

/// EXPERIMENTAL: Properties for configuring FileReader behavior.
class PARQUET_EXPORT ArrowReaderProperties {
 public:
  explicit ArrowReaderProperties(bool use_threads = kArrowDefaultUseThreads)
      : use_threads_(use_threads),
        read_dict_indices_(),
        batch_size_(kArrowDefaultBatchSize),
        pre_buffer_(false),
        cache_options_(::arrow::io::CacheOptions::Defaults()) {}

  void set_use_threads(bool use_threads) { use_threads_ = use_threads; }

  bool use_threads() const { return use_threads_; }

  void set_read_dictionary(int column_index, bool read_dict) {
    if (read_dict) {
      read_dict_indices_.insert(column_index);
    } else {
      read_dict_indices_.erase(column_index);
    }
  }
  bool read_dictionary(int column_index) const {
    if (read_dict_indices_.find(column_index) != read_dict_indices_.end()) {
      return true;
    } else {
      return false;
    }
  }

  void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }

  int64_t batch_size() const { return batch_size_; }

  /// Enable read coalescing.
  ///
  /// When enabled, the Arrow reader will pre-buffer necessary regions
  /// of the file in-memory. This is intended to improve performance on
  /// high-latency filesystems (e.g. Amazon S3).
  void set_pre_buffer(bool pre_buffer) { pre_buffer_ = pre_buffer; }

  bool pre_buffer() const { return pre_buffer_; }

  /// Set options for read coalescing. This can be used to tune the
  /// implementation for characteristics of different filesystems.
  void set_cache_options(::arrow::io::CacheOptions options) { cache_options_ = options; }

  ::arrow::io::CacheOptions cache_options() const { return cache_options_; }

  /// Set execution context for read coalescing.
  void set_async_context(::arrow::io::AsyncContext ctx) { async_context_ = ctx; }

  ::arrow::io::AsyncContext async_context() const { return async_context_; }

 private:
  bool use_threads_;
  std::unordered_set<int> read_dict_indices_;
  int64_t batch_size_;
  bool pre_buffer_;
  ::arrow::io::AsyncContext async_context_;
  ::arrow::io::CacheOptions cache_options_;
};

/// EXPERIMENTAL: Constructs the default ArrowReaderProperties
PARQUET_EXPORT
ArrowReaderProperties default_arrow_reader_properties();

class PARQUET_EXPORT ArrowWriterProperties {
 public:
  enum EngineVersion {
    V1,  // Supports only nested lists.
    V2   // Full support for all nesting combinations
  };
  class Builder {
   public:
    Builder()
        : write_timestamps_as_int96_(false),
          coerce_timestamps_enabled_(false),
          coerce_timestamps_unit_(::arrow::TimeUnit::SECOND),
          truncated_timestamps_allowed_(false),
          store_schema_(false),
          // TODO: At some point we should flip this.
          compliant_nested_types_(false),
          engine_version_(V2) {}
    virtual ~Builder() = default;

    Builder* disable_deprecated_int96_timestamps() {
      write_timestamps_as_int96_ = false;
      return this;
    }

    Builder* enable_deprecated_int96_timestamps() {
      write_timestamps_as_int96_ = true;
      return this;
    }

    Builder* coerce_timestamps(::arrow::TimeUnit::type unit) {
      coerce_timestamps_enabled_ = true;
      coerce_timestamps_unit_ = unit;
      return this;
    }

    Builder* allow_truncated_timestamps() {
      truncated_timestamps_allowed_ = true;
      return this;
    }

    Builder* disallow_truncated_timestamps() {
      truncated_timestamps_allowed_ = false;
      return this;
    }

    /// \brief EXPERIMENTAL: Write binary serialized Arrow schema to the file,
    /// to enable certain read options (like "read_dictionary") to be set
    /// automatically
    Builder* store_schema() {
      store_schema_ = true;
      return this;
    }

    Builder* enable_compliant_nested_types() {
      compliant_nested_types_ = true;
      return this;
    }

    Builder* disable_compliant_nested_types() {
      compliant_nested_types_ = false;
      return this;
    }

    Builder* set_engine_version(EngineVersion version) {
      engine_version_ = version;
      return this;
    }

    std::shared_ptr<ArrowWriterProperties> build() {
      return std::shared_ptr<ArrowWriterProperties>(new ArrowWriterProperties(
          write_timestamps_as_int96_, coerce_timestamps_enabled_, coerce_timestamps_unit_,
          truncated_timestamps_allowed_, store_schema_, compliant_nested_types_,
          engine_version_));
    }

   private:
    bool write_timestamps_as_int96_;

    bool coerce_timestamps_enabled_;
    ::arrow::TimeUnit::type coerce_timestamps_unit_;
    bool truncated_timestamps_allowed_;

    bool store_schema_;
    bool compliant_nested_types_;
    EngineVersion engine_version_;
  };

  bool support_deprecated_int96_timestamps() const { return write_timestamps_as_int96_; }

  bool coerce_timestamps_enabled() const { return coerce_timestamps_enabled_; }
  ::arrow::TimeUnit::type coerce_timestamps_unit() const {
    return coerce_timestamps_unit_;
  }

  bool truncated_timestamps_allowed() const { return truncated_timestamps_allowed_; }

  bool store_schema() const { return store_schema_; }

  /// \brief Enable nested type naming according to the parquet specification.
  ///
  /// Older versions of arrow wrote out field names for nested lists based on the name
  /// of the field.  According to the parquet specification they should always be
  /// "element".
  bool compliant_nested_types() const { return compliant_nested_types_; }

  /// \brief The underlying engine version to use when writing Arrow data.
  ///
  /// V2 is currently the latest V1 is considered deprecated but left in
  /// place in case there are bugs detected in V2.
  EngineVersion engine_version() const { return engine_version_; }

 private:
  explicit ArrowWriterProperties(bool write_nanos_as_int96,
                                 bool coerce_timestamps_enabled,
                                 ::arrow::TimeUnit::type coerce_timestamps_unit,
                                 bool truncated_timestamps_allowed, bool store_schema,
                                 bool compliant_nested_types,
                                 EngineVersion engine_version)
      : write_timestamps_as_int96_(write_nanos_as_int96),
        coerce_timestamps_enabled_(coerce_timestamps_enabled),
        coerce_timestamps_unit_(coerce_timestamps_unit),
        truncated_timestamps_allowed_(truncated_timestamps_allowed),
        store_schema_(store_schema),
        compliant_nested_types_(compliant_nested_types),
        engine_version_(engine_version) {}

  const bool write_timestamps_as_int96_;
  const bool coerce_timestamps_enabled_;
  const ::arrow::TimeUnit::type coerce_timestamps_unit_;
  const bool truncated_timestamps_allowed_;
  const bool store_schema_;
  const bool compliant_nested_types_;
  const EngineVersion engine_version_;
};

/// \brief State object used for writing Arrow data directly to a Parquet
/// column chunk. API possibly not stable
struct ArrowWriteContext {
  ArrowWriteContext(MemoryPool* memory_pool, ArrowWriterProperties* properties)
      : memory_pool(memory_pool),
        properties(properties),
        data_buffer(AllocateBuffer(memory_pool)),
        def_levels_buffer(AllocateBuffer(memory_pool)) {}

  template <typename T>
  ::arrow::Status GetScratchData(const int64_t num_values, T** out) {
    ARROW_RETURN_NOT_OK(this->data_buffer->Resize(num_values * sizeof(T), false));
    *out = reinterpret_cast<T*>(this->data_buffer->mutable_data());
    return ::arrow::Status::OK();
  }

  MemoryPool* memory_pool;
  const ArrowWriterProperties* properties;

  // Buffer used for storing the data of an array converted to the physical type
  // as expected by parquet-cpp.
  std::shared_ptr<ResizableBuffer> data_buffer;

  // We use the shared ownership of this buffer
  std::shared_ptr<ResizableBuffer> def_levels_buffer;
};

PARQUET_EXPORT
std::shared_ptr<ArrowWriterProperties> default_arrow_writer_properties();

}  // namespace parquet
