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

#ifndef PARQUET_COLUMN_PROPERTIES_H
#define PARQUET_COLUMN_PROPERTIES_H

#include <memory>
#include <string>
#include <unordered_map>

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace parquet {

struct ParquetVersion {
  enum type { PARQUET_1_0, PARQUET_2_0 };
};

static int64_t DEFAULT_BUFFER_SIZE = 0;
static bool DEFAULT_USE_BUFFERED_STREAM = false;

class PARQUET_EXPORT ReaderProperties {
 public:
  explicit ReaderProperties(MemoryAllocator* allocator = default_allocator())
      : allocator_(allocator) {
    buffered_stream_enabled_ = DEFAULT_USE_BUFFERED_STREAM;
    buffer_size_ = DEFAULT_BUFFER_SIZE;
  }

  MemoryAllocator* allocator() const { return allocator_; }

  std::unique_ptr<InputStream> GetStream(
      RandomAccessSource* source, int64_t start, int64_t num_bytes) {
    std::unique_ptr<InputStream> stream;
    if (buffered_stream_enabled_) {
      stream.reset(
          new BufferedInputStream(allocator_, buffer_size_, source, start, num_bytes));
    } else {
      stream.reset(new InMemoryInputStream(source, start, num_bytes));
    }
    return stream;
  }

  bool is_buffered_stream_enabled() const { return buffered_stream_enabled_; }

  void enable_buffered_stream() { buffered_stream_enabled_ = true; }

  void disable_buffered_stream() { buffered_stream_enabled_ = false; }

  void set_buffer_size(int64_t buf_size) { buffer_size_ = buf_size; }

  int64_t buffer_size() const { return buffer_size_; }

 private:
  MemoryAllocator* allocator_;
  int64_t buffer_size_;
  bool buffered_stream_enabled_;
};

ReaderProperties PARQUET_EXPORT default_reader_properties();

static constexpr int64_t DEFAULT_PAGE_SIZE = 1024 * 1024;
static constexpr bool DEFAULT_IS_DICTIONARY_ENABLED = true;
static constexpr int64_t DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT = DEFAULT_PAGE_SIZE;
static constexpr int64_t DEFAULT_WRITE_BATCH_SIZE = 1024;
static constexpr bool DEFAULT_ARE_STATISTICS_ENABLED = true;
static constexpr Encoding::type DEFAULT_ENCODING = Encoding::PLAIN;
static constexpr ParquetVersion::type DEFAULT_WRITER_VERSION =
    ParquetVersion::PARQUET_1_0;
static std::string DEFAULT_CREATED_BY = "parquet-cpp version 0.1.0";
static constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;

class PARQUET_EXPORT ColumnProperties {
 public:
  ColumnProperties(Encoding::type encoding = DEFAULT_ENCODING,
      Compression::type codec = DEFAULT_COMPRESSION_TYPE,
      bool dictionary_enabled = DEFAULT_IS_DICTIONARY_ENABLED,
      bool statistics_enabled = DEFAULT_ARE_STATISTICS_ENABLED)
      : encoding(encoding),
        codec(codec),
        dictionary_enabled(dictionary_enabled),
        statistics_enabled(statistics_enabled) {}

  Encoding::type encoding;
  Compression::type codec;
  bool dictionary_enabled;
  bool statistics_enabled;
};

class PARQUET_EXPORT WriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : allocator_(default_allocator()),
          dictionary_pagesize_limit_(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT),
          write_batch_size_(DEFAULT_WRITE_BATCH_SIZE),
          pagesize_(DEFAULT_PAGE_SIZE),
          version_(DEFAULT_WRITER_VERSION),
          created_by_(DEFAULT_CREATED_BY) {}
    virtual ~Builder() {}

    Builder* allocator(MemoryAllocator* allocator) {
      allocator_ = allocator;
      return this;
    }

    Builder* enable_dictionary() {
      default_column_properties_.dictionary_enabled = true;
      return this;
    }

    Builder* disable_dictionary() {
      default_column_properties_.dictionary_enabled = false;
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

    Builder* data_pagesize(int64_t pg_size) {
      pagesize_ = pg_size;
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

      default_column_properties_.encoding = encoding_type;
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
    Builder* encoding(
        const std::shared_ptr<schema::ColumnPath>& path, Encoding::type encoding_type) {
      return this->encoding(path->ToDotString(), encoding_type);
    }

    Builder* compression(Compression::type codec) {
      default_column_properties_.codec = codec;
      return this;
    }

    Builder* compression(const std::string& path, Compression::type codec) {
      codecs_[path] = codec;
      return this;
    }

    Builder* compression(
        const std::shared_ptr<schema::ColumnPath>& path, Compression::type codec) {
      return this->compression(path->ToDotString(), codec);
    }

    Builder* enable_statistics() {
      default_column_properties_.statistics_enabled = true;
      return this;
    }

    Builder* disable_statistics() {
      default_column_properties_.statistics_enabled = false;
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

      for (const auto& item : encodings_)
        get(item.first).encoding = item.second;
      for (const auto& item : codecs_)
        get(item.first).codec = item.second;
      for (const auto& item : dictionary_enabled_)
        get(item.first).dictionary_enabled = item.second;
      for (const auto& item : statistics_enabled_)
        get(item.first).statistics_enabled = item.second;

      return std::shared_ptr<WriterProperties>(new WriterProperties(allocator_,
          dictionary_pagesize_limit_, write_batch_size_, pagesize_, version_, created_by_,
          default_column_properties_, column_properties));
    }

   private:
    MemoryAllocator* allocator_;
    int64_t dictionary_pagesize_limit_;
    int64_t write_batch_size_;
    int64_t pagesize_;
    ParquetVersion::type version_;
    std::string created_by_;

    // Settings used for each column unless overridden in any of the maps below
    ColumnProperties default_column_properties_;
    std::unordered_map<std::string, Encoding::type> encodings_;
    std::unordered_map<std::string, Compression::type> codecs_;
    std::unordered_map<std::string, bool> dictionary_enabled_;
    std::unordered_map<std::string, bool> statistics_enabled_;
  };

  inline MemoryAllocator* allocator() const { return allocator_; }

  inline int64_t dictionary_pagesize_limit() const { return dictionary_pagesize_limit_; }

  inline int64_t write_batch_size() const { return write_batch_size_; }

  inline int64_t data_pagesize() const { return pagesize_; }

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
    return column_properties(path).encoding;
  }

  Compression::type compression(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).codec;
  }

  bool dictionary_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).dictionary_enabled;
  }

  bool statistics_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_properties(path).statistics_enabled;
  }

 private:
  explicit WriterProperties(MemoryAllocator* allocator, int64_t dictionary_pagesize_limit,
      int64_t write_batch_size, int64_t pagesize, ParquetVersion::type version,
      const std::string& created_by, const ColumnProperties& default_column_properties,
      const std::unordered_map<std::string, ColumnProperties>& column_properties)
      : allocator_(allocator),
        dictionary_pagesize_limit_(dictionary_pagesize_limit),
        write_batch_size_(write_batch_size),
        pagesize_(pagesize),
        parquet_version_(version),
        parquet_created_by_(created_by),
        default_column_properties_(default_column_properties),
        column_properties_(column_properties) {}

  MemoryAllocator* allocator_;
  int64_t dictionary_pagesize_limit_;
  int64_t write_batch_size_;
  int64_t pagesize_;
  ParquetVersion::type parquet_version_;
  std::string parquet_created_by_;
  ColumnProperties default_column_properties_;
  std::unordered_map<std::string, ColumnProperties> column_properties_;
};

std::shared_ptr<WriterProperties> PARQUET_EXPORT default_writer_properties();

}  // namespace parquet

#endif  // PARQUET_COLUMN_PROPERTIES_H
