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

#include "parquet/types.h"
#include "parquet/schema/types.h"
#include "parquet/util/input.h"
#include "parquet/util/mem-allocator.h"
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

  MemoryAllocator* allocator() { return allocator_; }

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

static int64_t DEFAULT_PAGE_SIZE = 1024 * 1024;
static int64_t DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
static Encoding::type DEFAULT_ENCODING = Encoding::PLAIN;
static constexpr ParquetVersion::type DEFAULT_WRITER_VERSION =
    ParquetVersion::PARQUET_1_0;
static constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;

using ColumnCodecs = std::unordered_map<std::string, Compression::type>;

class PARQUET_EXPORT WriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : allocator_(default_allocator()),
          dictionary_pagesize_(DEFAULT_DICTIONARY_PAGE_SIZE),
          default_encoding_(DEFAULT_ENCODING),
          pagesize_(DEFAULT_PAGE_SIZE),
          version_(DEFAULT_WRITER_VERSION),
          default_codec_(DEFAULT_COMPRESSION_TYPE) {}
    virtual ~Builder() {}

    Builder* allocator(MemoryAllocator* allocator) {
      allocator_ = allocator;
      return this;
    }

    Builder* dictionary_pagesize(int64_t dictionary_psize) {
      dictionary_pagesize_ = dictionary_psize;
      return this;
    }

    Builder* data_pagesize(int64_t pg_size) {
      pagesize_ = pg_size;
      return this;
    }

    Builder* encoding(
        const std::shared_ptr<schema::ColumnPath>& path, Encoding::type encoding_type) {
      return encoding(path->ToDotString(), encoding_type);
    }

    Builder* encoding(const std::string& column_path, Encoding::type encoding_type) {
      encodings_[column_path] = encoding_type;
      return this;
    }

    Builder* encoding(Encoding::type encoding_type) {
      default_encoding_ = encoding_type;
      return this;
    }

    Builder* version(ParquetVersion::type version) {
      version_ = version;
      return this;
    }

    Builder* compression(Compression::type codec) {
      default_codec_ = codec;
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

    std::shared_ptr<WriterProperties> build() {
      return std::shared_ptr<WriterProperties>(
          new WriterProperties(allocator_, dictionary_pagesize_, default_encoding_,
              encodings_, pagesize_, version_, default_codec_, codecs_));
    }

   private:
    MemoryAllocator* allocator_;
    int64_t dictionary_pagesize_;
    // Encoding used for each column if not a specialized one is defined as
    // part of encodings_
    Encoding::type default_encoding_;
    std::unordered_map<std::string, Encoding::type> encodings_;
    int64_t pagesize_;
    ParquetVersion::type version_;
    // Default compression codec. This will be used for all columns that do
    // not have a specific codec set as part of codecs_
    Compression::type default_codec_;
    ColumnCodecs codecs_;
  };

  MemoryAllocator* allocator() const { return allocator_; }

  int64_t dictionary_pagesize() const { return dictionary_pagesize_; }

  int64_t data_pagesize() const { return pagesize_; }

  ParquetVersion::type version() const { return parquet_version_; }

  Encoding::type encoding(const std::shared_ptr<schema::ColumnPath>& path) const {
    Encoding::type coding = default_encoding_;
    auto it = encodings_.find(path->ToDotString());
    if (it != encodings_.end()) { coding = it->second; }

    // Use the correct enum value for dictionary coding based on the used Parquet version
    if (coding == Encoding::PLAIN_DICTIONARY || coding == Encoding::RLE_DICTIONARY) {
      if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
        return Encoding::PLAIN_DICTIONARY;
      } else {
        return Encoding::RLE_DICTIONARY;
      }
    }
    return coding;
  }

  Compression::type compression(const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = codecs_.find(path->ToDotString());
    if (it != codecs_.end()) return it->second;
    return default_codec_;
  }

 private:
  explicit WriterProperties(MemoryAllocator* allocator, int64_t dictionary_pagesize,
      Encoding::type default_encoding,
      const std::unordered_map<std::string, Encoding::type>& encodings, int64_t pagesize,
      ParquetVersion::type version, Compression::type default_codec,
      const ColumnCodecs& codecs)
      : allocator_(allocator),
        dictionary_pagesize_(dictionary_pagesize),
        default_encoding_(default_encoding),
        encodings_(encodings),
        pagesize_(pagesize),
        parquet_version_(version),
        default_codec_(default_codec),
        codecs_(codecs) {}

  MemoryAllocator* allocator_;
  int64_t dictionary_pagesize_;
  Encoding::type default_encoding_;
  std::unordered_map<std::string, Encoding::type> encodings_;
  int64_t pagesize_;
  ParquetVersion::type parquet_version_;
  Compression::type default_codec_;
  ColumnCodecs codecs_;
};

std::shared_ptr<WriterProperties> PARQUET_EXPORT default_writer_properties();

}  // namespace parquet

#endif  // PARQUET_COLUMN_PROPERTIES_H
