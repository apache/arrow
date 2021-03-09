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

#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace adapters {

namespace orc {
// Components of ORC Writer Options

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_LZ4 = 4,
  CompressionKind_ZSTD = 5,
  CompressionKind_MAX = INT32_MAX
};

enum CompressionStrategy {
  CompressionStrategy_SPEED = 0,
  CompressionStrategy_COMPRESSION
};

enum RleVersion { RleVersion_1 = 0, RleVersion_2 = 1 };

enum BloomFilterVersion {
  // Include both the BLOOM_FILTER and BLOOM_FILTER_UTF8 streams to support
  // both old and new readers.
  ORIGINAL = 0,
  // Only include the BLOOM_FILTER_UTF8 streams that consistently use UTF8.
  // See ORC-101
  UTF8 = 1,
  FUTURE = INT32_MAX
};

class ARROW_EXPORT FileVersion {
 private:
  uint32_t major_version;
  uint32_t minor_version;

 public:
  static const FileVersion& v_0_11();
  static const FileVersion& v_0_12();

  FileVersion(uint32_t major, uint32_t minor)
      : major_version(major), minor_version(minor) {}

  /**
   * Get major version
   */
  uint32_t major() const { return this->major_version; }

  /**
   * Get minor version
   */
  uint32_t minor() const { return this->minor_version; }

  bool operator==(const FileVersion& right) const {
    return this->major_version == right.major() &&
           this->minor_version == right.minor();
  }

  bool operator!=(const FileVersion& right) const { return !(*this == right); }

  std::string ToString() const;
};

/**
 * Options for creating a Writer.
 */
class ARROW_EXPORT WriterOptions {
 private:
  class WriterOptionsPrivate;
  std::unique_ptr<WriterOptionsPrivate> private_bits_;

 public:
  WriterOptions();
  WriterOptions(const WriterOptions&);
  WriterOptions(WriterOptions&);
  WriterOptions& operator=(const WriterOptions&);
  virtual ~WriterOptions();

  /**
   * Set the strip size.
   */
  WriterOptions& set_stripe_size(uint64_t size);

  /**
   * Get the strip size.
   * @return if not set, return default value.
   */
  uint64_t stripe_size() const;

  /**
   * Set the data compression block size.
   */
  WriterOptions& set_compression_block_size(uint64_t size);

  /**
   * Get the data compression block size.
   * @return if not set, return default value.
   */
  uint64_t compression_block_size() const;

  /**
   * Set row index stride (the number of rows per an entry in the row index). Use value 0
   * to disable row index.
   */
  WriterOptions& set_row_index_stride(uint64_t stride);

  /**
   * Get the row index stride (the number of rows per an entry in the row index).
   * @return if not set, return default value.
   */
  uint64_t row_index_stride() const;

  /**
   * Set the dictionary key size threshold.
   * 0 to disable dictionary encoding.
   * 1 to always enable dictionary encoding.
   */
  WriterOptions& set_dictionary_key_size_threshold(double val);

  /**
   * Get the dictionary key size threshold.
   */
  double dictionary_key_size_threshold() const;

  /**
   * Set Orc file version
   */
  WriterOptions& set_file_version(const FileVersion& version);

  /**
   * Get Orc file version
   */
  FileVersion file_version() const;

  /**
   * Set compression kind.
   */
  WriterOptions& set_compression(CompressionKind comp);

  /**
   * Get the compression kind.
   * @return if not set, return default value which is ZLIB.
   */
  CompressionKind compression() const;

  /**
   * Set the compression strategy.
   */
  WriterOptions& set_compression_strategy(CompressionStrategy strategy);

  /**
   * Get the compression strategy.
   * @return if not set, return default value which is speed.
   */
  CompressionStrategy compression_strategy() const;

  /**
   * Get if the bitpacking should be aligned.
   * @return true if should be aligned, return false otherwise
   */
  bool aligned_bitpacking() const;

  /**
   * Set the padding tolerance.
   */
  WriterOptions& set_padding_tolerance(double tolerance);

  /**
   * Get the padding tolerance.
   * @return if not set, return default value which is zero.
   */
  double padding_tolerance() const;

  /**
   * Set the error stream.
   */
  WriterOptions& set_error_stream(std::ostream& err_stream);

  /**
   * Get the error stream.
   * @return if not set, return std::err.
   */
  std::ostream* error_stream() const;

  /**
   * Get the RLE version.
   */
  RleVersion rle_version() const;

  /**
   * Get whether or not to write row group index
   * @return if not set, the default is false
   */
  bool enable_index() const;

  /**
   * Get whether or not to enable dictionary encoding
   * @return if not set, the default is false
   */
  bool enable_dictionary() const;

  /**
   * Set columns that use BloomFilter
   */
  WriterOptions& set_columns_use_bloom_filter(const std::set<uint64_t>& columns);

  /**
   * Get whether this column uses BloomFilter
   */
  bool is_column_use_bloom_filter(uint64_t column) const;

  /**
   * Set false positive probability of BloomFilter
   */
  WriterOptions& set_bloom_filter_fpp(double fpp);

  /**
   * Get false positive probability of BloomFilter
   */
  double bloom_filter_fpp() const;

  /**
   * Get version of BloomFilter
   */
  BloomFilterVersion bloom_filter_version() const;
};
}  // namespace orc
}  // namespace adapters
}  // namespace arrow