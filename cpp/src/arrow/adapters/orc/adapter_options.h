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

#include <set>
#include <sstream>

#include "arrow/io/interfaces.h"
#include "arrow/util/visibility.h"
#include "orc/OrcFile.hh"

namespace liborc = orc;

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
    return this->major_version == right.major() && this->minor_version == right.minor();
  }

  bool operator!=(const FileVersion& right) const { return !(*this == right); }

  std::string ToString() const;
};

/**
 * Options for creating a Reader.
 */
class ARROW_EXPORT ReadOptions {
 public:
  ReadOptions();
  ReadOptions(const ReadOptions&);
  ReadOptions(ReadOptions&);
  ReadOptions& operator=(const ReadOptions&);
  virtual ~ReadOptions();

  /**
   * Set the stream to use for printing warning or error messages.
   */
  ReadOptions& set_error_stream(std::ostream& stream);

  /**
   * Set a serialized copy of the file tail to be used when opening the file.
   *
   * When one process opens the file and other processes need to read
   * the rows, we want to enable clients to just read the tail once.
   * By passing the string returned by Reader.serialized_file_tail(), to
   * this function, the second reader will not need to read the file tail
   * from disk.
   *
   * @param serialization the bytes of the serialized tail to use
   */
  ReadOptions& set_serialized_file_tail(const std::string& serialization);

  /**
   * Set the location of the tail as defined by the logical length of the
   * file.
   */
  ReadOptions& set_tail_location(uint64_t offset);

  /**
   * Get the stream to write warnings or errors to.
   */
  std::ostream* error_stream() const;

  /**
   * Get the serialized file tail that the user passed in.
   */
  std::string serialized_file_tail() const;

  /**
   * Get the desired tail location.
   * @return if not set, return the maximum long.
   */
  uint64_t tail_location() const;
};

/**
 * Options for creating a RowReader.
 */
class RowReaderOptions {
 public:
  RowReaderOptions();
  RowReaderOptions(const RowReaderOptions&);
  RowReaderOptions(RowReaderOptions&);
  RowReaderOptions& operator=(const RowReaderOptions&);
  virtual ~RowReaderOptions();

  /**
   * For files that have structs as the top-level object, select the fields
   * to read. The first field is 0, the second 1, and so on. By default,
   * all columns are read. This option clears any previous setting of
   * the selected columns.
   * @param include a list of fields to read
   * @return this
   */
  RowReaderOptions& include(const std::list<uint64_t>& include);

  /**
   * For files that have structs as the top-level object, select the fields
   * to read by name. By default, all columns are read. This option clears
   * any previous setting of the selected columns.
   * @param include a list of fields to read
   * @return this
   */
  RowReaderOptions& include(const std::list<std::string>& include);

  /**
   * Selects which type ids to read. The root type is always 0 and the
   * rest of the types are labeled in a preorder traversal of the tree.
   * The parent types are automatically selected, but the children are not.
   *
   * This option clears any previous setting of the selected columns or
   * types.
   * @param types a list of the type ids to read
   * @return this
   */
  RowReaderOptions& include_types(const std::list<uint64_t>& types);

  /**
   * Set the section of the file to process.
   * @param offset the starting byte offset
   * @param length the number of bytes to read
   * @return this
   */
  RowReaderOptions& range(uint64_t offset, uint64_t length);

  /**
   * For Hive 0.11 (and 0.12) decimals, the precision was unlimited
   * and thus may overflow the 38 digits that is supported. If one
   * of the Hive 0.11 decimals is too large, the reader may either convert
   * the value to NULL or throw an exception. That choice is controlled
   * by this setting.
   *
   * Defaults to true.
   *
   * @param should_throw should the reader throw a ParseError?
   * @return returns *this
   */
  RowReaderOptions& throw_on_hive11_decimal_overflow(bool should_throw);

  /**
   * For Hive 0.11 (and 0.12) written decimals, which have unlimited
   * scale and precision, the reader forces the scale to a consistent
   * number that is configured. This setting changes the scale that is
   * forced upon these old decimals. See also throwOnHive11DecimalOverflow.
   *
   * Defaults to 6.
   *
   * @param forced_scale the scale that will be forced on Hive 0.11 decimals
   * @return returns *this
   */
  RowReaderOptions& forced_scale_on_hive11_decimal(int32_t forced_scale);

  /**
   * Set enable encoding block mode.
   * By enable encoding block mode, Row Reader will not decode
   * dictionary encoded string vector, but instead return an index array with
   * reference to corresponding dictionary.
   */
  RowReaderOptions& set_enable_lazy_decoding(bool enable);

  /**
   * Set search argument for predicate push down
   */
  RowReaderOptions& searchArgument(std::unique_ptr<SearchArgument> sargs);

  /**
   * Should enable encoding block mode
   */
  bool get_enable_lazy_decoding() const;

  /**
   * Were the field ids set?
   */
  bool get_indexes_set() const;

  /**
   * Were the type ids set?
   */
  bool get_type_ids_set() const;

  /**
   * Get the list of selected field or type ids to read.
   */
  const std::list<uint64_t>& getInclude() const;

  /**
   * Were the include names set?
   */
  bool getNamesSet() const;

  /**
   * Get the list of selected columns to read. All children of the selected
   * columns are also selected.
   */
  const std::list<std::string>& getIncludeNames() const;

  /**
   * Get the start of the range for the data being processed.
   * @return if not set, return 0
   */
  uint64_t getOffset() const;

  /**
   * Get the end of the range for the data being processed.
   * @return if not set, return the maximum long
   */
  uint64_t getLength() const;

  /**
   * Should the reader throw a ParseError when a Hive 0.11 decimal is
   * larger than the supported 38 digits of precision? Otherwise, the
   * data item is replaced by a NULL.
   */
  bool getThrowOnHive11DecimalOverflow() const;

  /**
   * What scale should all Hive 0.11 decimals be normalized to?
   */
  int32_t getForcedScaleOnHive11Decimal() const;

  /**
   * Get search argument for predicate push down
   */
  std::shared_ptr<SearchArgument> getSearchArgument() const;

  /**
   * Set desired timezone to return data of timestamp type
   */
  RowReaderOptions& setTimezoneName(const std::string& zoneName);

  /**
   * Get desired timezone to return data of timestamp type
   */
  const std::string& getTimezoneName() const;
};

/**
 * Options for creating a Writer.
 */
class ARROW_EXPORT WriteOptions {
 public:
  WriteOptions();
  WriteOptions(const WriteOptions&);
  WriteOptions(WriteOptions&);
  WriteOptions& operator=(const WriteOptions&);
  virtual ~WriteOptions();

  /**
   * Get the ORC writer options
   * @return The ORC writer options this WriteOption encapsulates
   */
  std::shared_ptr<liborc::WriterOptions> get_orc_writer_options() const {
    return orc_writer_options_;
  }

  /**
   * Set the batch size.
   */
  WriteOptions& set_batch_size(uint64_t size);

  /**
   * Get the batch size.
   * @return if not set, return default value.
   */
  uint64_t batch_size() const;

  /**
   * Set the stripe size.
   */
  WriteOptions& set_stripe_size(uint64_t size);

  /**
   * Get the stripe size.
   * @return if not set, return default value.
   */
  uint64_t stripe_size() const;

  /**
   * Set the data compression block size.
   */
  WriteOptions& set_compression_block_size(uint64_t size);

  /**
   * Get the data compression block size.
   * @return if not set, return default value.
   */
  uint64_t compression_block_size() const;

  /**
   * Set row index stride (the number of rows per an entry in the row index). Use value
   * 0 to disable row index.
   */
  WriteOptions& set_row_index_stride(uint64_t stride);

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
  WriteOptions& set_dictionary_key_size_threshold(double val);

  /**
   * Get the dictionary key size threshold.
   */
  double dictionary_key_size_threshold() const;

  /**
   * Set Orc file version
   */
  WriteOptions& set_file_version(const FileVersion& version);

  /**
   * Get Orc file version
   */
  FileVersion file_version() const;

  /**
   * Set compression kind.
   */
  WriteOptions& set_compression(CompressionKind comp);

  /**
   * Get the compression kind.
   * @return if not set, return default value which is ZLIB.
   */
  CompressionKind compression() const;

  /**
   * Set the compression strategy.
   */
  WriteOptions& set_compression_strategy(CompressionStrategy strategy);

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
  WriteOptions& set_padding_tolerance(double tolerance);

  /**
   * Get the padding tolerance.
   * @return if not set, return default value which is zero.
   */
  double padding_tolerance() const;

  /**
   * Set the error stream.
   */
  WriteOptions& set_error_stream(std::ostream& err_stream);

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
  WriteOptions& set_columns_use_bloom_filter(const std::set<uint64_t>& columns);

  /**
   * Get whether this column uses BloomFilter
   */
  bool is_column_use_bloom_filter(uint64_t column) const;

  /**
   * Get columns that use BloomFilter
   * @return The set of columns that use BloomFilter
   */
  std::set<uint64_t> columns_use_bloom_filter() const;

  /**
   * Set false positive probability of BloomFilter
   */
  WriteOptions& set_bloom_filter_fpp(double fpp);

  /**
   * Get false positive probability of BloomFilter
   */
  double bloom_filter_fpp() const;

  /**
   * Get version of BloomFilter
   */
  BloomFilterVersion bloom_filter_version() const;

 private:
  std::shared_ptr<liborc::WriterOptions> orc_writer_options_;
  uint64_t batch_size_;
};

}  // namespace orc
}  // namespace adapters
}  // namespace arrow
