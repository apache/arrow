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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "parquet/exception.h"
#include "parquet/level_conversion.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

class Decryptor;
class Page;

// 16 MB is the default maximum page header size
static constexpr uint32_t kDefaultMaxPageHeaderSize = 16 * 1024 * 1024;

// 16 KB is the default expected page header size
static constexpr uint32_t kDefaultPageHeaderSize = 16 * 1024;

// \brief DataPageStats stores encoded statistics and number of values/rows for
// a page.
struct PARQUET_EXPORT DataPageStats {
  DataPageStats(const EncodedStatistics* encoded_statistics, int32_t num_values,
                std::optional<int32_t> num_rows)
      : encoded_statistics(encoded_statistics),
        num_values(num_values),
        num_rows(num_rows) {}

  // Encoded statistics extracted from the page header.
  // Nullptr if there are no statistics in the page header.
  const EncodedStatistics* encoded_statistics;
  // Number of values stored in the page. Filled for both V1 and V2 data pages.
  // For repeated fields, this can be greater than number of rows. For
  // non-repeated fields, this will be the same as the number of rows.
  int32_t num_values;
  // Number of rows stored in the page. std::nullopt if not available.
  std::optional<int32_t> num_rows;
};

/// Decoder for repetition of definition level.
///
/// This decoder is used with either a deprecated bit packed (`BIT_PACKED = 4`)
/// encoding or a mixed bit packed and RLE one (`RLE = 3`).
/// Because it take as input a single buffer, `SetData` and `Decode` are typically
/// used on each of the parquet `DataPage`.
/// The number of levels is guaranteed to fit into an `int32_t` by the specification.
///
/// @see https://research.google.com/pubs/archive/36632.pdf
class PARQUET_EXPORT LevelDecoder {
 public:
  explicit LevelDecoder(int16_t max_level = 0);

  LevelDecoder(LevelDecoder&&) = default;
  LevelDecoder& operator=(LevelDecoder&&) = default;

  ~LevelDecoder();

  /// Initialize the LevelDecoder state with new data from a legacy (V1) page.
  ///
  /// @return the number of bytes consumed
  int32_t SetData(Encoding::type encoding, int16_t max_level, int32_t num_buffered_values,
                  const uint8_t* data, int32_t data_size);

  /// Initialize the LevelDecoder state with new data from a V2 page.
  ///
  /// Repetition and definition levels in V2 pages are always RLE encoded.
  void SetDataV2(int32_t num_bytes, int16_t max_level, int32_t num_buffered_values,
                 const uint8_t* data);

  /// Decode a batch of levels int32_to an array and returns the number of levels decoded.
  int32_t Decode(int32_t batch_size, int16_t* levels);

  /// Advance the decoder and throw away decoder levels.
  int32_t Skip(int32_t batch_size);

  struct CountUpToResult {
    int32_t matching_count;
    int32_t processed_count;
  };

  /// Advance and count the number of occurrences of `value`.
  ///
  /// The count is limited to at most the next `batch_size` items.
  /// @return The matching value count and number of elements that were processed.
  CountUpToResult CountUpTo(int16_t value, int32_t batch_size);

  /// Return the max level used in this decoder.
  int32_t max_level() const { return max_level_; }

  /// Return the number of values left to be decoded.
  int32_t remaining() const { return num_values_remaining_; }

 private:
  struct Impl;

  std::unique_ptr<Impl> impl_;
  /// Number of value remaining. The underlying decoder zero pads bit packed values
  /// up to a multiple of 8 so it cannot know the exact number of remaining values.
  int32_t num_values_remaining_ = 0;
  int16_t max_level_;
};

struct CryptoContext {
  bool start_decrypt_with_dictionary_page = false;
  int16_t row_group_ordinal = -1;
  int16_t column_ordinal = -1;
  std::function<std::unique_ptr<Decryptor>()> meta_decryptor_factory;
  std::function<std::unique_ptr<Decryptor>()> data_decryptor_factory;
};

// Abstract page iterator interface. This way, we can feed column pages to the
// ColumnReader through whatever mechanism we choose
class PARQUET_EXPORT PageReader {
  using DataPageFilter = std::function<bool(const DataPageStats&)>;

 public:
  virtual ~PageReader() = default;

  static std::unique_ptr<PageReader> Open(std::shared_ptr<ArrowInputStream> stream,
                                          int64_t total_num_values,
                                          Compression::type codec,
                                          const ReaderProperties& properties,
                                          const ColumnDescriptor& descr,
                                          bool always_compressed = false,
                                          const CryptoContext* ctx = NULLPTR);

  PARQUET_DEPRECATED("Deprecated in 25.0.0. Use the ColumnDescriptor overload instead.")
  static std::unique_ptr<PageReader> Open(
      std::shared_ptr<ArrowInputStream> stream, int64_t total_num_values,
      Compression::type codec, bool always_compressed = false,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(),
      const CryptoContext* ctx = NULLPTR);
  PARQUET_DEPRECATED("Deprecated in 25.0.0. Use the ColumnDescriptor overload instead.")
  static std::unique_ptr<PageReader> Open(std::shared_ptr<ArrowInputStream> stream,
                                          int64_t total_num_values,
                                          Compression::type codec,
                                          const ReaderProperties& properties,
                                          bool always_compressed = false,
                                          const CryptoContext* ctx = NULLPTR);

  // If data_page_filter is present (not null), NextPage() will call the
  // callback function exactly once per page in the order the pages appear in
  // the column. If the callback function returns true the page will be
  // skipped. The callback will be called only if the page type is DATA_PAGE or
  // DATA_PAGE_V2. Dictionary pages will not be skipped.
  // Caller is responsible for checking that statistics are correct using
  // ApplicationVersion::HasCorrectStatistics().
  // \note API EXPERIMENTAL
  void set_data_page_filter(DataPageFilter data_page_filter) {
    data_page_filter_ = std::move(data_page_filter);
  }

  // @returns: shared_ptr<Page>(nullptr) on EOS, std::shared_ptr<Page>
  // containing new Page otherwise
  //
  // The returned Page may contain references that aren't guaranteed to live
  // beyond the next call to NextPage().
  virtual std::shared_ptr<Page> NextPage() = 0;

  virtual void set_max_page_header_size(uint32_t size) = 0;

 protected:
  // Callback that decides if we should skip a page or not.
  DataPageFilter data_page_filter_;
};

class PARQUET_EXPORT ColumnReader {
 public:
  virtual ~ColumnReader() = default;

  static std::shared_ptr<ColumnReader> Make(
      const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  // Returns true if there are still values in this column.
  virtual bool HasNext() = 0;

  virtual Type::type type() const = 0;

  virtual const ColumnDescriptor* descr() const = 0;

  // Get the encoding that can be exposed by this reader. If it returns
  // dictionary encoding, then ReadBatchWithDictionary can be used to read data.
  //
  // \note API EXPERIMENTAL
  virtual ExposedEncoding GetExposedEncoding() = 0;

 protected:
  friend class RowGroupReader;
  // Set the encoding that can be exposed by this reader.
  //
  // \note API EXPERIMENTAL
  virtual void SetExposedEncoding(ExposedEncoding encoding) = 0;
};

// API to read values from a single column. This is a main client facing API.
template <typename DType>
class TypedColumnReader : public ColumnReader {
 public:
  using T = typename DType::c_type;

  // Read a batch of repetition levels, definition levels, and values from the
  // column.
  //
  // Since null values are not stored in the values, the number of values read
  // may be less than the number of repetition and definition levels. With
  // nested data this is almost certainly true.
  //
  // Set def_levels or rep_levels to nullptr if you want to skip reading them.
  // This is only safe if you know through some other source that there are no
  // undefined values.
  //
  // To fully exhaust a row group, you must read batches until the number of
  // values read reaches the number of stored values according to the metadata.
  //
  // This API is the same for both V1 and V2 of the DataPage
  //
  // @returns: actual number of levels read (see values_read for number of values read)
  virtual int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                            T* values, int64_t* values_read) = 0;

  // Skip reading values. This method will work for both repeated and
  // non-repeated fields. Note that this method is skipping values and not
  // records. This distinction is important for repeated fields, meaning that
  // we are not skipping over the values to the next record. For example,
  // consider the following two consecutive records containing one repeated field:
  // {[1, 2, 3]}, {[4, 5]}. If we Skip(2), our next read value will be 3, which
  // is inside the first record.
  // Returns the number of values skipped.
  virtual int64_t Skip(int64_t num_values_to_skip) = 0;

  // Read a batch of repetition levels, definition levels, and indices from the
  // column. And read the dictionary if a dictionary page is encountered during
  // reading pages. This API is similar to ReadBatch(), with ability to read
  // dictionary and indices. It is only valid to call this method  when the reader can
  // expose dictionary encoding. (i.e., the reader's GetExposedEncoding() returns
  // DICTIONARY).
  //
  // The dictionary is read along with the data page. When there's no data page,
  // the dictionary won't be returned.
  //
  // @param batch_size The batch size to read
  // @param[out] def_levels The Parquet definition levels.
  // @param[out] rep_levels The Parquet repetition levels.
  // @param[out] indices The dictionary indices.
  // @param[out] indices_read The number of indices read.
  // @param[out] dict The pointer to dictionary values. It will return nullptr if
  // there's no data page. Each column chunk only has one dictionary page. The dictionary
  // is owned by the reader, so the caller is responsible for copying the dictionary
  // values before the reader gets destroyed.
  // @param[out] dict_len The dictionary length. It will return 0 if there's no data
  // page.
  // @returns: actual number of levels read (see indices_read for number of
  // indices read
  //
  // \note API EXPERIMENTAL
  virtual int64_t ReadBatchWithDictionary(int64_t batch_size, int16_t* def_levels,
                                          int16_t* rep_levels, int32_t* indices,
                                          int64_t* indices_read, const T** dict,
                                          int32_t* dict_len) = 0;
};

namespace internal {

/// \brief Stateful column reader that delimits semantic records for both flat
/// and nested columns
///
/// \note API EXPERIMENTAL
/// \since 1.3.0
class PARQUET_EXPORT RecordReader {
 public:
  /// \brief Creates a record reader.
  /// @param descr Column descriptor
  /// @param leaf_info Level info, used to determine if a column is nullable or not
  /// @param pool Memory pool to use for buffering values and rep/def levels
  /// @param read_dictionary True if reading directly as Arrow dictionary-encoded
  /// @param read_dense_for_nullable True if reading dense and not leaving space for null
  /// values
  /// @param arrow_type Which type to read this column as (optional). Currently
  /// only used for byte array columns (see BinaryRecordReader::GetBuilderChunks).
  static std::shared_ptr<RecordReader> Make(
      const ColumnDescriptor* descr, LevelInfo leaf_info,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(),
      bool read_dictionary = false, bool read_dense_for_nullable = false,
      const std::shared_ptr<::arrow::DataType>& arrow_type = NULLPTR);

  virtual ~RecordReader() = default;

  /// \brief Attempt to read indicated number of records from column chunk
  /// Note that for repeated fields, a record may have more than one value
  /// and all of them are read. If read_dense_for_nullable() it will
  /// not leave any space for null values. Otherwise, it will read spaced.
  /// \return number of records read
  virtual int64_t ReadRecords(int64_t num_records) = 0;

  /// \brief Attempt to skip indicated number of records from column chunk.
  /// Note that for repeated fields, a record may have more than one value
  /// and all of them are skipped.
  /// \return number of records skipped
  virtual int64_t SkipRecords(int64_t num_records) = 0;

  /// \brief Pre-allocate space for data. Results in better flat read performance
  virtual void Reserve(int64_t num_values) = 0;

  /// \brief Clear consumed values and repetition/definition levels as the
  /// result of calling ReadRecords
  /// For FLBA and ByteArray types, call GetBuilderChunks() to reset them.
  virtual void Reset() = 0;

  /// \brief Transfer filled values buffer to caller. A new one will be
  /// allocated in subsequent ReadRecords calls
  virtual std::shared_ptr<ResizableBuffer> ReleaseValues() = 0;

  /// \brief Transfer filled validity bitmap buffer to caller. A new one will
  /// be allocated in subsequent ReadRecords calls
  virtual std::shared_ptr<ResizableBuffer> ReleaseIsValid() = 0;

  /// \brief Return true if the record reader has more internal data yet to
  /// process
  virtual bool HasMoreData() const = 0;

  /// \brief Advance record reader to the next row group. Must be set before
  /// any records could be read/skipped.
  /// \param[in] reader obtained from RowGroupReader::GetColumnPageReader
  virtual void SetPageReader(std::unique_ptr<PageReader> reader) = 0;

  /// \brief Returns the underlying column reader's descriptor.
  virtual const ColumnDescriptor* descr() const = 0;

  virtual void DebugPrintState() = 0;

  /// \brief Returns the dictionary owned by the current decoder. Throws an
  /// exception if the current decoder is not for dictionary encoding. The caller is
  /// responsible for casting the returned pointer to proper type depending on the
  /// column's physical type. An example:
  ///   const ByteArray* dict = reinterpret_cast<const ByteArray*>(ReadDictionary(&len));
  /// or:
  ///   const float* dict = reinterpret_cast<const float*>(ReadDictionary(&len));
  /// \param[out] dictionary_length The number of dictionary entries.
  virtual const void* ReadDictionary(int32_t* dictionary_length) = 0;

  /// \brief Decoded definition levels
  virtual int16_t* def_levels() const = 0;

  /// \brief Decoded repetition levels
  virtual int16_t* rep_levels() const = 0;

  /// \brief Decoded values, including nulls, if any
  /// FLBA and ByteArray types do not use this array and read into their own
  /// builders.
  virtual uint8_t* values() const = 0;

  /// \brief Number of values written, including space left for nulls if any.
  /// If this Reader was constructed with read_dense_for_nullable(), there is no space for
  /// nulls and null_count() will be 0. There is no read-ahead/buffering for values. For
  /// FLBA and ByteArray types this value reflects the values written with the last
  /// ReadRecords call since those readers will reset the values after each call.
  virtual int64_t values_written() const = 0;

  /// \brief Number of definition / repetition levels (from those that have
  /// been decoded) that have been consumed inside the reader.
  virtual int64_t levels_position() const = 0;

  /// \brief Number of definition / repetition levels that have been written
  /// internally in the reader. This may be larger than values_written() because
  /// for repeated fields we need to look at the levels in advance to figure out
  /// the record boundaries.
  virtual int64_t levels_written() const = 0;

  /// \brief Number of nulls in the leaf that we have read so far into the
  /// values vector. This is only valid when !read_dense_for_nullable(). When
  /// read_dense_for_nullable() it will always be 0.
  virtual int64_t null_count() const = 0;

  /// \brief True if the leaf values are nullable
  virtual bool nullable_values() const = 0;

  /// \brief True if reading directly as Arrow dictionary-encoded
  virtual bool read_dictionary() const = 0;

  /// \brief True if reading dense for nullable columns.
  virtual bool read_dense_for_nullable() const = 0;
};

class BinaryRecordReader : virtual public RecordReader {
 public:
  virtual std::vector<std::shared_ptr<::arrow::Array>> GetBuilderChunks() = 0;
};

/// \brief Read records directly to dictionary-encoded Arrow form (int32
/// indices). Only valid for BYTE_ARRAY columns
class DictionaryRecordReader : virtual public RecordReader {
 public:
  virtual std::shared_ptr<::arrow::ChunkedArray> GetResult() = 0;
};

}  // namespace internal

using BoolReader = TypedColumnReader<BooleanType>;
using Int32Reader = TypedColumnReader<Int32Type>;
using Int64Reader = TypedColumnReader<Int64Type>;
using Int96Reader = TypedColumnReader<Int96Type>;
using FloatReader = TypedColumnReader<FloatType>;
using DoubleReader = TypedColumnReader<DoubleType>;
using ByteArrayReader = TypedColumnReader<ByteArrayType>;
using FixedLenByteArrayReader = TypedColumnReader<FLBAType>;

}  // namespace parquet
