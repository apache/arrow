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

#include "page_index.h"
#include "parquet/exception.h"
#include "parquet/level_conversion.h"
#include "parquet/metadata.h"
#include "parquet/platform.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace arrow {

class Array;
class ChunkedArray;

namespace bit_util {
class BitReader;
}  // namespace bit_util

namespace util {
class RleDecoder;
}  // namespace util

}  // namespace arrow

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

class PARQUET_EXPORT LevelDecoder {
 public:
  LevelDecoder();
  ~LevelDecoder();

  // Initialize the LevelDecoder state with new data
  // and return the number of bytes consumed
  int SetData(Encoding::type encoding, int16_t max_level, int num_buffered_values,
              const uint8_t* data, int32_t data_size);

  void SetDataV2(int32_t num_bytes, int16_t max_level, int num_buffered_values,
                 const uint8_t* data);

  // Decodes a batch of levels into an array and returns the number of levels decoded
  int Decode(int batch_size, int16_t* levels);

 private:
  int bit_width_;
  int num_values_remaining_;
  Encoding::type encoding_;
  std::unique_ptr<::arrow::util::RleDecoder> rle_decoder_;
  std::unique_ptr<::arrow::bit_util::BitReader> bit_packed_decoder_;
  int16_t max_level_;
};

struct CryptoContext {
  CryptoContext(bool start_with_dictionary_page, int16_t rg_ordinal, int16_t col_ordinal,
                std::shared_ptr<Decryptor> meta, std::shared_ptr<Decryptor> data)
      : start_decrypt_with_dictionary_page(start_with_dictionary_page),
        row_group_ordinal(rg_ordinal),
        column_ordinal(col_ordinal),
        meta_decryptor(std::move(meta)),
        data_decryptor(std::move(data)) {}
  CryptoContext() {}

  bool start_decrypt_with_dictionary_page = false;
  int16_t row_group_ordinal = -1;
  int16_t column_ordinal = -1;
  std::shared_ptr<Decryptor> meta_decryptor;
  std::shared_ptr<Decryptor> data_decryptor;
};

// Abstract page iterator interface. This way, we can feed column pages to the
// ColumnReader through whatever mechanism we choose
class PARQUET_EXPORT PageReader {
  using DataPageFilter = std::function<bool(const DataPageStats&)>;

 public:
  virtual ~PageReader() = default;

  static std::unique_ptr<PageReader> Open(
      std::shared_ptr<ArrowInputStream> stream, int64_t total_num_values,
      Compression::type codec, bool always_compressed = false,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(),
      const CryptoContext* ctx = NULLPTR);
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
  typedef typename DType::c_type T;

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

  /// Read a batch of repetition levels, definition levels, and values from the
  /// column and leave spaces for null entries on the lowest level in the values
  /// buffer.
  ///
  /// In comparison to ReadBatch the length of repetition and definition levels
  /// is the same as of the number of values read for max_definition_level == 1.
  /// In the case of max_definition_level > 1, the repetition and definition
  /// levels are larger than the values but the values include the null entries
  /// with definition_level == (max_definition_level - 1).
  ///
  /// To fully exhaust a row group, you must read batches until the number of
  /// values read reaches the number of stored values according to the metadata.
  ///
  /// @param batch_size the number of levels to read
  /// @param[out] def_levels The Parquet definition levels, output has
  ///   the length levels_read.
  /// @param[out] rep_levels The Parquet repetition levels, output has
  ///   the length levels_read.
  /// @param[out] values The values in the lowest nested level including
  ///   spacing for nulls on the lowest levels; output has the length
  ///   values_read.
  /// @param[out] valid_bits Memory allocated for a bitmap that indicates if
  ///   the row is null or on the maximum definition level. For performance
  ///   reasons the underlying buffer should be able to store 1 bit more than
  ///   required. If this requires an additional byte, this byte is only read
  ///   but never written to.
  /// @param valid_bits_offset The offset in bits of the valid_bits where the
  ///   first relevant bit resides.
  /// @param[out] levels_read The number of repetition/definition levels that were read.
  /// @param[out] values_read The number of values read, this includes all
  ///   non-null entries as well as all null-entries on the lowest level
  ///   (i.e. definition_level == max_definition_level - 1)
  /// @param[out] null_count The number of nulls on the lowest levels.
  ///   (i.e. (values_read - null_count) is total number of non-null entries)
  ///
  /// \deprecated Since 4.0.0
  ARROW_DEPRECATED("Doesn't handle nesting correctly and unused outside of unit tests.")
  virtual int64_t ReadBatchSpaced(int64_t batch_size, int16_t* def_levels,
                                  int16_t* rep_levels, T* values, uint8_t* valid_bits,
                                  int64_t valid_bits_offset, int64_t* levels_read,
                                  int64_t* values_read, int64_t* null_count) = 0;

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

// Represent a range to read. The range is inclusive on both ends.
struct IntervalRange {
  static IntervalRange Intersection(const IntervalRange& left,
                                    const IntervalRange& right) {
    if (left.start <= right.start) {
      if (left.end >= right.start) {
        return {right.start, std::min(left.end, right.end)};
      }
    } else if (right.end >= left.start) {
      return {left.start, std::min(left.end, right.end)};
    }
    return {-1, -1};  // Return a default Range object if no intersection range found
  }

  IntervalRange(const int64_t start_, const int64_t end_) : start(start_), end(end_) {
    if (start > end) {
      throw ParquetException("Invalid range with start: " + std::to_string(start) +
                             " and end: " + std::to_string(end));
    }
  }

  size_t Count() const {
    if(!IsValid()) {
      throw ParquetException("Invalid range with start: " + std::to_string(start) +
                             " and end: " + std::to_string(end));
    }
    return end - start + 1;
  }

  bool IsBefore(const IntervalRange& other) const { return end < other.start; }

  bool IsAfter(const IntervalRange& other) const { return start > other.end; }

  bool IsOverlap(const IntervalRange& other) const {
    return !IsBefore(other) && !IsAfter(other);
  }

  bool IsValid() const { return start >= 0 && end >= 0 && end >= start; }

  std::string ToString() const {
    return "(" + std::to_string(start) + ", " + std::to_string(end) + ")";
  }

  // inclusive
  int64_t start;
  // inclusive
  int64_t end;
};

struct BitmapRange {
  int64_t offset;
  // zero added to, if there are less than 64 elements left in the column.
  uint64_t bitmap;
};

struct End {};

// Represent a set of ranges to read. The ranges are sorted and non-overlapping.
class RowRanges {
 public:
  RowRanges() = default;
  virtual ~RowRanges() = default;
  virtual size_t RowCount() const = 0;
  virtual int64_t LastRow() const = 0;
  virtual bool IsValid() const = 0;
  virtual bool IsOverlapping(const IntervalRange& searchRange) const = 0;
  // Given a RowRanges with rows accross all RGs, split it into N RowRanges, where N = number of RGs
  // e.g.: suppose we have 2 RGs: [0-99] and [100-199], and user is interested in RowRanges [90-110], then
  // this function will return 2 RowRanges: [90-99] and [0-10]
  virtual std::vector<std::unique_ptr<RowRanges>> SplitByRowGroups(const std::vector<int64_t>& rows_per_rg) const = 0;
  virtual std::string ToString() const = 0;

  // Returns a vector of PageLocations that must be read all to get values for
  // all included in this range virtual std::vector<PageLocation>
  // PageIndexesToInclude(const std::vector<PageLocation>&  all_pages) = 0;

  class Iterator {
  public:
    virtual std::variant<IntervalRange, BitmapRange, End> NextRange() = 0;
    virtual ~Iterator() = default;
  };
  virtual std::unique_ptr<Iterator> NewIterator() const = 0;

};

class IntervalRanges : public RowRanges {
 public:
  IntervalRanges() = default;

  explicit IntervalRanges(const IntervalRange& range) { ranges_.push_back(range); }

  class IntervalRowRangesIterator : public Iterator {
   public:
    IntervalRowRangesIterator(const std::vector<IntervalRange>& ranges)
        : ranges_(ranges) {}
    ~IntervalRowRangesIterator() override {}

    std::variant<IntervalRange, BitmapRange, End> NextRange() override {
      if (current_index_ >= ranges_.size()) return End();

      return ranges_[current_index_++];
    }

   private:
    const std::vector<IntervalRange>& ranges_;
    size_t current_index_ = 0;
  };

  std::unique_ptr<Iterator> NewIterator() const override {
    return std::make_unique<IntervalRowRangesIterator>(ranges_);
  }

  size_t RowCount() const override {
    size_t cnt = 0;
    for (const IntervalRange& range : ranges_) {
      cnt += range.Count();
    }
    return cnt;
  }

  int64_t LastRow() const override { return ranges_.back().end; }

  bool IsValid() const override {
    if (ranges_.size() == 0) return true;
    if (ranges_[0].start < 0) {
      return false;
    }
    for (size_t i = 0; i < ranges_.size(); i++) {
      if (!ranges_[i].IsValid()) {
        return false;
      }
    }
    for (size_t i = 1; i < ranges_.size(); i++) {
      if (ranges_[i].start <= ranges_[i - 1].end) {
        return false;
      }
    }
    return true;
  }

  bool IsOverlapping(const IntervalRange& searchRange) const override {
    auto it = std::lower_bound(
        ranges_.begin(), ranges_.end(), searchRange,
        [](const IntervalRange& r1, const IntervalRange& r2) { return r1.IsBefore(r2); });
    return it != ranges_.end() && !(*it).IsAfter(searchRange);
  }

  std::string ToString() const override {
    std::string result = "[";
    for (const IntervalRange& range : ranges_) {
      result += range.ToString() + ", ";
    }
    if (!ranges_.empty()) {
      result = result.substr(0, result.size() - 2);
    }
    result += "]";
    return result;
  }

  std::vector<std::unique_ptr<RowRanges>> SplitByRowGroups(
      const std::vector<int64_t>& rows_per_rg) const override {
    if (rows_per_rg.size() <= 1) {
      std::unique_ptr<RowRanges> single =
          std::make_unique<IntervalRanges>(*this);  // return a copy of itself
      auto ret = std::vector<std::unique_ptr<RowRanges>>();
      ret.push_back(std::move(single));
      return ret;
    }

    std::vector<std::unique_ptr<RowRanges>> result;

    IntervalRanges spaces;
    int64_t rows_so_far = 0;
    for (size_t i = 0; i < rows_per_rg.size(); ++i) {
      auto start = rows_so_far;
      rows_so_far += rows_per_rg[i];
      auto end = rows_so_far - 1;
      spaces.Add({start, end});
    }

    // each RG's row range forms a space, we need to adjust RowRanges in each space to
    // zero based.
    for (IntervalRange space : spaces.GetRanges()) {
      auto intersection = Intersection(IntervalRanges(space), *this);

      std::unique_ptr<IntervalRanges> zero_based_ranges =
          std::make_unique<IntervalRanges>();
      for (const IntervalRange& range : intersection.GetRanges()) {
        zero_based_ranges->Add({range.start - space.start, range.end - space.start});
      }
      result.push_back(std::move(zero_based_ranges));
    }

    return result;
  }

  static IntervalRanges Intersection(const IntervalRanges& left,
                                     const IntervalRanges& right) {
    IntervalRanges result;

    size_t rightIndex = 0;
    for (const IntervalRange& l : left.ranges_) {
      for (size_t i = rightIndex, n = right.ranges_.size(); i < n; ++i) {
        const IntervalRange& r = right.ranges_[i];
        if (l.IsBefore(r)) {
          break;
        } else if (l.IsAfter(r)) {
          rightIndex = i + 1;
          continue;
        }
        result.Add(IntervalRange::Intersection(l, r));
      }
    }

    return result;
  }

  void Add(const IntervalRange& range) {
    const IntervalRange rangeToAdd = range;
    if (ranges_.size() > 1 && rangeToAdd.start <= ranges_.back().end) {
      throw ParquetException("Ranges must be added in order");
    }
    ranges_.push_back(rangeToAdd);
  }

  const std::vector<IntervalRange>& GetRanges() const { return ranges_; }

 private:
  std::vector<IntervalRange> ranges_;
};

namespace internal {

// A RecordSkipper is used to skip uncessary rows within each pages.
class PARQUET_EXPORT RecordSkipper {
 public:
  RecordSkipper(IntervalRanges& pages, const RowRanges& orig_row_ranges) {
    // copy row_ranges
    IntervalRanges skip_pages;
    for (auto& page : pages.GetRanges()) {
      if (!orig_row_ranges.IsOverlapping(page)) {
        skip_pages.Add(page);
      }
    }

    /// Since the skipped pages will be silently skipped without updating
    /// current_rg_processed_records or records_read_, we need to pre-process the row
    /// ranges as if these skipped pages never existed
    AdjustRanges(skip_pages, orig_row_ranges, row_ranges_);
    range_iter_ = row_ranges_->NewIterator();
    current_range_variant = range_iter_->NextRange();

    total_rows_to_process_ = pages.RowCount() - skip_pages.RowCount();
  }

  /// \brief Return the number of records to read or to skip
  /// if return values is positive, it means to read N records
  /// if return values is negative, it means to skip N records
  /// if return values is 0, it means end of RG
  int64_t AdviseNext(const int64_t current_rg_processed) {
    if (current_range_variant.index() == 2) {
      return 0;
    }

    auto & current_range = std::get<IntervalRange>(current_range_variant);

    if (current_range.end < current_rg_processed) {
      current_range_variant = range_iter_->NextRange();
      if (current_range_variant.index() == 2) {
        // negative, skip the ramaining rows
        return current_rg_processed - total_rows_to_process_;
      }
    }

    current_range = std::get<IntervalRange>(current_range_variant);

    if (current_range.start > current_rg_processed) {
      // negative, skip
      return current_rg_processed - current_range.start;
    }

    const auto ret = current_range.end - current_rg_processed + 1;
    return ret;
  }

private:
  void AdjustRanges(IntervalRanges& skip_pages, const RowRanges& orig_row_ranges, std::unique_ptr<RowRanges>& ret) {
    std::unique_ptr<IntervalRanges> temp = std::make_unique<IntervalRanges>();

    size_t skipped_rows = 0;
    const auto orig_range_iter = orig_row_ranges.NewIterator();
    auto orig_range_variant = orig_range_iter->NextRange();
    auto skip_iter = skip_pages.GetRanges().begin();
    while (orig_range_variant.index() != 2) {
      const auto & origin_range = std::get<IntervalRange>(orig_range_variant);
      while (skip_iter != skip_pages.GetRanges().end() && skip_iter->IsBefore(origin_range)) {
        skipped_rows += skip_iter->Count();
        ++skip_iter;
      }

      temp->Add(IntervalRange(origin_range.start - skipped_rows, origin_range.end - skipped_rows));
      orig_range_variant = orig_range_iter->NextRange();
    }
    ret = std::move(temp);
  }

  std::unique_ptr<RowRanges> row_ranges_;
  std::unique_ptr<RowRanges::Iterator> range_iter_;
  std::variant<IntervalRange, BitmapRange, End> current_range_variant = End();

  size_t total_rows_to_process_ = 0;
};

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
  static std::shared_ptr<RecordReader> Make(
      const ColumnDescriptor* descr, LevelInfo leaf_info,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(),
      bool read_dictionary = false, bool read_dense_for_nullable = false);

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

  /// \brief Decoded definition levels
  int16_t* def_levels() const {
    return reinterpret_cast<int16_t*>(def_levels_->mutable_data());
  }

  /// \brief Decoded repetition levels
  int16_t* rep_levels() const {
    return reinterpret_cast<int16_t*>(rep_levels_->mutable_data());
  }

  /// \brief Decoded values, including nulls, if any
  /// FLBA and ByteArray types do not use this array and read into their own
  /// builders.
  uint8_t* values() const { return values_->mutable_data(); }

  /// \brief Number of values written, including space left for nulls if any.
  /// If this Reader was constructed with read_dense_for_nullable(), there is no space for
  /// nulls and null_count() will be 0. There is no read-ahead/buffering for values. For
  /// FLBA and ByteArray types this value reflects the values written with the last
  /// ReadRecords call since those readers will reset the values after each call.
  int64_t values_written() const { return values_written_; }

  /// \brief Number of definition / repetition levels (from those that have
  /// been decoded) that have been consumed inside the reader.
  int64_t levels_position() const { return levels_position_; }

  /// \brief Number of definition / repetition levels that have been written
  /// internally in the reader. This may be larger than values_written() because
  /// for repeated fields we need to look at the levels in advance to figure out
  /// the record boundaries.
  int64_t levels_written() const { return levels_written_; }

  /// \brief Number of nulls in the leaf that we have read so far into the
  /// values vector. This is only valid when !read_dense_for_nullable(). When
  /// read_dense_for_nullable() it will always be 0.
  int64_t null_count() const { return null_count_; }

  /// \brief True if the leaf values are nullable
  bool nullable_values() const { return nullable_values_; }

  /// \brief True if reading directly as Arrow dictionary-encoded
  bool read_dictionary() const { return read_dictionary_; }

  /// \brief True if reading dense for nullable columns.
  bool read_dense_for_nullable() const { return read_dense_for_nullable_; }

  void reset_current_rg_processed_records() { current_rg_processed_records_ = 0; }

  void set_record_skipper(std::shared_ptr<RecordSkipper> skipper_) { skipper_ = skipper_; }

 protected:
  /// \brief Indicates if we can have nullable values. Note that repeated fields
  /// may or may not be nullable.
  bool nullable_values_;

  bool at_record_start_;
  int64_t records_read_;

  int64_t current_rg_processed_records_;  // counting both read and skip records

  /// \brief Stores values. These values are populated based on each ReadRecords
  /// call. No extra values are buffered for the next call. SkipRecords will not
  /// add any value to this buffer.
  std::shared_ptr<::arrow::ResizableBuffer> values_;
  /// \brief False for BYTE_ARRAY, in which case we don't allocate the values
  /// buffer and we directly read into builder classes.
  bool uses_values_;

  /// \brief Values that we have read into 'values_' + 'null_count_'.
  int64_t values_written_;
  int64_t values_capacity_;
  int64_t null_count_;

  /// \brief Each bit corresponds to one element in 'values_' and specifies if it
  /// is null or not null. Not set if read_dense_for_nullable_ is true.
  std::shared_ptr<::arrow::ResizableBuffer> valid_bits_;

  /// \brief Buffer for definition levels. May contain more levels than
  /// is actually read. This is because we read levels ahead to
  /// figure out record boundaries for repeated fields.
  /// For flat required fields, 'def_levels_' and 'rep_levels_' are not
  ///  populated. For non-repeated fields 'rep_levels_' is not populated.
  /// 'def_levels_' and 'rep_levels_' must be of the same size if present.
  std::shared_ptr<::arrow::ResizableBuffer> def_levels_;
  /// \brief Buffer for repetition levels. Only populated for repeated
  /// fields.
  std::shared_ptr<::arrow::ResizableBuffer> rep_levels_;

  /// \brief Number of definition / repetition levels that have been written
  /// internally in the reader. This may be larger than values_written() since
  /// for repeated fields we need to look at the levels in advance to figure out
  /// the record boundaries.
  int64_t levels_written_;
  /// \brief Position of the next level that should be consumed.
  int64_t levels_position_;
  int64_t levels_capacity_;

  bool read_dictionary_ = false;
  // If true, we will not leave any space for the null values in the values_
  // vector.
  bool read_dense_for_nullable_ = false;

  std::shared_ptr<RecordSkipper> skipper_ = NULLPTR;
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
