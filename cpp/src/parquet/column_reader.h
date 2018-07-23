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

#ifndef PARQUET_COLUMN_READER_H
#define PARQUET_COLUMN_READER_H

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit-util.h>

#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace arrow {

class BitReader;
class RleDecoder;

}  // namespace arrow

namespace parquet {

// 16 MB is the default maximum page header size
static constexpr uint32_t kDefaultMaxPageHeaderSize = 16 * 1024 * 1024;

// 16 KB is the default expected page header size
static constexpr uint32_t kDefaultPageHeaderSize = 16 * 1024;

namespace BitUtil = ::arrow::BitUtil;

class PARQUET_EXPORT LevelDecoder {
 public:
  LevelDecoder();
  ~LevelDecoder();

  // Initialize the LevelDecoder state with new data
  // and return the number of bytes consumed
  int SetData(Encoding::type encoding, int16_t max_level, int num_buffered_values,
              const uint8_t* data);

  // Decodes a batch of levels into an array and returns the number of levels decoded
  int Decode(int batch_size, int16_t* levels);

 private:
  int bit_width_;
  int num_values_remaining_;
  Encoding::type encoding_;
  std::unique_ptr<::arrow::RleDecoder> rle_decoder_;
  std::unique_ptr<::arrow::BitReader> bit_packed_decoder_;
};

// Abstract page iterator interface. This way, we can feed column pages to the
// ColumnReader through whatever mechanism we choose
class PARQUET_EXPORT PageReader {
 public:
  virtual ~PageReader() = default;

  static std::unique_ptr<PageReader> Open(
      std::unique_ptr<InputStream> stream, int64_t total_num_rows,
      Compression::type codec,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  // @returns: shared_ptr<Page>(nullptr) on EOS, std::shared_ptr<Page>
  // containing new Page otherwise
  virtual std::shared_ptr<Page> NextPage() = 0;

  virtual void set_max_page_header_size(uint32_t size) = 0;
};

class PARQUET_EXPORT ColumnReader {
 public:
  ColumnReader(const ColumnDescriptor*, std::unique_ptr<PageReader>,
               ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());
  virtual ~ColumnReader();

  static std::shared_ptr<ColumnReader> Make(
      const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  // Returns true if there are still values in this column.
  bool HasNext() {
    // Either there is no data page available yet, or the data page has been
    // exhausted
    if (num_buffered_values_ == 0 || num_decoded_values_ == num_buffered_values_) {
      if (!ReadNewPage() || num_buffered_values_ == 0) {
        return false;
      }
    }
    return true;
  }

  Type::type type() const { return descr_->physical_type(); }

  const ColumnDescriptor* descr() const { return descr_; }

 protected:
  virtual bool ReadNewPage() = 0;

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels);

  // Read multiple repetition levels into preallocated memory
  // Returns the number of decoded repetition levels
  int64_t ReadRepetitionLevels(int64_t batch_size, int16_t* levels);

  int64_t available_values_current_page() const {
    return num_buffered_values_ - num_decoded_values_;
  }

  void ConsumeBufferedValues(int64_t num_values) { num_decoded_values_ += num_values; }

  const ColumnDescriptor* descr_;

  std::unique_ptr<PageReader> pager_;
  std::shared_ptr<Page> current_page_;

  // Not set if full schema for this field has no optional or repeated elements
  LevelDecoder definition_level_decoder_;

  // Not set for flat schemas.
  LevelDecoder repetition_level_decoder_;

  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int64_t num_buffered_values_;

  // The number of values from the current data page that have been decoded
  // into memory
  int64_t num_decoded_values_;

  ::arrow::MemoryPool* pool_;
};

namespace internal {

static inline void DefinitionLevelsToBitmap(
    const int16_t* def_levels, int64_t num_def_levels, const int16_t max_definition_level,
    const int16_t max_repetition_level, int64_t* values_read, int64_t* null_count,
    uint8_t* valid_bits, const int64_t valid_bits_offset) {
  ::arrow::internal::BitmapWriter valid_bits_writer(valid_bits, valid_bits_offset,
                                                    num_def_levels);

  // TODO(itaiin): As an interim solution we are splitting the code path here
  // between repeated+flat column reads, and non-repeated+nested reads.
  // Those paths need to be merged in the future
  for (int i = 0; i < num_def_levels; ++i) {
    if (def_levels[i] == max_definition_level) {
      valid_bits_writer.Set();
    } else if (max_repetition_level > 0) {
      // repetition+flat case
      if (def_levels[i] == (max_definition_level - 1)) {
        valid_bits_writer.Clear();
        *null_count += 1;
      } else {
        continue;
      }
    } else {
      // non-repeated+nested case
      if (def_levels[i] < max_definition_level) {
        valid_bits_writer.Clear();
        *null_count += 1;
      } else {
        throw ParquetException("definition level exceeds maximum");
      }
    }

    valid_bits_writer.Next();
  }
  valid_bits_writer.Finish();
  *values_read = valid_bits_writer.position();
}

}  // namespace internal

// API to read values from a single column. This is a main client facing API.
template <typename DType>
class PARQUET_EXPORT TypedColumnReader : public ColumnReader {
 public:
  typedef typename DType::c_type T;

  TypedColumnReader(const ColumnDescriptor* schema, std::unique_ptr<PageReader> pager,
                    ::arrow::MemoryPool* pool = ::arrow::default_memory_pool())
      : ColumnReader(schema, std::move(pager), pool), current_decoder_(nullptr) {}

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
  int64_t ReadBatch(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                    T* values, int64_t* values_read);

  /// Read a batch of repetition levels, definition levels, and values from the
  /// column and leave spaces for null entries on the lowest level in the values
  /// buffer.
  ///
  /// In comparision to ReadBatch the length of repetition and definition levels
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
  int64_t ReadBatchSpaced(int64_t batch_size, int16_t* def_levels, int16_t* rep_levels,
                          T* values, uint8_t* valid_bits, int64_t valid_bits_offset,
                          int64_t* levels_read, int64_t* values_read,
                          int64_t* null_count);

  // Skip reading levels
  // Returns the number of levels skipped
  int64_t Skip(int64_t num_rows_to_skip);

 private:
  typedef Decoder<DType> DecoderType;

  // Advance to the next data page
  bool ReadNewPage() override;

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValues(int64_t batch_size, T* out);

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*, leaving spaces for null entries according
  // to the def_levels.
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValuesSpaced(int64_t batch_size, T* out, int64_t null_count,
                           uint8_t* valid_bits, int64_t valid_bits_offset);

  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::shared_ptr<DecoderType>> decoders_;

  void ConfigureDictionary(const DictionaryPage* page);

  DecoderType* current_decoder_;
};

// ----------------------------------------------------------------------
// Type column reader implementations

template <typename DType>
inline int64_t TypedColumnReader<DType>::ReadValues(int64_t batch_size, T* out) {
  int64_t num_decoded = current_decoder_->Decode(out, static_cast<int>(batch_size));
  return num_decoded;
}

template <typename DType>
inline int64_t TypedColumnReader<DType>::ReadValuesSpaced(int64_t batch_size, T* out,
                                                          int64_t null_count,
                                                          uint8_t* valid_bits,
                                                          int64_t valid_bits_offset) {
  return current_decoder_->DecodeSpaced(out, static_cast<int>(batch_size),
                                        static_cast<int>(null_count), valid_bits,
                                        valid_bits_offset);
}

template <typename DType>
inline int64_t TypedColumnReader<DType>::ReadBatch(int64_t batch_size,
                                                   int16_t* def_levels,
                                                   int16_t* rep_levels, T* values,
                                                   int64_t* values_read) {
  // HasNext invokes ReadNewPage
  if (!HasNext()) {
    *values_read = 0;
    return 0;
  }

  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  batch_size = std::min(batch_size, num_buffered_values_ - num_decoded_values_);

  int64_t num_def_levels = 0;
  int64_t num_rep_levels = 0;

  int64_t values_to_read = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0 && def_levels) {
    num_def_levels = ReadDefinitionLevels(batch_size, def_levels);
    // TODO(wesm): this tallying of values-to-decode can be performed with better
    // cache-efficiency if fused with the level decoding.
    for (int64_t i = 0; i < num_def_levels; ++i) {
      if (def_levels[i] == descr_->max_definition_level()) {
        ++values_to_read;
      }
    }
  } else {
    // Required field, read all values
    values_to_read = batch_size;
  }

  // Not present for non-repeated fields
  if (descr_->max_repetition_level() > 0 && rep_levels) {
    num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
    if (def_levels && num_def_levels != num_rep_levels) {
      throw ParquetException("Number of decoded rep / def levels did not match");
    }
  }

  *values_read = ReadValues(values_to_read, values);
  int64_t total_values = std::max(num_def_levels, *values_read);
  ConsumeBufferedValues(total_values);

  return total_values;
}

namespace internal {

// TODO(itaiin): another code path split to merge when the general case is done
static inline bool HasSpacedValues(const ColumnDescriptor* descr) {
  if (descr->max_repetition_level() > 0) {
    // repeated+flat case
    return !descr->schema_node()->is_required();
  } else {
    // non-repeated+nested case
    // Find if a node forces nulls in the lowest level along the hierarchy
    const schema::Node* node = descr->schema_node().get();
    while (node) {
      if (node->is_optional()) {
        return true;
      }
      node = node->parent();
    }
    return false;
  }
}

}  // namespace internal

template <typename DType>
inline int64_t TypedColumnReader<DType>::ReadBatchSpaced(
    int64_t batch_size, int16_t* def_levels, int16_t* rep_levels, T* values,
    uint8_t* valid_bits, int64_t valid_bits_offset, int64_t* levels_read,
    int64_t* values_read, int64_t* null_count_out) {
  // HasNext invokes ReadNewPage
  if (!HasNext()) {
    *levels_read = 0;
    *values_read = 0;
    *null_count_out = 0;
    return 0;
  }

  int64_t total_values;
  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  batch_size = std::min(batch_size, num_buffered_values_ - num_decoded_values_);

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0) {
    int64_t num_def_levels = ReadDefinitionLevels(batch_size, def_levels);

    // Not present for non-repeated fields
    if (descr_->max_repetition_level() > 0) {
      int64_t num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
      if (num_def_levels != num_rep_levels) {
        throw ParquetException("Number of decoded rep / def levels did not match");
      }
    }

    const bool has_spaced_values = internal::HasSpacedValues(descr_);

    int64_t null_count = 0;
    if (!has_spaced_values) {
      int values_to_read = 0;
      for (int64_t i = 0; i < num_def_levels; ++i) {
        if (def_levels[i] == descr_->max_definition_level()) {
          ++values_to_read;
        }
      }
      total_values = ReadValues(values_to_read, values);
      for (int64_t i = 0; i < total_values; i++) {
        ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
      }
      *values_read = total_values;
    } else {
      int16_t max_definition_level = descr_->max_definition_level();
      int16_t max_repetition_level = descr_->max_repetition_level();
      internal::DefinitionLevelsToBitmap(def_levels, num_def_levels, max_definition_level,
                                         max_repetition_level, values_read, &null_count,
                                         valid_bits, valid_bits_offset);
      total_values = ReadValuesSpaced(*values_read, values, static_cast<int>(null_count),
                                      valid_bits, valid_bits_offset);
    }
    *levels_read = num_def_levels;
    *null_count_out = null_count;

  } else {
    // Required field, read all values
    total_values = ReadValues(batch_size, values);
    for (int64_t i = 0; i < total_values; i++) {
      ::arrow::BitUtil::SetBit(valid_bits, valid_bits_offset + i);
    }
    *null_count_out = 0;
    *levels_read = total_values;
  }

  ConsumeBufferedValues(*levels_read);
  return total_values;
}

template <typename DType>
int64_t TypedColumnReader<DType>::Skip(int64_t num_rows_to_skip) {
  int64_t rows_to_skip = num_rows_to_skip;
  while (HasNext() && rows_to_skip > 0) {
    // If the number of rows to skip is more than the number of undecoded values, skip the
    // Page.
    if (rows_to_skip > (num_buffered_values_ - num_decoded_values_)) {
      rows_to_skip -= num_buffered_values_ - num_decoded_values_;
      num_decoded_values_ = num_buffered_values_;
    } else {
      // We need to read this Page
      // Jump to the right offset in the Page
      int64_t batch_size = 1024;  // ReadBatch with a smaller memory footprint
      int64_t values_read = 0;

      std::shared_ptr<ResizableBuffer> vals = AllocateBuffer(
          this->pool_, batch_size * type_traits<DType::type_num>::value_byte_size);
      std::shared_ptr<ResizableBuffer> def_levels =
          AllocateBuffer(this->pool_, batch_size * sizeof(int16_t));

      std::shared_ptr<ResizableBuffer> rep_levels =
          AllocateBuffer(this->pool_, batch_size * sizeof(int16_t));

      do {
        batch_size = std::min(batch_size, rows_to_skip);
        values_read = ReadBatch(static_cast<int>(batch_size),
                                reinterpret_cast<int16_t*>(def_levels->mutable_data()),
                                reinterpret_cast<int16_t*>(rep_levels->mutable_data()),
                                reinterpret_cast<T*>(vals->mutable_data()), &values_read);
        rows_to_skip -= values_read;
      } while (values_read > 0 && rows_to_skip > 0);
    }
  }
  return num_rows_to_skip - rows_to_skip;
}

// ----------------------------------------------------------------------
// Template instantiations

typedef TypedColumnReader<BooleanType> BoolReader;
typedef TypedColumnReader<Int32Type> Int32Reader;
typedef TypedColumnReader<Int64Type> Int64Reader;
typedef TypedColumnReader<Int96Type> Int96Reader;
typedef TypedColumnReader<FloatType> FloatReader;
typedef TypedColumnReader<DoubleType> DoubleReader;
typedef TypedColumnReader<ByteArrayType> ByteArrayReader;
typedef TypedColumnReader<FLBAType> FixedLenByteArrayReader;

extern template class PARQUET_EXPORT TypedColumnReader<BooleanType>;
extern template class PARQUET_EXPORT TypedColumnReader<Int32Type>;
extern template class PARQUET_EXPORT TypedColumnReader<Int64Type>;
extern template class PARQUET_EXPORT TypedColumnReader<Int96Type>;
extern template class PARQUET_EXPORT TypedColumnReader<FloatType>;
extern template class PARQUET_EXPORT TypedColumnReader<DoubleType>;
extern template class PARQUET_EXPORT TypedColumnReader<ByteArrayType>;
extern template class PARQUET_EXPORT TypedColumnReader<FLBAType>;

}  // namespace parquet

#endif  // PARQUET_COLUMN_READER_H
