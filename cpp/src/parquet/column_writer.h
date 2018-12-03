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

#ifndef PARQUET_COLUMN_WRITER_H
#define PARQUET_COLUMN_WRITER_H

#include <memory>
#include <vector>

#include "parquet/column_page.h"
#include "parquet/encoding.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/statistics.h"
#include "parquet/types.h"
#include "parquet/util/macros.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace arrow {

namespace BitUtil {
class BitWriter;
}  // namespace BitUtil

namespace util {
class RleEncoder;
}  // namespace util

}  // namespace arrow

namespace parquet {

class PARQUET_EXPORT LevelEncoder {
 public:
  LevelEncoder();
  ~LevelEncoder();

  static int MaxBufferSize(Encoding::type encoding, int16_t max_level,
                           int num_buffered_values);

  // Initialize the LevelEncoder.
  void Init(Encoding::type encoding, int16_t max_level, int num_buffered_values,
            uint8_t* data, int data_size);

  // Encodes a batch of levels from an array and returns the number of levels encoded
  int Encode(int batch_size, const int16_t* levels);

  int32_t len() {
    if (encoding_ != Encoding::RLE) {
      throw ParquetException("Only implemented for RLE encoding");
    }
    return rle_length_;
  }

 private:
  int bit_width_;
  int rle_length_;
  Encoding::type encoding_;
  std::unique_ptr<::arrow::util::RleEncoder> rle_encoder_;
  std::unique_ptr<::arrow::BitUtil::BitWriter> bit_packed_encoder_;
};

class PARQUET_EXPORT PageWriter {
 public:
  virtual ~PageWriter() {}

  static std::unique_ptr<PageWriter> Open(
      OutputStream* sink, Compression::type codec, ColumnChunkMetaDataBuilder* metadata,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool(),
      bool buffered_row_group = false);

  // The Column Writer decides if dictionary encoding is used if set and
  // if the dictionary encoding has fallen back to default encoding on reaching dictionary
  // page limit
  virtual void Close(bool has_dictionary, bool fallback) = 0;

  virtual int64_t WriteDataPage(const CompressedDataPage& page) = 0;

  virtual int64_t WriteDictionaryPage(const DictionaryPage& page) = 0;

  virtual bool has_compressor() = 0;

  virtual void Compress(const Buffer& src_buffer, ResizableBuffer* dest_buffer) = 0;
};

static constexpr int WRITE_BATCH_SIZE = 1000;
class PARQUET_EXPORT ColumnWriter {
 public:
  ColumnWriter(ColumnChunkMetaDataBuilder*, std::unique_ptr<PageWriter>,
               bool has_dictionary, Encoding::type encoding,
               const WriterProperties* properties);

  virtual ~ColumnWriter() = default;

  static std::shared_ptr<ColumnWriter> Make(ColumnChunkMetaDataBuilder*,
                                            std::unique_ptr<PageWriter>,
                                            const WriterProperties* properties);

  Type::type type() const { return descr_->physical_type(); }

  const ColumnDescriptor* descr() const { return descr_; }

  /**
   * Closes the ColumnWriter, commits any buffered values to pages.
   *
   * @return Total size of the column in bytes
   */
  int64_t Close();

  int64_t rows_written() const { return rows_written_; }

  // Only considers the size of the compressed pages + page header
  // Some values might be still buffered an not written to a page yet
  int64_t total_compressed_bytes() const { return total_compressed_bytes_; }

  int64_t total_bytes_written() const { return total_bytes_written_; }

  const WriterProperties* properties() { return properties_; }

 protected:
  virtual std::shared_ptr<Buffer> GetValuesBuffer() = 0;

  // Serializes Dictionary Page if enabled
  virtual void WriteDictionaryPage() = 0;

  // Checks if the Dictionary Page size limit is reached
  // If the limit is reached, the Dictionary and Data Pages are serialized
  // The encoding is switched to PLAIN

  virtual void CheckDictionarySizeLimit() = 0;

  // Plain-encoded statistics of the current page
  virtual EncodedStatistics GetPageStatistics() = 0;

  // Plain-encoded statistics of the whole chunk
  virtual EncodedStatistics GetChunkStatistics() = 0;

  // Merges page statistics into chunk statistics, then resets the values
  virtual void ResetPageStatistics() = 0;

  // Adds Data Pages to an in memory buffer in dictionary encoding mode
  // Serializes the Data Pages in other encoding modes
  void AddDataPage();

  // Serializes Data Pages
  void WriteDataPage(const CompressedDataPage& page);

  // Write multiple definition levels
  void WriteDefinitionLevels(int64_t num_levels, const int16_t* levels);

  // Write multiple repetition levels
  void WriteRepetitionLevels(int64_t num_levels, const int16_t* levels);

  // RLE encode the src_buffer into dest_buffer and return the encoded size
  int64_t RleEncodeLevels(const Buffer& src_buffer, ResizableBuffer* dest_buffer,
                          int16_t max_level);

  // Serialize the buffered Data Pages
  void FlushBufferedDataPages();

  ColumnChunkMetaDataBuilder* metadata_;
  const ColumnDescriptor* descr_;

  std::unique_ptr<PageWriter> pager_;

  bool has_dictionary_;
  Encoding::type encoding_;
  const WriterProperties* properties_;

  LevelEncoder level_encoder_;

  ::arrow::MemoryPool* allocator_;

  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int64_t num_buffered_values_;

  // The total number of stored values. For repeated or optional values, this
  // number may be lower than num_buffered_values_.
  int64_t num_buffered_encoded_values_;

  // Total number of rows written with this ColumnWriter
  int rows_written_;

  // Records the total number of bytes written by the serializer
  int64_t total_bytes_written_;

  // Records the current number of compressed bytes in a column
  int64_t total_compressed_bytes_;

  // Flag to check if the Writer has been closed
  bool closed_;

  // Flag to infer if dictionary encoding has fallen back to PLAIN
  bool fallback_;

  std::unique_ptr<InMemoryOutputStream> definition_levels_sink_;
  std::unique_ptr<InMemoryOutputStream> repetition_levels_sink_;

  std::shared_ptr<ResizableBuffer> definition_levels_rle_;
  std::shared_ptr<ResizableBuffer> repetition_levels_rle_;

  std::shared_ptr<ResizableBuffer> uncompressed_data_;
  std::shared_ptr<ResizableBuffer> compressed_data_;

  std::vector<CompressedDataPage> data_pages_;

 private:
  void InitSinks();
};

// API to write values to a single column. This is the main client facing API.
template <typename DType>
class PARQUET_TEMPLATE_CLASS_EXPORT TypedColumnWriter : public ColumnWriter {
 public:
  typedef typename DType::c_type T;

  TypedColumnWriter(ColumnChunkMetaDataBuilder* metadata,
                    std::unique_ptr<PageWriter> pager, Encoding::type encoding,
                    const WriterProperties* properties);

  // Write a batch of repetition levels, definition levels, and values to the
  // column.
  void WriteBatch(int64_t num_values, const int16_t* def_levels,
                  const int16_t* rep_levels, const T* values);

  /// Write a batch of repetition levels, definition levels, and values to the
  /// column.
  ///
  /// In comparision to WriteBatch the length of repetition and definition levels
  /// is the same as of the number of values read for max_definition_level == 1.
  /// In the case of max_definition_level > 1, the repetition and definition
  /// levels are larger than the values but the values include the null entries
  /// with definition_level == (max_definition_level - 1). Thus we have to differentiate
  /// in the parameters of this function if the input has the length of num_values or the
  /// _number of rows in the lowest nesting level_.
  ///
  /// In the case that the most inner node in the Parquet is required, the _number of rows
  /// in the lowest nesting level_ is equal to the number of non-null values. If the
  /// inner-most schema node is optional, the _number of rows in the lowest nesting level_
  /// also includes all values with definition_level == (max_definition_level - 1).
  ///
  /// @param num_values number of levels to write.
  /// @param def_levels The Parquet definiton levels, length is num_values
  /// @param rep_levels The Parquet repetition levels, length is num_values
  /// @param valid_bits Bitmap that indicates if the row is null on the lowest nesting
  ///   level. The length is number of rows in the lowest nesting level.
  /// @param valid_bits_offset The offset in bits of the valid_bits where the
  ///   first relevant bit resides.
  /// @param values The values in the lowest nested level including
  ///   spacing for nulls on the lowest levels; input has the length
  ///   of the number of rows on the lowest nesting level.
  void WriteBatchSpaced(int64_t num_values, const int16_t* def_levels,
                        const int16_t* rep_levels, const uint8_t* valid_bits,
                        int64_t valid_bits_offset, const T* values);

  // Estimated size of the values that are not written to a page yet
  int64_t EstimatedBufferedValueBytes() const {
    return current_encoder_->EstimatedDataEncodedSize();
  }

 protected:
  std::shared_ptr<Buffer> GetValuesBuffer() override {
    return current_encoder_->FlushValues();
  }
  void WriteDictionaryPage() override;
  void CheckDictionarySizeLimit() override;
  EncodedStatistics GetPageStatistics() override;
  EncodedStatistics GetChunkStatistics() override;
  void ResetPageStatistics() override;

 private:
  int64_t WriteMiniBatch(int64_t num_values, const int16_t* def_levels,
                         const int16_t* rep_levels, const T* values);

  int64_t WriteMiniBatchSpaced(int64_t num_values, const int16_t* def_levels,
                               const int16_t* rep_levels, const uint8_t* valid_bits,
                               int64_t valid_bits_offset, const T* values,
                               int64_t* num_spaced_written);

  typedef Encoder<DType> EncoderType;

  // Write values to a temporary buffer before they are encoded into pages
  void WriteValues(int64_t num_values, const T* values);
  void WriteValuesSpaced(int64_t num_values, const uint8_t* valid_bits,
                         int64_t valid_bits_offset, const T* values);
  std::unique_ptr<EncoderType> current_encoder_;

  typedef TypedRowGroupStatistics<DType> TypedStats;
  std::unique_ptr<TypedStats> page_statistics_;
  std::unique_ptr<TypedStats> chunk_statistics_;
};

typedef TypedColumnWriter<BooleanType> BoolWriter;
typedef TypedColumnWriter<Int32Type> Int32Writer;
typedef TypedColumnWriter<Int64Type> Int64Writer;
typedef TypedColumnWriter<Int96Type> Int96Writer;
typedef TypedColumnWriter<FloatType> FloatWriter;
typedef TypedColumnWriter<DoubleType> DoubleWriter;
typedef TypedColumnWriter<ByteArrayType> ByteArrayWriter;
typedef TypedColumnWriter<FLBAType> FixedLenByteArrayWriter;

PARQUET_EXTERN_TEMPLATE TypedColumnWriter<BooleanType>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<Int32Type>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<Int64Type>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<Int96Type>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<FloatType>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<DoubleType>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<ByteArrayType>;
PARQUET_EXTERN_TEMPLATE TypedColumnWriter<FLBAType>;

}  // namespace parquet

#endif  // PARQUET_COLUMN_READER_H
