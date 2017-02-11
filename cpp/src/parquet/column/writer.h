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

#include <vector>

#include "parquet/column/levels.h"
#include "parquet/column/page.h"
#include "parquet/column/properties.h"
#include "parquet/column/statistics.h"
#include "parquet/encoding.h"
#include "parquet/file/metadata.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"
#include "parquet/util/visibility.h"

namespace parquet {

static constexpr int WRITE_BATCH_SIZE = 1000;
class PARQUET_EXPORT ColumnWriter {
 public:
  ColumnWriter(ColumnChunkMetaDataBuilder*, std::unique_ptr<PageWriter>,
      int64_t expected_rows, bool has_dictionary, Encoding::type encoding,
      const WriterProperties* properties);

  static std::shared_ptr<ColumnWriter> Make(ColumnChunkMetaDataBuilder*,
      std::unique_ptr<PageWriter>, int64_t expected_rows,
      const WriterProperties* properties);

  Type::type type() const { return descr_->physical_type(); }

  const ColumnDescriptor* descr() const { return descr_; }

  /**
   * Closes the ColumnWriter, commits any buffered values to pages.
   *
   * @return Total size of the column in bytes
   */
  int64_t Close();

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

  std::shared_ptr<Buffer> RleEncodeLevels(
      const std::shared_ptr<Buffer>& buffer, int16_t max_level);

  // Serialize the buffered Data Pages
  void FlushBufferedDataPages();

  ColumnChunkMetaDataBuilder* metadata_;
  const ColumnDescriptor* descr_;

  std::unique_ptr<PageWriter> pager_;

  // The number of rows that should be written in this column chunk.
  int64_t expected_rows_;
  bool has_dictionary_;
  Encoding::type encoding_;
  const WriterProperties* properties_;

  LevelEncoder level_encoder_;

  ::arrow::MemoryPool* allocator_;
  ChunkedAllocator pool_;

  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int num_buffered_values_;

  // The total number of stored values. For repeated or optional values, this
  // number may be lower than num_buffered_values_.
  int num_buffered_encoded_values_;

  // Total number of rows written with this ColumnWriter
  int num_rows_;

  // Records the total number of bytes written by the serializer
  int total_bytes_written_;

  // Flag to check if the Writer has been closed
  bool closed_;

  // Flag to infer if dictionary encoding has fallen back to PLAIN
  bool fallback_;

  std::unique_ptr<InMemoryOutputStream> definition_levels_sink_;
  std::unique_ptr<InMemoryOutputStream> repetition_levels_sink_;

  std::vector<CompressedDataPage> data_pages_;

 private:
  void InitSinks();
};

// API to write values to a single column. This is the main client facing API.
template <typename DType>
class PARQUET_EXPORT TypedColumnWriter : public ColumnWriter {
 public:
  typedef typename DType::c_type T;

  TypedColumnWriter(ColumnChunkMetaDataBuilder* metadata,
      std::unique_ptr<PageWriter> pager, int64_t expected_rows, Encoding::type encoding,
      const WriterProperties* properties);

  // Write a batch of repetition levels, definition levels, and values to the
  // column.
  void WriteBatch(int64_t num_values, const int16_t* def_levels,
      const int16_t* rep_levels, const T* values);

  // Write a batch of repetition levels, definition levels, and values to the
  // column.
  void WriteBatchSpaced(int64_t num_values, const int16_t* def_levels,
      const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset,
      const T* values);

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
      const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset,
      const T* values, int64_t* num_spaced_written);

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

extern template class PARQUET_EXPORT TypedColumnWriter<BooleanType>;
extern template class PARQUET_EXPORT TypedColumnWriter<Int32Type>;
extern template class PARQUET_EXPORT TypedColumnWriter<Int64Type>;
extern template class PARQUET_EXPORT TypedColumnWriter<Int96Type>;
extern template class PARQUET_EXPORT TypedColumnWriter<FloatType>;
extern template class PARQUET_EXPORT TypedColumnWriter<DoubleType>;
extern template class PARQUET_EXPORT TypedColumnWriter<ByteArrayType>;
extern template class PARQUET_EXPORT TypedColumnWriter<FLBAType>;

}  // namespace parquet

#endif  // PARQUET_COLUMN_READER_H
