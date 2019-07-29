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

#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/types.h"

namespace arrow {

namespace BitUtil {
class BitWriter;
}  // namespace BitUtil

namespace util {
class RleEncoder;
}  // namespace util

}  // namespace arrow

namespace parquet {

class ColumnDescriptor;
class CompressedDataPage;
class DictionaryPage;
class ColumnChunkMetaDataBuilder;
class WriterProperties;

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
      const std::shared_ptr<ArrowOutputStream>& sink, Compression::type codec,
      ColumnChunkMetaDataBuilder* metadata,
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
  virtual ~ColumnWriter() = default;

  static std::shared_ptr<ColumnWriter> Make(ColumnChunkMetaDataBuilder*,
                                            std::unique_ptr<PageWriter>,
                                            const WriterProperties* properties);

  /// \brief Closes the ColumnWriter, commits any buffered values to pages.
  /// \return Total size of the column in bytes
  virtual int64_t Close() = 0;

  /// \brief The physical Parquet type of the column
  virtual Type::type type() const = 0;

  /// \brief The schema for the column
  virtual const ColumnDescriptor* descr() const = 0;

  /// \brief The number of rows written so far
  virtual int64_t rows_written() const = 0;

  /// \brief The total size of the compressed pages + page headers. Some values
  /// might be still buffered an not written to a page yet
  virtual int64_t total_compressed_bytes() const = 0;

  /// \brief The total number of bytes written as serialized data and
  /// dictionary pages to the ColumnChunk so far
  virtual int64_t total_bytes_written() const = 0;

  /// \brief The file-level writer properties
  virtual const WriterProperties* properties() = 0;
};

// API to write values to a single column. This is the main client facing API.
template <typename DType>
class TypedColumnWriter : public ColumnWriter {
 public:
  using T = typename DType::c_type;

  // Write a batch of repetition levels, definition levels, and values to the
  // column.
  virtual void WriteBatch(int64_t num_values, const int16_t* def_levels,
                          const int16_t* rep_levels, const T* values) = 0;

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
  virtual void WriteBatchSpaced(int64_t num_values, const int16_t* def_levels,
                                const int16_t* rep_levels, const uint8_t* valid_bits,
                                int64_t valid_bits_offset, const T* values) = 0;

  // Estimated size of the values that are not written to a page yet
  virtual int64_t EstimatedBufferedValueBytes() const = 0;
};

using BoolWriter = TypedColumnWriter<BooleanType>;
using Int32Writer = TypedColumnWriter<Int32Type>;
using Int64Writer = TypedColumnWriter<Int64Type>;
using Int96Writer = TypedColumnWriter<Int96Type>;
using FloatWriter = TypedColumnWriter<FloatType>;
using DoubleWriter = TypedColumnWriter<DoubleType>;
using ByteArrayWriter = TypedColumnWriter<ByteArrayType>;
using FixedLenByteArrayWriter = TypedColumnWriter<FLBAType>;

}  // namespace parquet
