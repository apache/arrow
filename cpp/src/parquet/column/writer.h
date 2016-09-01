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
#include "parquet/encodings/encoder.h"
#include "parquet/schema/descriptor.h"
#include "parquet/types.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/util/mem-pool.h"
#include "parquet/util/output.h"
#include "parquet/util/visibility.h"

namespace parquet {

class PARQUET_EXPORT ColumnWriter {
 public:
  ColumnWriter(const ColumnDescriptor*, std::unique_ptr<PageWriter>,
      int64_t expected_rows, bool has_dictionary, Encoding::type encoding,
      const WriterProperties* properties);

  static std::shared_ptr<ColumnWriter> Make(const ColumnDescriptor*,
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
  virtual void WriteDictionaryPage() = 0;

  void AddDataPage();
  void WriteDataPage(const DataPage& page);

  // Write multiple definition levels
  void WriteDefinitionLevels(int64_t num_levels, const int16_t* levels);

  // Write multiple repetition levels
  void WriteRepetitionLevels(int64_t num_levels, const int16_t* levels);

  std::shared_ptr<Buffer> RleEncodeLevels(
      const std::shared_ptr<Buffer>& buffer, int16_t max_level);

  const ColumnDescriptor* descr_;

  std::unique_ptr<PageWriter> pager_;

  // The number of rows that should be written in this column chunk.
  int64_t expected_rows_;
  bool has_dictionary_;
  Encoding::type encoding_;
  const WriterProperties* properties_;

  LevelEncoder level_encoder_;

  MemoryAllocator* allocator_;
  MemPool pool_;

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

  int total_bytes_written_;
  bool closed_;

  std::unique_ptr<InMemoryOutputStream> definition_levels_sink_;
  std::unique_ptr<InMemoryOutputStream> repetition_levels_sink_;

 private:
  void InitSinks();

  std::vector<DataPage> data_pages_;
};

// API to write values to a single column. This is the main client facing API.
template <typename DType>
class PARQUET_EXPORT TypedColumnWriter : public ColumnWriter {
 public:
  typedef typename DType::c_type T;

  TypedColumnWriter(const ColumnDescriptor* schema, std::unique_ptr<PageWriter> pager,
      int64_t expected_rows, Encoding::type encoding, const WriterProperties* properties);

  // Write a batch of repetition levels, definition levels, and values to the
  // column.
  void WriteBatch(int64_t num_values, const int16_t* def_levels,
      const int16_t* rep_levels, const T* values);

 protected:
  std::shared_ptr<Buffer> GetValuesBuffer() override {
    return current_encoder_->FlushValues();
  }
  void WriteDictionaryPage() override;

 private:
  typedef Encoder<DType> EncoderType;

  // Write values to a temporary buffer before they are encoded into pages
  void WriteValues(int64_t num_values, const T* values);

  // Map of encoding type to the respective encoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::shared_ptr<EncoderType>> encoders_;

  std::unique_ptr<EncoderType> current_encoder_;
};

template <typename DType>
inline void TypedColumnWriter<DType>::WriteBatch(int64_t num_values,
    const int16_t* def_levels, const int16_t* rep_levels, const T* values) {
  int64_t values_to_write = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0) {
    for (int64_t i = 0; i < num_values; ++i) {
      if (def_levels[i] == descr_->max_definition_level()) { ++values_to_write; }
    }

    WriteDefinitionLevels(num_values, def_levels);
  } else {
    // Required field, write all values
    values_to_write = num_values;
  }

  // Not present for non-repeated fields
  if (descr_->max_repetition_level() > 0) {
    // A row could include more than one value
    // Count the occasions where we start a new row
    for (int64_t i = 0; i < num_values; ++i) {
      if (rep_levels[i] == 0) { num_rows_++; }
    }

    WriteRepetitionLevels(num_values, rep_levels);
  } else {
    // Each value is exactly one row
    num_rows_ += num_values;
  }

  if (num_rows_ > expected_rows_) {
    throw ParquetException("More rows were written in the column chunk then expected");
  }

  WriteValues(values_to_write, values);

  num_buffered_values_ += num_values;
  num_buffered_encoded_values_ += values_to_write;

  if (current_encoder_->EstimatedDataEncodedSize() >= properties_->data_pagesize()) {
    AddDataPage();
  }
}

template <typename DType>
void TypedColumnWriter<DType>::WriteValues(int64_t num_values, const T* values) {
  current_encoder_->Put(values, num_values);
}

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
