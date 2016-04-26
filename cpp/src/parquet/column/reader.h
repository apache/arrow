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
#include <cstdint>
#include <cstring>
#include <memory>
#include <unordered_map>

#include "parquet/column/levels.h"
#include "parquet/column/page.h"
#include "parquet/encodings/decoder.h"
#include "parquet/exception.h"
#include "parquet/schema/descriptor.h"
#include "parquet/types.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

class ColumnReader {
 public:
  ColumnReader(const ColumnDescriptor*, std::unique_ptr<PageReader>,
      MemoryAllocator* allocator = default_allocator());

  static std::shared_ptr<ColumnReader> Make(const ColumnDescriptor*,
      std::unique_ptr<PageReader>, MemoryAllocator* allocator = default_allocator());

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

  Type::type type() const {
    return descr_->physical_type();
  }

  const ColumnDescriptor* descr() const {
    return descr_;
  }

 protected:
  virtual bool ReadNewPage() = 0;

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  int64_t ReadDefinitionLevels(int64_t batch_size, int16_t* levels);

  // Read multiple repetition levels into preallocated memory
  //
  // Returns the number of decoded repetition levels
  int64_t ReadRepetitionLevels(int64_t batch_size, int16_t* levels);

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
  int num_buffered_values_;

  // The number of values from the current data page that have been decoded
  // into memory
  int num_decoded_values_;

  MemoryAllocator* allocator_;
};

// API to read values from a single column. This is the main client facing API.
template <int TYPE>
class TypedColumnReader : public ColumnReader {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  TypedColumnReader(const ColumnDescriptor* schema,
      std::unique_ptr<PageReader> pager,
      MemoryAllocator* allocator = default_allocator()) :
      ColumnReader(schema, std::move(pager), allocator),
      current_decoder_(NULL) {
  }

  // Read a batch of repetition levels, definition levels, and values from the
  // column.
  //
  // Since null values are not stored in the values, the number of values read
  // may be less than the number of repetition and definition levels. With
  // nested data this is almost certainly true.
  //
  // To fully exhaust a row group, you must read batches until the number of
  // values read reaches the number of stored values according to the metadata.
  //
  // This API is the same for both V1 and V2 of the DataPage
  //
  // @returns: actual number of levels read (see values_read for number of values read)
  int64_t ReadBatch(int32_t batch_size, int16_t* def_levels, int16_t* rep_levels,
      T* values, int64_t* values_read);

 private:
  typedef Decoder<TYPE> DecoderType;

  // Advance to the next data page
  virtual bool ReadNewPage();

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValues(int64_t batch_size, T* out);

  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::shared_ptr<DecoderType> > decoders_;

  void ConfigureDictionary(const DictionaryPage* page);

  DecoderType* current_decoder_;
};


template <int TYPE>
inline int64_t TypedColumnReader<TYPE>::ReadValues(int64_t batch_size, T* out) {
  int64_t num_decoded = current_decoder_->Decode(out, batch_size);
  return num_decoded;
}

template <int TYPE>
inline int64_t TypedColumnReader<TYPE>::ReadBatch(int batch_size, int16_t* def_levels,
    int16_t* rep_levels, T* values, int64_t* values_read) {
  // HasNext invokes ReadNewPage
  if (!HasNext()) {
    *values_read = 0;
    return 0;
  }

  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  batch_size = std::min(batch_size, num_buffered_values_);

  int64_t num_def_levels = 0;
  int64_t num_rep_levels = 0;

  int64_t values_to_read = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0) {
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
  if (descr_->max_repetition_level() > 0) {
    num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);
    if (num_def_levels != num_rep_levels) {
      throw ParquetException("Number of decoded rep / def levels did not match");
    }
  }

  *values_read = ReadValues(values_to_read, values);
  int64_t total_values = std::max(num_def_levels, *values_read);
  num_decoded_values_ += total_values;

  return total_values;
}


typedef TypedColumnReader<Type::BOOLEAN> BoolReader;
typedef TypedColumnReader<Type::INT32> Int32Reader;
typedef TypedColumnReader<Type::INT64> Int64Reader;
typedef TypedColumnReader<Type::INT96> Int96Reader;
typedef TypedColumnReader<Type::FLOAT> FloatReader;
typedef TypedColumnReader<Type::DOUBLE> DoubleReader;
typedef TypedColumnReader<Type::BYTE_ARRAY> ByteArrayReader;
typedef TypedColumnReader<Type::FIXED_LEN_BYTE_ARRAY> FixedLenByteArrayReader;

} // namespace parquet

#endif // PARQUET_COLUMN_READER_H
