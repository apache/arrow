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
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/thrift/parquet_constants.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/util/input_stream.h"
#include "parquet/encodings/encodings.h"
#include "parquet/util/rle-encoding.h"

namespace std {

template <>
struct hash<parquet::Encoding::type> {
  std::size_t operator()(const parquet::Encoding::type& k) const {
    return hash<int>()(static_cast<int>(k));
  }
};

} // namespace std

namespace parquet_cpp {

class Codec;
class Scanner;

class ColumnReader {
 public:
  struct Config {
    int batch_size;

    static Config DefaultConfig() {
      Config config;
      config.batch_size = 128;
      return config;
    }
  };

  ColumnReader(const parquet::ColumnMetaData*, const parquet::SchemaElement*,
      std::unique_ptr<InputStream> stream);

  static std::shared_ptr<ColumnReader> Make(const parquet::ColumnMetaData*,
      const parquet::SchemaElement*, std::unique_ptr<InputStream> stream);

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

  parquet::Type::type type() const {
    return metadata_->type;
  }

  const parquet::ColumnMetaData* metadata() const {
    return metadata_;
  }

  const parquet::SchemaElement* schema() const {
    return schema_;
  }

 protected:
  virtual bool ReadNewPage() = 0;

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  size_t ReadDefinitionLevels(size_t batch_size, int16_t* levels);

  // Read multiple repetition levels into preallocated memory
  //
  // Returns the number of decoded repetition levels
  size_t ReadRepetitionLevels(size_t batch_size, int16_t* levels);

  Config config_;

  const parquet::ColumnMetaData* metadata_;
  const parquet::SchemaElement* schema_;
  std::unique_ptr<InputStream> stream_;

  // Compression codec to use.
  std::unique_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;

  parquet::PageHeader current_page_header_;

  // Not set if full schema for this field has no optional or repeated elements
  std::unique_ptr<RleDecoder> definition_level_decoder_;

  // Not set for flat schemas.
  std::unique_ptr<RleDecoder> repetition_level_decoder_;

  // Temporarily storing this to assist with batch reading
  int16_t max_definition_level_;

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
};

// API to read values from a single column. This is the main client facing API.
template <int TYPE>
class TypedColumnReader : public ColumnReader {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  TypedColumnReader(const parquet::ColumnMetaData* metadata,
      const parquet::SchemaElement* schema, std::unique_ptr<InputStream> stream) :
      ColumnReader(metadata, schema, std::move(stream)),
      current_decoder_(NULL) {
    size_t value_byte_size = type_traits<TYPE>::value_byte_size;
    values_buffer_.resize(config_.batch_size * value_byte_size);
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
  size_t ReadBatch(int batch_size, int16_t* def_levels, int16_t* rep_levels,
      T* values, size_t* values_read);

 private:
  typedef Decoder<TYPE> DecoderType;

  // Advance to the next data page
  virtual bool ReadNewPage();

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  size_t ReadValues(size_t batch_size, T* out);

  // Map of compression type to decompressor object.
  std::unordered_map<parquet::Encoding::type, std::shared_ptr<DecoderType> > decoders_;

  DecoderType* current_decoder_;
  std::vector<uint8_t> values_buffer_;
};


template <int TYPE>
inline size_t TypedColumnReader<TYPE>::ReadValues(size_t batch_size, T* out) {
  size_t num_decoded = current_decoder_->Decode(out, batch_size);
  num_decoded_values_ += num_decoded;
  return num_decoded;
}

template <int TYPE>
inline size_t TypedColumnReader<TYPE>::ReadBatch(int batch_size, int16_t* def_levels,
    int16_t* rep_levels, T* values, size_t* values_read) {
  // HasNext invokes ReadNewPage
  if (!HasNext()) {
    *values_read = 0;
    return 0;
  }

  // TODO(wesm): keep reading data pages until batch_size is reached, or the
  // row group is finished
  batch_size = std::min(batch_size, num_buffered_values_);

  size_t num_def_levels = 0;
  size_t num_rep_levels = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (definition_level_decoder_) {
    num_def_levels = ReadDefinitionLevels(batch_size, def_levels);
  }

  // Not present for non-repeated fields
  if (repetition_level_decoder_) {
    num_rep_levels = ReadRepetitionLevels(batch_size, rep_levels);

    if (num_def_levels != num_rep_levels) {
      throw ParquetException("Number of decoded rep / def levels did not match");
    }
  }

  // TODO(wesm): this tallying of values-to-decode can be performed with better
  // cache-efficiency if fused with the level decoding.
  size_t values_to_read = 0;
  for (size_t i = 0; i < num_def_levels; ++i) {
    if (def_levels[i] == max_definition_level_) {
      ++values_to_read;
    }
  }

  *values_read = ReadValues(values_to_read, values);

  return num_def_levels;
}


typedef TypedColumnReader<parquet::Type::BOOLEAN> BoolReader;
typedef TypedColumnReader<parquet::Type::INT32> Int32Reader;
typedef TypedColumnReader<parquet::Type::INT64> Int64Reader;
typedef TypedColumnReader<parquet::Type::INT96> Int96Reader;
typedef TypedColumnReader<parquet::Type::FLOAT> FloatReader;
typedef TypedColumnReader<parquet::Type::DOUBLE> DoubleReader;
typedef TypedColumnReader<parquet::Type::BYTE_ARRAY> ByteArrayReader;
typedef TypedColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY> FixedLenByteArrayReader;

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_READER_H
