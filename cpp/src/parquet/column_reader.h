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

#include <exception>
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

  ColumnReader(const parquet::ColumnMetaData*,
      const parquet::SchemaElement*, InputStream* stream);

  virtual ~ColumnReader();

  static std::shared_ptr<ColumnReader> Make(const parquet::ColumnMetaData*,
      const parquet::SchemaElement*, InputStream* stream);

  virtual bool ReadNewPage() = 0;

  // Returns true if there are still values in this column.
  bool HasNext() {
    if (num_buffered_values_ == 0) {
      ReadNewPage();
      if (num_buffered_values_ == 0) return false;
    }
    return true;
  }

  parquet::Type::type type() const {
    return metadata_->type;
  }

  const parquet::ColumnMetaData* metadata() const {
    return metadata_;
  }

 protected:
  // Reads the next definition and repetition level. Returns true if the value is NULL.
  bool ReadDefinitionRepetitionLevels(int* def_level, int* rep_level);

  Config config_;

  const parquet::ColumnMetaData* metadata_;
  const parquet::SchemaElement* schema_;
  InputStream* stream_;

  // Compression codec to use.
  std::unique_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;

  parquet::PageHeader current_page_header_;

  // Not set if field is required.
  std::unique_ptr<RleDecoder> definition_level_decoder_;
  // Not set for flat schemas.
  std::unique_ptr<RleDecoder> repetition_level_decoder_;
  int num_buffered_values_;

  int num_decoded_values_;
  int buffered_values_offset_;
};


// API to read values from a single column. This is the main client facing API.
template <int TYPE>
class TypedColumnReader : public ColumnReader {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  TypedColumnReader(const parquet::ColumnMetaData* metadata,
      const parquet::SchemaElement* schema, InputStream* stream) :
      ColumnReader(metadata, schema, stream),
      current_decoder_(NULL) {
    size_t value_byte_size = type_traits<TYPE>::value_byte_size;
    values_buffer_.resize(config_.batch_size * value_byte_size);
  }

  // Returns the next value of this type.
  // TODO: batchify this interface.
  T NextValue(int* def_level, int* rep_level) {
    if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return T();
    if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
    return reinterpret_cast<T*>(&values_buffer_[0])[buffered_values_offset_++];
  }

 private:
  void BatchDecode();

  virtual bool ReadNewPage();

  typedef Decoder<TYPE> DecoderType;

  // Map of compression type to decompressor object.
  std::unordered_map<parquet::Encoding::type, std::shared_ptr<DecoderType> > decoders_;

  DecoderType* current_decoder_;
  std::vector<uint8_t> values_buffer_;
};

typedef TypedColumnReader<parquet::Type::BOOLEAN> BoolReader;
typedef TypedColumnReader<parquet::Type::INT32> Int32Reader;
typedef TypedColumnReader<parquet::Type::INT64> Int64Reader;
typedef TypedColumnReader<parquet::Type::INT96> Int96Reader;
typedef TypedColumnReader<parquet::Type::FLOAT> FloatReader;
typedef TypedColumnReader<parquet::Type::DOUBLE> DoubleReader;
typedef TypedColumnReader<parquet::Type::BYTE_ARRAY> ByteArrayReader;


template <int TYPE>
void TypedColumnReader<TYPE>::BatchDecode() {
  buffered_values_offset_ = 0;
  T* buf = reinterpret_cast<T*>(&values_buffer_[0]);
  int batch_size = config_.batch_size;
  num_decoded_values_ = current_decoder_->Decode(buf, batch_size);
}

inline bool ColumnReader::ReadDefinitionRepetitionLevels(int* def_level, int* rep_level) {
  *rep_level = 1;
  if (definition_level_decoder_ && !definition_level_decoder_->Get(def_level)) {
    ParquetException::EofException();
  }
  --num_buffered_values_;
  return *def_level == 0;
}

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_READER_H
