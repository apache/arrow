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

#ifndef PARQUET_PARQUET_H
#define PARQUET_PARQUET_H

#include <exception>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/exception.h"
#include "parquet/thrift/parquet_constants.h"
#include "parquet/thrift/parquet_types.h"
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
class Decoder;

struct ByteArray {
  uint32_t len;
  const uint8_t* ptr;
};

// Interface for the column reader to get the bytes. The interface is a stream
// interface, meaning the bytes in order and once a byte is read, it does not
// need to be read again.
class InputStream {
 public:
  // Returns the next 'num_to_peek' without advancing the current position.
  // *num_bytes will contain the number of bytes returned which can only be
  // less than num_to_peek at end of stream cases.
  // Since the position is not advanced, calls to this function are idempotent.
  // The buffer returned to the caller is still owned by the input stream and must
  // stay valid until the next call to Peek() or Read().
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes) = 0;

  // Identical to Peek(), except the current position in the stream is advanced by
  // *num_bytes.
  virtual const uint8_t* Read(int num_to_read, int* num_bytes) = 0;

  virtual ~InputStream() {}

 protected:
  InputStream() {}
};

// Implementation of an InputStream when all the bytes are in memory.
class InMemoryInputStream : public InputStream {
 public:
  InMemoryInputStream(const uint8_t* buffer, int64_t len);
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes);
  virtual const uint8_t* Read(int num_to_read, int* num_bytes);

 private:
  const uint8_t* buffer_;
  int64_t len_;
  int64_t offset_;
};

// A wrapper for InMemoryInputStream to manage the memory.
class ScopedInMemoryInputStream : public InputStream {
 public:
  ScopedInMemoryInputStream(int64_t len);
  uint8_t* data();
  int64_t size();
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes);
  virtual const uint8_t* Read(int num_to_read, int* num_bytes);

 private:
  std::vector<uint8_t> buffer_;
  std::unique_ptr<InMemoryInputStream> stream_;
};

// API to read values from a single column. This is the main client facing API.
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

  ~ColumnReader();

  // Returns true if there are still values in this column.
  bool HasNext();

  // Returns the next value of this type.
  // TODO: batchify this interface.
  bool GetBool(int* definition_level, int* repetition_level);
  int32_t GetInt32(int* definition_level, int* repetition_level);
  int64_t GetInt64(int* definition_level, int* repetition_level);
  float GetFloat(int* definition_level, int* repetition_level);
  double GetDouble(int* definition_level, int* repetition_level);
  ByteArray GetByteArray(int* definition_level, int* repetition_level);

 private:
  bool ReadNewPage();
  // Reads the next definition and repetition level. Returns true if the value is NULL.
  bool ReadDefinitionRepetitionLevels(int* def_level, int* rep_level);

  void BatchDecode();

  Config config_;

  const parquet::ColumnMetaData* metadata_;
  const parquet::SchemaElement* schema_;
  InputStream* stream_;

  // Compression codec to use.
  std::unique_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;

  // Map of compression type to decompressor object.
  std::unordered_map<parquet::Encoding::type, std::shared_ptr<Decoder> > decoders_;

  parquet::PageHeader current_page_header_;

  // Not set if field is required.
  std::unique_ptr<RleDecoder> definition_level_decoder_;
  // Not set for flat schemas.
  std::unique_ptr<RleDecoder> repetition_level_decoder_;
  Decoder* current_decoder_;
  int num_buffered_values_;

  std::vector<uint8_t> values_buffer_;
  int num_decoded_values_;
  int buffered_values_offset_;
};


inline bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

inline bool ColumnReader::GetBool(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return bool();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<bool*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline int32_t ColumnReader::GetInt32(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return int32_t();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<int32_t*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline int64_t ColumnReader::GetInt64(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return int64_t();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<int64_t*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline float ColumnReader::GetFloat(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return float();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<float*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline double ColumnReader::GetDouble(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return double();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<double*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline ByteArray ColumnReader::GetByteArray(int* def_level, int* rep_level) {
  if (ReadDefinitionRepetitionLevels(def_level, rep_level)) return ByteArray();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<ByteArray*>(&values_buffer_[0])[buffered_values_offset_++];
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

#endif
