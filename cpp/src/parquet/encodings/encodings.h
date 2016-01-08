// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PARQUET_ENCODINGS_ENCODINGS_H
#define PARQUET_ENCODINGS_ENCODINGS_H

#include <cstdint>

#include "parquet/thrift/parquet_constants.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/util/rle-encoding.h"
#include "parquet/util/bit-stream-utils.inline.h"

namespace parquet_cpp {

class Decoder {
 public:
  virtual ~Decoder() {}

  // Sets the data for a new page. This will be called multiple times on the same
  // decoder and should reset all internal state.
  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support. In each of these functions,
  // the decoder would decode put to 'max_values', storing the result in 'buffer'.
  // The function returns the number of values decoded, which should be max_values
  // except for end of the current data page.
  virtual int GetBool(bool* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int GetInt32(int32_t* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int GetInt64(int64_t* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int GetFloat(float* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int GetDouble(double* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int GetByteArray(ByteArray* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }

  // Returns the number of values left (for the last call to SetData()). This is
  // the number of values left in this page.
  int values_left() const { return num_values_; }

  const parquet::Encoding::type encoding() const { return encoding_; }

 protected:
  Decoder(const parquet::Type::type& type, const parquet::Encoding::type& encoding)
    : type_(type), encoding_(encoding), num_values_(0) {}

  const parquet::Type::type type_;
  const parquet::Encoding::type encoding_;
  int num_values_;
};

} // namespace parquet_cpp

#include "parquet/encodings/bool-encoding.h"
#include "parquet/encodings/plain-encoding.h"
#include "parquet/encodings/dictionary-encoding.h"
#include "parquet/encodings/delta-bit-pack-encoding.h"
#include "parquet/encodings/delta-length-byte-array-encoding.h"
#include "parquet/encodings/delta-byte-array-encoding.h"

#endif // PARQUET_ENCODINGS_ENCODINGS_H
