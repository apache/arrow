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

#ifndef PARQUET_ENCODINGS_DECODER_H
#define PARQUET_ENCODINGS_DECODER_H

#include <cstdint>

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {

class ColumnDescriptor;

// The Decoder template is parameterized on parquet::DataType subclasses
template <typename DType>
class Decoder {
 public:
  typedef typename DType::c_type T;

  virtual ~Decoder() {}

  // Sets the data for a new page. This will be called multiple times on the same
  // decoder and should reset all internal state.
  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support. In each of these functions,
  // the decoder would decode put to 'max_values', storing the result in 'buffer'.
  // The function returns the number of values decoded, which should be max_values
  // except for end of the current data page.
  virtual int Decode(T* buffer, int max_values) {
    throw ParquetException("Decoder does not implement this type.");
  }

  // Returns the number of values left (for the last call to SetData()). This is
  // the number of values left in this page.
  int values_left() const { return num_values_; }

  const Encoding::type encoding() const { return encoding_; }

 protected:
  explicit Decoder(const ColumnDescriptor* descr, const Encoding::type& encoding)
      : descr_(descr), encoding_(encoding), num_values_(0) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;

  const Encoding::type encoding_;
  int num_values_;
};

}  // namespace parquet

#endif  // PARQUET_ENCODINGS_DECODER_H
