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

#ifndef PARQUET_DICTIONARY_ENCODING_H
#define PARQUET_DICTIONARY_ENCODING_H

#include "parquet/encodings/encodings.h"

#include <algorithm>
#include <vector>

namespace parquet_cpp {

class DictionaryDecoder : public Decoder {
 public:
  // Initializes the dictionary with values from 'dictionary'. The data in dictionary
  // is not guaranteed to persist in memory after this call so the dictionary decoder
  // needs to copy the data out if necessary.
  DictionaryDecoder(const parquet::Type::type& type, Decoder* dictionary)
    : Decoder(type, parquet::Encoding::RLE_DICTIONARY) {
    int num_dictionary_values = dictionary->values_left();
    switch (type) {
      case parquet::Type::BOOLEAN:
        throw ParquetException("Boolean cols should not be dictionary encoded.");

      case parquet::Type::INT32:
        int32_dictionary_.resize(num_dictionary_values);
        dictionary->GetInt32(&int32_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::INT64:
        int64_dictionary_.resize(num_dictionary_values);
        dictionary->GetInt64(&int64_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::FLOAT:
        float_dictionary_.resize(num_dictionary_values);
        dictionary->GetFloat(&float_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::DOUBLE:
        double_dictionary_.resize(num_dictionary_values);
        dictionary->GetDouble(&double_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::BYTE_ARRAY: {
        byte_array_dictionary_.resize(num_dictionary_values);
        dictionary->GetByteArray(&byte_array_dictionary_[0], num_dictionary_values);
        int total_size = 0;
        for (int i = 0; i < num_dictionary_values; ++i) {
          total_size += byte_array_dictionary_[i].len;
        }
        byte_array_data_.resize(total_size);
        int offset = 0;
        for (int i = 0; i < num_dictionary_values; ++i) {
          memcpy(&byte_array_data_[offset],
              byte_array_dictionary_[i].ptr, byte_array_dictionary_[i].len);
          byte_array_dictionary_[i].ptr = &byte_array_data_[offset];
          offset += byte_array_dictionary_[i].len;
        }
        break;
      }
      default:
        ParquetException::NYI("Unsupported dictionary type");
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    uint8_t bit_width = *data;
    ++data;
    --len;
    idx_decoder_ = RleDecoder(data, len, bit_width);
  }

  virtual int GetInt32(int32_t* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = int32_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetInt64(int64_t* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = int64_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetFloat(float* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = float_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetDouble(double* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = double_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetByteArray(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = byte_array_dictionary_[index()];
    }
    return max_values;
  }

 private:
  int index() {
    int idx = 0;
    if (!idx_decoder_.Get(&idx)) ParquetException::EofException();
    --num_values_;
    return idx;
  }

  // Only one is set.
  std::vector<int32_t> int32_dictionary_;
  std::vector<int64_t> int64_dictionary_;
  std::vector<float> float_dictionary_;
  std::vector<double> double_dictionary_;
  std::vector<ByteArray> byte_array_dictionary_;

  // Data that contains the byte array data (byte_array_dictionary_ just has the
  // pointers).
  std::vector<uint8_t> byte_array_data_;

  RleDecoder idx_decoder_;
};

} // namespace parquet_cpp

#endif
