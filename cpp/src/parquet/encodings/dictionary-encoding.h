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

#ifndef PARQUET_DICTIONARY_ENCODING_H
#define PARQUET_DICTIONARY_ENCODING_H

#include "parquet/encodings/encodings.h"

#include <algorithm>
#include <vector>

namespace parquet_cpp {

template <int TYPE>
class DictionaryDecoder : public Decoder<TYPE> {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  // Initializes the dictionary with values from 'dictionary'. The data in dictionary
  // is not guaranteed to persist in memory after this call so the dictionary decoder
  // needs to copy the data out if necessary.
  DictionaryDecoder(const parquet::SchemaElement* schema, Decoder<TYPE>* dictionary)
      : Decoder<TYPE>(schema, parquet::Encoding::RLE_DICTIONARY) {
    Init(dictionary);
  }

  // Perform type-specific initiatialization
  void Init(Decoder<TYPE>* dictionary);

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    uint8_t bit_width = *data;
    ++data;
    --len;
    idx_decoder_ = RleDecoder(data, len, bit_width);
  }

  virtual int Decode(T* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = dictionary_[index()];
    }
    return max_values;
  }

 private:
  using Decoder<TYPE>::num_values_;

  int index() {
    int idx = 0;
    if (!idx_decoder_.Get(&idx)) ParquetException::EofException();
    --num_values_;
    return idx;
  }

  // Only one is set.
  std::vector<T> dictionary_;

  // Data that contains the byte array data (byte_array_dictionary_ just has the
  // pointers).
  std::vector<uint8_t> byte_array_data_;

  RleDecoder idx_decoder_;
};

template <int TYPE>
inline void DictionaryDecoder<TYPE>::Init(Decoder<TYPE>* dictionary) {
  int num_dictionary_values = dictionary->values_left();
  dictionary_.resize(num_dictionary_values);
  dictionary->Decode(&dictionary_[0], num_dictionary_values);
}

template <>
inline void DictionaryDecoder<parquet::Type::BOOLEAN>::Init(
    Decoder<parquet::Type::BOOLEAN>* dictionary) {
  ParquetException::NYI("Dictionary encoding is not implemented for boolean values");
}

template <>
inline void DictionaryDecoder<parquet::Type::BYTE_ARRAY>::Init(
    Decoder<parquet::Type::BYTE_ARRAY>* dictionary) {
  int num_dictionary_values = dictionary->values_left();
  dictionary_.resize(num_dictionary_values);
  dictionary->Decode(&dictionary_[0], num_dictionary_values);

  int total_size = 0;
  for (int i = 0; i < num_dictionary_values; ++i) {
    total_size += dictionary_[i].len;
  }
  byte_array_data_.resize(total_size);
  int offset = 0;
  for (int i = 0; i < num_dictionary_values; ++i) {
    memcpy(&byte_array_data_[offset], dictionary_[i].ptr, dictionary_[i].len);
    dictionary_[i].ptr = &byte_array_data_[offset];
    offset += dictionary_[i].len;
  }
}

} // namespace parquet_cpp

#endif
