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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#ifndef PARQUET_COLUMN_TEST_UTIL_H
#define PARQUET_COLUMN_TEST_UTIL_H

#include <algorithm>
#include <memory>
#include <vector>

#include "parquet/column/page.h"

namespace parquet_cpp {

namespace test {

class MockPageReader : public PageReader {
 public:
  explicit MockPageReader(const std::vector<std::shared_ptr<Page> >& pages) :
      pages_(pages),
      page_index_(0) {}

  // Implement the PageReader interface
  virtual std::shared_ptr<Page> NextPage() {
    if (page_index_ == pages_.size()) {
      // EOS to consumer
      return std::shared_ptr<Page>(nullptr);
    }
    return pages_[page_index_++];
  }

 private:
  std::vector<std::shared_ptr<Page> > pages_;
  size_t page_index_;
};

// TODO(wesm): this is only used for testing for now

static constexpr int DEFAULT_DATA_PAGE_SIZE = 64 * 1024;
static constexpr int INIT_BUFFER_SIZE = 1024;

template <int TYPE>
class DataPageBuilder {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  // The passed vector is the owner of the page's data
  explicit DataPageBuilder(std::vector<uint8_t>* out) :
      out_(out),
      buffer_size_(0),
      num_values_(0),
      have_def_levels_(false),
      have_rep_levels_(false),
      have_values_(false) {
    out_->resize(INIT_BUFFER_SIZE);
    buffer_capacity_ = INIT_BUFFER_SIZE;
  }

  void AppendDefLevels(const std::vector<int16_t>& levels,
      int16_t max_level, parquet::Encoding::type encoding) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(levels.size(), num_values_);
    header_.__set_definition_level_encoding(encoding);
    have_def_levels_ = true;
  }

  void AppendRepLevels(const std::vector<int16_t>& levels,
      int16_t max_level, parquet::Encoding::type encoding) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(levels.size(), num_values_);
    header_.__set_repetition_level_encoding(encoding);
    have_rep_levels_ = true;
  }

  void AppendValues(const std::vector<T>& values,
      parquet::Encoding::type encoding) {
    if (encoding != parquet::Encoding::PLAIN) {
      ParquetException::NYI("only plain encoding currently implemented");
    }
    size_t bytes_to_encode = values.size() * sizeof(T);
    Reserve(bytes_to_encode);

    PlainEncoder<TYPE> encoder(nullptr);
    size_t nbytes = encoder.Encode(&values[0], values.size(), Head());
    // In case for some reason it's fewer than bytes_to_encode
    buffer_size_ += nbytes;

    num_values_ = std::max(values.size(), num_values_);
    header_.__set_encoding(encoding);
    have_values_ = true;
  }

  std::shared_ptr<Page> Finish() {
    if (!have_values_) {
      throw ParquetException("A data page must at least contain values");
    }
    header_.__set_num_values(num_values_);
    return std::make_shared<DataPage>(&(*out_)[0], buffer_size_, header_);
  }

 private:
  std::vector<uint8_t>* out_;

  size_t buffer_size_;
  size_t buffer_capacity_;

  parquet::DataPageHeader header_;

  size_t num_values_;

  bool have_def_levels_;
  bool have_rep_levels_;
  bool have_values_;

  void Reserve(size_t nbytes) {
    while ((nbytes + buffer_size_) > buffer_capacity_) {
      // TODO(wesm): limit to one reserve when this loop runs more than once
      size_t new_capacity = 2 * buffer_capacity_;
      out_->resize(new_capacity);
      buffer_capacity_ = new_capacity;
    }
  }

  uint8_t* Head() {
    return &(*out_)[buffer_size_];
  }

  // Used internally for both repetition and definition levels
  void AppendLevels(const std::vector<int16_t>& levels, int16_t max_level,
      parquet::Encoding::type encoding) {
    if (encoding != parquet::Encoding::RLE) {
      ParquetException::NYI("only rle encoding currently implemented");
    }

    // TODO: compute a more precise maximum size for the encoded levels
    std::vector<uint8_t> encode_buffer(DEFAULT_DATA_PAGE_SIZE);

    RleEncoder encoder(&encode_buffer[0], encode_buffer.size(),
        BitUtil::NumRequiredBits(max_level));

    // TODO(wesm): push down vector encoding
    for (int16_t level : levels) {
      if (!encoder.Put(level)) {
        throw ParquetException("out of space");
      }
    }

    uint32_t rle_bytes = encoder.Flush();
    size_t levels_footprint = sizeof(uint32_t) + rle_bytes;
    Reserve(levels_footprint);

    *reinterpret_cast<uint32_t*>(Head()) = rle_bytes;
    memcpy(Head() + sizeof(uint32_t), encoder.buffer(), rle_bytes);
    buffer_size_ += levels_footprint;
  }
};

} // namespace test

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_TEST_UTIL_H
