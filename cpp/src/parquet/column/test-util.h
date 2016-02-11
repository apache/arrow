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

// TODO(wesm): this is only used for testing for now. Refactor to form part of
// primary file write path

template <int TYPE>
class DataPageBuilder {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  // This class writes data and metadata to the passed inputs
  explicit DataPageBuilder(InMemoryOutputStream* sink, parquet::DataPageHeader* header) :
      sink_(sink),
      header_(header),
      num_values_(0),
      have_def_levels_(false),
      have_rep_levels_(false),
      have_values_(false) {
  }

  void AppendDefLevels(const std::vector<int16_t>& levels,
      int16_t max_level, parquet::Encoding::type encoding) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(levels.size(), num_values_);
    header_->__set_definition_level_encoding(encoding);
    have_def_levels_ = true;
  }

  void AppendRepLevels(const std::vector<int16_t>& levels,
      int16_t max_level, parquet::Encoding::type encoding) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(levels.size(), num_values_);
    header_->__set_repetition_level_encoding(encoding);
    have_rep_levels_ = true;
  }

  void AppendValues(const std::vector<T>& values,
      parquet::Encoding::type encoding) {
    if (encoding != parquet::Encoding::PLAIN) {
      ParquetException::NYI("only plain encoding currently implemented");
    }
    size_t bytes_to_encode = values.size() * sizeof(T);

    PlainEncoder<TYPE> encoder(nullptr);
    encoder.Encode(&values[0], values.size(), sink_);

    num_values_ = std::max(values.size(), num_values_);
    header_->__set_encoding(encoding);
    have_values_ = true;
  }

  void Finish() {
    if (!have_values_) {
      throw ParquetException("A data page must at least contain values");
    }
    header_->__set_num_values(num_values_);
  }

 private:
  InMemoryOutputStream* sink_;
  parquet::DataPageHeader* header_;

  size_t num_values_;
  bool have_def_levels_;
  bool have_rep_levels_;
  bool have_values_;

  // Used internally for both repetition and definition levels
  void AppendLevels(const std::vector<int16_t>& levels, int16_t max_level,
      parquet::Encoding::type encoding) {
    if (encoding != parquet::Encoding::RLE) {
      ParquetException::NYI("only rle encoding currently implemented");
    }

    // TODO: compute a more precise maximum size for the encoded levels
    std::vector<uint8_t> encode_buffer(levels.size() * 4);

    // We encode into separate memory from the output stream because the
    // RLE-encoded bytes have to be preceded in the stream by their absolute
    // size.
    LevelEncoder encoder;
    encoder.Init(encoding, max_level, levels.size(),
        encode_buffer.data(), encode_buffer.size());

    encoder.Encode(levels.size(), levels.data());

    uint32_t rle_bytes = encoder.len();
    sink_->Write(reinterpret_cast<const uint8_t*>(&rle_bytes), sizeof(uint32_t));
    sink_->Write(encode_buffer.data(), rle_bytes);
  }
};

template <int TYPE, typename T>
static std::shared_ptr<DataPage> MakeDataPage(const std::vector<T>& values,
    const std::vector<int16_t>& def_levels, int16_t max_def_level,
    const std::vector<int16_t>& rep_levels, int16_t max_rep_level,
    std::vector<uint8_t>* out_buffer) {
  size_t num_values = values.size();

  InMemoryOutputStream page_stream;
  parquet::DataPageHeader page_header;

  test::DataPageBuilder<TYPE> page_builder(&page_stream, &page_header);

  if (!rep_levels.empty()) {
    page_builder.AppendRepLevels(rep_levels, max_rep_level,
        parquet::Encoding::RLE);
  }

  if (!def_levels.empty()) {
    page_builder.AppendDefLevels(def_levels, max_def_level,
        parquet::Encoding::RLE);
  }

  page_builder.AppendValues(values, parquet::Encoding::PLAIN);
  page_builder.Finish();

  // Hand off the data stream to the passed std::vector
  page_stream.Transfer(out_buffer);

  return std::make_shared<DataPage>(&(*out_buffer)[0], out_buffer->size(), page_header);
}


} // namespace test

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_TEST_UTIL_H
