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
#include <string>

#include "parquet/column/levels.h"
#include "parquet/column/page.h"

// Depended on by SerializedPageReader test utilities for now
#include "parquet/encodings/plain-encoding.h"
#include "parquet/util/input.h"
#include "parquet/util/test-common.h"

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
  explicit DataPageBuilder(InMemoryOutputStream* sink) :
      sink_(sink),
      num_values_(0),
      encoding_(Encoding::PLAIN),
      definition_level_encoding_(Encoding::RLE),
      repetition_level_encoding_(Encoding::RLE),
      have_def_levels_(false),
      have_rep_levels_(false),
      have_values_(false) {
  }

  void AppendDefLevels(const std::vector<int16_t>& levels, int16_t max_level,
      Encoding::type encoding = Encoding::RLE) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(static_cast<int32_t>(levels.size()), num_values_);
    definition_level_encoding_ = encoding;
    have_def_levels_ = true;
  }

  void AppendRepLevels(const std::vector<int16_t>& levels, int16_t max_level,
      Encoding::type encoding = Encoding::RLE) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(static_cast<int32_t>(levels.size()), num_values_);
    repetition_level_encoding_ = encoding;
    have_rep_levels_ = true;
  }

  void AppendValues(const ColumnDescriptor *d, const std::vector<T>& values,
      Encoding::type encoding = Encoding::PLAIN) {
    if (encoding != Encoding::PLAIN) {
      ParquetException::NYI("only plain encoding currently implemented");
    }
    size_t bytes_to_encode = values.size() * sizeof(T);

    PlainEncoder<TYPE> encoder(d);
    encoder.Encode(&values[0], values.size(), sink_);

    num_values_ = std::max(static_cast<int32_t>(values.size()), num_values_);
    encoding_ = encoding;
    have_values_ = true;
  }

  int32_t num_values() const {
    return num_values_;
  }

  Encoding::type encoding() const {
    return encoding_;
  }

  Encoding::type rep_level_encoding() const {
    return repetition_level_encoding_;
  }

  Encoding::type def_level_encoding() const {
    return definition_level_encoding_;
  }

 private:
  InMemoryOutputStream* sink_;

  int32_t num_values_;
  Encoding::type encoding_;
  Encoding::type definition_level_encoding_;
  Encoding::type repetition_level_encoding_;

  bool have_def_levels_;
  bool have_rep_levels_;
  bool have_values_;

  // Used internally for both repetition and definition levels
  void AppendLevels(const std::vector<int16_t>& levels, int16_t max_level,
      Encoding::type encoding) {
    if (encoding != Encoding::RLE) {
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

template<>
void DataPageBuilder<Type::BOOLEAN>::AppendValues(const ColumnDescriptor *d,
    const std::vector<bool>& values, Encoding::type encoding) {
  if (encoding != Encoding::PLAIN) {
    ParquetException::NYI("only plain encoding currently implemented");
  }
  size_t bytes_to_encode = values.size() * sizeof(bool);

  PlainEncoder<Type::BOOLEAN> encoder(d);
  encoder.Encode(values, values.size(), sink_);

  num_values_ = std::max(static_cast<int32_t>(values.size()), num_values_);
  encoding_ = encoding;
  have_values_ = true;
}

template <int TYPE, typename T>
static std::shared_ptr<DataPage> MakeDataPage(const ColumnDescriptor *d,
    const std::vector<T>& values,
    const std::vector<int16_t>& def_levels, int16_t max_def_level,
    const std::vector<int16_t>& rep_levels, int16_t max_rep_level) {
  size_t num_values = values.size();

  InMemoryOutputStream page_stream;
  test::DataPageBuilder<TYPE> page_builder(&page_stream);

  if (!rep_levels.empty()) {
    page_builder.AppendRepLevels(rep_levels, max_rep_level);
  }

  if (!def_levels.empty()) {
    page_builder.AppendDefLevels(def_levels, max_def_level);
  }

  page_builder.AppendValues(d, values);

  auto buffer = page_stream.GetBuffer();

  return std::make_shared<DataPage>(buffer, page_builder.num_values(),
      page_builder.encoding(),
      page_builder.def_level_encoding(),
      page_builder.rep_level_encoding());
}

template <int TYPE, typename T>
static void Paginate(const ColumnDescriptor *d,
    const std::vector<T>& values,
    const std::vector<int16_t>& def_levels, int16_t max_def_level,
    const std::vector<int16_t>& rep_levels, int16_t max_rep_level,
    int num_levels_per_page, const std::vector<int>& values_per_page,
    std::vector<std::shared_ptr<Page> >& pages) {
  int num_pages = values_per_page.size();
  int def_level_start = 0;
  int def_level_end = 0;
  int rep_level_start = 0;
  int rep_level_end = 0;
  int value_start = 0;
  for (int i = 0; i < num_pages; i++) {
    if (max_def_level > 0) {
      def_level_start = i * num_levels_per_page;
      def_level_end = (i + 1) * num_levels_per_page;
    }
    if (max_rep_level > 0) {
      rep_level_start = i * num_levels_per_page;
      rep_level_end = (i + 1) * num_levels_per_page;
    }
    std::shared_ptr<DataPage> page = MakeDataPage<TYPE>(d,
        slice(values, value_start, value_start + values_per_page[i]),
        slice(def_levels, def_level_start, def_level_end), max_def_level,
        slice(rep_levels, rep_level_start, rep_level_end), max_rep_level);
    pages.push_back(page);
    value_start += values_per_page[i];
  }
}

} // namespace test

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_TEST_UTIL_H
