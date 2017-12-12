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
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encoding-internal.h"
#include "parquet/util/memory.h"
#include "parquet/util/test-common.h"

using std::vector;
using std::shared_ptr;

namespace parquet {

static constexpr int FLBA_LENGTH = 12;

bool operator==(const FixedLenByteArray& a, const FixedLenByteArray& b) {
  return 0 == memcmp(a.ptr, b.ptr, FLBA_LENGTH);
}

namespace test {

template <typename T>
static void InitValues(int num_values, vector<T>& values, vector<uint8_t>& buffer) {
  random_numbers(num_values, 0, std::numeric_limits<T>::min(),
                 std::numeric_limits<T>::max(), values.data());
}

template <typename T>
static void InitDictValues(int num_values, int num_dicts, vector<T>& values,
                           vector<uint8_t>& buffer) {
  int repeat_factor = num_values / num_dicts;
  InitValues<T>(num_dicts, values, buffer);
  // add some repeated values
  for (int j = 1; j < repeat_factor; ++j) {
    for (int i = 0; i < num_dicts; ++i) {
      std::memcpy(&values[num_dicts * j + i], &values[i], sizeof(T));
    }
  }
  // computed only dict_per_page * repeat_factor - 1 values < num_values
  // compute remaining
  for (int i = num_dicts * repeat_factor; i < num_values; ++i) {
    std::memcpy(&values[i], &values[i - num_dicts * repeat_factor], sizeof(T));
  }
}

class MockPageReader : public PageReader {
 public:
  explicit MockPageReader(const vector<shared_ptr<Page>>& pages)
      : pages_(pages), page_index_(0) {}

  shared_ptr<Page> NextPage() override {
    if (page_index_ == static_cast<int>(pages_.size())) {
      // EOS to consumer
      return shared_ptr<Page>(nullptr);
    }
    return pages_[page_index_++];
  }

  // No-op
  void set_max_page_header_size(uint32_t size) override {}

 private:
  vector<shared_ptr<Page>> pages_;
  int page_index_;
};

// TODO(wesm): this is only used for testing for now. Refactor to form part of
// primary file write path
template <typename Type>
class DataPageBuilder {
 public:
  typedef typename Type::c_type T;

  // This class writes data and metadata to the passed inputs
  explicit DataPageBuilder(InMemoryOutputStream* sink)
      : sink_(sink),
        num_values_(0),
        encoding_(Encoding::PLAIN),
        definition_level_encoding_(Encoding::RLE),
        repetition_level_encoding_(Encoding::RLE),
        have_def_levels_(false),
        have_rep_levels_(false),
        have_values_(false) {}

  void AppendDefLevels(const vector<int16_t>& levels, int16_t max_level,
                       Encoding::type encoding = Encoding::RLE) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(static_cast<int32_t>(levels.size()), num_values_);
    definition_level_encoding_ = encoding;
    have_def_levels_ = true;
  }

  void AppendRepLevels(const vector<int16_t>& levels, int16_t max_level,
                       Encoding::type encoding = Encoding::RLE) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(static_cast<int32_t>(levels.size()), num_values_);
    repetition_level_encoding_ = encoding;
    have_rep_levels_ = true;
  }

  void AppendValues(const ColumnDescriptor* d, const vector<T>& values,
                    Encoding::type encoding = Encoding::PLAIN) {
    PlainEncoder<Type> encoder(d);
    encoder.Put(&values[0], static_cast<int>(values.size()));
    std::shared_ptr<Buffer> values_sink = encoder.FlushValues();
    sink_->Write(values_sink->data(), values_sink->size());

    num_values_ = std::max(static_cast<int32_t>(values.size()), num_values_);
    encoding_ = encoding;
    have_values_ = true;
  }

  int32_t num_values() const { return num_values_; }

  Encoding::type encoding() const { return encoding_; }

  Encoding::type rep_level_encoding() const { return repetition_level_encoding_; }

  Encoding::type def_level_encoding() const { return definition_level_encoding_; }

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
  void AppendLevels(const vector<int16_t>& levels, int16_t max_level,
                    Encoding::type encoding) {
    if (encoding != Encoding::RLE) {
      ParquetException::NYI("only rle encoding currently implemented");
    }

    // TODO: compute a more precise maximum size for the encoded levels
    vector<uint8_t> encode_buffer(levels.size() * 2);

    // We encode into separate memory from the output stream because the
    // RLE-encoded bytes have to be preceded in the stream by their absolute
    // size.
    LevelEncoder encoder;
    encoder.Init(encoding, max_level, static_cast<int>(levels.size()),
                 encode_buffer.data(), static_cast<int>(encode_buffer.size()));

    encoder.Encode(static_cast<int>(levels.size()), levels.data());

    int32_t rle_bytes = encoder.len();
    sink_->Write(reinterpret_cast<const uint8_t*>(&rle_bytes), sizeof(int32_t));
    sink_->Write(encode_buffer.data(), rle_bytes);
  }
};

template <>
void DataPageBuilder<BooleanType>::AppendValues(const ColumnDescriptor* d,
                                                const vector<bool>& values,
                                                Encoding::type encoding) {
  if (encoding != Encoding::PLAIN) {
    ParquetException::NYI("only plain encoding currently implemented");
  }
  PlainEncoder<BooleanType> encoder(d);
  encoder.Put(values, static_cast<int>(values.size()));
  std::shared_ptr<Buffer> buffer = encoder.FlushValues();
  sink_->Write(buffer->data(), buffer->size());

  num_values_ = std::max(static_cast<int32_t>(values.size()), num_values_);
  encoding_ = encoding;
  have_values_ = true;
}

template <typename Type>
static shared_ptr<DataPage> MakeDataPage(
    const ColumnDescriptor* d, const vector<typename Type::c_type>& values, int num_vals,
    Encoding::type encoding, const uint8_t* indices, int indices_size,
    const vector<int16_t>& def_levels, int16_t max_def_level,
    const vector<int16_t>& rep_levels, int16_t max_rep_level) {
  int num_values = 0;

  InMemoryOutputStream page_stream;
  test::DataPageBuilder<Type> page_builder(&page_stream);

  if (!rep_levels.empty()) {
    page_builder.AppendRepLevels(rep_levels, max_rep_level);
  }
  if (!def_levels.empty()) {
    page_builder.AppendDefLevels(def_levels, max_def_level);
  }

  if (encoding == Encoding::PLAIN) {
    page_builder.AppendValues(d, values, encoding);
    num_values = page_builder.num_values();
  } else {  // DICTIONARY PAGES
    page_stream.Write(indices, indices_size);
    num_values = std::max(page_builder.num_values(), num_vals);
  }

  auto buffer = page_stream.GetBuffer();

  return std::make_shared<DataPage>(buffer, num_values, encoding,
                                    page_builder.def_level_encoding(),
                                    page_builder.rep_level_encoding());
}

template <typename TYPE>
class DictionaryPageBuilder {
 public:
  typedef typename TYPE::c_type TC;
  static constexpr int TN = TYPE::type_num;

  // This class writes data and metadata to the passed inputs
  explicit DictionaryPageBuilder(const ColumnDescriptor* d)
      : num_dict_values_(0), have_values_(false) {
    encoder_.reset(new DictEncoder<TYPE>(d, &pool_));
  }

  ~DictionaryPageBuilder() { pool_.FreeAll(); }

  shared_ptr<Buffer> AppendValues(const vector<TC>& values) {
    int num_values = static_cast<int>(values.size());
    // Dictionary encoding
    encoder_->Put(values.data(), num_values);
    num_dict_values_ = encoder_->num_entries();
    have_values_ = true;
    return encoder_->FlushValues();
  }

  shared_ptr<Buffer> WriteDict() {
    std::shared_ptr<PoolBuffer> dict_buffer =
        AllocateBuffer(::arrow::default_memory_pool(), encoder_->dict_encoded_size());
    encoder_->WriteDict(dict_buffer->mutable_data());
    return dict_buffer;
  }

  int32_t num_values() const { return num_dict_values_; }

 private:
  ChunkedAllocator pool_;
  shared_ptr<DictEncoder<TYPE>> encoder_;
  int32_t num_dict_values_;
  bool have_values_;
};

template <>
DictionaryPageBuilder<BooleanType>::DictionaryPageBuilder(const ColumnDescriptor* d) {
  ParquetException::NYI("only plain encoding currently implemented for boolean");
}

template <>
shared_ptr<Buffer> DictionaryPageBuilder<BooleanType>::WriteDict() {
  ParquetException::NYI("only plain encoding currently implemented for boolean");
  return nullptr;
}

template <>
shared_ptr<Buffer> DictionaryPageBuilder<BooleanType>::AppendValues(
    const vector<TC>& values) {
  ParquetException::NYI("only plain encoding currently implemented for boolean");
  return nullptr;
}

template <typename Type>
static shared_ptr<DictionaryPage> MakeDictPage(
    const ColumnDescriptor* d, const vector<typename Type::c_type>& values,
    const vector<int>& values_per_page, Encoding::type encoding,
    vector<shared_ptr<Buffer>>& rle_indices) {
  InMemoryOutputStream page_stream;
  test::DictionaryPageBuilder<Type> page_builder(d);
  int num_pages = static_cast<int>(values_per_page.size());
  int value_start = 0;

  for (int i = 0; i < num_pages; i++) {
    rle_indices.push_back(page_builder.AppendValues(
        slice(values, value_start, value_start + values_per_page[i])));
    value_start += values_per_page[i];
  }

  auto buffer = page_builder.WriteDict();

  return std::make_shared<DictionaryPage>(buffer, page_builder.num_values(),
                                          Encoding::PLAIN);
}

// Given def/rep levels and values create multiple dict pages
template <typename Type>
static void PaginateDict(const ColumnDescriptor* d,
                         const vector<typename Type::c_type>& values,
                         const vector<int16_t>& def_levels, int16_t max_def_level,
                         const vector<int16_t>& rep_levels, int16_t max_rep_level,
                         int num_levels_per_page, const vector<int>& values_per_page,
                         vector<shared_ptr<Page>>& pages,
                         Encoding::type encoding = Encoding::RLE_DICTIONARY) {
  int num_pages = static_cast<int>(values_per_page.size());
  vector<shared_ptr<Buffer>> rle_indices;
  shared_ptr<DictionaryPage> dict_page =
      MakeDictPage<Type>(d, values, values_per_page, encoding, rle_indices);
  pages.push_back(dict_page);
  int def_level_start = 0;
  int def_level_end = 0;
  int rep_level_start = 0;
  int rep_level_end = 0;
  for (int i = 0; i < num_pages; i++) {
    if (max_def_level > 0) {
      def_level_start = i * num_levels_per_page;
      def_level_end = (i + 1) * num_levels_per_page;
    }
    if (max_rep_level > 0) {
      rep_level_start = i * num_levels_per_page;
      rep_level_end = (i + 1) * num_levels_per_page;
    }
    shared_ptr<DataPage> data_page = MakeDataPage<Int32Type>(
        d, {}, values_per_page[i], encoding, rle_indices[i]->data(),
        static_cast<int>(rle_indices[i]->size()),
        slice(def_levels, def_level_start, def_level_end), max_def_level,
        slice(rep_levels, rep_level_start, rep_level_end), max_rep_level);
    pages.push_back(data_page);
  }
}

// Given def/rep levels and values create multiple plain pages
template <typename Type>
static void PaginatePlain(const ColumnDescriptor* d,
                          const vector<typename Type::c_type>& values,
                          const vector<int16_t>& def_levels, int16_t max_def_level,
                          const vector<int16_t>& rep_levels, int16_t max_rep_level,
                          int num_levels_per_page, const vector<int>& values_per_page,
                          vector<shared_ptr<Page>>& pages,
                          Encoding::type encoding = Encoding::PLAIN) {
  int num_pages = static_cast<int>(values_per_page.size());
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
    shared_ptr<DataPage> page = MakeDataPage<Type>(
        d, slice(values, value_start, value_start + values_per_page[i]),
        values_per_page[i], encoding, nullptr, 0,
        slice(def_levels, def_level_start, def_level_end), max_def_level,
        slice(rep_levels, rep_level_start, rep_level_end), max_rep_level);
    pages.push_back(page);
    value_start += values_per_page[i];
  }
}

// Generates pages from randomly generated data
template <typename Type>
static int MakePages(const ColumnDescriptor* d, int num_pages, int levels_per_page,
                     vector<int16_t>& def_levels, vector<int16_t>& rep_levels,
                     vector<typename Type::c_type>& values, vector<uint8_t>& buffer,
                     vector<shared_ptr<Page>>& pages,
                     Encoding::type encoding = Encoding::PLAIN) {
  int num_levels = levels_per_page * num_pages;
  int num_values = 0;
  uint32_t seed = 0;
  int16_t zero = 0;
  int16_t max_def_level = d->max_definition_level();
  int16_t max_rep_level = d->max_repetition_level();
  vector<int> values_per_page(num_pages, levels_per_page);
  // Create definition levels
  if (max_def_level > 0) {
    def_levels.resize(num_levels);
    random_numbers(num_levels, seed, zero, max_def_level, def_levels.data());
    for (int p = 0; p < num_pages; p++) {
      int num_values_per_page = 0;
      for (int i = 0; i < levels_per_page; i++) {
        if (def_levels[i + p * levels_per_page] == max_def_level) {
          num_values_per_page++;
          num_values++;
        }
      }
      values_per_page[p] = num_values_per_page;
    }
  } else {
    num_values = num_levels;
  }
  // Create repitition levels
  if (max_rep_level > 0) {
    rep_levels.resize(num_levels);
    random_numbers(num_levels, seed, zero, max_rep_level, rep_levels.data());
  }
  // Create values
  values.resize(num_values);
  if (encoding == Encoding::PLAIN) {
    InitValues<typename Type::c_type>(num_values, values, buffer);
    PaginatePlain<Type>(d, values, def_levels, max_def_level, rep_levels, max_rep_level,
                        levels_per_page, values_per_page, pages);
  } else if (encoding == Encoding::RLE_DICTIONARY ||
             encoding == Encoding::PLAIN_DICTIONARY) {
    // Calls InitValues and repeats the data
    InitDictValues<typename Type::c_type>(num_values, levels_per_page, values, buffer);
    PaginateDict<Type>(d, values, def_levels, max_def_level, rep_levels, max_rep_level,
                       levels_per_page, values_per_page, pages);
  }

  return num_values;
}

}  // namespace test

}  // namespace parquet

#endif  // PARQUET_COLUMN_TEST_UTIL_H
