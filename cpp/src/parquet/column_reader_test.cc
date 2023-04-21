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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/util/macros.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

namespace parquet {

using ParquetType = parquet::Type;

using internal::BinaryRecordReader;
using internal::LevelInfo;
using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;
using testing::ElementsAre;

namespace test {

template <typename T>
static inline bool vector_equal_with_def_levels(const std::vector<T>& left,
                                                const std::vector<int16_t>& def_levels,
                                                int16_t max_def_levels,
                                                int16_t max_rep_levels,
                                                const std::vector<T>& right) {
  size_t i_left = 0;
  size_t i_right = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    if (def_levels[i] == max_def_levels) {
      // Compare
      if (left[i_left] != right[i_right]) {
        std::cerr << "index " << i << " left was " << left[i_left] << " right was "
                  << right[i] << std::endl;
        return false;
      }
      i_left++;
      i_right++;
    } else if (def_levels[i] == (max_def_levels - 1)) {
      // Null entry on the lowest nested level
      i_right++;
    } else if (def_levels[i] < (max_def_levels - 1)) {
      // Null entry on a higher nesting level, only supported for non-repeating data
      if (max_rep_levels == 0) {
        i_right++;
      }
    }
  }

  return true;
}

class TestPrimitiveReader : public ::testing::Test {
 public:
  void InitReader(const ColumnDescriptor* d) {
    auto pager = std::make_unique<MockPageReader>(pages_);
    reader_ = ColumnReader::Make(d, std::move(pager));
  }

  void CheckResults() {
    std::vector<int32_t> vresult(num_values_, -1);
    std::vector<int16_t> dresult(num_levels_, -1);
    std::vector<int16_t> rresult(num_levels_, -1);
    int64_t values_read = 0;
    int total_values_read = 0;
    int batch_actual = 0;

    Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());
    int32_t batch_size = 8;
    int batch = 0;
    // This will cover both the cases
    // 1) batch_size < page_size (multiple ReadBatch from a single page)
    // 2) batch_size > page_size (BatchRead limits to a single page)
    do {
      batch = static_cast<int>(reader->ReadBatch(
          batch_size, &dresult[0] + batch_actual, &rresult[0] + batch_actual,
          &vresult[0] + total_values_read, &values_read));
      total_values_read += static_cast<int>(values_read);
      batch_actual += batch;
      batch_size = std::min(1 << 24, std::max(batch_size * 2, 4096));
    } while (batch > 0);

    ASSERT_EQ(num_levels_, batch_actual);
    ASSERT_EQ(num_values_, total_values_read);
    ASSERT_TRUE(vector_equal(values_, vresult));
    if (max_def_level_ > 0) {
      ASSERT_TRUE(vector_equal(def_levels_, dresult));
    }
    if (max_rep_level_ > 0) {
      ASSERT_TRUE(vector_equal(rep_levels_, rresult));
    }
    // catch improper writes at EOS
    batch_actual =
        static_cast<int>(reader->ReadBatch(5, nullptr, nullptr, nullptr, &values_read));
    ASSERT_EQ(0, batch_actual);
    ASSERT_EQ(0, values_read);
  }
  void CheckResultsSpaced() {
    std::vector<int32_t> vresult(num_levels_, -1);
    std::vector<int16_t> dresult(num_levels_, -1);
    std::vector<int16_t> rresult(num_levels_, -1);
    std::vector<uint8_t> valid_bits(num_levels_, 255);
    int total_values_read = 0;
    int batch_actual = 0;
    int levels_actual = 0;
    int64_t null_count = -1;
    int64_t levels_read = 0;
    int64_t values_read;

    Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());
    int32_t batch_size = 8;
    int batch = 0;
    // This will cover both the cases
    // 1) batch_size < page_size (multiple ReadBatch from a single page)
    // 2) batch_size > page_size (BatchRead limits to a single page)
    do {
      ARROW_SUPPRESS_DEPRECATION_WARNING
      batch = static_cast<int>(reader->ReadBatchSpaced(
          batch_size, dresult.data() + levels_actual, rresult.data() + levels_actual,
          vresult.data() + batch_actual, valid_bits.data() + batch_actual, 0,
          &levels_read, &values_read, &null_count));
      ARROW_UNSUPPRESS_DEPRECATION_WARNING
      total_values_read += batch - static_cast<int>(null_count);
      batch_actual += batch;
      levels_actual += static_cast<int>(levels_read);
      batch_size = std::min(1 << 24, std::max(batch_size * 2, 4096));
    } while ((batch > 0) || (levels_read > 0));

    ASSERT_EQ(num_levels_, levels_actual);
    ASSERT_EQ(num_values_, total_values_read);
    if (max_def_level_ > 0) {
      ASSERT_TRUE(vector_equal(def_levels_, dresult));
      ASSERT_TRUE(vector_equal_with_def_levels(values_, dresult, max_def_level_,
                                               max_rep_level_, vresult));
    } else {
      ASSERT_TRUE(vector_equal(values_, vresult));
    }
    if (max_rep_level_ > 0) {
      ASSERT_TRUE(vector_equal(rep_levels_, rresult));
    }
    // catch improper writes at EOS
    ARROW_SUPPRESS_DEPRECATION_WARNING
    batch_actual = static_cast<int>(
        reader->ReadBatchSpaced(5, nullptr, nullptr, nullptr, valid_bits.data(), 0,
                                &levels_read, &values_read, &null_count));
    ARROW_UNSUPPRESS_DEPRECATION_WARNING
    ASSERT_EQ(0, batch_actual);
    ASSERT_EQ(0, null_count);
  }

  void Clear() {
    values_.clear();
    def_levels_.clear();
    rep_levels_.clear();
    pages_.clear();
    reader_.reset();
  }

  void ExecutePlain(int num_pages, int levels_per_page, const ColumnDescriptor* d) {
    num_values_ =
        MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_, rep_levels_,
                             values_, data_buffer_, pages_, Encoding::PLAIN);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResults();
    Clear();

    num_values_ =
        MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_, rep_levels_,
                             values_, data_buffer_, pages_, Encoding::PLAIN);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResultsSpaced();
    Clear();
  }

  void ExecuteDict(int num_pages, int levels_per_page, const ColumnDescriptor* d) {
    num_values_ =
        MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_, rep_levels_,
                             values_, data_buffer_, pages_, Encoding::RLE_DICTIONARY);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResults();
    Clear();

    num_values_ =
        MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_, rep_levels_,
                             values_, data_buffer_, pages_, Encoding::RLE_DICTIONARY);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResultsSpaced();
    Clear();
  }

 protected:
  int num_levels_;
  int num_values_;
  int16_t max_def_level_;
  int16_t max_rep_level_;
  std::vector<std::shared_ptr<Page>> pages_;
  std::shared_ptr<ColumnReader> reader_;
  std::vector<int32_t> values_;
  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  std::vector<uint8_t> data_buffer_;  // For BA and FLBA
};

TEST_F(TestPrimitiveReader, TestInt32FlatRequired) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  ASSERT_NO_FATAL_FAILURE(ExecutePlain(num_pages, levels_per_page, &descr));
  ASSERT_NO_FATAL_FAILURE(ExecuteDict(num_pages, levels_per_page, &descr));
}

TEST_F(TestPrimitiveReader, TestInt32FlatOptional) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 4;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  ASSERT_NO_FATAL_FAILURE(ExecutePlain(num_pages, levels_per_page, &descr));
  ASSERT_NO_FATAL_FAILURE(ExecuteDict(num_pages, levels_per_page, &descr));
}

TEST_F(TestPrimitiveReader, TestInt32FlatRepeated) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 4;
  max_rep_level_ = 2;
  NodePtr type = schema::Int32("c", Repetition::REPEATED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  ASSERT_NO_FATAL_FAILURE(ExecutePlain(num_pages, levels_per_page, &descr));
  ASSERT_NO_FATAL_FAILURE(ExecuteDict(num_pages, levels_per_page, &descr));
}

// Tests skipping around page boundaries.
TEST_F(TestPrimitiveReader, TestSkipAroundPageBoundries) {
  int levels_per_page = 100;
  int num_pages = 7;
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("b", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  MakePages<Int32Type>(&descr, num_pages, levels_per_page, def_levels_, rep_levels_,
                       values_, data_buffer_, pages_, Encoding::PLAIN);
  InitReader(&descr);
  std::vector<int32_t> vresult(levels_per_page / 2, -1);
  std::vector<int16_t> dresult(levels_per_page / 2, -1);
  std::vector<int16_t> rresult(levels_per_page / 2, -1);

  Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());
  int64_t values_read = 0;

  // 1) skip_size > page_size (multiple pages skipped)
  // Skip first 2 pages
  int64_t levels_skipped = reader->Skip(2 * levels_per_page);
  ASSERT_EQ(2 * levels_per_page, levels_skipped);
  // Read half a page
  reader->ReadBatch(levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(),
                    &values_read);
  std::vector<int32_t> sub_values(
      values_.begin() + 2 * levels_per_page,
      values_.begin() + static_cast<int>(2.5 * levels_per_page));
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 2) skip_size == page_size (skip across two pages from page 2.5 to 3.5)
  levels_skipped = reader->Skip(levels_per_page);
  ASSERT_EQ(levels_per_page, levels_skipped);
  // Read half a page (page 3.5 to 4)
  reader->ReadBatch(levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(),
                    &values_read);
  sub_values.clear();
  sub_values.insert(sub_values.end(),
                    values_.begin() + static_cast<int>(3.5 * levels_per_page),
                    values_.begin() + 4 * levels_per_page);
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 3) skip_size == page_size (skip page 4 from start of the page to the end)
  levels_skipped = reader->Skip(levels_per_page);
  ASSERT_EQ(levels_per_page, levels_skipped);
  // Read half a page (page 5 to 5.5)
  reader->ReadBatch(levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(),
                    &values_read);
  sub_values.clear();
  sub_values.insert(sub_values.end(),
                    values_.begin() + static_cast<int>(5.0 * levels_per_page),
                    values_.begin() + static_cast<int>(5.5 * levels_per_page));
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 4) skip_size < page_size (skip limited to a single page)
  // Skip half a page (page 5.5 to 6)
  levels_skipped = reader->Skip(levels_per_page / 2);
  ASSERT_EQ(0.5 * levels_per_page, levels_skipped);
  // Read half a page (6 to 6.5)
  reader->ReadBatch(levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(),
                    &values_read);
  sub_values.clear();
  sub_values.insert(sub_values.end(),
                    values_.begin() + static_cast<int>(6.0 * levels_per_page),
                    values_.begin() + static_cast<int>(6.5 * levels_per_page));
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 5) skip_size = 0
  levels_skipped = reader->Skip(0);
  ASSERT_EQ(0, levels_skipped);

  // 6) Skip past the end page.
  levels_skipped = reader->Skip(levels_per_page / 2 + 10);
  ASSERT_EQ(levels_per_page / 2, levels_skipped);

  values_.clear();
  def_levels_.clear();
  rep_levels_.clear();
  pages_.clear();
  reader_.reset();
}

// Skip with repeated field. This test makes it clear that we are skipping
// values and not records.
TEST_F(TestPrimitiveReader, TestSkipRepeatedField) {
  // Example schema: message M { repeated int32 b = 1 }
  max_def_level_ = 1;
  max_rep_level_ = 1;
  NodePtr type = schema::Int32("b", Repetition::REPEATED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  // Example rows: {}, {[10, 10]}, {[20, 20, 20]}
  std::vector<int32_t> values = {10, 10, 20, 20, 20};
  std::vector<int16_t> def_levels = {0, 1, 1, 1, 1, 1};
  std::vector<int16_t> rep_levels = {0, 0, 1, 0, 1, 1};
  num_values_ = static_cast<int>(def_levels.size());
  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      &descr, values, num_values_, Encoding::PLAIN, /*indices=*/{},
      /*indices_size=*/0, def_levels, max_def_level_, rep_levels, max_rep_level_);

  pages_.push_back(std::move(page));

  InitReader(&descr);
  Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());

  // Vecotrs to hold read values, definition levels, and repetition levels.
  std::vector<int32_t> read_vals(4, -1);
  std::vector<int16_t> read_defs(4, -1);
  std::vector<int16_t> read_reps(4, -1);

  // Skip two levels.
  int64_t levels_skipped = reader->Skip(2);
  ASSERT_EQ(2, levels_skipped);

  int64_t num_read_values = 0;
  // Read the next set of values
  reader->ReadBatch(10, read_defs.data(), read_reps.data(), read_vals.data(),
                    &num_read_values);
  ASSERT_EQ(num_read_values, 4);
  // Note that we end up in the record with {[10, 10]}
  ASSERT_TRUE(vector_equal({10, 20, 20, 20}, read_vals));
  ASSERT_TRUE(vector_equal({1, 1, 1, 1}, read_defs));
  ASSERT_TRUE(vector_equal({1, 0, 1, 1}, read_reps));

  // No values remain in data page
  levels_skipped = reader->Skip(2);
  ASSERT_EQ(0, levels_skipped);
  reader->ReadBatch(10, read_defs.data(), read_reps.data(), read_vals.data(),
                    &num_read_values);
  ASSERT_EQ(num_read_values, 0);
}

// Page claims to have two values but only 1 is present.
TEST_F(TestPrimitiveReader, TestReadValuesMissing) {
  max_def_level_ = 1;
  max_rep_level_ = 0;
  constexpr int batch_size = 1;
  std::vector<bool> values(1, false);
  std::vector<int16_t> input_def_levels(1, 1);
  NodePtr type = schema::Boolean("a", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);

  // The data page falls back to plain encoding
  std::shared_ptr<ResizableBuffer> dummy = AllocateBuffer();
  std::shared_ptr<DataPageV1> data_page = MakeDataPage<BooleanType>(
      &descr, values, /*num_values=*/2, Encoding::PLAIN, /*indices=*/{},
      /*indices_size=*/0, /*def_levels=*/input_def_levels, max_def_level_,
      /*rep_levels=*/{},
      /*max_rep_level=*/0);
  pages_.push_back(data_page);
  InitReader(&descr);
  auto reader = static_cast<BoolReader*>(reader_.get());
  ASSERT_TRUE(reader->HasNext());
  std::vector<int16_t> def_levels(batch_size, 0);
  std::vector<int16_t> rep_levels(batch_size, 0);
  bool values_out[batch_size];
  int64_t values_read;
  EXPECT_EQ(1, reader->ReadBatch(batch_size, def_levels.data(), rep_levels.data(),
                                 values_out, &values_read));
  ASSERT_THROW(reader->ReadBatch(batch_size, def_levels.data(), rep_levels.data(),
                                 values_out, &values_read),
               ParquetException);
}

// Repetition level byte length reported in Page but Max Repetition level
// is zero for the column.
TEST_F(TestPrimitiveReader, TestRepetitionLvlBytesWithMaxRepetitionZero) {
  constexpr int batch_size = 4;
  max_def_level_ = 1;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  // Bytes here came from the example parquet file in ARROW-17453's int32
  // column which was delta bit-packed. The key part is the first three
  // bytes: the page header reports 1 byte for repetition levels even
  // though the max rep level is 0. If that byte isn't skipped then
  // we get def levels of [1, 1, 0, 0] instead of the correct [1, 1, 1, 0].
  const std::vector<uint8_t> page_data{0x3,  0x3, 0x7, 0x80, 0x1, 0x4, 0x3,
                                       0x18, 0x1, 0x2, 0x0,  0x0, 0x0, 0xc,
                                       0x0,  0x0, 0x0, 0x0,  0x0, 0x0, 0x0};

  std::shared_ptr<DataPageV2> data_page =
      std::make_shared<DataPageV2>(Buffer::Wrap(page_data.data(), page_data.size()), 4, 1,
                                   4, Encoding::DELTA_BINARY_PACKED, 2, 1, 21);

  pages_.push_back(data_page);
  InitReader(&descr);
  auto reader = static_cast<Int32Reader*>(reader_.get());
  int16_t def_levels_out[batch_size];
  int32_t values[batch_size];
  int64_t values_read;
  ASSERT_TRUE(reader->HasNext());
  EXPECT_EQ(4, reader->ReadBatch(batch_size, def_levels_out, /*replevels=*/nullptr,
                                 values, &values_read));
  EXPECT_EQ(3, values_read);
}

// Page claims to have two values but only 1 is present.
TEST_F(TestPrimitiveReader, TestReadValuesMissingWithDictionary) {
  constexpr int batch_size = 1;
  max_def_level_ = 1;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  std::shared_ptr<ResizableBuffer> dummy = AllocateBuffer();

  std::shared_ptr<DictionaryPage> dict_page =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN);
  std::vector<int16_t> input_def_levels(1, 0);
  std::shared_ptr<DataPageV1> data_page = MakeDataPage<Int32Type>(
      &descr, {}, /*num_values=*/2, Encoding::RLE_DICTIONARY, /*indices=*/{},
      /*indices_size=*/0, /*def_levels=*/input_def_levels, max_def_level_,
      /*rep_levels=*/{},
      /*max_rep_level=*/0);
  pages_.push_back(dict_page);
  pages_.push_back(data_page);
  InitReader(&descr);
  auto reader = static_cast<ByteArrayReader*>(reader_.get());
  const ByteArray* dict = nullptr;
  int32_t dict_len = 0;
  int64_t indices_read = 0;
  int32_t indices[batch_size];
  int16_t def_levels_out[batch_size];
  ASSERT_TRUE(reader->HasNext());
  EXPECT_EQ(1, reader->ReadBatchWithDictionary(batch_size, def_levels_out,
                                               /*rep_levels=*/nullptr, indices,
                                               &indices_read, &dict, &dict_len));
  ASSERT_THROW(reader->ReadBatchWithDictionary(batch_size, def_levels_out,
                                               /*rep_levels=*/nullptr, indices,
                                               &indices_read, &dict, &dict_len),
               ParquetException);
}

TEST_F(TestPrimitiveReader, TestDictionaryEncodedPages) {
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  std::shared_ptr<ResizableBuffer> dummy = AllocateBuffer();

  std::shared_ptr<DictionaryPage> dict_page =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN);
  std::shared_ptr<DataPageV1> data_page = MakeDataPage<Int32Type>(
      &descr, {}, 0, Encoding::RLE_DICTIONARY, {}, 0, {}, 0, {}, 0);
  pages_.push_back(dict_page);
  pages_.push_back(data_page);
  InitReader(&descr);
  // Tests Dict : PLAIN, Data : RLE_DICTIONARY
  ASSERT_NO_THROW(reader_->HasNext());
  pages_.clear();

  dict_page = std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN_DICTIONARY);
  data_page = MakeDataPage<Int32Type>(&descr, {}, 0, Encoding::PLAIN_DICTIONARY, {}, 0,
                                      {}, 0, {}, 0);
  pages_.push_back(dict_page);
  pages_.push_back(data_page);
  InitReader(&descr);
  // Tests Dict : PLAIN_DICTIONARY, Data : PLAIN_DICTIONARY
  ASSERT_NO_THROW(reader_->HasNext());
  pages_.clear();

  data_page = MakeDataPage<Int32Type>(&descr, {}, 0, Encoding::RLE_DICTIONARY, {}, 0, {},
                                      0, {}, 0);
  pages_.push_back(data_page);
  InitReader(&descr);
  // Tests dictionary page must occur before data page
  ASSERT_THROW(reader_->HasNext(), ParquetException);
  pages_.clear();

  dict_page = std::make_shared<DictionaryPage>(dummy, 0, Encoding::DELTA_BYTE_ARRAY);
  pages_.push_back(dict_page);
  InitReader(&descr);
  // Tests only RLE_DICTIONARY is supported
  ASSERT_THROW(reader_->HasNext(), ParquetException);
  pages_.clear();

  std::shared_ptr<DictionaryPage> dict_page1 =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN_DICTIONARY);
  std::shared_ptr<DictionaryPage> dict_page2 =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN);
  pages_.push_back(dict_page1);
  pages_.push_back(dict_page2);
  InitReader(&descr);
  // Column cannot have more than one dictionary
  ASSERT_THROW(reader_->HasNext(), ParquetException);
  pages_.clear();

  data_page = MakeDataPage<Int32Type>(&descr, {}, 0, Encoding::DELTA_BYTE_ARRAY, {}, 0,
                                      {}, 0, {}, 0);
  pages_.push_back(data_page);
  InitReader(&descr);
  // unsupported encoding
  ASSERT_THROW(reader_->HasNext(), ParquetException);
  pages_.clear();
}

TEST_F(TestPrimitiveReader, TestDictionaryEncodedPagesWithExposeEncoding) {
  max_def_level_ = 0;
  max_rep_level_ = 0;
  int levels_per_page = 100;
  int num_pages = 5;
  std::vector<int16_t> def_levels;
  std::vector<int16_t> rep_levels;
  std::vector<ByteArray> values;
  std::vector<uint8_t> buffer;
  NodePtr type = schema::ByteArray("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);

  // Fully dictionary encoded
  MakePages<ByteArrayType>(&descr, num_pages, levels_per_page, def_levels, rep_levels,
                           values, buffer, pages_, Encoding::RLE_DICTIONARY);
  InitReader(&descr);

  auto reader = static_cast<ByteArrayReader*>(reader_.get());
  const ByteArray* dict = nullptr;
  int32_t dict_len = 0;
  int64_t total_indices = 0;
  int64_t indices_read = 0;
  int64_t value_size = values.size();
  auto indices = std::make_unique<int32_t[]>(value_size);
  while (total_indices < value_size && reader->HasNext()) {
    const ByteArray* tmp_dict = nullptr;
    int32_t tmp_dict_len = 0;
    EXPECT_NO_THROW(reader->ReadBatchWithDictionary(
        value_size, /*def_levels=*/nullptr,
        /*rep_levels=*/nullptr, indices.get() + total_indices, &indices_read, &tmp_dict,
        &tmp_dict_len));
    if (tmp_dict != nullptr) {
      // Dictionary is read along with data
      EXPECT_GT(indices_read, 0);
      dict = tmp_dict;
      dict_len = tmp_dict_len;
    } else {
      // Dictionary is not read when there's no data
      EXPECT_EQ(indices_read, 0);
    }
    total_indices += indices_read;
  }

  EXPECT_EQ(total_indices, value_size);
  for (int64_t i = 0; i < total_indices; ++i) {
    EXPECT_LT(indices[i], dict_len);
    EXPECT_EQ(dict[indices[i]].len, values[i].len);
    EXPECT_EQ(memcmp(dict[indices[i]].ptr, values[i].ptr, values[i].len), 0);
  }
  pages_.clear();
}

TEST_F(TestPrimitiveReader, TestNonDictionaryEncodedPagesWithExposeEncoding) {
  max_def_level_ = 0;
  max_rep_level_ = 0;
  int64_t value_size = 100;
  std::vector<int32_t> values(value_size, 0);
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);

  // The data page falls back to plain encoding
  std::shared_ptr<ResizableBuffer> dummy = AllocateBuffer();
  std::shared_ptr<DictionaryPage> dict_page =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN);
  std::shared_ptr<DataPageV1> data_page = MakeDataPage<Int32Type>(
      &descr, values, static_cast<int>(value_size), Encoding::PLAIN, /*indices=*/{},
      /*indices_size=*/0, /*def_levels=*/{}, /*max_def_level=*/0, /*rep_levels=*/{},
      /*max_rep_level=*/0);
  pages_.push_back(dict_page);
  pages_.push_back(data_page);
  InitReader(&descr);

  auto reader = static_cast<ByteArrayReader*>(reader_.get());
  const ByteArray* dict = nullptr;
  int32_t dict_len = 0;
  int64_t indices_read = 0;
  auto indices = std::make_unique<int32_t[]>(value_size);
  // Dictionary cannot be exposed when it's not fully dictionary encoded
  EXPECT_THROW(reader->ReadBatchWithDictionary(value_size, /*def_levels=*/nullptr,
                                               /*rep_levels=*/nullptr, indices.get(),
                                               &indices_read, &dict, &dict_len),
               ParquetException);
  pages_.clear();
}

namespace {

internal::LevelInfo ComputeLevelInfo(const ColumnDescriptor* descr) {
  internal::LevelInfo level_info;
  level_info.def_level = descr->max_definition_level();
  level_info.rep_level = descr->max_repetition_level();

  int16_t min_spaced_def_level = descr->max_definition_level();
  const ::parquet::schema::Node* node = descr->schema_node().get();
  while (node != nullptr && !node->is_repeated()) {
    if (node->is_optional()) {
      min_spaced_def_level--;
    }
    node = node->parent();
  }
  level_info.repeated_ancestor_def_level = min_spaced_def_level;
  return level_info;
}

}  // namespace

using ReadDenseForNullable = bool;
class RecordReaderPrimitiveTypeTest
    : public ::testing::TestWithParam<ReadDenseForNullable> {
 public:
  const int32_t kNullValue = -1;

  void Init(NodePtr column) {
    NodePtr root = GroupNode::Make("root", Repetition::REQUIRED, {column});
    schema_descriptor_.Init(root);
    descr_ = schema_descriptor_.Column(0);
    record_reader_ = internal::RecordReader::Make(descr_, ComputeLevelInfo(descr_),
                                                  ::arrow::default_memory_pool(),
                                                  /*read_dictionary=*/false, GetParam());
  }

  void CheckReadValues(std::vector<int32_t> expected_values,
                       std::vector<int16_t> expected_defs,
                       std::vector<int16_t> expected_reps) {
    const auto read_values = reinterpret_cast<const int32_t*>(record_reader_->values());
    std::vector<int32_t> read_vals(read_values,
                                   read_values + record_reader_->values_written());
    ASSERT_EQ(read_vals.size(), expected_values.size());
    for (size_t i = 0; i < expected_values.size(); ++i) {
      if (expected_values[i] != kNullValue) {
        ASSERT_EQ(expected_values[i], read_values[i]);
      }
    }

    if (!descr_->schema_node()->is_required()) {
      std::vector<int16_t> read_defs(
          record_reader_->def_levels(),
          record_reader_->def_levels() + record_reader_->levels_position());
      ASSERT_TRUE(vector_equal(expected_defs, read_defs));
    }

    if (descr_->schema_node()->is_repeated()) {
      std::vector<int16_t> read_reps(
          record_reader_->rep_levels(),
          record_reader_->rep_levels() + record_reader_->levels_position());
      ASSERT_TRUE(vector_equal(expected_reps, read_reps));
    }
  }

  void CheckState(int64_t values_written, int64_t null_count, int64_t levels_written,
                  int64_t levels_position) {
    ASSERT_EQ(record_reader_->values_written(), values_written);
    ASSERT_EQ(record_reader_->null_count(), null_count);
    ASSERT_EQ(record_reader_->levels_written(), levels_written);
    ASSERT_EQ(record_reader_->levels_position(), levels_position);
  }

 protected:
  SchemaDescriptor schema_descriptor_;
  std::shared_ptr<internal::RecordReader> record_reader_;
  const ColumnDescriptor* descr_;
};

// Tests reading a required field. The expected results are the same for
// reading dense and spaced.
TEST_P(RecordReaderPrimitiveTypeTest, ReadRequired) {
  Init(schema::Int32("b", Repetition::REQUIRED));

  // Records look like: {10, 20, 20, 30, 30, 30}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};
  std::vector<int16_t> def_levels = {};
  std::vector<int16_t> rep_levels = {};

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(def_levels.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(), rep_levels,
      descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  // Read [10]
  int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
  ASSERT_EQ(records_read, 1);
  CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
  CheckReadValues(/*expected_values=*/{10}, /*expected_defs=*/{},
                  /*expected_reps=*/{});
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);

  // Read 20, 20, 30, 30, 30
  records_read = record_reader_->ReadRecords(/*num_records=*/10);
  ASSERT_EQ(records_read, 5);
  CheckState(/*values_written=*/5, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
  CheckReadValues(/*expected_values=*/{20, 20, 30, 30, 30},
                  /*expected_defs=*/{},
                  /*expected_reps=*/{});
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
}

// Tests reading an optional field.
// Use a max definition field > 1 to test both cases where parent is present or
// parent is missing.
TEST_P(RecordReaderPrimitiveTypeTest, ReadOptional) {
  NodePtr column = GroupNode::Make(
      "a", Repetition::OPTIONAL,
      {PrimitiveNode::Make("element", Repetition::OPTIONAL, ParquetType::INT32)});
  Init(column);

  // Records look like: {10, null, 20, 20, null, 30, 30, 30, null}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};
  std::vector<int16_t> def_levels = {2, 0, 2, 2, 1, 2, 2, 2, 0};

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(def_levels.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(), /*rep_levels=*/{},
      descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  // Read 10, null
  int64_t records_read = record_reader_->ReadRecords(/*num_records=*/2);
  ASSERT_EQ(records_read, 2);
  if (GetParam() == /*read_dense_for_nullable=*/true) {
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/9,
               /*levels_position=*/2);
    CheckReadValues(/*expected_values=*/{10}, /*expected_defs=*/{2, 0},
                    /*expected_reps=*/{});
  } else {
    CheckState(/*values_written=*/2, /*null_count=*/1, /*levels_written=*/9,
               /*levels_position=*/2);
    CheckReadValues(/*expected_values=*/{10, kNullValue}, /*expected_defs=*/{2, 0},
                    /*expected_reps=*/{});
  }
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/7,
             /*levels_position=*/0);

  // Read 20, 20, null (parent present), 30, 30, 30
  records_read = record_reader_->ReadRecords(/*num_records=*/6);
  ASSERT_EQ(records_read, 6);
  if (GetParam() == /*read_dense_for_nullable=*/true) {
    CheckState(/*values_written=*/5, /*null_count=*/0, /*levels_written=*/7,
               /*levels_position=*/6);
    CheckReadValues(/*expected_values=*/{20, 20, 30, 30, 30},
                    /*expected_defs=*/{2, 2, 1, 2, 2, 2},
                    /*expected_reps=*/{});
  } else {
    CheckState(/*values_written=*/6, /*null_count=*/1, /*levels_written=*/7,
               /*levels_position=*/6);
    CheckReadValues(/*expected_values=*/{20, 20, kNullValue, 30, 30, 30},
                    /*expected_defs=*/{2, 2, 1, 2, 2, 2},
                    /*expected_reps=*/{});
  }
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/1,
             /*levels_position=*/0);

  // Read the last null value and read past the end.
  records_read = record_reader_->ReadRecords(/*num_records=*/3);
  ASSERT_EQ(records_read, 1);
  if (GetParam() == /*read_dense_for_nullable=*/true) {
    CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/1,
               /*levels_position=*/1);
    CheckReadValues(/*expected_values=*/{},
                    /*expected_defs=*/{0},
                    /*expected_reps=*/{});
  } else {
    CheckState(/*values_written=*/1, /*null_count=*/1, /*levels_written=*/1,
               /*levels_position=*/1);
    CheckReadValues(/*expected_values=*/{kNullValue},
                    /*expected_defs=*/{0},
                    /*expected_reps=*/{});
  }
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
}

// Tests reading a required repeated field. The results are the same for reading
// dense or spaced.
TEST_P(RecordReaderPrimitiveTypeTest, ReadRequiredRepeated) {
  NodePtr column = GroupNode::Make(
      "p", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})});
  Init(column);

  // Records look like: {[10], [20, 20], [30, 30, 30]}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};
  std::vector<int16_t> def_levels = {1, 1, 1, 1, 1, 1};
  std::vector<int16_t> rep_levels = {0, 0, 1, 0, 1, 1};

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(def_levels.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(), rep_levels,
      descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  // Read [10]
  int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
  ASSERT_EQ(records_read, 1);
  CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/6,
             /*levels_position=*/1);
  CheckReadValues(/*expected_values=*/{10}, /*expected_defs=*/{1},
                  /*expected_reps=*/{0});
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/5,
             /*levels_position=*/0);

  // Read [20, 20], [30, 30, 30]
  records_read = record_reader_->ReadRecords(/*num_records=*/3);
  ASSERT_EQ(records_read, 2);
  CheckState(/*values_written=*/5, /*null_count=*/0, /*levels_written=*/5,
             /*levels_position=*/5);
  CheckReadValues(/*expected_values=*/{20, 20, 30, 30, 30},
                  /*expected_defs=*/{1, 1, 1, 1, 1},
                  /*expected_reps=*/{0, 1, 0, 1, 1});
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
}

// Tests reading a nullable repeated field. Tests reading null values at
// differnet levels and reading an empty list.
TEST_P(RecordReaderPrimitiveTypeTest, ReadNullableRepeated) {
  NodePtr column = GroupNode::Make(
      "p", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::OPTIONAL, ParquetType::INT32)})});
  Init(column);

  // Records look like: {[10], null, [20, 20], [], [30, 30, null, 30]}
  // Some explanation regarding the behavior. When reading spaced, for an empty list or
  // for a top-level null, we do not leave a space and we do not count it towards
  // null_count.  For a leaf-level null, we leave a space for it and we count it towards
  // null_count. When reading dense, null_count is always 0, and we do not leave any space
  // for values.
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};
  std::vector<int16_t> def_levels = {3, 0, 3, 3, 1, 3, 3, 2, 3};
  std::vector<int16_t> rep_levels = {0, 0, 0, 1, 0, 0, 1, 1, 1};

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(def_levels.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(), rep_levels,
      descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  // Test reading 0 records.
  int64_t records_read = record_reader_->ReadRecords(/*num_records=*/0);
  ASSERT_EQ(records_read, 0);

  // Test the descr() accessor.
  ASSERT_EQ(record_reader_->descr()->max_definition_level(), 3);

  // Read [10], null
  // We do not read this null for both reading dense and spaced.
  records_read = record_reader_->ReadRecords(/*num_records=*/2);
  ASSERT_EQ(records_read, 2);
  if (GetParam() == /*read_dense_for_nullable=*/true) {
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/9,
               /*levels_position=*/2);
    CheckReadValues(/*expected_values=*/{10}, /*expected_defs=*/{3, 0},
                    /*expected_reps=*/{0, 0});
  } else {
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/9,
               /*levels_position=*/2);
    CheckReadValues(/*expected_values=*/{10}, /*expected_defs=*/{3, 0},
                    /*expected_reps=*/{0, 0});
  }
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/7,
             /*levels_position=*/0);

  // Read [20, 20], []
  // We do not read any value for this, it will be counted towards null count
  // when reading spaced.
  records_read = record_reader_->ReadRecords(/*num_records=*/2);
  ASSERT_EQ(records_read, 2);
  if (GetParam() == /*read_dense_for_nullable=*/true) {
    CheckState(/*values_written=*/2, /*null_count=*/0, /*levels_written=*/7,
               /*levels_position=*/3);
    CheckReadValues(/*expected_values=*/{20, 20},
                    /*expected_defs=*/{3, 3, 1},
                    /*expected_reps=*/{0, 1, 0});
  } else {
    CheckState(/*values_written=*/2, /*null_count=*/0, /*levels_written=*/7,
               /*levels_position=*/3);
    CheckReadValues(/*expected_values=*/{20, 20},
                    /*expected_defs=*/{3, 3, 1},
                    /*expected_reps=*/{0, 1, 0});
  }
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/4,
             /*levels_position=*/0);

  // Test reading 0 records.
  records_read = record_reader_->ReadRecords(/*num_records=*/0);
  ASSERT_EQ(records_read, 0);

  // Read the last record.
  records_read = record_reader_->ReadRecords(/*num_records=*/1);
  ASSERT_EQ(records_read, 1);
  if (GetParam() == /*read_dense_for_nullable=*/true) {
    CheckState(/*values_written=*/3, /*null_count=*/0, /*levels_written=*/4,
               /*levels_position=*/4);
    CheckReadValues(/*expected_values=*/{30, 30, 30},
                    /*expected_defs=*/{3, 3, 2, 3},
                    /*expected_reps=*/{0, 1, 1, 1});
  } else {
    CheckState(/*values_written=*/4, /*null_count=*/1, /*levels_written=*/4,
               /*levels_position=*/4);
    CheckReadValues(/*expected_values=*/{30, 30, kNullValue, 30},
                    /*expected_defs=*/{3, 3, 2, 3},
                    /*expected_reps=*/{0, 1, 1, 1});
  }
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
}

// Test that we can skip required top level field.
TEST_P(RecordReaderPrimitiveTypeTest, SkipRequiredTopLevel) {
  Init(schema::Int32("b", Repetition::REQUIRED));

  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};
  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(values.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, /*def_levels=*/{}, descr_->max_definition_level(),
      /*rep_levels=*/{}, descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/3);
  ASSERT_EQ(records_skipped, 3);
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);

  int64_t records_read = record_reader_->ReadRecords(/*num_records=*/2);
  ASSERT_EQ(records_read, 2);
  CheckState(/*values_written=*/2, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
  CheckReadValues(/*expected_values=*/{30, 30}, /*expected_defs=*/{},
                  /*expected_reps=*/{});
  record_reader_->Reset();
  CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
             /*levels_position=*/0);
}

// Skip an optional field. Intentionally included some null values.
TEST_P(RecordReaderPrimitiveTypeTest, SkipOptional) {
  Init(schema::Int32("b", Repetition::OPTIONAL));

  // Records look like {null, 10, 20, 30, null, 40, 50, 60}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 30, 40, 50, 60};
  std::vector<int16_t> def_levels = {0, 1, 1, 0, 1, 1, 1, 1};

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(values.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(),
      /*rep_levels=*/{}, descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  {
    // Skip {null, 10}
    // This also tests when we start with a Skip.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/2);
    ASSERT_EQ(records_skipped, 2);
    CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/0,
               /*levels_position=*/0);
  }

  {
    // Read 3 records: {20, null, 30}
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/3);

    ASSERT_EQ(records_read, 3);
    if (GetParam() == /*read_dense_for_nullable=*/true) {
      // We had skipped 2 of the levels above. So there is only 6 left in total to
      // read, and we read 3 of them here.
      CheckState(/*values_written=*/2, /*null_count=*/0, /*levels_written=*/6,
                 /*levels_position=*/3);
      CheckReadValues(/*expected_values=*/{20, 30}, /*expected_defs=*/{1, 0, 1},
                      /*expected_reps=*/{});
    } else {
      CheckState(/*values_written=*/3, /*null_count=*/1, /*levels_written=*/6,
                 /*levels_position=*/3);
      CheckReadValues(/*expected_values=*/{20, kNullValue, 30},
                      /*expected_defs=*/{1, 0, 1},
                      /*expected_reps=*/{});
    }

    record_reader_->Reset();
  }

  {
    // Skip {40, 50}.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/2);
    ASSERT_EQ(records_skipped, 2);
    CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/1,
               /*levels_position=*/0);
    CheckReadValues(/*expected_values=*/{}, /*expected_defs=*/{},
                    /*expected_reps=*/{});
    // Try reset after a Skip.
    record_reader_->Reset();
    CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/1,
               /*levels_position=*/0);
  }

  {
    // Read to the end of the column. Read {60}
    // This test checks that ReadAndThrowAwayValues works, since if it
    // does not we would read the wrong values.
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/1,
               /*levels_position=*/1);
    CheckReadValues(/*expected_values=*/{60},
                    /*expected_defs=*/{1},
                    /*expected_reps=*/{});
  }

  // We have exhausted all the records.
  ASSERT_EQ(record_reader_->ReadRecords(/*num_records=*/3), 0);
  ASSERT_EQ(record_reader_->SkipRecords(/*num_records=*/3), 0);
}

// Test skipping for repeated fields.
TEST_P(RecordReaderPrimitiveTypeTest, SkipRepeated) {
  Init(schema::Int32("b", Repetition::REPEATED));

  // Records look like {null, [20, 20, 20], null, [30, 30], [40]}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {20, 20, 20, 30, 30, 40};
  std::vector<int16_t> def_levels = {0, 1, 1, 1, 0, 1, 1, 1};
  std::vector<int16_t> rep_levels = {0, 0, 1, 1, 0, 0, 1, 0};

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(values.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(), rep_levels,
      descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  {
    // Skip 0 records.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/0);
    ASSERT_EQ(records_skipped, 0);
  }

  {
    // This should skip the first null record.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/1);
    ASSERT_EQ(records_skipped, 1);
    ASSERT_EQ(record_reader_->values_written(), 0);
    ASSERT_EQ(record_reader_->null_count(), 0);
    // For repeated fields, we need to read the levels to find the record
    // boundaries and skip. So some levels are read, however, the skipped
    // level should not be there after the skip. That's why levels_position()
    // is 0.
    CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/7,
               /*levels_position=*/0);
    CheckReadValues(/*expected_values=*/{},
                    /*expected_defs=*/{},
                    /*expected_reps=*/{});
  }

  {
    // Read [20, 20, 20]
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/3, /*null_count=*/0, /*levels_written=*/7,
               /*levels_position=*/3);
    CheckReadValues(/*expected_values=*/{20, 20, 20},
                    /*expected_defs=*/{1, 1, 1},
                    /*expected_reps=*/{0, 1, 1});
  }

  {
    // Skip 0 records.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/0);
    ASSERT_EQ(records_skipped, 0);
  }

  {
    // Skip the null record and also skip [30, 30]
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/2);
    ASSERT_EQ(records_skipped, 2);
    // We remove the skipped levels from the buffer.
    CheckState(/*values_written=*/3, /*null_count=*/0, /*levels_written=*/4,
               /*levels_position=*/3);
    CheckReadValues(/*expected_values=*/{20, 20, 20},
                    /*expected_defs=*/{1, 1, 1},
                    /*expected_reps=*/{0, 1, 1});
  }

  {
    // Read [40]
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/4, /*null_count=*/0, /*levels_written=*/4,
               /*levels_position=*/4);
    CheckReadValues(/*expected_values=*/{20, 20, 20, 40},
                    /*expected_defs=*/{1, 1, 1, 1},
                    /*expected_reps=*/{0, 1, 1, 0});
  }
}

// Tests that for repeated fields, we first consume what is in the buffer
// before reading more levels.
TEST_P(RecordReaderPrimitiveTypeTest, SkipRepeatedConsumeBufferFirst) {
  Init(schema::Int32("b", Repetition::REPEATED));

  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values(2048, 10);
  std::vector<int16_t> def_levels(2048, 1);
  std::vector<int16_t> rep_levels(2048, 0);

  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      descr_, values, /*num_values=*/static_cast<int>(values.size()), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, descr_->max_definition_level(), rep_levels,
      descr_->max_repetition_level());
  pages.push_back(std::move(page));
  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));
  {
    // Read 1000 records. We will read 1024 levels because that is the minimum
    // number of levels to read.
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1000);
    ASSERT_EQ(records_read, 1000);
    CheckState(/*values_written=*/1000, /*null_count=*/0, /*levels_written=*/1024,
               /*levels_position=*/1000);
    std::vector<int32_t> expected_values(1000, 10);
    std::vector<int16_t> expected_def_levels(1000, 1);
    std::vector<int16_t> expected_rep_levels(1000, 0);
    CheckReadValues(expected_values, expected_def_levels, expected_rep_levels);
    // Reset removes the already consumed values and levels.
    record_reader_->Reset();
  }

  {  // Skip 12 records. Since we already have 24 in the buffer, we should not be
    // reading any more levels into the buffer, we will just consume 12 of it.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/12);
    ASSERT_EQ(records_skipped, 12);
    CheckState(/*values_written=*/0, /*null_count=*/0, /*levels_written=*/12,
               /*levels_position=*/0);
    // Everthing is empty because we reset the reader before this skip.
    CheckReadValues(/*expected_values=*/{}, /*expected_def_levels=*/{},
                    /*expected_rep_levels=*/{});
  }
}

// Test reading when one record spans multiple pages for a repeated field.
TEST_P(RecordReaderPrimitiveTypeTest, ReadPartialRecord) {
  Init(schema::Int32("b", Repetition::REPEATED));

  std::vector<std::shared_ptr<Page>> pages;

  // Page 1: {[10], [20, 20, 20 ... } continues to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        descr_, /*values=*/{10, 20, 20, 20}, /*num_values=*/4, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1, 1, 1}, descr_->max_definition_level(),
        /*rep_levels=*/{0, 0, 1, 1}, descr_->max_repetition_level());
    pages.push_back(std::move(page));
  }

  // Page 2: {... 20, 20, ...} continues from previous page and to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        descr_, /*values=*/{20, 20}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, descr_->max_definition_level(),
        /*rep_levels=*/{1, 1}, descr_->max_repetition_level());
    pages.push_back(std::move(page));
  }

  // Page 3: { ... 20], [30]} continues from previous page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        descr_, /*values=*/{20, 30}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, descr_->max_definition_level(),
        /*rep_levels=*/{1, 0}, descr_->max_repetition_level());
    pages.push_back(std::move(page));
  }

  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  {
    // Read [10]
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/4,
               /*levels_position=*/1);
    CheckReadValues(/*expected_values=*/{10},
                    /*expected_defs=*/{1},
                    /*expected_reps=*/{0});
  }

  {
    // Read [20, 20, 20, 20, 20, 20] that spans multiple pages.
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/7, /*null_count=*/0, /*levels_written=*/8,
               /*levels_position=*/7);
    CheckReadValues(/*expected_values=*/{10, 20, 20, 20, 20, 20, 20},
                    /*expected_defs=*/{1, 1, 1, 1, 1, 1, 1},
                    /*expected_reps=*/{0, 0, 1, 1, 1, 1, 1});
  }

  {
    // Read [30]
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/8, /*null_count=*/0, /*levels_written=*/8,
               /*levels_position=*/8);
    CheckReadValues(/*expected_values=*/{10, 20, 20, 20, 20, 20, 20, 30},
                    /*expected_defs=*/{1, 1, 1, 1, 1, 1, 1, 1},
                    /*expected_reps=*/{0, 0, 1, 1, 1, 1, 1, 0});
  }
}

// Test skipping for repeated fields for the case when one record spans multiple
// pages.
TEST_P(RecordReaderPrimitiveTypeTest, SkipPartialRecord) {
  Init(schema::Int32("b", Repetition::REPEATED));

  std::vector<std::shared_ptr<Page>> pages;

  // Page 1: {[10], [20, 20, 20 ... } continues to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        descr_, /*values=*/{10, 20, 20, 20}, /*num_values=*/4, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1, 1, 1}, descr_->max_definition_level(),
        /*rep_levels=*/{0, 0, 1, 1}, descr_->max_repetition_level());
    pages.push_back(std::move(page));
  }

  // Page 2: {... 20, 20, ...} continues from previous page and to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        descr_, /*values=*/{20, 20}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, descr_->max_definition_level(),
        /*rep_levels=*/{1, 1}, descr_->max_repetition_level());
    pages.push_back(std::move(page));
  }

  // Page 3: { ... 20, [30]} continues from previous page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        descr_, /*values=*/{20, 30}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, descr_->max_definition_level(),
        /*rep_levels=*/{1, 0}, descr_->max_repetition_level());
    pages.push_back(std::move(page));
  }

  auto pager = std::make_unique<MockPageReader>(pages);
  record_reader_->SetPageReader(std::move(pager));

  {
    // Read [10]
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);
    ASSERT_EQ(records_read, 1);
    // There are 4 levels in the first page.
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/4,
               /*levels_position=*/1);
    CheckReadValues(/*expected_values=*/{10},
                    /*expected_defs=*/{1},
                    /*expected_reps=*/{0});
  }

  {
    // Skip the record that goes across pages.
    int64_t records_skipped = record_reader_->SkipRecords(/*num_records=*/1);
    ASSERT_EQ(records_skipped, 1);
    CheckState(/*values_written=*/1, /*null_count=*/0, /*levels_written=*/2,
               /*levels_position=*/1);
    CheckReadValues(/*expected_values=*/{10},
                    /*expected_defs=*/{1},
                    /*expected_reps=*/{0});
  }

  {
    // Read [30]
    int64_t records_read = record_reader_->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);
    CheckState(/*values_written=*/2, /*null_count=*/0, /*levels_written=*/2,
               /*levels_position=*/2);
    CheckReadValues(/*expected_values=*/{10, 30},
                    /*expected_defs=*/{1, 1},
                    /*expected_reps=*/{0, 0});
  }
}

INSTANTIATE_TEST_SUITE_P(RecordReaderPrimitveTypeTests, RecordReaderPrimitiveTypeTest,
                         ::testing::Values(/*read_dense_for_nullable=*/true, false),
                         testing::PrintToStringParamName());

// Parameterized test for FLBA record reader.
class FLBARecordReaderTest : public ::testing::TestWithParam<bool> {
 public:
  bool read_dense_for_nullable() { return GetParam(); }

  void MakeRecordReader(int levels_per_page, int num_pages, int FLBA_type_length) {
    levels_per_page_ = levels_per_page;
    FLBA_type_length_ = FLBA_type_length;
    internal::LevelInfo level_info;
    level_info.def_level = 1;
    level_info.rep_level = 0;
    NodePtr type = ::parquet::schema::PrimitiveNode::Make(
        "b", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::NONE,
        FLBA_type_length_);
    descr_ = std::make_unique<ColumnDescriptor>(type, level_info.def_level,
                                                level_info.rep_level);
    MakePages<FLBAType>(descr_.get(), num_pages, levels_per_page, def_levels_,
                        rep_levels_, values_, buffer_, pages_, Encoding::PLAIN);
    auto pager = std::make_unique<MockPageReader>(pages_);
    record_reader_ = internal::RecordReader::Make(
        descr_.get(), level_info, ::arrow::default_memory_pool(),
        /*read_dictionary=*/false, read_dense_for_nullable());
    record_reader_->SetPageReader(std::move(pager));
  }

  // Returns expected values in row range.
  // We need this since some values are null.
  std::vector<std::string_view> expected_values(int start, int end) {
    std::vector<std::string_view> result;
    // Find out where in the values_ vector we start from.
    size_t values_index = 0;
    for (int i = 0; i < start; ++i) {
      if (def_levels_[i] != 0) {
        ++values_index;
      }
    }

    for (int i = start; i < end; ++i) {
      if (def_levels_[i] == 0) {
        if (!read_dense_for_nullable()) {
          result.emplace_back();
        }
        continue;
      }
      result.emplace_back(reinterpret_cast<const char*>(values_[values_index].ptr),
                          FLBA_type_length_);
      ++values_index;
    }
    return result;
  }

  void CheckReadValues(int start, int end) {
    auto binary_reader = dynamic_cast<BinaryRecordReader*>(record_reader_.get());
    ASSERT_NE(binary_reader, nullptr);
    // Chunks are reset after this call.
    ::arrow::ArrayVector array_vector = binary_reader->GetBuilderChunks();
    ASSERT_EQ(array_vector.size(), 1);
    auto binary_array =
        dynamic_cast<::arrow::FixedSizeBinaryArray*>(array_vector[0].get());

    ASSERT_NE(binary_array, nullptr);
    ASSERT_EQ(binary_array->length(), record_reader_->values_written());
    if (read_dense_for_nullable()) {
      ASSERT_EQ(binary_array->null_count(), 0);
      ASSERT_EQ(record_reader_->null_count(), 0);
    } else {
      ASSERT_EQ(binary_array->null_count(), record_reader_->null_count());
    }

    std::vector<std::string_view> expected = expected_values(start, end);
    for (size_t i = 0; i < expected.size(); ++i) {
      if (def_levels_[i + start] == 0) {
        ASSERT_EQ(!read_dense_for_nullable(), binary_array->IsNull(i));
      } else {
        ASSERT_EQ(expected[i].compare(binary_array->GetView(i)), 0);
        ASSERT_FALSE(binary_array->IsNull(i));
      }
    }
  }

 protected:
  std::shared_ptr<internal::RecordReader> record_reader_;

 private:
  int levels_per_page_;
  int FLBA_type_length_;
  std::vector<std::shared_ptr<Page>> pages_;
  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  std::vector<FixedLenByteArray> values_;
  std::vector<uint8_t> buffer_;
  std::unique_ptr<ColumnDescriptor> descr_;
};

// Similar to above, except for Byte arrays. FLBA and Byte arrays are
// sufficiently different to warrant a separate class for readability.
class ByteArrayRecordReaderTest : public ::testing::TestWithParam<bool> {
 public:
  bool read_dense_for_nullable() { return GetParam(); }

  void MakeRecordReader(int levels_per_page, int num_pages) {
    levels_per_page_ = levels_per_page;
    internal::LevelInfo level_info;
    level_info.def_level = 1;
    level_info.rep_level = 0;
    NodePtr type = schema::ByteArray("b", Repetition::OPTIONAL);
    descr_ = std::make_unique<ColumnDescriptor>(type, level_info.def_level,
                                                level_info.rep_level);
    MakePages<ByteArrayType>(descr_.get(), num_pages, levels_per_page, def_levels_,
                             rep_levels_, values_, buffer_, pages_, Encoding::PLAIN);

    auto pager = std::make_unique<MockPageReader>(pages_);

    record_reader_ = internal::RecordReader::Make(
        descr_.get(), level_info, ::arrow::default_memory_pool(),
        /*read_dictionary=*/false, read_dense_for_nullable());
    record_reader_->SetPageReader(std::move(pager));
  }

  // Returns expected values in row range.
  // We need this since some values are null.
  std::vector<std::string_view> expected_values(int start, int end) {
    std::vector<std::string_view> result;
    // Find out where in the values_ vector we start from.
    size_t values_index = 0;
    for (int i = 0; i < start; ++i) {
      if (def_levels_[i] != 0) {
        ++values_index;
      }
    }

    for (int i = start; i < end; ++i) {
      if (def_levels_[i] == 0) {
        if (!read_dense_for_nullable()) {
          result.emplace_back();
        }
        continue;
      }
      result.emplace_back(reinterpret_cast<const char*>(values_[values_index].ptr),
                          values_[values_index].len);
      ++values_index;
    }
    return result;
  }

  void CheckReadValues(int start, int end) {
    auto binary_reader = dynamic_cast<BinaryRecordReader*>(record_reader_.get());
    ASSERT_NE(binary_reader, nullptr);
    // Chunks are reset after this call.
    ::arrow::ArrayVector array_vector = binary_reader->GetBuilderChunks();
    ASSERT_EQ(array_vector.size(), 1);
    ::arrow::BinaryArray* binary_array =
        dynamic_cast<::arrow::BinaryArray*>(array_vector[0].get());

    ASSERT_NE(binary_array, nullptr);
    ASSERT_EQ(binary_array->length(), record_reader_->values_written());
    if (read_dense_for_nullable()) {
      ASSERT_EQ(binary_array->null_count(), 0);
      ASSERT_EQ(record_reader_->null_count(), 0);
    } else {
      ASSERT_EQ(binary_array->null_count(), record_reader_->null_count());
    }

    std::vector<std::string_view> expected = expected_values(start, end);
    for (size_t i = 0; i < expected.size(); ++i) {
      if (def_levels_[i + start] == 0) {
        ASSERT_EQ(!read_dense_for_nullable(), binary_array->IsNull(i));
      } else {
        ASSERT_EQ(expected[i].compare(binary_array->GetView(i)), 0);
        ASSERT_FALSE(binary_array->IsNull(i));
      }
    }
  }

 protected:
  std::shared_ptr<internal::RecordReader> record_reader_;

 private:
  int levels_per_page_;
  std::vector<std::shared_ptr<Page>> pages_;
  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  std::vector<ByteArray> values_;
  std::vector<uint8_t> buffer_;
  std::unique_ptr<ColumnDescriptor> descr_;
};

// Tests reading and skipping a ByteArray field.
// The binary readers only differ in DeocdeDense and DecodeSpaced functions, so
// testing optional is sufficient in excercising those code paths.
TEST_P(ByteArrayRecordReaderTest, ReadAndSkipOptional) {
  MakeRecordReader(/*levels_per_page=*/90, /*num_pages=*/1);

  // Read one-third of the page.
  ASSERT_EQ(record_reader_->ReadRecords(/*num_records=*/30), 30);
  CheckReadValues(0, 30);
  record_reader_->Reset();

  // Skip 30 records.
  ASSERT_EQ(record_reader_->SkipRecords(/*num_records=*/30), 30);

  // Read 60 more records. Only 30 will be read, since we read 30 and skipped 30,
  // so only 30 is left.
  ASSERT_EQ(record_reader_->ReadRecords(/*num_records=*/60), 30);
  CheckReadValues(60, 90);
  record_reader_->Reset();
}

// Tests reading and skipping an optional FLBA field.
// The binary readers only differ in DeocdeDense and DecodeSpaced functions, so
// testing optional is sufficient in excercising those code paths.
TEST_P(FLBARecordReaderTest, ReadAndSkipOptional) {
  MakeRecordReader(/*levels_per_page=*/90, /*num_pages=*/1, /*FLBA_type_length=*/4);

  // Read one-third of the page.
  ASSERT_EQ(record_reader_->ReadRecords(/*num_records=*/30), 30);
  CheckReadValues(0, 30);
  record_reader_->Reset();

  // Skip 30 records.
  ASSERT_EQ(record_reader_->SkipRecords(/*num_records=*/30), 30);

  // Read 60 more records. Only 30 will be read, since we read 30 and skipped 30,
  // so only 30 is left.
  ASSERT_EQ(record_reader_->ReadRecords(/*num_records=*/60), 30);
  CheckReadValues(60, 90);
  record_reader_->Reset();
}

INSTANTIATE_TEST_SUITE_P(ByteArrayRecordReaderTests, ByteArrayRecordReaderTest,
                         testing::Bool());

INSTANTIATE_TEST_SUITE_P(FLBARecordReaderTests, FLBARecordReaderTest, testing::Bool());

// Test random combination of ReadRecords and SkipRecords.
class RecordReaderStressTest : public ::testing::TestWithParam<Repetition::type> {};

TEST_P(RecordReaderStressTest, StressTest) {
  internal::LevelInfo level_info;
  // Define these boolean variables for improving readability below.
  bool repeated = false, required = false;
  if (GetParam() == Repetition::REQUIRED) {
    level_info.def_level = 0;
    level_info.rep_level = 0;
    required = true;
  } else if (GetParam() == Repetition::OPTIONAL) {
    level_info.def_level = 1;
    level_info.rep_level = 0;
  } else {
    level_info.def_level = 1;
    level_info.rep_level = 1;
    repeated = true;
  }

  NodePtr type = schema::Int32("b", GetParam());
  const ColumnDescriptor descr(type, level_info.def_level, level_info.rep_level);

  auto seed1 = static_cast<uint32_t>(time(0));
  std::default_random_engine gen(seed1);
  // Generate random number of pages with random number of values per page.
  std::uniform_int_distribution<int> d(0, 2000);
  const int num_pages = d(gen);
  const int levels_per_page = d(gen);
  std::vector<int32_t> values;
  std::vector<int16_t> def_levels;
  std::vector<int16_t> rep_levels;
  std::vector<uint8_t> data_buffer;
  std::vector<std::shared_ptr<Page>> pages;
  auto seed2 = static_cast<uint32_t>(time(0));
  // Uses time(0) as seed so it would run a different test every time it is
  // run.
  MakePages<Int32Type>(&descr, num_pages, levels_per_page, def_levels, rep_levels, values,
                       data_buffer, pages, Encoding::PLAIN, seed2);
  std::unique_ptr<PageReader> pager;
  pager.reset(new test::MockPageReader(pages));

  // Set up the RecordReader.
  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  // Figure out how many total records.
  int total_records = 0;
  if (repeated) {
    for (int16_t rep : rep_levels) {
      if (rep == 0) {
        ++total_records;
      }
    }
  } else {
    total_records = static_cast<int>(def_levels.size());
  }

  // Generate a sequence of reads and skips.
  int records_left = total_records;
  // The first element of the pair is 1 if SkipRecords and 0 if ReadRecords.
  // The second element indicates the number of records for reading or
  // skipping.
  std::vector<std::pair<bool, int>> sequence;
  while (records_left > 0) {
    std::uniform_int_distribution<int> d(0, records_left);
    // Generate a number to decide if this is a skip or read.
    bool is_skip = d(gen) < records_left / 2;
    int num_records = d(gen);

    sequence.emplace_back(is_skip, num_records);
    records_left -= num_records;
  }

  // The levels_index and values_index are over the original vectors that have
  // all the rep/def values for all the records. In the following loop, we will
  // read/skip a numebr of records and Reset the reader after each iteration.
  // This is on-par with how the record reader is used.
  size_t levels_index = 0;
  size_t values_index = 0;
  for (const auto& [is_skip, num_records] : sequence) {
    // Reset the reader before the next round of read/skip.
    record_reader->Reset();

    // Prepare the expected result and do the SkipRecords and ReadRecords.
    std::vector<int32_t> expected_values;
    std::vector<int16_t> expected_def_levels;
    std::vector<int16_t> expected_rep_levels;
    bool inside_repeated_field = false;

    int read_records = 0;
    while (read_records < num_records || inside_repeated_field) {
      if (!repeated || (repeated && rep_levels[levels_index] == 0)) {
        ++read_records;
      }

      bool has_value =
          required || (!required && def_levels[levels_index] == level_info.def_level);

      // If we are not skipping, we need to update the expected values and
      // rep/defs. If we are skipping, we just keep going.
      if (!is_skip) {
        if (!required) {
          expected_def_levels.push_back(def_levels[levels_index]);
          if (!has_value) {
            expected_values.push_back(-1);
          }
        }
        if (repeated) {
          expected_rep_levels.push_back(rep_levels[levels_index]);
        }
        if (has_value) {
          expected_values.push_back(values[values_index]);
        }
      }

      if (has_value) {
        ++values_index;
      }

      // If we are in the middle of a repeated field, we should keep going
      // until we consume it all.
      if (repeated && levels_index + 1 < rep_levels.size() &&
          rep_levels[levels_index + 1] == 1) {
        inside_repeated_field = true;
      } else {
        inside_repeated_field = false;
      }

      ++levels_index;
    }

    // Print out the seeds with each failing ASSERT to easily reproduce the bug.
    std::string seeds = "seeds: " + std::to_string(seed1) + " " + std::to_string(seed2);

    // Perform the actual read/skip.
    if (is_skip) {
      int64_t skipped_records = record_reader->SkipRecords(num_records);
      ASSERT_EQ(skipped_records, num_records) << seeds;
    } else {
      int64_t read_records = record_reader->ReadRecords(num_records);
      ASSERT_EQ(read_records, num_records) << seeds;
    }

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    if (required) {
      ASSERT_EQ(record_reader->null_count(), 0) << seeds;
    }
    std::vector<int32_t> read_vals(read_values,
                                   read_values + record_reader->values_written());
    for (size_t i = 0; i < expected_values.size(); ++i) {
      if (expected_values[i] != -1) {
        ASSERT_EQ(read_vals[i], expected_values[i]) << seeds;
      }
    }

    if (!required) {
      std::vector<int16_t> read_def_levels(
          record_reader->def_levels(),
          record_reader->def_levels() + record_reader->levels_position());
      ASSERT_TRUE(vector_equal(read_def_levels, expected_def_levels)) << seeds;
    }

    if (repeated) {
      std::vector<int16_t> read_rep_levels(
          record_reader->rep_levels(),
          record_reader->rep_levels() + record_reader->levels_position());
      ASSERT_TRUE(vector_equal(read_rep_levels, expected_rep_levels)) << seeds;
    }
  }
}

INSTANTIATE_TEST_SUITE_P(Repetition_type, RecordReaderStressTest,
                         ::testing::Values(Repetition::REQUIRED, Repetition::OPTIONAL,
                                           Repetition::REPEATED));

}  // namespace test
}  // namespace parquet
