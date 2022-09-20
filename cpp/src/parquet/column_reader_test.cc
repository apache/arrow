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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/make_unique.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/schema.h"
#include "parquet/test_util.h"
#include "parquet/types.h"

namespace parquet {

using schema::NodePtr;

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
    std::unique_ptr<PageReader> pager_;
    pager_.reset(new test::MockPageReader(pages_));
    reader_ = ColumnReader::Make(d, std::move(pager_));
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

TEST_F(TestPrimitiveReader, TestInt32FlatRequiredSkip) {
  int levels_per_page = 100;
  int num_pages = 5;
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
      values_.begin() + static_cast<int>(2.5 * static_cast<double>(levels_per_page)));
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 2) skip_size == page_size (skip across two pages)
  levels_skipped = reader->Skip(levels_per_page);
  ASSERT_EQ(levels_per_page, levels_skipped);
  // Read half a page
  reader->ReadBatch(levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(),
                    &values_read);
  sub_values.clear();
  sub_values.insert(
      sub_values.end(),
      values_.begin() + static_cast<int>(3.5 * static_cast<double>(levels_per_page)),
      values_.begin() + 4 * levels_per_page);
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 3) skip_size < page_size (skip limited to a single page)
  // Skip half a page
  levels_skipped = reader->Skip(levels_per_page / 2);
  ASSERT_EQ(0.5 * levels_per_page, levels_skipped);
  // Read half a page
  reader->ReadBatch(levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(),
                    &values_read);
  sub_values.clear();
  sub_values.insert(
      sub_values.end(),
      values_.begin() + static_cast<int>(4.5 * static_cast<double>(levels_per_page)),
      values_.end());
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  values_.clear();
  def_levels_.clear();
  rep_levels_.clear();
  pages_.clear();
  reader_.reset();
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
  auto indices = ::arrow::internal::make_unique<int32_t[]>(value_size);
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
  auto indices = ::arrow::internal::make_unique<int32_t[]>(value_size);
  // Dictionary cannot be exposed when it's not fully dictionary encoded
  EXPECT_THROW(reader->ReadBatchWithDictionary(value_size, /*def_levels=*/nullptr,
                                               /*rep_levels=*/nullptr, indices.get(),
                                               &indices_read, &dict, &dict_len),
               ParquetException);
  pages_.clear();
}

// Tests reading a repeated field using the RecordReader.
TEST(RecordReaderTest, BasicReadRepeatedField) {
  internal::LevelInfo level_info;
  level_info.def_level = 1;
  level_info.rep_level = 1;

  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, level_info.def_level,
                               level_info.rep_level);


  // Records look like: {[10], [20, 20], [30, 30, 30]}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};
  std::vector<int16_t> def_levels = {1, 1, 1, 1, 1, 1};
  std::vector<int16_t> rep_levels = {0, 0, 1, 0, 1, 1};

  std::unique_ptr<PageReader> pager;
  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      &descr, values, /*num_values=*/def_levels.size(), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, level_info.def_level, rep_levels,
      level_info.rep_level);
  pages.push_back(std::move(page));
  pager.reset(new test::MockPageReader(pages));

  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  int64_t records_read = record_reader->ReadRecords(/*num_records=*/2);

  ASSERT_EQ(records_read, 2);
  ASSERT_EQ(record_reader->values_written(), 3);
  ASSERT_EQ(record_reader->null_count(), 0);
  ASSERT_EQ(record_reader->levels_written(), 6);
  ASSERT_EQ(record_reader->levels_position(), 3);

  const auto read_values =
      reinterpret_cast<const int32_t*>(record_reader->values());
  std::vector<int32_t> read_vals(read_values,
                                 read_values + record_reader->values_written());
  std::vector<int16_t> read_defs(
      record_reader->def_levels(),
      record_reader->def_levels() + record_reader->levels_position());
  std::vector<int16_t> read_reps(
      record_reader->rep_levels(),
      record_reader->rep_levels() + record_reader->levels_position());

  ASSERT_TRUE(vector_equal(read_vals, {10, 20, 20}));
  ASSERT_TRUE(vector_equal(read_defs, {1, 1, 1}));
  ASSERT_TRUE(vector_equal(read_reps, {0, 0, 1}));
}

// Test that we can skip required top level field.
TEST(RecordReaderTest, SkipRequiredTopLevel) {
  internal::LevelInfo level_info;
  level_info.def_level = 0;
  level_info.rep_level = 0;

  NodePtr type = schema::Int32("b", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, level_info.def_level,
                               level_info.rep_level);

  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {10, 20, 20, 30, 30, 30};

  std::unique_ptr<PageReader> pager;
  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      &descr, values, /*num_values=*/values.size(), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, /*def_levels=*/{}, level_info.def_level,
      /*rep_levels=*/{}, level_info.rep_level);
  pages.push_back(std::move(page));
  pager.reset(new test::MockPageReader(pages));

  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  int64_t records_skipped = record_reader->SkipRecords(/*num_records=*/3);

  ASSERT_EQ(records_skipped, 3);
  ASSERT_EQ(record_reader->values_written(), 0);
  ASSERT_EQ(record_reader->levels_written(), 0);
  ASSERT_EQ(record_reader->levels_position(), 0);

  int64_t records_read = record_reader->ReadRecords(/*num_records=*/2);

  ASSERT_EQ(records_read, 2);
  ASSERT_EQ(record_reader->values_written(), 2);
  ASSERT_EQ(record_reader->levels_written(), 0);
  ASSERT_EQ(record_reader->levels_position(), 0);

  const auto read_values =
      reinterpret_cast<const int32_t*>(record_reader->values());
  std::vector<int32_t> read_vals(read_values,
                                 read_values + record_reader->values_written());

  ASSERT_TRUE(vector_equal(read_vals, {30, 30}));
}

// Skip an optional field. Intentionally included some null values.
TEST(RecordReaderTest, SkipOptional) {
  internal::LevelInfo level_info;
  level_info.def_level = 1;
  level_info.rep_level = 0;

  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, level_info.def_level, level_info.rep_level);

  // Records look like {null, 20, 20, 20, null, 30, 30}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {20, 20, 20, 30, 30};
  std::vector<int16_t> def_levels = {0, 1, 1, 1, 0, 1, 1};

  std::unique_ptr<PageReader> pager;
  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      &descr, values, /*num_values=*/values.size(), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, level_info.def_level,
      /*rep_levels=*/{}, level_info.rep_level);
  pages.push_back(std::move(page));
  pager.reset(new test::MockPageReader(pages));

  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  {
    // Skip {null, 20}
    // This also tests when we start with a Skip.
    int64_t records_skipped = record_reader->SkipRecords(/*num_records=*/2);

    ASSERT_EQ(records_skipped, 2);
    ASSERT_EQ(record_reader->values_written(), 0);
    // Since we started with skipping, there was nothing in the buffer to consume
    // for skipping.
    ASSERT_EQ(record_reader->levels_written(), 0);
    ASSERT_EQ(record_reader->levels_position(), 0);
  }

  {
    // Read 3 records: {20, 20, null}
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/3);

    ASSERT_EQ(records_read, 3);
    // One of these values is null. values_written() includes null values.
    ASSERT_EQ(record_reader->values_written(), 3);
    // When we read, we buffer some levels. Since there are only a few levels
    // in our whole column, all of them are read.
    // We had skipped 2 of the levels above. So there is only 5 left in total to
    // read, and we read 3 of them here.
    ASSERT_EQ(record_reader->levels_written(), 5);
    ASSERT_EQ(record_reader->levels_position(), 3);

    // Check that definition levels are correct. Repetition levels are not
    // relevant since this is not a repeated field.
    std::vector<int16_t> read_defs(
        record_reader->def_levels(),
        record_reader->def_levels() + record_reader->levels_position());

    // Double check that the values are untouched.
    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(
        read_values,
        read_values + record_reader->values_written() - record_reader->null_count());

    ASSERT_TRUE(vector_equal(read_vals, {20, 20}));
    ASSERT_TRUE(vector_equal(read_defs, {1, 1, 0}));
  }

  {
    // Skip to the end of the column. Skip {30, 30}.
    int64_t records_skipped = record_reader->SkipRecords(/*num_records=*/5);

    ASSERT_EQ(records_skipped, 2);

    // When we skip number of values written does not change. However, we will
    // throw away the levels that were skipped from the buffer, thus levels_written()
    // is reduced by 2.
    ASSERT_EQ(record_reader->values_written(), 3);
    ASSERT_EQ(record_reader->levels_written(), 3);
    ASSERT_EQ(record_reader->levels_position(), 3);

    // Check the definition levels to make sure we can read it correctly and we
    // threw away the levels in the buffer that were skipped.
    std::vector<int16_t> read_defs(
        record_reader->def_levels(),
        record_reader->def_levels() + record_reader->levels_position());

    // Double check that the values are untouched.
    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(
        read_values,
        read_values + record_reader->values_written() - record_reader->null_count());

    ASSERT_TRUE(vector_equal(read_vals, {20, 20}));
    ASSERT_TRUE(vector_equal(read_defs, {1, 1, 0}));
  }

  // We have exhausted all the records.
  ASSERT_EQ(record_reader->ReadRecords(/*num_records=*/3), 0);
  ASSERT_EQ(record_reader->SkipRecords(/*num_records=*/3), 0);
}

// Test skipping for repeated fields.
TEST(RecordReaderTest, SkipRepeated) {
  internal::LevelInfo level_info;
  level_info.def_level = 1;
  level_info.rep_level = 1;

  NodePtr type = schema::Int32("b", Repetition::REPEATED);
  const ColumnDescriptor descr(type, level_info.def_level, level_info.rep_level);

  // Records look like {null, [20, 20, 20], null, [30, 30], [40]}
  std::vector<std::shared_ptr<Page>> pages;
  std::vector<int32_t> values = {20, 20, 20, 30, 30, 40};
  std::vector<int16_t> def_levels = {0, 1, 1, 1, 0, 1, 1, 1};
  std::vector<int16_t> rep_levels = {0, 0, 1, 1, 0, 0, 1, 0};

  std::unique_ptr<PageReader> pager;
  std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
      &descr, values, /*num_values=*/values.size(), Encoding::PLAIN,
      /*indices=*/{},
      /*indices_size=*/0, def_levels, level_info.def_level, rep_levels,
      level_info.rep_level);
  pages.push_back(std::move(page));
  pager.reset(new test::MockPageReader(pages));

  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  {
    // This should skip the first null record.
    int64_t records_skipped = record_reader->SkipRecords(/*num_records=*/1);

    ASSERT_EQ(records_skipped, 1);
    ASSERT_EQ(record_reader->values_written(), 0);
    ASSERT_EQ(record_reader->null_count(), 0);
    // For repeated fields, we need to read the levels to find the record
    // boundaries and skip. So some levels are read, however, the skipped
    // level should not be there after the skip. That's why levels_position()
    // is 0.
    ASSERT_EQ(record_reader->levels_written(), 7);
    ASSERT_EQ(record_reader->levels_position(), 0);
  }

  {
    // Read [20, 20, 20]
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);
    ASSERT_EQ(record_reader->values_written(), 3);
    ASSERT_EQ(record_reader->null_count(), 0);
    ASSERT_EQ(record_reader->levels_written(), 7);
    ASSERT_EQ(record_reader->levels_position(), 3);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(read_values,
                                   read_values + record_reader->values_written());
    std::vector<int16_t> read_defs(
        record_reader->def_levels(),
        record_reader->def_levels() + record_reader->levels_position());
    std::vector<int16_t> read_reps(
        record_reader->rep_levels(),
        record_reader->rep_levels() + record_reader->levels_position());

    ASSERT_TRUE(vector_equal(read_vals, {20, 20, 20}));
    ASSERT_TRUE(vector_equal(read_defs, {1, 1, 1}));
    ASSERT_TRUE(vector_equal(read_reps, {0, 1, 1}));
  }

  {
    // Skip the null record and also skip [30, 30]
    int64_t records_skipped = record_reader->SkipRecords(/*num_records=*/2);

    ASSERT_EQ(records_skipped, 2);
    ASSERT_EQ(record_reader->values_written(), 3);
    ASSERT_EQ(record_reader->null_count(), 0);
    // We remove the skipped levels from the buffer.
    ASSERT_EQ(record_reader->levels_written(), 4);
    ASSERT_EQ(record_reader->levels_position(), 3);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(read_values,
                                   read_values + record_reader->values_written());
    std::vector<int16_t> read_defs(
        record_reader->def_levels(),
        record_reader->def_levels() + record_reader->levels_position());
    std::vector<int16_t> read_reps(
        record_reader->rep_levels(),
        record_reader->rep_levels() + record_reader->levels_position());

    ASSERT_TRUE(vector_equal(read_vals, {20, 20, 20}));
    ASSERT_TRUE(vector_equal(read_defs, {1, 1, 1}));
    ASSERT_TRUE(vector_equal(read_reps, {0, 1, 1}));
  }

  {
    // Read [40]
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);
    ASSERT_EQ(record_reader->values_written(), 4);
    ASSERT_EQ(record_reader->null_count(), 0);
    ASSERT_EQ(record_reader->levels_written(), 4);
    ASSERT_EQ(record_reader->levels_position(), 4);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(read_values,
                                   read_values + record_reader->values_written());
    std::vector<int16_t> read_defs(
        record_reader->def_levels(),
        record_reader->def_levels() + record_reader->levels_position());
    std::vector<int16_t> read_reps(
        record_reader->rep_levels(),
        record_reader->rep_levels() + record_reader->levels_position());

    ASSERT_TRUE(vector_equal(read_vals, {20, 20, 20, 40}));
    ASSERT_TRUE(vector_equal(read_defs, {1, 1, 1, 1}));
    ASSERT_TRUE(vector_equal(read_reps, {0, 1, 1, 0}));
  }
}

// Test reading when one record spans multiple pages for a repeated field.
TEST(RecordReaderTest, ReadPartialRecord) {
  internal::LevelInfo level_info;
  level_info.def_level = 1;
  level_info.rep_level = 1;

  NodePtr type = schema::Int32("b", Repetition::REPEATED);
  const ColumnDescriptor descr(type, level_info.def_level, level_info.rep_level);

  std::vector<std::shared_ptr<Page>> pages;
  std::unique_ptr<PageReader> pager;

  // Page 1: {[10], [20, 20, 20 ... } continues to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        &descr, /*values=*/{10, 20, 20, 20}, /*num_values=*/4, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1, 1, 1}, level_info.def_level,
        /*rep_levels=*/{0, 0, 1, 1}, level_info.rep_level);
    pages.push_back(std::move(page));
  }

  // Page 2: {... 20, 20, ...} continues from previous page and to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        &descr, /*values=*/{20, 20}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, level_info.def_level,
        /*rep_levels=*/{1, 1}, level_info.rep_level);
    pages.push_back(std::move(page));
  }

  // Page 3: { ... 20, [30]} continues from previous page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        &descr, /*values=*/{20, 30}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, level_info.def_level,
        /*rep_levels=*/{1, 0}, level_info.rep_level);
    pages.push_back(std::move(page));
  }

  pager.reset(new test::MockPageReader(pages));

  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  {
    // Read [10]
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(
        read_values,
        read_values + record_reader->values_written() - record_reader->null_count());

    ASSERT_TRUE(vector_equal(read_vals, {10}));
  }

  {
    // Read [20, 20, 20, 20, 20, 20] that spans multiple pages.
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(
        read_values,
        read_values + record_reader->values_written() - record_reader->null_count());

    ASSERT_TRUE(vector_equal(read_vals, {10, 20, 20, 20, 20, 20, 20}));
  }

  {
    // Read [30]
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(
        read_values,
        read_values + record_reader->values_written() - record_reader->null_count());

    ASSERT_TRUE(vector_equal(read_vals, {10, 20, 20, 20, 20, 20, 20, 30}));
  }
}

// Test skipping for repeated fields for the case when one record spans multiple
// pages.
TEST(RecordReaderTest, SkipPartialRecord) {
  internal::LevelInfo level_info;
  level_info.def_level = 1;
  level_info.rep_level = 1;

  NodePtr type = schema::Int32("b", Repetition::REPEATED);
  const ColumnDescriptor descr(type, level_info.def_level, level_info.rep_level);

  std::vector<std::shared_ptr<Page>> pages;
  std::unique_ptr<PageReader> pager;

  // Page 1: {[10], [20, 20, 20 ... } continues to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        &descr, /*values=*/{10, 20, 20, 20}, /*num_values=*/4, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1, 1, 1}, level_info.def_level,
        /*rep_levels=*/{0, 0, 1, 1}, level_info.rep_level);
    pages.push_back(std::move(page));
  }

  // Page 2: {... 20, 20, ...} continues from previous page and to next page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        &descr, /*values=*/{20, 20}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, level_info.def_level,
        /*rep_levels=*/{1, 1}, level_info.rep_level);
    pages.push_back(std::move(page));
  }

  // Page 3: { ... 20, [30]} continues from previous page.
  {
    std::shared_ptr<DataPageV1> page = MakeDataPage<Int32Type>(
        &descr, /*values=*/{20, 30}, /*num_values=*/2, Encoding::PLAIN,
        /*indices=*/{},
        /*indices_size=*/0, /*def_levels=*/{1, 1}, level_info.def_level,
        /*rep_levels=*/{1, 0}, level_info.rep_level);
    pages.push_back(std::move(page));
  }

  pager.reset(new test::MockPageReader(pages));

  std::shared_ptr<internal::RecordReader> record_reader =
      internal::RecordReader::Make(&descr, level_info);
  record_reader->SetPageReader(std::move(pager));

  {
    // Read [10]
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(
        read_values,
        read_values + record_reader->values_written() - record_reader->null_count());

    ASSERT_TRUE(vector_equal(read_vals, {10}));
    ASSERT_EQ(record_reader->values_written(), 1);
    ASSERT_EQ(record_reader->null_count(), 0);
    // There are 4 levels in the first page.
    ASSERT_EQ(record_reader->levels_written(), 4);
    ASSERT_EQ(record_reader->levels_position(), 1);
  }

  {
    // Skip the record that goes across pages.
    int64_t records_skipped = record_reader->SkipRecords(/*num_records=*/1);

    ASSERT_EQ(records_skipped, 1);
    ASSERT_EQ(record_reader->values_written(), 1);
    ASSERT_EQ(record_reader->null_count(), 0);
    ASSERT_EQ(record_reader->levels_written(), 2);
    ASSERT_EQ(record_reader->levels_position(), 1);
  }

  {
    // Read [30]
    int64_t records_read = record_reader->ReadRecords(/*num_records=*/1);

    ASSERT_EQ(records_read, 1);
    ASSERT_EQ(record_reader->values_written(), 2);
    ASSERT_EQ(record_reader->null_count(), 0);
    ASSERT_EQ(record_reader->levels_written(), 2);
    ASSERT_EQ(record_reader->levels_position(), 2);

    const auto read_values = reinterpret_cast<const int32_t*>(record_reader->values());
    std::vector<int32_t> read_vals(read_values,
                                   read_values + record_reader->values_written());
    std::vector<int16_t> read_defs(
        record_reader->def_levels(),
        record_reader->def_levels() + record_reader->levels_position());
    std::vector<int16_t> read_reps(
        record_reader->rep_levels(),
        record_reader->rep_levels() + record_reader->levels_position());

    ASSERT_TRUE(vector_equal(read_vals, {10, 30}));
    ASSERT_TRUE(vector_equal(read_defs, {1, 1}));
    ASSERT_TRUE(vector_equal(read_reps, {0, 0}));
  }
}

}  // namespace test
}  // namespace parquet
