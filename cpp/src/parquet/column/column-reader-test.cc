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
#include <vector>

#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/test-util.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;
using std::shared_ptr;

namespace parquet {

using schema::NodePtr;

namespace test {

template <typename T>
static inline bool vector_equal_with_def_levels(const vector<T>& left,
    const vector<int16_t>& def_levels, int16_t max_def_levels, int16_t max_rep_levels,
    const vector<T>& right) {
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
    } else if (def_levels[i] == (max_def_levels -1)) {
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
    vector<int32_t> vresult(num_values_, -1);
    vector<int16_t> dresult(num_levels_, -1);
    vector<int16_t> rresult(num_levels_, -1);
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
      batch = static_cast<int>(reader->ReadBatch(batch_size, &dresult[0] + batch_actual,
          &rresult[0] + batch_actual, &vresult[0] + total_values_read, &values_read));
      total_values_read += static_cast<int>(values_read);
      batch_actual += batch;
      batch_size = std::max(batch_size * 2, 4096);
    } while (batch > 0);

    ASSERT_EQ(num_levels_, batch_actual);
    ASSERT_EQ(num_values_, total_values_read);
    ASSERT_TRUE(vector_equal(values_, vresult));
    if (max_def_level_ > 0) { ASSERT_TRUE(vector_equal(def_levels_, dresult)); }
    if (max_rep_level_ > 0) { ASSERT_TRUE(vector_equal(rep_levels_, rresult)); }
    // catch improper writes at EOS
    batch_actual =
        static_cast<int>(reader->ReadBatch(5, nullptr, nullptr, nullptr, &values_read));
    ASSERT_EQ(0, batch_actual);
    ASSERT_EQ(0, values_read);
  }

  void CheckResultsSpaced() {
    vector<int32_t> vresult(num_levels_, -1);
    vector<int16_t> dresult(num_levels_, -1);
    vector<int16_t> rresult(num_levels_, -1);
    vector<uint8_t> valid_bits(num_levels_, 255);
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
      batch = static_cast<int>(reader->ReadBatchSpaced(batch_size,
          dresult.data() + levels_actual, rresult.data() + levels_actual,
          vresult.data() + batch_actual, valid_bits.data() + batch_actual, 0,
          &levels_read, &values_read, &null_count));
      total_values_read += batch - static_cast<int>(null_count);
      batch_actual += batch;
      levels_actual += static_cast<int>(levels_read);
      batch_size = std::max(batch_size * 2, 4096);
    } while ((batch > 0) || (levels_read > 0));

    ASSERT_EQ(num_levels_, levels_actual);
    ASSERT_EQ(num_values_, total_values_read);
    if (max_def_level_ > 0) {
      ASSERT_TRUE(vector_equal(def_levels_, dresult));
      ASSERT_TRUE(
          vector_equal_with_def_levels(values_, dresult, max_def_level_,
            max_rep_level_, vresult));
    } else {
      ASSERT_TRUE(vector_equal(values_, vresult));
    }
    if (max_rep_level_ > 0) { ASSERT_TRUE(vector_equal(rep_levels_, rresult)); }
    // catch improper writes at EOS
    batch_actual = static_cast<int>(reader->ReadBatchSpaced(5, nullptr, nullptr, nullptr,
        valid_bits.data(), 0, &levels_read, &values_read, &null_count));
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
    num_values_ = MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_,
        rep_levels_, values_, data_buffer_, pages_, Encoding::PLAIN);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResults();
    Clear();

    num_values_ = MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_,
        rep_levels_, values_, data_buffer_, pages_, Encoding::PLAIN);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResultsSpaced();
    Clear();
  }

  void ExecuteDict(int num_pages, int levels_per_page, const ColumnDescriptor* d) {
    num_values_ = MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_,
        rep_levels_, values_, data_buffer_, pages_, Encoding::RLE_DICTIONARY);
    num_levels_ = num_pages * levels_per_page;
    InitReader(d);
    CheckResults();
    Clear();

    num_values_ = MakePages<Int32Type>(d, num_pages, levels_per_page, def_levels_,
        rep_levels_, values_, data_buffer_, pages_, Encoding::RLE_DICTIONARY);
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
  vector<shared_ptr<Page>> pages_;
  std::shared_ptr<ColumnReader> reader_;
  vector<int32_t> values_;
  vector<int16_t> def_levels_;
  vector<int16_t> rep_levels_;
  vector<uint8_t> data_buffer_;  // For BA and FLBA
};

TEST_F(TestPrimitiveReader, TestInt32FlatRequired) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  ExecutePlain(num_pages, levels_per_page, &descr);
  ExecuteDict(num_pages, levels_per_page, &descr);
}

TEST_F(TestPrimitiveReader, TestInt32FlatOptional) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 4;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  ExecutePlain(num_pages, levels_per_page, &descr);
  ExecuteDict(num_pages, levels_per_page, &descr);
}

TEST_F(TestPrimitiveReader, TestInt32FlatRepeated) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 4;
  max_rep_level_ = 2;
  NodePtr type = schema::Int32("c", Repetition::REPEATED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  ExecutePlain(num_pages, levels_per_page, &descr);
  ExecuteDict(num_pages, levels_per_page, &descr);
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
  vector<int32_t> vresult(levels_per_page / 2, -1);
  vector<int16_t> dresult(levels_per_page / 2, -1);
  vector<int16_t> rresult(levels_per_page / 2, -1);

  Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());
  int64_t values_read = 0;

  // 1) skip_size > page_size (multiple pages skipped)
  // Skip first 2 pages
  int64_t levels_skipped = reader->Skip(2 * levels_per_page);
  ASSERT_EQ(2 * levels_per_page, levels_skipped);
  // Read half a page
  reader->ReadBatch(
      levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(), &values_read);
  vector<int32_t> sub_values(values_.begin() + 2 * levels_per_page,
      values_.begin() + static_cast<int>(2.5 * static_cast<double>(levels_per_page)));
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 2) skip_size == page_size (skip across two pages)
  levels_skipped = reader->Skip(levels_per_page);
  ASSERT_EQ(levels_per_page, levels_skipped);
  // Read half a page
  reader->ReadBatch(
      levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(), &values_read);
  sub_values.clear();
  sub_values.insert(sub_values.end(),
      values_.begin() + static_cast<int>(3.5 * static_cast<double>(levels_per_page)),
      values_.begin() + 4 * levels_per_page);
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  // 3) skip_size < page_size (skip limited to a single page)
  // Skip half a page
  levels_skipped = reader->Skip(levels_per_page / 2);
  ASSERT_EQ(0.5 * levels_per_page, levels_skipped);
  // Read half a page
  reader->ReadBatch(
      levels_per_page / 2, dresult.data(), rresult.data(), vresult.data(), &values_read);
  sub_values.clear();
  sub_values.insert(sub_values.end(),
      values_.begin() + static_cast<int>(4.5 * static_cast<double>(levels_per_page)),
      values_.end());
  ASSERT_TRUE(vector_equal(sub_values, vresult));

  values_.clear();
  def_levels_.clear();
  rep_levels_.clear();
  pages_.clear();
  reader_.reset();
}

TEST_F(TestPrimitiveReader, TestDictionaryEncodedPages) {
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  shared_ptr<PoolBuffer> dummy = std::make_shared<PoolBuffer>();

  shared_ptr<DictionaryPage> dict_page =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN);
  shared_ptr<DataPage> data_page = MakeDataPage<Int32Type>(
      &descr, {}, 0, Encoding::RLE_DICTIONARY, {}, 0, {}, 0, {}, 0);
  pages_.push_back(dict_page);
  pages_.push_back(data_page);
  InitReader(&descr);
  // Tests Dict : PLAIN, Data : RLE_DICTIONARY
  ASSERT_NO_THROW(reader_->HasNext());
  pages_.clear();

  dict_page = std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN_DICTIONARY);
  data_page = MakeDataPage<Int32Type>(
      &descr, {}, 0, Encoding::PLAIN_DICTIONARY, {}, 0, {}, 0, {}, 0);
  pages_.push_back(dict_page);
  pages_.push_back(data_page);
  InitReader(&descr);
  // Tests Dict : PLAIN_DICTIONARY, Data : PLAIN_DICTIONARY
  ASSERT_NO_THROW(reader_->HasNext());
  pages_.clear();

  data_page = MakeDataPage<Int32Type>(
      &descr, {}, 0, Encoding::RLE_DICTIONARY, {}, 0, {}, 0, {}, 0);
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

  shared_ptr<DictionaryPage> dict_page1 =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN_DICTIONARY);
  shared_ptr<DictionaryPage> dict_page2 =
      std::make_shared<DictionaryPage>(dummy, 0, Encoding::PLAIN);
  pages_.push_back(dict_page1);
  pages_.push_back(dict_page2);
  InitReader(&descr);
  // Column cannot have more than one dictionary
  ASSERT_THROW(reader_->HasNext(), ParquetException);
  pages_.clear();

  data_page = MakeDataPage<Int32Type>(
      &descr, {}, 0, Encoding::DELTA_BYTE_ARRAY, {}, 0, {}, 0, {}, 0);
  pages_.push_back(data_page);
  InitReader(&descr);
  // unsupported encoding
  ASSERT_THROW(reader_->HasNext(), ParquetException);
  pages_.clear();
}

}  // namespace test
}  // namespace parquet
