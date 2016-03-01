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

#include "parquet/types.h"
#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/test-util.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;
using std::shared_ptr;

namespace parquet_cpp {

using schema::NodePtr;

namespace test {

class TestPrimitiveReader : public ::testing::Test {
 public:
  void MakePages(const ColumnDescriptor *d, int num_pages, int levels_per_page) {
    num_levels_ = levels_per_page * num_pages;
    num_values_ = 0;
    uint32_t seed = 0;
    int16_t zero = 0;
    vector<int> values_per_page(num_pages, levels_per_page);
    // Create definition levels
    if (max_def_level_ > 0) {
      def_levels_.resize(num_levels_);
      random_numbers(num_levels_, seed, zero, max_def_level_, def_levels_.data());
      for (int p = 0; p < num_pages; p++) {
        int num_values_per_page = 0;
        for (int i = 0; i < levels_per_page; i++) {
          if (def_levels_[i + p * levels_per_page] == max_def_level_) {
            num_values_per_page++;
            num_values_++;
          }
        }
        values_per_page[p] = num_values_per_page;
      }
    } else {
      num_values_ = num_levels_;
    }
    // Create repitition levels
    if (max_rep_level_ > 0) {
      rep_levels_.resize(num_levels_);
      random_numbers(num_levels_, seed, zero, max_rep_level_, rep_levels_.data());
    }
    // Create values
    values_.resize(num_values_);
    random_numbers(num_values_, seed, std::numeric_limits<int32_t>::min(),
       std::numeric_limits<int32_t>::max(), values_.data());
    Paginate<Type::INT32, int32_t>(d, values_, def_levels_, max_def_level_,
        rep_levels_, max_rep_level_, levels_per_page, values_per_page, pages_);
  }

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
      batch = reader->ReadBatch(batch_size, &dresult[0] + batch_actual,
          &rresult[0] + batch_actual, &vresult[0] + total_values_read, &values_read);
      total_values_read += values_read;
      batch_actual += batch;
      batch_size = std::max(batch_size * 2, 4096);
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
    batch_actual = reader->ReadBatch(5, nullptr, nullptr, nullptr, &values_read);
    ASSERT_EQ(0, batch_actual);
    ASSERT_EQ(0, values_read);
  }

  void execute(int num_pages, int levels_page, const ColumnDescriptor *d) {
    MakePages(d, num_pages, levels_page);
    InitReader(d);
    CheckResults();
  }

 protected:
  int num_levels_;
  int num_values_;
  int16_t max_def_level_;
  int16_t max_rep_level_;
  vector<shared_ptr<Page> > pages_;
  std::shared_ptr<ColumnReader> reader_;
  vector<int32_t> values_;
  vector<int16_t> def_levels_;
  vector<int16_t> rep_levels_;
};

TEST_F(TestPrimitiveReader, TestInt32FlatRequired) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  execute(num_pages, levels_per_page, &descr);
}

TEST_F(TestPrimitiveReader, TestInt32FlatOptional) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 4;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  execute(num_pages, levels_per_page, &descr);
}

TEST_F(TestPrimitiveReader, TestInt32FlatRepeated) {
  int levels_per_page = 100;
  int num_pages = 50;
  max_def_level_ = 4;
  max_rep_level_ = 2;
  NodePtr type = schema::Int32("c", Repetition::REPEATED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  execute(num_pages, levels_per_page, &descr);
}

} // namespace test
} // namespace parquet_cpp
