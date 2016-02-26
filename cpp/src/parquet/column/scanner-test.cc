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
#include <memory>
#include <string>
#include <vector>

#include "parquet/types.h"
#include "parquet/column/page.h"
#include "parquet/column/scanner.h"
#include "parquet/column/test-util.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;
using std::shared_ptr;

namespace parquet_cpp {

using schema::NodePtr;

static int FLBA_LENGTH = 12;
bool operator==(const FixedLenByteArray& a, const FixedLenByteArray& b) {
  return 0 == memcmp(a.ptr, b.ptr, FLBA_LENGTH);
}

namespace test {

template <typename Type>
class TestFlatScanner : public ::testing::Test {
 public:
  typedef typename Type::c_type T;

  void InitValues() {
    random_numbers(num_values_, 0, std::numeric_limits<T>::min(),
        std::numeric_limits<T>::max(), values_.data());
  }

  void MakePages(const ColumnDescriptor *d, int num_pages, int levels_per_page) {
    num_levels_ = levels_per_page * num_pages;
    num_values_ = 0;
    uint32_t seed = 0;
    int16_t zero = 0;
    int16_t max_def_level = d->max_definition_level();
    int16_t max_rep_level = d->max_repetition_level();
    vector<int> values_per_page(num_pages, levels_per_page);
    // Create definition levels
    if (max_def_level > 0) {
      def_levels_.resize(num_levels_);
      random_numbers(num_levels_, seed, zero, max_def_level, def_levels_.data());
      for (int p = 0; p < num_pages; p++) {
        int num_values_per_page = 0;
        for (int i = 0; i < levels_per_page; i++) {
          if (def_levels_[i + p * levels_per_page] == max_def_level) {
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
    if (max_rep_level > 0) {
      rep_levels_.resize(num_levels_);
      random_numbers(num_levels_, seed, zero, max_rep_level, rep_levels_.data());
    }
    // Create values
    values_.resize(num_values_);
    InitValues();
    Paginate<Type::type_num>(d, values_, def_levels_, max_def_level,
        rep_levels_, max_rep_level, levels_per_page, values_per_page, pages_);
  }

  void InitScanner(const ColumnDescriptor *d) {
    std::unique_ptr<PageReader> pager(new test::MockPageReader(pages_));
    scanner_ = Scanner::Make(ColumnReader::Make(d, std::move(pager)));
  }

  void CheckResults(int batch_size, const ColumnDescriptor *d) {
    TypedScanner<Type::type_num>* scanner =
      reinterpret_cast<TypedScanner<Type::type_num>* >(scanner_.get());
    T val;
    bool is_null;
    int16_t def_level;
    int16_t rep_level;
    size_t j = 0;
    scanner->SetBatchSize(batch_size);
    for (size_t i = 0; i < num_levels_; i++) {
      ASSERT_TRUE(scanner->Next(&val, &def_level, &rep_level, &is_null)) << i << j;
      if (!is_null) {
        ASSERT_EQ(values_[j++], val) << i <<"V"<< j;
      }
      if (!d->is_required()) {
        ASSERT_EQ(def_levels_[i], def_level) << i <<"D"<< j;
      }
      if (d->is_repeated()) {
        ASSERT_EQ(rep_levels_[i], rep_level) << i <<"R"<< j;
      }
    }
    ASSERT_EQ(num_values_, j);
    ASSERT_FALSE(scanner->HasNext());
  }

  void Clear() {
    pages_.clear();
    values_.clear();
    def_levels_.clear();
    rep_levels_.clear();
  }

  void Execute(int num_pages, int levels_page, int batch_size,
      const ColumnDescriptor *d) {
    MakePages(d, num_pages, levels_page);
    InitScanner(d);
    CheckResults(batch_size, d);
    Clear();
  }

  void InitDescriptors(std::shared_ptr<ColumnDescriptor>& d1,
      std::shared_ptr<ColumnDescriptor>& d2, std::shared_ptr<ColumnDescriptor>& d3) {
    NodePtr type;
    type = schema::PrimitiveNode::Make("c1", Repetition::REQUIRED, Type::type_num);
    d1.reset(new ColumnDescriptor(type, 0, 0));
    type = schema::PrimitiveNode::Make("c2", Repetition::OPTIONAL, Type::type_num);
    d2.reset(new ColumnDescriptor(type, 4, 0));
    type = schema::PrimitiveNode::Make("c3", Repetition::REPEATED, Type::type_num);
    d3.reset(new ColumnDescriptor(type, 4, 2));
  }

  void ExecuteAll(int num_pages, int num_levels, int batch_size) {
    std::shared_ptr<ColumnDescriptor> d1;
    std::shared_ptr<ColumnDescriptor> d2;
    std::shared_ptr<ColumnDescriptor> d3;
    InitDescriptors(d1, d2, d3);
    // evaluate REQUIRED pages
    Execute(num_pages, num_levels, batch_size, d1.get());
    // evaluate OPTIONAL pages
    Execute(num_pages, num_levels, batch_size, d2.get());
    // evaluate REPEATED pages
    Execute(num_pages, num_levels, batch_size, d3.get());
  }

 protected:
  int num_levels_;
  int num_values_;
  vector<shared_ptr<Page> > pages_;
  std::shared_ptr<Scanner> scanner_;
  vector<T> values_;
  vector<int16_t> def_levels_;
  vector<int16_t> rep_levels_;
  vector<uint8_t> data_buffer_; // For BA and FLBA
};

template<>
void TestFlatScanner<BooleanType>::InitValues() {
  values_ = flip_coins(num_values_, 0);
}

template<>
void TestFlatScanner<Int96Type>::InitValues() {
  random_Int96_numbers(num_values_, 0, std::numeric_limits<int32_t>::min(),
      std::numeric_limits<int32_t>::max(), values_.data());
}

template<>
void TestFlatScanner<ByteArrayType>::InitValues() {
  int max_byte_array_len = 12;
  int num_bytes = max_byte_array_len + sizeof(uint32_t);
  size_t nbytes = num_values_ * num_bytes;
  data_buffer_.resize(nbytes);
  random_byte_array(num_values_, 0, data_buffer_.data(), values_.data(),
      max_byte_array_len);
}

template<>
void TestFlatScanner<FLBAType>::InitValues() {
  size_t nbytes = num_values_ * FLBA_LENGTH;
  data_buffer_.resize(nbytes);
  random_fixed_byte_array(num_values_, 0, data_buffer_.data(), FLBA_LENGTH,
      values_.data());
}

template<>
void TestFlatScanner<FLBAType>::InitDescriptors(
    std::shared_ptr<ColumnDescriptor>& d1, std::shared_ptr<ColumnDescriptor>& d2,
    std::shared_ptr<ColumnDescriptor>& d3) {
  NodePtr type = schema::PrimitiveNode::MakeFLBA("c1", Repetition::REQUIRED,
      FLBA_LENGTH, LogicalType::UTF8);
  d1.reset(new ColumnDescriptor(type, 0, 0));
  type = schema::PrimitiveNode::MakeFLBA("c2", Repetition::OPTIONAL,
      FLBA_LENGTH, LogicalType::UTF8);
  d2.reset(new ColumnDescriptor(type, 4, 0));
  type = schema::PrimitiveNode::MakeFLBA("c3", Repetition::REPEATED,
      FLBA_LENGTH, LogicalType::UTF8);
  d3.reset(new ColumnDescriptor(type, 4, 2));
}

typedef TestFlatScanner<FLBAType> TestFlatFLBAScanner;

static int num_levels_per_page = 100;
static int num_pages = 20;
static int batch_size = 32;

TYPED_TEST_CASE(TestFlatScanner, ParquetTypes);

TYPED_TEST(TestFlatScanner, TestScanner) {
  this->ExecuteAll(num_pages, num_levels_per_page, batch_size);
}

//PARQUET 502
TEST_F(TestFlatFLBAScanner, TestSmallBatch) {
  NodePtr type = schema::PrimitiveNode::MakeFLBA("c1", Repetition::REQUIRED,
      FLBA_LENGTH, LogicalType::UTF8);
  const ColumnDescriptor d(type, 0, 0);
  MakePages(&d, 1, 100);
  InitScanner(&d);
  CheckResults(1, &d);
}

} // namespace test
} // namespace parquet_cpp
