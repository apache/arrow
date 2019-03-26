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

#include "parquet/column_page.h"
#include "parquet/column_scanner.h"
#include "parquet/schema.h"
#include "parquet/test-specialization.h"
#include "parquet/test-util.h"
#include "parquet/types.h"
#include "parquet/util/test-common.h"

namespace parquet {

using schema::NodePtr;

namespace test {

template <>
void InitDictValues<bool>(int num_values, int dict_per_page, std::vector<bool>& values,
                          std::vector<uint8_t>& buffer) {
  // No op for bool
}

template <typename Type>
class TestFlatScanner : public ::testing::Test {
 public:
  typedef typename Type::c_type T;

  void InitScanner(const ColumnDescriptor* d) {
    std::unique_ptr<PageReader> pager(new test::MockPageReader(pages_));
    scanner_ = Scanner::Make(ColumnReader::Make(d, std::move(pager)));
  }

  void CheckResults(int batch_size, const ColumnDescriptor* d) {
    TypedScanner<Type>* scanner = reinterpret_cast<TypedScanner<Type>*>(scanner_.get());
    T val;
    bool is_null = false;
    int16_t def_level;
    int16_t rep_level;
    int j = 0;
    scanner->SetBatchSize(batch_size);
    for (int i = 0; i < num_levels_; i++) {
      ASSERT_TRUE(scanner->Next(&val, &def_level, &rep_level, &is_null)) << i << j;
      if (!is_null) {
        ASSERT_EQ(values_[j], val) << i << "V" << j;
        j++;
      }
      if (d->max_definition_level() > 0) {
        ASSERT_EQ(def_levels_[i], def_level) << i << "D" << j;
      }
      if (d->max_repetition_level() > 0) {
        ASSERT_EQ(rep_levels_[i], rep_level) << i << "R" << j;
      }
    }
    ASSERT_EQ(num_values_, j);
    ASSERT_FALSE(scanner->Next(&val, &def_level, &rep_level, &is_null));
  }

  void Clear() {
    pages_.clear();
    values_.clear();
    def_levels_.clear();
    rep_levels_.clear();
  }

  void Execute(int num_pages, int levels_per_page, int batch_size,
               const ColumnDescriptor* d, Encoding::type encoding) {
    num_values_ = MakePages<Type>(d, num_pages, levels_per_page, def_levels_, rep_levels_,
                                  values_, data_buffer_, pages_, encoding);
    num_levels_ = num_pages * levels_per_page;
    InitScanner(d);
    CheckResults(batch_size, d);
    Clear();
  }

  void InitDescriptors(std::shared_ptr<ColumnDescriptor>& d1,
                       std::shared_ptr<ColumnDescriptor>& d2,
                       std::shared_ptr<ColumnDescriptor>& d3, int length) {
    NodePtr type;
    type = schema::PrimitiveNode::Make("c1", Repetition::REQUIRED, Type::type_num,
                                       LogicalType::NONE, length);
    d1.reset(new ColumnDescriptor(type, 0, 0));
    type = schema::PrimitiveNode::Make("c2", Repetition::OPTIONAL, Type::type_num,
                                       LogicalType::NONE, length);
    d2.reset(new ColumnDescriptor(type, 4, 0));
    type = schema::PrimitiveNode::Make("c3", Repetition::REPEATED, Type::type_num,
                                       LogicalType::NONE, length);
    d3.reset(new ColumnDescriptor(type, 4, 2));
  }

  void ExecuteAll(int num_pages, int num_levels, int batch_size, int type_length,
                  Encoding::type encoding = Encoding::PLAIN) {
    std::shared_ptr<ColumnDescriptor> d1;
    std::shared_ptr<ColumnDescriptor> d2;
    std::shared_ptr<ColumnDescriptor> d3;
    InitDescriptors(d1, d2, d3, type_length);
    // evaluate REQUIRED pages
    Execute(num_pages, num_levels, batch_size, d1.get(), encoding);
    // evaluate OPTIONAL pages
    Execute(num_pages, num_levels, batch_size, d2.get(), encoding);
    // evaluate REPEATED pages
    Execute(num_pages, num_levels, batch_size, d3.get(), encoding);
  }

 protected:
  int num_levels_;
  int num_values_;
  std::vector<std::shared_ptr<Page>> pages_;
  std::shared_ptr<Scanner> scanner_;
  std::vector<T> values_;
  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  std::vector<uint8_t> data_buffer_;  // For BA and FLBA
};

static int num_levels_per_page = 100;
static int num_pages = 20;
static int batch_size = 32;

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                         ByteArrayType>
    TestTypes;

using TestBooleanFlatScanner = TestFlatScanner<BooleanType>;
using TestFLBAFlatScanner = TestFlatScanner<FLBAType>;

TYPED_TEST_CASE(TestFlatScanner, TestTypes);

TYPED_TEST(TestFlatScanner, TestPlainScanner) {
  ASSERT_NO_FATAL_FAILURE(
      this->ExecuteAll(num_pages, num_levels_per_page, batch_size, 0, Encoding::PLAIN));
}

TYPED_TEST(TestFlatScanner, TestDictScanner) {
  ASSERT_NO_FATAL_FAILURE(this->ExecuteAll(num_pages, num_levels_per_page, batch_size, 0,
                                           Encoding::RLE_DICTIONARY));
}

TEST_F(TestBooleanFlatScanner, TestPlainScanner) {
  ASSERT_NO_FATAL_FAILURE(
      this->ExecuteAll(num_pages, num_levels_per_page, batch_size, 0));
}

TEST_F(TestFLBAFlatScanner, TestPlainScanner) {
  ASSERT_NO_FATAL_FAILURE(
      this->ExecuteAll(num_pages, num_levels_per_page, batch_size, FLBA_LENGTH));
}

TEST_F(TestFLBAFlatScanner, TestDictScanner) {
  ASSERT_NO_FATAL_FAILURE(this->ExecuteAll(num_pages, num_levels_per_page, batch_size,
                                           FLBA_LENGTH, Encoding::RLE_DICTIONARY));
}

TEST_F(TestFLBAFlatScanner, TestPlainDictScanner) {
  ASSERT_NO_FATAL_FAILURE(this->ExecuteAll(num_pages, num_levels_per_page, batch_size,
                                           FLBA_LENGTH, Encoding::PLAIN_DICTIONARY));
}

// PARQUET 502
TEST_F(TestFLBAFlatScanner, TestSmallBatch) {
  NodePtr type =
      schema::PrimitiveNode::Make("c1", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY,
                                  LogicalType::DECIMAL, FLBA_LENGTH, 10, 2);
  const ColumnDescriptor d(type, 0, 0);
  num_values_ = MakePages<FLBAType>(&d, 1, 100, def_levels_, rep_levels_, values_,
                                    data_buffer_, pages_);
  num_levels_ = 1 * 100;
  InitScanner(&d);
  ASSERT_NO_FATAL_FAILURE(CheckResults(1, &d));
}

TEST_F(TestFLBAFlatScanner, TestDescriptorAPI) {
  NodePtr type =
      schema::PrimitiveNode::Make("c1", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY,
                                  LogicalType::DECIMAL, FLBA_LENGTH, 10, 2);
  const ColumnDescriptor d(type, 4, 0);
  num_values_ = MakePages<FLBAType>(&d, 1, 100, def_levels_, rep_levels_, values_,
                                    data_buffer_, pages_);
  num_levels_ = 1 * 100;
  InitScanner(&d);
  TypedScanner<FLBAType>* scanner =
      reinterpret_cast<TypedScanner<FLBAType>*>(scanner_.get());
  ASSERT_EQ(10, scanner->descr()->type_precision());
  ASSERT_EQ(2, scanner->descr()->type_scale());
  ASSERT_EQ(FLBA_LENGTH, scanner->descr()->type_length());
}

TEST_F(TestFLBAFlatScanner, TestFLBAPrinterNext) {
  NodePtr type =
      schema::PrimitiveNode::Make("c1", Repetition::OPTIONAL, Type::FIXED_LEN_BYTE_ARRAY,
                                  LogicalType::DECIMAL, FLBA_LENGTH, 10, 2);
  const ColumnDescriptor d(type, 4, 0);
  num_values_ = MakePages<FLBAType>(&d, 1, 100, def_levels_, rep_levels_, values_,
                                    data_buffer_, pages_);
  num_levels_ = 1 * 100;
  InitScanner(&d);
  TypedScanner<FLBAType>* scanner =
      reinterpret_cast<TypedScanner<FLBAType>*>(scanner_.get());
  scanner->SetBatchSize(batch_size);
  std::stringstream ss_fail;
  for (int i = 0; i < num_levels_; i++) {
    std::stringstream ss;
    scanner->PrintNext(ss, 17);
    std::string result = ss.str();
    ASSERT_LE(17, result.size()) << i;
  }
  ASSERT_THROW(scanner->PrintNext(ss_fail, 17), ParquetException);
}

}  // namespace test
}  // namespace parquet
