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

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "parquet/types.h"
#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/test-util.h"

#include "parquet/util/test-common.h"

using std::string;
using std::vector;
using std::shared_ptr;
using parquet::FieldRepetitionType;
using parquet::SchemaElement;

namespace parquet_cpp {

using schema::NodePtr;

namespace test {

class TestPrimitiveReader : public ::testing::Test {
 public:
  void SetUp() {}

  void TearDown() {}

  void InitReader(const ColumnDescriptor* descr) {
    pager_.reset(new test::MockPageReader(pages_));
    reader_ = ColumnReader::Make(descr, std::move(pager_));
  }

 protected:
  std::shared_ptr<ColumnReader> reader_;
  std::unique_ptr<PageReader> pager_;
  vector<shared_ptr<Page> > pages_;
};

template <typename T>
static vector<T> slice(const vector<T>& values, size_t start, size_t end) {
  if (end < start) {
    return vector<T>(0);
  }

  vector<T> out(end - start);
  for (size_t i = start; i < end; ++i) {
    out[i - start] = values[i];
  }
  return out;
}


TEST_F(TestPrimitiveReader, TestInt32FlatRequired) {
  vector<int32_t> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  size_t num_values = values.size();
  parquet::Encoding::type value_encoding = parquet::Encoding::PLAIN;

  vector<uint8_t> page1;
  test::DataPageBuilder<Type::INT32> page_builder(&page1);
  page_builder.AppendValues(values, parquet::Encoding::PLAIN);
  pages_.push_back(page_builder.Finish());

  // TODO: simplify this
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  ColumnDescriptor descr(type, 0, 0);
  InitReader(&descr);

  Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());

  vector<int32_t> result(10, -1);

  size_t values_read = 0;
  size_t batch_actual = reader->ReadBatch(10, nullptr, nullptr,
      &result[0], &values_read);
  ASSERT_EQ(10, batch_actual);
  ASSERT_EQ(10, values_read);

  ASSERT_TRUE(vector_equal(result, values));
}

TEST_F(TestPrimitiveReader, TestInt32FlatOptional) {
  vector<int32_t> values = {1, 2, 3, 4, 5};
  vector<int16_t> def_levels = {1, 0, 0, 1, 1, 0, 0, 0, 1, 1};

  size_t num_values = values.size();
  parquet::Encoding::type value_encoding = parquet::Encoding::PLAIN;

  vector<uint8_t> page1;
  test::DataPageBuilder<Type::INT32> page_builder(&page1);

  // Definition levels precede the values
  page_builder.AppendDefLevels(def_levels, 1, parquet::Encoding::RLE);
  page_builder.AppendValues(values, parquet::Encoding::PLAIN);

  pages_.push_back(page_builder.Finish());

  NodePtr type = schema::Int32("a", Repetition::OPTIONAL);
  ColumnDescriptor descr(type, 1, 0);
  InitReader(&descr);

  Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());

  size_t values_read = 0;
  size_t batch_actual = 0;

  vector<int32_t> vresult(3, -1);
  vector<int16_t> dresult(5, -1);

  batch_actual = reader->ReadBatch(5, &dresult[0], nullptr,
      &vresult[0], &values_read);
  ASSERT_EQ(5, batch_actual);
  ASSERT_EQ(3, values_read);

  ASSERT_TRUE(vector_equal(vresult, slice(values, 0, 3)));
  ASSERT_TRUE(vector_equal(dresult, slice(def_levels, 0, 5)));

  batch_actual = reader->ReadBatch(5, &dresult[0], nullptr,
      &vresult[0], &values_read);
  ASSERT_EQ(5, batch_actual);
  ASSERT_EQ(2, values_read);

  ASSERT_TRUE(vector_equal(slice(vresult, 0, 2), slice(values, 3, 5)));
  ASSERT_TRUE(vector_equal(dresult, slice(def_levels, 5, 10)));

  // EOS, pass all nullptrs to check for improper writes. Do not segfault /
  // core dump
  batch_actual = reader->ReadBatch(5, nullptr, nullptr,
      nullptr, &values_read);
  ASSERT_EQ(0, batch_actual);
  ASSERT_EQ(0, values_read);
}

TEST_F(TestPrimitiveReader, TestInt32FlatRepeated) {
  vector<int32_t> values = {1, 2, 3, 4, 5};
  vector<int16_t> def_levels = {2, 1, 1, 2, 2, 1, 1, 2, 2, 1};
  vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 1, 0, 0, 1};

  size_t num_values = values.size();
  parquet::Encoding::type value_encoding = parquet::Encoding::PLAIN;

  vector<uint8_t> page1;
  test::DataPageBuilder<Type::INT32> page_builder(&page1);

  // Definition levels precede the values
  page_builder.AppendRepLevels(rep_levels, 1, parquet::Encoding::RLE);
  page_builder.AppendDefLevels(def_levels, 2, parquet::Encoding::RLE);
  page_builder.AppendValues(values, parquet::Encoding::PLAIN);

  pages_.push_back(page_builder.Finish());

  NodePtr type = schema::Int32("a", Repetition::REPEATED);
  ColumnDescriptor descr(type, 2, 1);
  InitReader(&descr);

  Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());

  size_t values_read = 0;
  size_t batch_actual = 0;

  vector<int32_t> vresult(3, -1);
  vector<int16_t> dresult(5, -1);
  vector<int16_t> rresult(5, -1);

  batch_actual = reader->ReadBatch(5, &dresult[0], &rresult[0],
      &vresult[0], &values_read);
  ASSERT_EQ(5, batch_actual);
  ASSERT_EQ(3, values_read);

  ASSERT_TRUE(vector_equal(vresult, slice(values, 0, 3)));
  ASSERT_TRUE(vector_equal(dresult, slice(def_levels, 0, 5)));
  ASSERT_TRUE(vector_equal(rresult, slice(rep_levels, 0, 5)));

  batch_actual = reader->ReadBatch(5, &dresult[0], &rresult[0],
      &vresult[0], &values_read);
  ASSERT_EQ(5, batch_actual);
  ASSERT_EQ(2, values_read);

  ASSERT_TRUE(vector_equal(slice(vresult, 0, 2), slice(values, 3, 5)));
  ASSERT_TRUE(vector_equal(dresult, slice(def_levels, 5, 10)));
  ASSERT_TRUE(vector_equal(rresult, slice(rep_levels, 5, 10)));

  // EOS, pass all nullptrs to check for improper writes. Do not segfault /
  // core dump
  batch_actual = reader->ReadBatch(5, nullptr, nullptr,
      nullptr, &values_read);
  ASSERT_EQ(0, batch_actual);
  ASSERT_EQ(0, values_read);
}
} // namespace test

} // namespace parquet_cpp
