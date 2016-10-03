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

#include "parquet/column/reader.h"
#include "parquet/column/test-specialization.h"
#include "parquet/column/test-util.h"
#include "parquet/column/writer.h"
#include "parquet/file/reader.h"
#include "parquet/file/writer.h"
#include "parquet/types.h"
#include "parquet/util/input.h"
#include "parquet/util/output.h"

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

template <typename TestType>
class TestSerialize : public PrimitiveTypedTest<TestType> {
 public:
  typedef typename TestType::c_type T;

  void SetUp() {
    num_columns_ = 4;
    this->SetUpSchema(Repetition::OPTIONAL, num_columns_);
  }

 protected:
  int num_columns_;

  void FileSerializeTest(Compression::type codec_type) {
    std::shared_ptr<InMemoryOutputStream> sink(new InMemoryOutputStream());
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);

    WriterProperties::Builder prop_builder;

    for (int i = 0; i < num_columns_; ++i) {
      prop_builder.compression(this->schema_.Column(i)->name(), codec_type);
    }
    std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

    auto file_writer = ParquetFileWriter::Open(sink, gnode, writer_properties);
    auto row_group_writer = file_writer->AppendRowGroup(100);

    this->GenerateData(100);
    for (int i = 0; i < num_columns_; ++i) {
      auto column_writer =
          static_cast<TypedColumnWriter<TestType>*>(row_group_writer->NextColumn());
      column_writer->WriteBatch(
          100, this->def_levels_.data(), nullptr, this->values_ptr_);
      column_writer->Close();
    }

    row_group_writer->Close();
    file_writer->Close();

    auto buffer = sink->GetBuffer();
    std::unique_ptr<RandomAccessSource> source(new BufferReader(buffer));
    auto file_reader = ParquetFileReader::Open(std::move(source));
    ASSERT_EQ(num_columns_, file_reader->metadata()->num_columns());
    ASSERT_EQ(1, file_reader->metadata()->num_row_groups());
    ASSERT_EQ(100, file_reader->metadata()->num_rows());

    auto rg_reader = file_reader->RowGroup(0);
    ASSERT_EQ(num_columns_, rg_reader->metadata()->num_columns());
    ASSERT_EQ(100, rg_reader->metadata()->num_rows());
    // Check that the specified compression was actually used.
    ASSERT_EQ(codec_type, rg_reader->metadata()->ColumnChunk(0)->compression());

    int64_t values_read;

    for (int i = 0; i < num_columns_; ++i) {
      std::vector<int16_t> def_levels_out(100);
      std::vector<int16_t> rep_levels_out(100);
      auto col_reader =
          std::static_pointer_cast<TypedColumnReader<TestType>>(rg_reader->Column(i));
      this->SetupValuesOut(100);
      col_reader->ReadBatch(100, def_levels_out.data(), rep_levels_out.data(),
          this->values_out_ptr_, &values_read);
      this->SyncValuesOut();
      ASSERT_EQ(100, values_read);
      ASSERT_EQ(this->values_, this->values_out_);
      ASSERT_EQ(this->def_levels_, def_levels_out);
    }
  }
};

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
    BooleanType, ByteArrayType, FLBAType> TestTypes;

TYPED_TEST_CASE(TestSerialize, TestTypes);

TYPED_TEST(TestSerialize, SmallFileUncompressed) {
  this->FileSerializeTest(Compression::UNCOMPRESSED);
}

TYPED_TEST(TestSerialize, SmallFileSnappy) {
  this->FileSerializeTest(Compression::SNAPPY);
}

TYPED_TEST(TestSerialize, SmallFileGzip) {
  this->FileSerializeTest(Compression::GZIP);
}

}  // namespace test

}  // namespace parquet
