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
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "arrow/io/memory.h"
#include "arrow/ipc/feather-internal.h"
#include "arrow/ipc/feather.h"
#include "arrow/ipc/test-common.h"
#include "arrow/loader.h"
#include "arrow/pretty_print.h"
#include "arrow/test-util.h"

namespace arrow {
namespace ipc {
namespace feather {

template <typename T>
inline void assert_vector_equal(const std::vector<T>& left, const std::vector<T>& right) {
  ASSERT_EQ(left.size(), right.size());

  for (size_t i = 0; i < left.size(); ++i) {
    ASSERT_EQ(left[i], right[i]) << i;
  }
}

class TestTableBuilder : public ::testing::Test {
 public:
  void SetUp() { tb_.reset(new TableBuilder(1000)); }

  virtual void Finish() {
    tb_->Finish();

    table_.reset(new TableMetadata());
    ASSERT_OK(table_->Open(tb_->GetBuffer()));
  }

 protected:
  std::unique_ptr<TableBuilder> tb_;
  std::unique_ptr<TableMetadata> table_;
};

TEST_F(TestTableBuilder, Version) {
  Finish();
  ASSERT_EQ(kFeatherVersion, table_->version());
}

TEST_F(TestTableBuilder, EmptyTable) {
  Finish();

  ASSERT_FALSE(table_->HasDescription());
  ASSERT_EQ("", table_->GetDescription());
  ASSERT_EQ(1000, table_->num_rows());
  ASSERT_EQ(0, table_->num_columns());
}

TEST_F(TestTableBuilder, SetDescription) {
  std::string desc("this is some good data");
  tb_->SetDescription(desc);
  Finish();
  ASSERT_TRUE(table_->HasDescription());
  ASSERT_EQ(desc, table_->GetDescription());
}

void AssertArrayEquals(const ArrayMetadata& left, const ArrayMetadata& right) {
  EXPECT_EQ(left.type, right.type);
  EXPECT_EQ(left.offset, right.offset);
  EXPECT_EQ(left.length, right.length);
  EXPECT_EQ(left.null_count, right.null_count);
  EXPECT_EQ(left.total_bytes, right.total_bytes);
}

TEST_F(TestTableBuilder, AddPrimitiveColumn) {
  std::unique_ptr<ColumnBuilder> cb = tb_->AddColumn("f0");

  ArrayMetadata values1;
  ArrayMetadata values2;
  values1.type = fbs::Type_INT32;
  values1.offset = 10000;
  values1.length = 1000;
  values1.null_count = 100;
  values1.total_bytes = 4000;

  cb->SetValues(values1);

  std::string user_meta = "as you wish";
  cb->SetUserMetadata(user_meta);

  cb->Finish();

  cb = tb_->AddColumn("f1");

  values2.type = fbs::Type_UTF8;
  values2.offset = 14000;
  values2.length = 1000;
  values2.null_count = 100;
  values2.total_bytes = 10000;

  cb->SetValues(values2);
  cb->Finish();

  Finish();

  ASSERT_EQ(2, table_->num_columns());

  auto col = table_->column(0);

  ASSERT_EQ("f0", col->name()->str());
  ASSERT_EQ(user_meta, col->user_metadata()->str());

  ArrayMetadata values3;
  FromFlatbuffer(col->values(), &values3);
  AssertArrayEquals(values3, values1);

  col = table_->column(1);
  ASSERT_EQ("f1", col->name()->str());

  ArrayMetadata values4;
  FromFlatbuffer(col->values(), &values4);
  AssertArrayEquals(values4, values2);
}

TEST_F(TestTableBuilder, AddCategoryColumn) {
  ArrayMetadata values1(fbs::Type_UINT8, 10000, 1000, 100, 4000);
  ArrayMetadata levels(fbs::Type_UTF8, 14000, 10, 0, 300);

  std::unique_ptr<ColumnBuilder> cb = tb_->AddColumn("c0");
  cb->SetValues(values1);
  cb->SetCategory(levels);
  cb->Finish();

  cb = tb_->AddColumn("c1");
  cb->SetValues(values1);
  cb->SetCategory(levels, true);
  cb->Finish();

  Finish();

  auto col = table_->column(0);
  ASSERT_EQ(fbs::TypeMetadata_CategoryMetadata, col->metadata_type());

  ArrayMetadata result;
  FromFlatbuffer(col->values(), &result);
  AssertArrayEquals(result, values1);

  auto cat_ptr = static_cast<const fbs::CategoryMetadata*>(col->metadata());
  ASSERT_FALSE(cat_ptr->ordered());

  FromFlatbuffer(cat_ptr->levels(), &result);
  AssertArrayEquals(result, levels);

  col = table_->column(1);
  cat_ptr = static_cast<const fbs::CategoryMetadata*>(col->metadata());
  ASSERT_TRUE(cat_ptr->ordered());
  FromFlatbuffer(cat_ptr->levels(), &result);
  AssertArrayEquals(result, levels);
}

TEST_F(TestTableBuilder, AddTimestampColumn) {
  ArrayMetadata values1(fbs::Type_INT64, 10000, 1000, 100, 4000);
  std::unique_ptr<ColumnBuilder> cb = tb_->AddColumn("c0");
  cb->SetValues(values1);
  cb->SetTimestamp(TimeUnit::MILLI);
  cb->Finish();

  cb = tb_->AddColumn("c1");

  std::string tz("America/Los_Angeles");

  cb->SetValues(values1);
  cb->SetTimestamp(TimeUnit::SECOND, tz);
  cb->Finish();

  Finish();

  auto col = table_->column(0);

  ASSERT_EQ(fbs::TypeMetadata_TimestampMetadata, col->metadata_type());

  ArrayMetadata result;
  FromFlatbuffer(col->values(), &result);
  AssertArrayEquals(result, values1);

  auto ts_ptr = static_cast<const fbs::TimestampMetadata*>(col->metadata());
  ASSERT_EQ(fbs::TimeUnit_MILLISECOND, ts_ptr->unit());

  col = table_->column(1);
  ts_ptr = static_cast<const fbs::TimestampMetadata*>(col->metadata());
  ASSERT_EQ(fbs::TimeUnit_SECOND, ts_ptr->unit());
  ASSERT_EQ(tz, ts_ptr->timezone()->str());
}

TEST_F(TestTableBuilder, AddDateColumn) {
  ArrayMetadata values1(fbs::Type_INT64, 10000, 1000, 100, 4000);
  std::unique_ptr<ColumnBuilder> cb = tb_->AddColumn("d0");
  cb->SetValues(values1);
  cb->SetDate();
  cb->Finish();

  Finish();

  auto col = table_->column(0);

  ASSERT_EQ(fbs::TypeMetadata_DateMetadata, col->metadata_type());
  ArrayMetadata result;
  FromFlatbuffer(col->values(), &result);
  AssertArrayEquals(result, values1);
}

TEST_F(TestTableBuilder, AddTimeColumn) {
  ArrayMetadata values1(fbs::Type_INT64, 10000, 1000, 100, 4000);
  std::unique_ptr<ColumnBuilder> cb = tb_->AddColumn("c0");
  cb->SetValues(values1);
  cb->SetTime(TimeUnit::SECOND);
  cb->Finish();
  Finish();

  auto col = table_->column(0);

  ASSERT_EQ(fbs::TypeMetadata_TimeMetadata, col->metadata_type());
  ArrayMetadata result;
  FromFlatbuffer(col->values(), &result);
  AssertArrayEquals(result, values1);

  auto t_ptr = static_cast<const fbs::TimeMetadata*>(col->metadata());
  ASSERT_EQ(fbs::TimeUnit_SECOND, t_ptr->unit());
}

void CheckArrays(const Array& expected, const Array& result) {
  if (!result.Equals(expected)) {
    std::stringstream pp_result;
    std::stringstream pp_expected;

    EXPECT_OK(PrettyPrint(result, 0, &pp_result));
    EXPECT_OK(PrettyPrint(expected, 0, &pp_expected));
    FAIL() << "Got: " << pp_result.str() << "\nExpected: " << pp_expected.str();
  }
}

class TestTableWriter : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(io::BufferOutputStream::Create(1024, default_memory_pool(), &stream_));
    ASSERT_OK(TableWriter::Open(stream_, &writer_));
  }

  void Finish() {
    // Write table footer
    ASSERT_OK(writer_->Finalize());

    ASSERT_OK(stream_->Finish(&output_));

    std::shared_ptr<io::BufferReader> buffer(new io::BufferReader(output_));
    ASSERT_OK(TableReader::Open(buffer, &reader_));
  }

  void CheckBatch(const RecordBatch& batch) {
    for (int i = 0; i < batch.num_columns(); ++i) {
      ASSERT_OK(writer_->Append(batch.column_name(i), *batch.column(i)));
    }
    Finish();

    std::shared_ptr<Column> col;
    for (int i = 0; i < batch.num_columns(); ++i) {
      ASSERT_OK(reader_->GetColumn(i, &col));
      ASSERT_EQ(batch.column_name(i), col->name());
      CheckArrays(*batch.column(i), *col->data()->chunk(0));
    }
  }

 protected:
  std::shared_ptr<io::BufferOutputStream> stream_;
  std::unique_ptr<TableWriter> writer_;
  std::unique_ptr<TableReader> reader_;

  std::shared_ptr<Buffer> output_;
};

TEST_F(TestTableWriter, EmptyTable) {
  Finish();

  ASSERT_FALSE(reader_->HasDescription());
  ASSERT_EQ("", reader_->GetDescription());

  ASSERT_EQ(0, reader_->num_rows());
  ASSERT_EQ(0, reader_->num_columns());
}

TEST_F(TestTableWriter, SetNumRows) {
  writer_->SetNumRows(1000);
  Finish();
  ASSERT_EQ(1000, reader_->num_rows());
}

TEST_F(TestTableWriter, SetDescription) {
  std::string desc("contents of the file");
  writer_->SetDescription(desc);
  Finish();

  ASSERT_TRUE(reader_->HasDescription());
  ASSERT_EQ(desc, reader_->GetDescription());

  ASSERT_EQ(0, reader_->num_rows());
  ASSERT_EQ(0, reader_->num_columns());
}

TEST_F(TestTableWriter, PrimitiveRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeIntRecordBatch(&batch));

  ASSERT_OK(writer_->Append("f0", *batch->column(0)));
  ASSERT_OK(writer_->Append("f1", *batch->column(1)));
  Finish();

  std::shared_ptr<Column> col;
  ASSERT_OK(reader_->GetColumn(0, &col));
  ASSERT_TRUE(col->data()->chunk(0)->Equals(batch->column(0)));
  ASSERT_EQ("f0", col->name());

  ASSERT_OK(reader_->GetColumn(1, &col));
  ASSERT_TRUE(col->data()->chunk(0)->Equals(batch->column(1)));
  ASSERT_EQ("f1", col->name());
}

TEST_F(TestTableWriter, CategoryRoundtrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeDictionaryFlat(&batch));
  CheckBatch(*batch);
}

TEST_F(TestTableWriter, TimeTypes) {
  std::vector<bool> is_valid = {true, true, true, false, true, true, true};
  auto f0 = field("f0", date32());
  auto f1 = field("f1", time32(TimeUnit::MILLI));
  auto f2 = field("f2", timestamp(TimeUnit::NANO));
  auto f3 = field("f3", timestamp(TimeUnit::SECOND, "US/Los_Angeles"));
  std::shared_ptr<Schema> schema(new Schema({f0, f1, f2, f3}));

  std::vector<int64_t> values_vec = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> values;
  ArrayFromVector<Int64Type, int64_t>(is_valid, values_vec, &values);

  std::vector<int32_t> date_values_vec = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> date_array;
  ArrayFromVector<Date32Type, int32_t>(is_valid, date_values_vec, &date_array);

  std::vector<FieldMetadata> fields(1);
  fields[0].length = values->length();
  fields[0].null_count = values->null_count();
  fields[0].offset = 0;

  const auto& prim_values = static_cast<const PrimitiveArray&>(*values);
  std::vector<std::shared_ptr<Buffer>> buffers = {
      prim_values.null_bitmap(), prim_values.data()};

  std::vector<std::shared_ptr<Array>> arrays;
  arrays.push_back(date_array);

  for (int i = 1; i < schema->num_fields(); ++i) {
    std::shared_ptr<Array> arr;
    LoadArray(schema->field(i)->type(), fields, buffers, &arr);
    arrays.push_back(arr);
  }

  RecordBatch batch(schema, values->length(), arrays);
  CheckBatch(batch);
}

TEST_F(TestTableWriter, VLenPrimitiveRoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeStringTypesRecordBatch(&batch));
  CheckBatch(*batch);
}

}  // namespace feather
}  // namespace ipc
}  // namespace arrow
