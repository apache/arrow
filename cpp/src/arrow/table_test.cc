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

#include "arrow/table.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/chunked_array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

class TestTable : public ::testing::Test {
 public:
  void MakeExample1(int length) {
    auto f0 = field("f0", int32());
    auto f1 = field("f1", uint8());
    auto f2 = field("f2", int16());

    std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
    schema_ = std::make_shared<Schema>(fields);

    arrays_ = {gen_.ArrayOf(int32(), length), gen_.ArrayOf(uint8(), length),
               gen_.ArrayOf(int16(), length)};

    columns_ = {std::make_shared<ChunkedArray>(arrays_[0]),
                std::make_shared<ChunkedArray>(arrays_[1]),
                std::make_shared<ChunkedArray>(arrays_[2])};
  }

 protected:
  random::RandomArrayGenerator gen_{42};
  std::shared_ptr<Table> table_;
  std::shared_ptr<Schema> schema_;

  std::vector<std::shared_ptr<Array>> arrays_;
  std::vector<std::shared_ptr<ChunkedArray>> columns_;
};

TEST_F(TestTable, EmptySchema) {
  auto empty_schema = ::arrow::schema({});
  table_ = Table::Make(empty_schema, columns_);
  ASSERT_OK(table_->ValidateFull());
  ASSERT_EQ(0, table_->num_rows());
  ASSERT_EQ(0, table_->num_columns());
}

TEST_F(TestTable, Ctors) {
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_);
  ASSERT_OK(table_->ValidateFull());
  ASSERT_EQ(length, table_->num_rows());
  ASSERT_EQ(3, table_->num_columns());

  auto array_ctor = Table::Make(schema_, arrays_);
  ASSERT_TRUE(table_->Equals(*array_ctor));

  table_ = Table::Make(schema_, columns_, length);
  ASSERT_OK(table_->ValidateFull());
  ASSERT_EQ(length, table_->num_rows());

  table_ = Table::Make(schema_, arrays_);
  ASSERT_OK(table_->ValidateFull());
  ASSERT_EQ(length, table_->num_rows());
  ASSERT_EQ(3, table_->num_columns());
}

TEST_F(TestTable, Metadata) {
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_);

  ASSERT_TRUE(table_->schema()->Equals(*schema_));

  auto col = table_->column(0);
  ASSERT_EQ(schema_->field(0)->type(), col->type());
}

TEST_F(TestTable, InvalidColumns) {
  // Check that columns are all the same length
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_, length - 1);
  ASSERT_RAISES(Invalid, table_->ValidateFull());

  // Wrong number of columns
  columns_.clear();
  table_ = Table::Make(schema_, columns_, length);
  ASSERT_RAISES(Invalid, table_->ValidateFull());

  columns_ = {std::make_shared<ChunkedArray>(gen_.ArrayOf(int32(), length)),
              std::make_shared<ChunkedArray>(gen_.ArrayOf(uint8(), length)),
              std::make_shared<ChunkedArray>(gen_.ArrayOf(int16(), length - 1))};

  table_ = Table::Make(schema_, columns_, length);
  ASSERT_RAISES(Invalid, table_->ValidateFull());
}

TEST_F(TestTable, AllColumnsAndFields) {
  const int length = 100;
  MakeExample1(length);
  table_ = Table::Make(schema_, columns_);

  auto columns = table_->columns();
  auto fields = table_->fields();

  for (int i = 0; i < table_->num_columns(); ++i) {
    AssertChunkedEqual(*table_->column(i), *columns[i]);
    AssertFieldEqual(*table_->field(i), *fields[i]);
  }

  // Zero length
  std::vector<std::shared_ptr<Array>> t2_columns;
  auto t2 = Table::Make(::arrow::schema({}), t2_columns);
  columns = t2->columns();
  fields = t2->fields();

  ASSERT_EQ(0, columns.size());
  ASSERT_EQ(0, fields.size());
}

TEST_F(TestTable, Equals) {
  const int length = 100;
  MakeExample1(length);

  table_ = Table::Make(schema_, columns_);

  ASSERT_TRUE(table_->Equals(*table_));
  // Differing schema
  auto f0 = field("f3", int32());
  auto f1 = field("f4", uint8());
  auto f2 = field("f5", int16());
  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto other_schema = std::make_shared<Schema>(fields);
  auto other = Table::Make(other_schema, columns_);
  ASSERT_FALSE(table_->Equals(*other));
  // Differing columns
  std::vector<std::shared_ptr<ChunkedArray>> other_columns = {
      std::make_shared<ChunkedArray>(
          gen_.ArrayOf(int32(), length, /*null_probability=*/0.3)),
      std::make_shared<ChunkedArray>(
          gen_.ArrayOf(uint8(), length, /*null_probability=*/0.3)),
      std::make_shared<ChunkedArray>(
          gen_.ArrayOf(int16(), length, /*null_probability=*/0.3))};

  other = Table::Make(schema_, other_columns);
  ASSERT_FALSE(table_->Equals(*other));

  // Differring schema metadata
  other_schema = schema_->WithMetadata(::arrow::key_value_metadata({"key"}, {"value"}));
  other = Table::Make(other_schema, columns_);
  ASSERT_TRUE(table_->Equals(*other));
  ASSERT_FALSE(table_->Equals(*other, /*check_metadata=*/true));
}

TEST_F(TestTable, MakeEmpty) {
  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema({f0, f1, f2});

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Table> empty, Table::MakeEmpty(schema));
  AssertSchemaEqual(*schema, *empty->schema());
  ASSERT_OK(empty->ValidateFull());
  ASSERT_EQ(empty->num_rows(), 0);
}

TEST_F(TestTable, FromRecordBatches) {
  const int64_t length = 10;
  MakeExample1(length);

  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  ASSERT_OK_AND_ASSIGN(auto result, Table::FromRecordBatches({batch1}));

  auto expected = Table::Make(schema_, columns_);
  ASSERT_TRUE(result->Equals(*expected));

  std::vector<std::shared_ptr<ChunkedArray>> other_columns;
  for (int i = 0; i < schema_->num_fields(); ++i) {
    std::vector<std::shared_ptr<Array>> col_arrays = {arrays_[i], arrays_[i]};
    other_columns.push_back(std::make_shared<ChunkedArray>(col_arrays));
  }

  ASSERT_OK_AND_ASSIGN(result, Table::FromRecordBatches({batch1, batch1}));
  expected = Table::Make(schema_, other_columns);
  ASSERT_TRUE(result->Equals(*expected));

  // Error states
  std::vector<std::shared_ptr<RecordBatch>> empty_batches;
  ASSERT_RAISES(Invalid, Table::FromRecordBatches(empty_batches));

  auto other_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});

  std::vector<std::shared_ptr<Array>> other_arrays = {arrays_[0], arrays_[1]};
  auto batch2 = RecordBatch::Make(other_schema, length, other_arrays);
  ASSERT_RAISES(Invalid, Table::FromRecordBatches({batch1, batch2}));
}

TEST_F(TestTable, FromRecordBatchesZeroLength) {
  // ARROW-2307
  MakeExample1(10);

  ASSERT_OK_AND_ASSIGN(auto result, Table::FromRecordBatches(schema_, {}));

  ASSERT_EQ(0, result->num_rows());
  ASSERT_TRUE(result->schema()->Equals(*schema_));
}

TEST_F(TestTable, CombineChunksZeroColumn) {
  // ARROW-11232
  auto record_batch = RecordBatch::Make(schema({}), /*num_rows=*/10,
                                        std::vector<std::shared_ptr<Array>>{});

  ASSERT_OK_AND_ASSIGN(
      auto table,
      Table::FromRecordBatches(record_batch->schema(), {record_batch, record_batch}));
  ASSERT_EQ(20, table->num_rows());

  ASSERT_OK_AND_ASSIGN(auto combined, table->CombineChunks());

  EXPECT_EQ(20, combined->num_rows());
  EXPECT_TRUE(combined->Equals(*table));
}

TEST_F(TestTable, CombineChunksZeroRow) {
  MakeExample1(10);

  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches(schema_, {}));
  ASSERT_EQ(0, table->num_rows());

  ASSERT_OK_AND_ASSIGN(auto compacted, table->CombineChunks());
  ASSERT_TRUE(compacted->Equals(*table));

  ASSERT_OK_AND_ASSIGN(auto batch, table->CombineChunksToBatch());
  ASSERT_OK_AND_ASSIGN(auto expected, RecordBatch::MakeEmpty(schema_));
  ASSERT_NO_FATAL_FAILURE(AssertBatchesEqual(*expected, *batch, /*verbose=*/true));
}

TEST_F(TestTable, CombineChunks) {
  MakeExample1(10);
  auto batch1 = RecordBatch::Make(schema_, 10, arrays_);

  MakeExample1(15);
  auto batch2 = RecordBatch::Make(schema_, 15, arrays_);

  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches({batch1, batch2}));
  for (int i = 0; i < table->num_columns(); ++i) {
    ASSERT_EQ(2, table->column(i)->num_chunks());
  }

  ASSERT_OK_AND_ASSIGN(auto compacted, table->CombineChunks());

  EXPECT_TRUE(compacted->Equals(*table));
  for (int i = 0; i < compacted->num_columns(); ++i) {
    EXPECT_EQ(1, compacted->column(i)->num_chunks());
  }
}

TEST_F(TestTable, LARGE_MEMORY_TEST(CombineChunksStringColumn)) {
  schema_ = schema({field("str", utf8())});
  arrays_ = {nullptr};

  std::string value(1 << 16, '-');

  auto num_rows = kBinaryMemoryLimit / static_cast<int64_t>(value.size());
  StringBuilder builder;
  ASSERT_OK(builder.Resize(num_rows));
  ASSERT_OK(builder.ReserveData(value.size() * num_rows));
  for (int i = 0; i < num_rows; ++i) builder.UnsafeAppend(value);
  ASSERT_OK(builder.Finish(&arrays_[0]));

  auto batch = RecordBatch::Make(schema_, num_rows, arrays_);

  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches({batch, batch}));
  ASSERT_EQ(table->column(0)->num_chunks(), 2);

  ASSERT_OK_AND_ASSIGN(auto compacted, table->CombineChunks());
  EXPECT_TRUE(compacted->Equals(*table));

  // can't compact these columns any further; they contain too much character data
  ASSERT_EQ(compacted->column(0)->num_chunks(), 2);
}

TEST_F(TestTable, ConcatenateTables) {
  const int64_t length = 10;

  MakeExample1(length);
  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  // generate different data
  MakeExample1(length);
  auto batch2 = RecordBatch::Make(schema_, length, arrays_);

  ASSERT_OK_AND_ASSIGN(auto t1, Table::FromRecordBatches({batch1}));
  ASSERT_OK_AND_ASSIGN(auto t2, Table::FromRecordBatches({batch2}));

  ASSERT_OK_AND_ASSIGN(auto result, ConcatenateTables({t1, t2}));
  ASSERT_OK_AND_ASSIGN(auto expected, Table::FromRecordBatches({batch1, batch2}));
  AssertTablesEqual(*expected, *result);

  // Error states
  std::vector<std::shared_ptr<Table>> empty_tables;
  ASSERT_RAISES(Invalid, ConcatenateTables(empty_tables));

  auto other_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});

  std::vector<std::shared_ptr<Array>> other_arrays = {arrays_[0], arrays_[1]};
  auto batch3 = RecordBatch::Make(other_schema, length, other_arrays);
  ASSERT_OK_AND_ASSIGN(auto t3, Table::FromRecordBatches({batch3}));

  ASSERT_RAISES(Invalid, ConcatenateTables({t1, t3}));
}

std::shared_ptr<Table> MakeTableWithOneNullFilledColumn(
    const std::string& column_name, const std::shared_ptr<DataType>& data_type,
    const int length) {
  auto array_of_nulls = *MakeArrayOfNull(data_type, length);
  return Table::Make(schema({field(column_name, data_type)}), {array_of_nulls});
}

using TestPromoteTableToSchema = TestTable;

TEST_F(TestPromoteTableToSchema, IdenticalSchema) {
  const int length = 10;
  auto metadata = std::make_shared<KeyValueMetadata>(std::vector<std::string>{"foo"},
                                                     std::vector<std::string>{"bar"});
  MakeExample1(length);
  std::shared_ptr<Table> table = Table::Make(schema_, arrays_);

  ASSERT_OK_AND_ASSIGN(auto result,
                       PromoteTableToSchema(table, schema_->WithMetadata(metadata)));

  std::shared_ptr<Table> expected = table->ReplaceSchemaMetadata(metadata);

  ASSERT_TRUE(result->Equals(*expected));
}

// The promoted table's fields are ordered the same as the promote-to schema.
TEST_F(TestPromoteTableToSchema, FieldsReorderedAfterPromotion) {
  const int length = 10;
  MakeExample1(length);

  std::vector<std::shared_ptr<Field>> reversed_fields(schema_->fields().crbegin(),
                                                      schema_->fields().crend());
  std::vector<std::shared_ptr<Array>> reversed_arrays(arrays_.crbegin(), arrays_.crend());

  std::shared_ptr<Table> table = Table::Make(schema(reversed_fields), reversed_arrays);

  ASSERT_OK_AND_ASSIGN(auto result, PromoteTableToSchema(table, schema_));

  ASSERT_TRUE(result->schema()->Equals(*schema_));
}

TEST_F(TestPromoteTableToSchema, PromoteNullTypeField) {
  const int length = 10;
  auto metadata = std::make_shared<KeyValueMetadata>(std::vector<std::string>{"foo"},
                                                     std::vector<std::string>{"bar"});
  auto table_with_null_column = MakeTableWithOneNullFilledColumn("field", null(), length)
                                    ->ReplaceSchemaMetadata(metadata);
  auto promoted_schema = schema({field("field", int32())});

  ASSERT_OK_AND_ASSIGN(auto result,
                       PromoteTableToSchema(table_with_null_column, promoted_schema));

  ASSERT_TRUE(
      result->Equals(*MakeTableWithOneNullFilledColumn("field", int32(), length)));
}

TEST_F(TestPromoteTableToSchema, AddMissingField) {
  const int length = 10;
  auto f0 = field("f0", int32());
  auto table = Table::Make(schema({}), std::vector<std::shared_ptr<Array>>(), length);
  auto promoted_schema = schema({field("field", int32())});

  ASSERT_OK_AND_ASSIGN(auto result, PromoteTableToSchema(table, promoted_schema));

  ASSERT_TRUE(
      result->Equals(*MakeTableWithOneNullFilledColumn("field", int32(), length)));
}

TEST_F(TestPromoteTableToSchema, IncompatibleTypes) {
  const int length = 10;
  auto table = MakeTableWithOneNullFilledColumn("field", int32(), length);

  // Invalid promotion: int32 to null.
  ASSERT_RAISES(Invalid, PromoteTableToSchema(table, schema({field("field", null())})));

  // Invalid promotion: int32 to uint32.
  ASSERT_RAISES(Invalid, PromoteTableToSchema(table, schema({field("field", uint32())})));
}

TEST_F(TestPromoteTableToSchema, IncompatibleNullity) {
  const int length = 10;
  auto table = MakeTableWithOneNullFilledColumn("field", int32(), length);
  ASSERT_RAISES(Invalid,
                PromoteTableToSchema(
                    table, schema({field("field", uint32())->WithNullable(false)})));
}

TEST_F(TestPromoteTableToSchema, DuplicateFieldNames) {
  const int length = 10;

  auto table = Table::Make(schema({field("field", int32()), field("field", null())}),
                           {gen_.ArrayOf(int32(), length), gen_.ArrayOf(null(), length)});

  ASSERT_RAISES(Invalid, PromoteTableToSchema(table, schema({field("field", int32())})));
}

TEST_F(TestPromoteTableToSchema, TableFieldAbsentFromSchema) {
  const int length = 10;

  auto table =
      Table::Make(schema({field("f0", int32())}), {gen_.ArrayOf(int32(), length)});

  std::shared_ptr<Table> result;
  ASSERT_RAISES(Invalid, PromoteTableToSchema(table, schema({field("f1", int32())})));
}

class ConcatenateTablesWithPromotionTest : public TestTable {
 protected:
  ConcatenateTablesOptions GetOptions() {
    ConcatenateTablesOptions options;
    options.unify_schemas = true;
    return options;
  }

  void MakeExample2(int length) {
    auto f0 = field("f0", int32());
    auto f1 = field("f1", null());

    std::vector<std::shared_ptr<Field>> fields = {f0, f1};
    schema_ = std::make_shared<Schema>(fields);

    arrays_ = {gen_.ArrayOf(int32(), length), gen_.ArrayOf(null(), length)};

    columns_ = {std::make_shared<ChunkedArray>(arrays_[0]),
                std::make_shared<ChunkedArray>(arrays_[1])};
  }

  void AssertTablesEqualUnorderedFields(const Table& lhs, const Table& rhs) {
    ASSERT_EQ(lhs.schema()->num_fields(), rhs.schema()->num_fields());
    if (lhs.schema()->metadata()) {
      ASSERT_NE(nullptr, rhs.schema()->metadata());
      ASSERT_TRUE(lhs.schema()->metadata()->Equals(*rhs.schema()->metadata()));
    } else {
      ASSERT_EQ(nullptr, rhs.schema()->metadata());
    }
    for (int i = 0; i < lhs.schema()->num_fields(); ++i) {
      const auto& lhs_field = lhs.schema()->field(i);
      const auto& rhs_field = rhs.schema()->GetFieldByName(lhs_field->name());
      ASSERT_NE(nullptr, rhs_field);
      ASSERT_TRUE(lhs_field->Equals(rhs_field, true));
      const auto& lhs_column = lhs.column(i);
      const auto& rhs_column = rhs.GetColumnByName(lhs_field->name());
      AssertChunkedEqual(*lhs_column, *rhs_column);
    }
  }
};

TEST_F(ConcatenateTablesWithPromotionTest, Simple) {
  const int64_t length = 10;

  MakeExample1(length);
  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  ASSERT_OK_AND_ASSIGN(auto f1_nulls, MakeArrayOfNull(schema_->field(1)->type(), length));
  ASSERT_OK_AND_ASSIGN(auto f2_nulls, MakeArrayOfNull(schema_->field(2)->type(), length));

  MakeExample2(length);
  auto batch2 = RecordBatch::Make(schema_, length, arrays_);

  auto batch2_null_filled =
      RecordBatch::Make(batch1->schema(), length, {arrays_[0], f1_nulls, f2_nulls});

  ASSERT_OK_AND_ASSIGN(auto t1, Table::FromRecordBatches({batch1}));
  ASSERT_OK_AND_ASSIGN(auto t2, Table::FromRecordBatches({batch2}));
  ASSERT_OK_AND_ASSIGN(auto t3, Table::FromRecordBatches({batch2_null_filled}));

  ASSERT_OK_AND_ASSIGN(auto result, ConcatenateTables({t1, t2}, GetOptions()));
  ASSERT_OK_AND_ASSIGN(auto expected, ConcatenateTables({t1, t3}));
  AssertTablesEqualUnorderedFields(*expected, *result);

  ASSERT_OK_AND_ASSIGN(result, ConcatenateTables({t2, t1}, GetOptions()));
  ASSERT_OK_AND_ASSIGN(expected, ConcatenateTables({t3, t1}));
  AssertTablesEqualUnorderedFields(*expected, *result);
}

TEST_F(TestTable, Slice) {
  const int64_t length = 10;

  MakeExample1(length);
  auto batch = RecordBatch::Make(schema_, length, arrays_);

  ASSERT_OK_AND_ASSIGN(auto half, Table::FromRecordBatches({batch}));
  ASSERT_OK_AND_ASSIGN(auto whole, Table::FromRecordBatches({batch, batch}));
  ASSERT_OK_AND_ASSIGN(auto three, Table::FromRecordBatches({batch, batch, batch}));

  AssertTablesEqual(*whole->Slice(0, length), *half);
  AssertTablesEqual(*whole->Slice(length), *half);
  AssertTablesEqual(*whole->Slice(length / 3, 2 * (length - length / 3)),
                    *three->Slice(length + length / 3, 2 * (length - length / 3)));
}

TEST_F(TestTable, RemoveColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  ASSERT_OK_AND_ASSIGN(auto result, table.RemoveColumn(0));

  auto ex_schema = ::arrow::schema({schema_->field(1), schema_->field(2)});
  std::vector<std::shared_ptr<ChunkedArray>> ex_columns = {table.column(1),
                                                           table.column(2)};

  auto expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK_AND_ASSIGN(result, table.RemoveColumn(1));
  ex_schema = ::arrow::schema({schema_->field(0), schema_->field(2)});
  ex_columns = {table.column(0), table.column(2)};

  expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK_AND_ASSIGN(result, table.RemoveColumn(2));
  ex_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});
  ex_columns = {table.column(0), table.column(1)};
  expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));
}

TEST_F(TestTable, SetColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  ASSERT_OK_AND_ASSIGN(auto result,
                       table.SetColumn(0, schema_->field(1), table.column(1)));

  auto ex_schema =
      ::arrow::schema({schema_->field(1), schema_->field(1), schema_->field(2)});

  auto expected =
      Table::Make(ex_schema, {table.column(1), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));
}

TEST_F(TestTable, RenameColumns) {
  MakeExample1(10);
  auto table = Table::Make(schema_, columns_);
  EXPECT_THAT(table->ColumnNames(), testing::ElementsAre("f0", "f1", "f2"));

  ASSERT_OK_AND_ASSIGN(auto renamed, table->RenameColumns({"zero", "one", "two"}));
  EXPECT_THAT(renamed->ColumnNames(), testing::ElementsAre("zero", "one", "two"));
  ASSERT_OK(renamed->ValidateFull());

  ASSERT_RAISES(Invalid, table->RenameColumns({"hello", "world"}));
}

TEST_F(TestTable, SelectColumns) {
  MakeExample1(10);
  auto table = Table::Make(schema_, columns_);

  ASSERT_OK_AND_ASSIGN(auto subset, table->SelectColumns({0, 2}));
  ASSERT_OK(subset->ValidateFull());

  auto expexted_schema = ::arrow::schema({schema_->field(0), schema_->field(2)});
  auto expected = Table::Make(expexted_schema, {table->column(0), table->column(2)});
  ASSERT_TRUE(subset->Equals(*expected));

  // Out of bounds indices
  ASSERT_RAISES(Invalid, table->SelectColumns({0, 3}));
  ASSERT_RAISES(Invalid, table->SelectColumns({-1}));
}

TEST_F(TestTable, RemoveColumnEmpty) {
  // ARROW-1865
  const int64_t length = 10;

  auto f0 = field("f0", int32());
  auto schema = ::arrow::schema({f0});
  auto a0 = gen_.ArrayOf(int32(), length);

  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(a0)});

  ASSERT_OK_AND_ASSIGN(auto empty, table->RemoveColumn(0));

  ASSERT_EQ(table->num_rows(), empty->num_rows());

  ASSERT_OK_AND_ASSIGN(auto added, empty->AddColumn(0, f0, table->column(0)));
  ASSERT_EQ(table->num_rows(), added->num_rows());
}

TEST_F(TestTable, AddColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  auto f0 = schema_->field(0);

  // Some negative tests with invalid index
  ASSERT_RAISES(Invalid, table.AddColumn(10, f0, columns_[0]));
  ASSERT_RAISES(Invalid, table.AddColumn(4, f0, columns_[0]));
  ASSERT_RAISES(Invalid, table.AddColumn(-1, f0, columns_[0]));

  // Add column with wrong length
  auto longer_col = std::make_shared<ChunkedArray>(gen_.ArrayOf(int32(), length + 1));
  ASSERT_RAISES(Invalid, table.AddColumn(0, f0, longer_col));

  // Add column 0 in different places
  ASSERT_OK_AND_ASSIGN(auto result, table.AddColumn(0, f0, columns_[0]));
  auto ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(0), schema_->field(1), schema_->field(2)});

  auto expected = Table::Make(
      ex_schema, {table.column(0), table.column(0), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK_AND_ASSIGN(result, table.AddColumn(1, f0, columns_[0]));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(0), schema_->field(1), schema_->field(2)});

  expected = Table::Make(
      ex_schema, {table.column(0), table.column(0), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK_AND_ASSIGN(result, table.AddColumn(2, f0, columns_[0]));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(1), schema_->field(0), schema_->field(2)});
  expected = Table::Make(
      ex_schema, {table.column(0), table.column(1), table.column(0), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK_AND_ASSIGN(result, table.AddColumn(3, f0, columns_[0]));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(1), schema_->field(2), schema_->field(0)});
  expected = Table::Make(
      ex_schema, {table.column(0), table.column(1), table.column(2), table.column(0)});
  ASSERT_TRUE(result->Equals(*expected));
}

class TestTableBatchReader : public ::testing::Test {
 protected:
  random::RandomArrayGenerator gen_{42};
};

TEST_F(TestTableBatchReader, ReadNext) {
  ArrayVector c1, c2;

  auto a1 = gen_.ArrayOf(int32(), 10);
  auto a2 = gen_.ArrayOf(int32(), 20);
  auto a3 = gen_.ArrayOf(int32(), 30);
  auto a4 = gen_.ArrayOf(int32(), 10);

  auto sch1 = arrow::schema({field("f1", int32()), field("f2", int32())});

  std::vector<std::shared_ptr<ChunkedArray>> columns;

  std::shared_ptr<RecordBatch> batch;

  std::vector<std::shared_ptr<Array>> arrays_1 = {a1, a4, a2};
  std::vector<std::shared_ptr<Array>> arrays_2 = {a2, a2};
  columns = {std::make_shared<ChunkedArray>(arrays_1),
             std::make_shared<ChunkedArray>(arrays_2)};
  auto t1 = Table::Make(sch1, columns);

  TableBatchReader i1(*t1);

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(20, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);

  arrays_1 = {a1};
  arrays_2 = {a4};
  columns = {std::make_shared<ChunkedArray>(arrays_1),
             std::make_shared<ChunkedArray>(arrays_2)};
  auto t2 = Table::Make(sch1, columns);

  TableBatchReader i2(*t2);

  ASSERT_OK(i2.ReadNext(&batch));
  ASSERT_EQ(10, batch->num_rows());

  // Ensure non-sliced
  ASSERT_EQ(a1->data().get(), batch->column_data(0).get());
  ASSERT_EQ(a4->data().get(), batch->column_data(1).get());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

TEST_F(TestTableBatchReader, Chunksize) {
  auto a1 = gen_.ArrayOf(int32(), 10);
  auto a2 = gen_.ArrayOf(int32(), 20);
  auto a3 = gen_.ArrayOf(int32(), 10);

  auto sch1 = arrow::schema({field("f1", int32())});

  std::vector<std::shared_ptr<Array>> arrays = {a1, a2, a3};
  auto t1 = Table::Make(sch1, {std::make_shared<ChunkedArray>(arrays)});

  TableBatchReader i1(*t1);

  i1.set_chunksize(15);

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(15, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(5, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(10, batch->num_rows());

  ASSERT_OK(i1.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

TEST_F(TestTableBatchReader, NoColumns) {
  std::shared_ptr<Table> table =
      Table::Make(schema({}), std::vector<std::shared_ptr<Array>>{}, 100);
  TableBatchReader reader(*table);
  reader.set_chunksize(60);

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(reader.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(60, batch->num_rows());

  ASSERT_OK(reader.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(40, batch->num_rows());

  ASSERT_OK(reader.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

TEST_F(TestTableBatchReader, OwnedTableNoColumns) {
  std::shared_ptr<Table> table =
      Table::Make(schema({}), std::vector<std::shared_ptr<Array>>{}, 100);
  TableBatchReader reader(table);
  table.reset();
  reader.set_chunksize(80);

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(reader.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(80, batch->num_rows());

  ASSERT_OK(reader.ReadNext(&batch));
  ASSERT_OK(batch->ValidateFull());
  ASSERT_EQ(20, batch->num_rows());

  ASSERT_OK(reader.ReadNext(&batch));
  ASSERT_EQ(nullptr, batch);
}

}  // namespace arrow
