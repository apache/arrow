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
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {

class TestChunkedArray : public TestBase {
 protected:
  virtual void Construct() {
    one_ = std::make_shared<ChunkedArray>(arrays_one_);
    if (!arrays_another_.empty()) {
      another_ = std::make_shared<ChunkedArray>(arrays_another_);
    }
  }

  ArrayVector arrays_one_;
  ArrayVector arrays_another_;

  std::shared_ptr<ChunkedArray> one_;
  std::shared_ptr<ChunkedArray> another_;
};

TEST_F(TestChunkedArray, BasicEquals) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data, &array);
  arrays_one_.push_back(array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(one_));
  ASSERT_FALSE(one_->Equals(nullptr));
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingTypes) {
  std::vector<bool> null_bitmap(100, true);
  std::vector<int32_t> data32(100, 1);
  std::vector<int64_t> data64(100, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap, data32, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int64Type, int64_t>(null_bitmap, data64, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, EqualsDifferingLengths) {
  std::vector<bool> null_bitmap100(100, true);
  std::vector<bool> null_bitmap101(101, true);
  std::vector<int32_t> data100(100, 1);
  std::vector<int32_t> data101(101, 1);
  std::shared_ptr<Array> array;
  ArrayFromVector<Int32Type, int32_t>(null_bitmap100, data100, &array);
  arrays_one_.push_back(array);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap101, data101, &array);
  arrays_another_.push_back(array);

  Construct();
  ASSERT_FALSE(one_->Equals(another_));
  ASSERT_FALSE(one_->Equals(*another_.get()));

  std::vector<bool> null_bitmap1(1, true);
  std::vector<int32_t> data1(1, 1);
  ArrayFromVector<Int32Type, int32_t>(null_bitmap1, data1, &array);
  arrays_one_.push_back(array);

  Construct();
  ASSERT_TRUE(one_->Equals(another_));
  ASSERT_TRUE(one_->Equals(*another_.get()));
}

TEST_F(TestChunkedArray, SliceEquals) {
  arrays_one_.push_back(MakeRandomArray<Int32Array>(100));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  arrays_one_.push_back(MakeRandomArray<Int32Array>(50));
  Construct();

  std::shared_ptr<ChunkedArray> slice = one_->Slice(125, 50);
  ASSERT_EQ(slice->length(), 50);
  AssertChunkedEqual(*one_->Slice(125, 50), *slice);

  std::shared_ptr<ChunkedArray> slice2 = one_->Slice(75)->Slice(25)->Slice(25, 50);
  ASSERT_EQ(slice2->length(), 50);
  AssertChunkedEqual(*slice, *slice2);

  // Making empty slices of a ChunkedArray
  std::shared_ptr<ChunkedArray> slice3 = one_->Slice(one_->length(), 99);
  ASSERT_EQ(slice3->length(), 0);
  ASSERT_EQ(slice3->num_chunks(), 0);
  ASSERT_TRUE(slice3->type()->Equals(one_->type()));

  std::shared_ptr<ChunkedArray> slice4 = one_->Slice(10, 0);
  ASSERT_EQ(slice4->length(), 0);
  ASSERT_EQ(slice4->num_chunks(), 0);
  ASSERT_TRUE(slice4->type()->Equals(one_->type()));

  // Slicing an empty ChunkedArray
  std::shared_ptr<ChunkedArray> slice5 = slice4->Slice(0, 10);
  ASSERT_EQ(slice5->length(), 0);
  ASSERT_EQ(slice5->num_chunks(), 0);
  ASSERT_TRUE(slice5->type()->Equals(one_->type()));
}

TEST_F(TestChunkedArray, Validate) {
  // Valid if empty
  ArrayVector empty = {};
  auto no_chunks = std::make_shared<ChunkedArray>(empty, utf8());
  ASSERT_OK(no_chunks->ValidateFull());

  random::RandomArrayGenerator gen(0);
  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();
  ASSERT_OK(one_->ValidateFull());

  arrays_one_.push_back(gen.Int32(50, 0, 100, 0.1));
  Construct();
  ASSERT_OK(one_->ValidateFull());

  arrays_one_.push_back(gen.String(50, 0, 10, 0.1));
  Construct();
  ASSERT_RAISES(Invalid, one_->ValidateFull());
}

TEST_F(TestChunkedArray, View) {
  auto in_ty = int32();
  auto out_ty = fixed_size_binary(4);
  auto arr = ArrayFromJSON(in_ty, "[2020568934, 2054316386, null]");
  auto arr2 = ArrayFromJSON(in_ty, "[2020568934, 2054316386]");
  auto ex = ArrayFromJSON(out_ty, R"(["foox", "barz", null])");
  auto ex2 = ArrayFromJSON(out_ty, R"(["foox", "barz"])");

  ArrayVector chunks = {arr, arr2};
  ArrayVector ex_chunks = {ex, ex2};
  auto carr = std::make_shared<ChunkedArray>(chunks);
  auto expected = std::make_shared<ChunkedArray>(ex_chunks);

  std::shared_ptr<ChunkedArray> result;
  ASSERT_OK(carr->View(out_ty, &result));
  AssertChunkedEqual(*expected, *result);

  // Zero length
  ArrayVector empty = {};
  carr = std::make_shared<ChunkedArray>(empty, in_ty);
  expected = std::make_shared<ChunkedArray>(empty, out_ty);
  ASSERT_OK(carr->View(out_ty, &result));
  AssertChunkedEqual(*expected, *result);
}

class TestTable : public TestBase {
 public:
  void MakeExample1(int length) {
    auto f0 = field("f0", int32());
    auto f1 = field("f1", uint8());
    auto f2 = field("f2", int16());

    std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
    schema_ = std::make_shared<Schema>(fields);

    arrays_ = {MakeRandomArray<Int32Array>(length), MakeRandomArray<UInt8Array>(length),
               MakeRandomArray<Int16Array>(length)};

    columns_ = {std::make_shared<ChunkedArray>(arrays_[0]),
                std::make_shared<ChunkedArray>(arrays_[1]),
                std::make_shared<ChunkedArray>(arrays_[2])};
  }

 protected:
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

  columns_.clear();

  // Wrong number of columns
  table_ = Table::Make(schema_, columns_, length);
  ASSERT_RAISES(Invalid, table_->ValidateFull());

  columns_ = {std::make_shared<ChunkedArray>(MakeRandomArray<Int32Array>(length)),
              std::make_shared<ChunkedArray>(MakeRandomArray<UInt8Array>(length)),
              std::make_shared<ChunkedArray>(MakeRandomArray<Int16Array>(length - 1))};

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
      std::make_shared<ChunkedArray>(MakeRandomArray<Int32Array>(length, 10)),
      std::make_shared<ChunkedArray>(MakeRandomArray<UInt8Array>(length, 10)),
      std::make_shared<ChunkedArray>(MakeRandomArray<Int16Array>(length, 10))};

  other = Table::Make(schema_, other_columns);
  ASSERT_FALSE(table_->Equals(*other));
}

TEST_F(TestTable, FromRecordBatches) {
  const int64_t length = 10;
  MakeExample1(length);

  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  std::shared_ptr<Table> result, expected;
  ASSERT_OK(Table::FromRecordBatches({batch1}, &result));

  expected = Table::Make(schema_, columns_);
  ASSERT_TRUE(result->Equals(*expected));

  std::vector<std::shared_ptr<ChunkedArray>> other_columns;
  for (int i = 0; i < schema_->num_fields(); ++i) {
    std::vector<std::shared_ptr<Array>> col_arrays = {arrays_[i], arrays_[i]};
    other_columns.push_back(std::make_shared<ChunkedArray>(col_arrays));
  }

  ASSERT_OK(Table::FromRecordBatches({batch1, batch1}, &result));
  expected = Table::Make(schema_, other_columns);
  ASSERT_TRUE(result->Equals(*expected));

  // Error states
  std::vector<std::shared_ptr<RecordBatch>> empty_batches;
  ASSERT_RAISES(Invalid, Table::FromRecordBatches(empty_batches, &result));

  auto other_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});

  std::vector<std::shared_ptr<Array>> other_arrays = {arrays_[0], arrays_[1]};
  auto batch2 = RecordBatch::Make(other_schema, length, other_arrays);
  ASSERT_RAISES(Invalid, Table::FromRecordBatches({batch1, batch2}, &result));
}

TEST_F(TestTable, FromRecordBatchesZeroLength) {
  // ARROW-2307
  MakeExample1(10);

  std::shared_ptr<Table> result;
  ASSERT_OK(Table::FromRecordBatches(schema_, {}, &result));

  ASSERT_EQ(0, result->num_rows());
  ASSERT_TRUE(result->schema()->Equals(*schema_));
}

TEST_F(TestTable, CombineChunksEmptyTable) {
  MakeExample1(10);

  std::shared_ptr<Table> table;
  ASSERT_OK(Table::FromRecordBatches(schema_, {}, &table));
  ASSERT_EQ(0, table->num_rows());

  std::shared_ptr<Table> compacted;
  ASSERT_OK(table->CombineChunks(default_memory_pool(), &compacted));

  EXPECT_TRUE(compacted->Equals(*table));
}

TEST_F(TestTable, CombineChunks) {
  MakeExample1(10);
  auto batch1 = RecordBatch::Make(schema_, 10, arrays_);

  MakeExample1(15);
  auto batch2 = RecordBatch::Make(schema_, 15, arrays_);

  std::shared_ptr<Table> table;
  ASSERT_OK(Table::FromRecordBatches({batch1, batch2}, &table));
  for (int i = 0; i < table->num_columns(); ++i) {
    ASSERT_EQ(2, table->column(i)->num_chunks());
  }

  std::shared_ptr<Table> compacted;
  ASSERT_OK(table->CombineChunks(default_memory_pool(), &compacted));

  EXPECT_TRUE(compacted->Equals(*table));
  for (int i = 0; i < compacted->num_columns(); ++i) {
    EXPECT_EQ(1, compacted->column(i)->num_chunks());
  }
}

TEST_F(TestTable, ConcatenateTables) {
  const int64_t length = 10;

  MakeExample1(length);
  auto batch1 = RecordBatch::Make(schema_, length, arrays_);

  // generate different data
  MakeExample1(length);
  auto batch2 = RecordBatch::Make(schema_, length, arrays_);

  std::shared_ptr<Table> t1, t2, t3, result, expected;
  ASSERT_OK(Table::FromRecordBatches({batch1}, &t1));
  ASSERT_OK(Table::FromRecordBatches({batch2}, &t2));

  ASSERT_OK_AND_ASSIGN(result, ConcatenateTables({t1, t2}));
  ASSERT_OK(Table::FromRecordBatches({batch1, batch2}, &expected));
  AssertTablesEqual(*expected, *result);

  // Error states
  std::vector<std::shared_ptr<Table>> empty_tables;
  ASSERT_RAISES(Invalid, ConcatenateTables(empty_tables));

  auto other_schema = ::arrow::schema({schema_->field(0), schema_->field(1)});

  std::vector<std::shared_ptr<Array>> other_arrays = {arrays_[0], arrays_[1]};
  auto batch3 = RecordBatch::Make(other_schema, length, other_arrays);
  ASSERT_OK(Table::FromRecordBatches({batch3}, &t3));

  ASSERT_RAISES(Invalid, ConcatenateTables({t1, t3}));
}

std::shared_ptr<Table> MakeTableWithOneNullFilledColumn(
    const std::string& column_name, const std::shared_ptr<DataType>& data_type,
    const int length) {
  std::shared_ptr<Array> array_of_nulls;
  DCHECK_OK(MakeArrayOfNull(data_type, length, &array_of_nulls));
  return Table::Make(schema({field(column_name, data_type)}), {array_of_nulls});
}

using TestPromoteTableToSchema = TestTable;

TEST_F(TestPromoteTableToSchema, IdenticalSchema) {
  const int length = 10;
  auto metadata =
      std::shared_ptr<KeyValueMetadata>(new KeyValueMetadata({"foo"}, {"bar"}));
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
  auto metadata =
      std::shared_ptr<KeyValueMetadata>(new KeyValueMetadata({"foo"}, {"bar"}));
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

  auto table = Table::Make(
      schema({field("field", int32()), field("field", null())}),
      {MakeRandomArray<Int32Array>(length), MakeRandomArray<NullArray>(length)});

  ASSERT_RAISES(Invalid, PromoteTableToSchema(table, schema({field("field", int32())})));
}

TEST_F(TestPromoteTableToSchema, TableFieldAbsentFromSchema) {
  const int length = 10;

  auto table =
      Table::Make(schema({field("f0", int32())}), {MakeRandomArray<Int32Array>(length)});

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

    arrays_ = {MakeRandomArray<Int32Array>(length), MakeRandomArray<NullArray>(length)};

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

  std::shared_ptr<Array> f1_nulls;
  ASSERT_OK(MakeArrayOfNull(schema_->field(1)->type(), length, &f1_nulls));
  std::shared_ptr<Array> f2_nulls;
  ASSERT_OK(MakeArrayOfNull(schema_->field(2)->type(), length, &f2_nulls));

  MakeExample2(length);
  auto batch2 = RecordBatch::Make(schema_, length, arrays_);

  auto batch2_null_filled =
      RecordBatch::Make(batch1->schema(), length, {arrays_[0], f1_nulls, f2_nulls});

  std::shared_ptr<Table> t1, t2, t3, result, expected;
  ASSERT_OK(Table::FromRecordBatches({batch1}, &t1));
  ASSERT_OK(Table::FromRecordBatches({batch2}, &t2));
  ASSERT_OK(Table::FromRecordBatches({batch2_null_filled}, &t3));

  ASSERT_OK_AND_ASSIGN(result, ConcatenateTables({t1, t2}, GetOptions()));
  ASSERT_OK_AND_ASSIGN(expected, ConcatenateTables({t1, t3}));
  AssertTablesEqualUnorderedFields(*expected, *result);

  ASSERT_OK_AND_ASSIGN(result, ConcatenateTables({t2, t1}, GetOptions()));
  ASSERT_OK_AND_ASSIGN(expected, ConcatenateTables({t3, t1}));
  AssertTablesEqualUnorderedFields(*expected, *result);
}

TEST_F(TestTable, Slice) {
  const int64_t length = 10;

  MakeExample1(length);
  auto batch = RecordBatch::Make(schema_, length, arrays_);

  std::shared_ptr<Table> half, whole, three;
  ASSERT_OK(Table::FromRecordBatches({batch}, &half));
  ASSERT_OK(Table::FromRecordBatches({batch, batch}, &whole));
  ASSERT_OK(Table::FromRecordBatches({batch, batch, batch}, &three));

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

  std::shared_ptr<Table> result;
  ASSERT_OK(table.RemoveColumn(0, &result));

  auto ex_schema = ::arrow::schema({schema_->field(1), schema_->field(2)});
  std::vector<std::shared_ptr<ChunkedArray>> ex_columns = {table.column(1),
                                                           table.column(2)};

  auto expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.RemoveColumn(1, &result));
  ex_schema = ::arrow::schema({schema_->field(0), schema_->field(2)});
  ex_columns = {table.column(0), table.column(2)};

  expected = Table::Make(ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.RemoveColumn(2, &result));
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

  std::shared_ptr<Table> result;
  ASSERT_OK(table.SetColumn(0, schema_->field(1), table.column(1), &result));

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

  std::shared_ptr<Table> renamed;
  ASSERT_OK(table->RenameColumns({"zero", "one", "two"}, &renamed));
  EXPECT_THAT(renamed->ColumnNames(), testing::ElementsAre("zero", "one", "two"));
  ASSERT_OK(renamed->ValidateFull());

  ASSERT_RAISES(Invalid, table->RenameColumns({"hello", "world"}, &renamed));
}

TEST_F(TestTable, RemoveColumnEmpty) {
  // ARROW-1865
  const int64_t length = 10;

  auto f0 = field("f0", int32());
  auto schema = ::arrow::schema({f0});
  auto a0 = MakeRandomArray<Int32Array>(length);

  auto table = Table::Make(schema, {std::make_shared<ChunkedArray>(a0)});

  std::shared_ptr<Table> empty;
  ASSERT_OK(table->RemoveColumn(0, &empty));

  ASSERT_EQ(table->num_rows(), empty->num_rows());

  std::shared_ptr<Table> added;
  ASSERT_OK(empty->AddColumn(0, f0, table->column(0), &added));
  ASSERT_EQ(table->num_rows(), added->num_rows());
}

TEST_F(TestTable, AddColumn) {
  const int64_t length = 10;
  MakeExample1(length);

  auto table_sp = Table::Make(schema_, columns_);
  const Table& table = *table_sp;

  auto f0 = schema_->field(0);

  std::shared_ptr<Table> result;
  // Some negative tests with invalid index
  Status status = table.AddColumn(10, f0, columns_[0], &result);
  ASSERT_TRUE(status.IsInvalid());
  status = table.AddColumn(4, f0, columns_[0], &result);
  ASSERT_TRUE(status.IsInvalid());
  status = table.AddColumn(-1, f0, columns_[0], &result);
  ASSERT_TRUE(status.IsInvalid());

  // Add column with wrong length
  auto longer_col =
      std::make_shared<ChunkedArray>(MakeRandomArray<Int32Array>(length + 1));
  status = table.AddColumn(0, f0, longer_col, &result);
  ASSERT_TRUE(status.IsInvalid());

  // Add column 0 in different places
  ASSERT_OK(table.AddColumn(0, f0, columns_[0], &result));
  auto ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(0), schema_->field(1), schema_->field(2)});

  auto expected = Table::Make(
      ex_schema, {table.column(0), table.column(0), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.AddColumn(1, f0, columns_[0], &result));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(0), schema_->field(1), schema_->field(2)});

  expected = Table::Make(
      ex_schema, {table.column(0), table.column(0), table.column(1), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.AddColumn(2, f0, columns_[0], &result));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(1), schema_->field(0), schema_->field(2)});
  expected = Table::Make(
      ex_schema, {table.column(0), table.column(1), table.column(0), table.column(2)});
  ASSERT_TRUE(result->Equals(*expected));

  ASSERT_OK(table.AddColumn(3, f0, columns_[0], &result));
  ex_schema = ::arrow::schema(
      {schema_->field(0), schema_->field(1), schema_->field(2), schema_->field(0)});
  expected = Table::Make(
      ex_schema, {table.column(0), table.column(1), table.column(2), table.column(0)});
  ASSERT_TRUE(result->Equals(*expected));
}

class TestRecordBatch : public TestBase {};

TEST_F(TestRecordBatch, Equals) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1, f2};
  auto schema = ::arrow::schema({f0, f1, f2});
  auto schema2 = ::arrow::schema({f0, f1});

  auto a0 = MakeRandomArray<Int32Array>(length);
  auto a1 = MakeRandomArray<UInt8Array>(length);
  auto a2 = MakeRandomArray<Int16Array>(length);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});
  auto b3 = RecordBatch::Make(schema2, length, {a0, a1});
  auto b4 = RecordBatch::Make(schema, length, {a0, a1, a1});

  ASSERT_TRUE(b1->Equals(*b1));
  ASSERT_FALSE(b1->Equals(*b3));
  ASSERT_FALSE(b1->Equals(*b4));
}

TEST_F(TestRecordBatch, Validate) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());
  auto f2 = field("f2", int16());

  auto schema = ::arrow::schema({f0, f1, f2});

  auto a0 = MakeRandomArray<Int32Array>(length);
  auto a1 = MakeRandomArray<UInt8Array>(length);
  auto a2 = MakeRandomArray<Int16Array>(length);
  auto a3 = MakeRandomArray<Int16Array>(5);

  auto b1 = RecordBatch::Make(schema, length, {a0, a1, a2});

  ASSERT_OK(b1->ValidateFull());

  // Length mismatch
  auto b2 = RecordBatch::Make(schema, length, {a0, a1, a3});
  ASSERT_RAISES(Invalid, b2->ValidateFull());

  // Type mismatch
  auto b3 = RecordBatch::Make(schema, length, {a0, a1, a0});
  ASSERT_RAISES(Invalid, b3->ValidateFull());
}

TEST_F(TestRecordBatch, Slice) {
  const int length = 10;

  auto f0 = field("f0", int32());
  auto f1 = field("f1", uint8());

  std::vector<std::shared_ptr<Field>> fields = {f0, f1};
  auto schema = ::arrow::schema(fields);

  auto a0 = MakeRandomArray<Int32Array>(length);
  auto a1 = MakeRandomArray<UInt8Array>(length);

  auto batch = RecordBatch::Make(schema, length, {a0, a1});

  auto batch_slice = batch->Slice(2);
  auto batch_slice2 = batch->Slice(1, 5);

  ASSERT_EQ(batch_slice->num_rows(), batch->num_rows() - 2);

  for (int i = 0; i < batch->num_columns(); ++i) {
    ASSERT_EQ(2, batch_slice->column(i)->offset());
    ASSERT_EQ(length - 2, batch_slice->column(i)->length());

    ASSERT_EQ(1, batch_slice2->column(i)->offset());
    ASSERT_EQ(5, batch_slice2->column(i)->length());
  }
}

TEST_F(TestRecordBatch, AddColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2});
  auto schema2 = ::arrow::schema({field2, field3});
  auto schema3 = ::arrow::schema({field2});

  auto array1 = MakeRandomArray<Int32Array>(length);
  auto array2 = MakeRandomArray<UInt8Array>(length);
  auto array3 = MakeRandomArray<Int16Array>(length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2});
  auto batch2 = RecordBatch::Make(schema2, length, {array2, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array2});

  const RecordBatch& batch = *batch3;
  std::shared_ptr<RecordBatch> result;

  // Negative tests with invalid index
  Status status = batch.AddColumn(5, field1, array1, &result);
  ASSERT_TRUE(status.IsInvalid());
  status = batch.AddColumn(2, field1, array1, &result);
  ASSERT_TRUE(status.IsInvalid());
  status = batch.AddColumn(-1, field1, array1, &result);
  ASSERT_TRUE(status.IsInvalid());

  // Negative test with wrong length
  auto longer_col = MakeRandomArray<Int32Array>(length + 1);
  status = batch.AddColumn(0, field1, longer_col, &result);
  ASSERT_TRUE(status.IsInvalid());

  // Negative test with mismatch type
  status = batch.AddColumn(0, field1, array2, &result);
  ASSERT_TRUE(status.IsInvalid());

  ASSERT_OK(batch.AddColumn(0, field1, array1, &result));
  ASSERT_TRUE(result->Equals(*batch1));

  ASSERT_OK(batch.AddColumn(1, field3, array3, &result));
  ASSERT_TRUE(result->Equals(*batch2));

  std::shared_ptr<RecordBatch> result2;
  ASSERT_OK(batch.AddColumn(1, "f3", array3, &result2));
  ASSERT_TRUE(result2->Equals(*result));

  ASSERT_TRUE(result2->schema()->field(1)->nullable());
}

TEST_F(TestRecordBatch, RemoveColumn) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto field2 = field("f2", uint8());
  auto field3 = field("f3", int16());

  auto schema1 = ::arrow::schema({field1, field2, field3});
  auto schema2 = ::arrow::schema({field2, field3});
  auto schema3 = ::arrow::schema({field1, field3});
  auto schema4 = ::arrow::schema({field1, field2});

  auto array1 = MakeRandomArray<Int32Array>(length);
  auto array2 = MakeRandomArray<UInt8Array>(length);
  auto array3 = MakeRandomArray<Int16Array>(length);

  auto batch1 = RecordBatch::Make(schema1, length, {array1, array2, array3});
  auto batch2 = RecordBatch::Make(schema2, length, {array2, array3});
  auto batch3 = RecordBatch::Make(schema3, length, {array1, array3});
  auto batch4 = RecordBatch::Make(schema4, length, {array1, array2});

  const RecordBatch& batch = *batch1;
  std::shared_ptr<RecordBatch> result;

  // Negative tests with invalid index
  Status status = batch.RemoveColumn(3, &result);
  ASSERT_TRUE(status.IsInvalid());
  status = batch.RemoveColumn(-1, &result);
  ASSERT_TRUE(status.IsInvalid());

  ASSERT_OK(batch.RemoveColumn(0, &result));
  ASSERT_TRUE(result->Equals(*batch2));

  ASSERT_OK(batch.RemoveColumn(1, &result));
  ASSERT_TRUE(result->Equals(*batch3));

  ASSERT_OK(batch.RemoveColumn(2, &result));
  ASSERT_TRUE(result->Equals(*batch4));
}

TEST_F(TestRecordBatch, RemoveColumnEmpty) {
  const int length = 10;

  auto field1 = field("f1", int32());
  auto schema1 = ::arrow::schema({field1});
  auto array1 = MakeRandomArray<Int32Array>(length);
  auto batch1 = RecordBatch::Make(schema1, length, {array1});

  std::shared_ptr<RecordBatch> empty;
  ASSERT_OK(batch1->RemoveColumn(0, &empty));
  ASSERT_EQ(batch1->num_rows(), empty->num_rows());

  std::shared_ptr<RecordBatch> added;
  ASSERT_OK(empty->AddColumn(0, field1, array1, &added));
  ASSERT_TRUE(added->Equals(*batch1));
}

class TestTableBatchReader : public TestBase {};

TEST_F(TestTableBatchReader, ReadNext) {
  ArrayVector c1, c2;

  auto a1 = MakeRandomArray<Int32Array>(10);
  auto a2 = MakeRandomArray<Int32Array>(20);
  auto a3 = MakeRandomArray<Int32Array>(30);
  auto a4 = MakeRandomArray<Int32Array>(10);

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
  auto a1 = MakeRandomArray<Int32Array>(10);
  auto a2 = MakeRandomArray<Int32Array>(20);
  auto a3 = MakeRandomArray<Int32Array>(10);

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

}  // namespace arrow
