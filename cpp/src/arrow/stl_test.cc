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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/memory_pool.h"
#include "arrow/stl.h"
#include "arrow/stl_allocator.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

using primitive_types_tuple = std::tuple<int8_t, int16_t, int32_t, int64_t, uint8_t,
                                         uint16_t, uint32_t, uint64_t, bool, std::string>;

using raw_pointer_optional_types_tuple =
    std::tuple<int8_t*, int16_t*, int32_t*, int64_t*, uint8_t*, uint16_t*, uint32_t*,
               uint64_t*, bool*, std::string*>;

struct CustomType {
  int8_t i8;
  int16_t i16;
  int32_t i32;
  int64_t i64;
  uint8_t u8;
  uint16_t u16;
  uint32_t u32;
  uint64_t u64;
  bool b;
  std::string s;

#define ARROW_CUSTOM_TYPE_TIED std::tie(i8, i16, i32, i64, u8, u16, u32, u64, b, s)
  auto tie() const -> decltype(ARROW_CUSTOM_TYPE_TIED) { return ARROW_CUSTOM_TYPE_TIED; }
#undef ARROW_CUSTOM_TYPE_TIED
};

// Mock optional object returning null, "yes", "no", null, "yes", "no", ...
// Note: This mock optional object will advance its state every time it's casted
// to bool. Successive castings to bool may give inconsistent results. It
// doesn't mock entire optional logic. It is used only for ensuring user
// specialization isn't broken with templated Optionals.
struct CustomOptionalTypeMock {
  static int counter;
  mutable bool was_casted_once_ = false;

  CustomOptionalTypeMock() = default;
  explicit operator bool() const {
    if (!was_casted_once_) {
      was_casted_once_ = true;
      counter++;
      return counter % 3 != 0;
    }
    ADD_FAILURE() << "A CustomOptionalTypeMock should be casted to bool only once.";
    return false;
  }
  std::string operator*() const {
    switch (counter % 3) {
      case 0:
        ADD_FAILURE() << "Optional dereferenced in null value";
        break;
      case 1:
        return "yes";
      case 2:
        return "no";
    }
    return "error";
  }
};

int CustomOptionalTypeMock::counter = -1;

// This is for testing appending list values with custom types
struct TestInt32Type {
  int32_t value;
};

namespace arrow {

using optional_types_tuple =
    std::tuple<std::optional<int8_t>, std::optional<int16_t>, std::optional<int32_t>,
               std::optional<int64_t>, std::optional<uint8_t>, std::optional<uint16_t>,
               std::optional<uint32_t>, std::optional<uint64_t>, std::optional<bool>,
               std::optional<std::string>>;

template <>
struct CTypeTraits<CustomOptionalTypeMock> {
  using ArrowType = ::arrow::StringType;

  static std::shared_ptr<::arrow::DataType> type_singleton() { return ::arrow::utf8(); }
};

template <>
struct CTypeTraits<TestInt32Type> {
  using ArrowType = ::arrow::Int32Type;

  static std::shared_ptr<::arrow::DataType> type_singleton() { return ::arrow::int32(); }
};

namespace stl {

template <>
struct ConversionTraits<CustomOptionalTypeMock>
    : public CTypeTraits<CustomOptionalTypeMock> {
  static Status AppendRow(typename TypeTraits<ArrowType>::BuilderType& builder,
                          const CustomOptionalTypeMock& cell) {
    if (cell) {
      return builder.Append("mock " + *cell);
    } else {
      return builder.AppendNull();
    }
  }
};

template <>
struct ConversionTraits<TestInt32Type> : public CTypeTraits<TestInt32Type> {
  // AppendRow is not needed, explicitly elide an implementation
};

template <>
Status AppendListValues<TestInt32Type, const std::vector<TestInt32Type>&>(
    Int32Builder& value_builder, const std::vector<TestInt32Type>& cell_range) {
  return value_builder.AppendValues(reinterpret_cast<const int32_t*>(cell_range.data()),
                                    cell_range.size());
}

TEST(TestSchemaFromTuple, PrimitiveTypesVector) {
  Schema expected_schema(
      {field("column1", int8(), false), field("column2", int16(), false),
       field("column3", int32(), false), field("column4", int64(), false),
       field("column5", uint8(), false), field("column6", uint16(), false),
       field("column7", uint32(), false), field("column8", uint64(), false),
       field("column9", boolean(), false), field("column10", utf8(), false)});

  std::shared_ptr<Schema> schema = SchemaFromTuple<primitive_types_tuple>::MakeSchema(
      std::vector<std::string>({"column1", "column2", "column3", "column4", "column5",
                                "column6", "column7", "column8", "column9", "column10"}));
  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, PrimitiveTypesTuple) {
  Schema expected_schema(
      {field("column1", int8(), false), field("column2", int16(), false),
       field("column3", int32(), false), field("column4", int64(), false),
       field("column5", uint8(), false), field("column6", uint16(), false),
       field("column7", uint32(), false), field("column8", uint64(), false),
       field("column9", boolean(), false), field("column10", utf8(), false)});

  std::shared_ptr<Schema> schema = SchemaFromTuple<primitive_types_tuple>::MakeSchema(
      std::make_tuple("column1", "column2", "column3", "column4", "column5", "column6",
                      "column7", "column8", "column9", "column10"));
  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, SimpleList) {
  Schema expected_schema({field("column1", list(utf8()), false)});
  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<std::vector<std::string>>>::MakeSchema({"column1"});

  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestSchemaFromTuple, NestedList) {
  Schema expected_schema({field("column1", list(list(boolean())), false)});
  std::shared_ptr<Schema> schema =
      SchemaFromTuple<std::tuple<std::vector<std::vector<bool>>>>::MakeSchema(
          {"column1"});

  ASSERT_TRUE(expected_schema.Equals(*schema));
}

TEST(TestTableFromTupleVector, PrimitiveTypes) {
  std::vector<std::string> names{"column1", "column2", "column3", "column4", "column5",
                                 "column6", "column7", "column8", "column9", "column10"};
  std::vector<primitive_types_tuple> rows{
      primitive_types_tuple(-1, -2, -3, -4, 1, 2, 3, 4, true, "Tests"),
      primitive_types_tuple(-10, -20, -30, -40, 10, 20, 30, 40, false, "Other")};
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));

  std::shared_ptr<Schema> expected_schema =
      schema({field("column1", int8(), false), field("column2", int16(), false),
              field("column3", int32(), false), field("column4", int64(), false),
              field("column5", uint8(), false), field("column6", uint16(), false),
              field("column7", uint32(), false), field("column8", uint64(), false),
              field("column9", boolean(), false), field("column10", utf8(), false)});

  // Construct expected arrays
  std::shared_ptr<Array> int8_array = ArrayFromJSON(int8(), "[-1, -10]");
  std::shared_ptr<Array> int16_array = ArrayFromJSON(int16(), "[-2, -20]");
  std::shared_ptr<Array> int32_array = ArrayFromJSON(int32(), "[-3, -30]");
  std::shared_ptr<Array> int64_array = ArrayFromJSON(int64(), "[-4, -40]");
  std::shared_ptr<Array> uint8_array = ArrayFromJSON(uint8(), "[1, 10]");
  std::shared_ptr<Array> uint16_array = ArrayFromJSON(uint16(), "[2, 20]");
  std::shared_ptr<Array> uint32_array = ArrayFromJSON(uint32(), "[3, 30]");
  std::shared_ptr<Array> uint64_array = ArrayFromJSON(uint64(), "[4, 40]");
  std::shared_ptr<Array> bool_array = ArrayFromJSON(boolean(), "[true, false]");
  std::shared_ptr<Array> string_array = ArrayFromJSON(utf8(), R"(["Tests", "Other"])");
  auto expected_table =
      Table::Make(expected_schema,
                  {int8_array, int16_array, int32_array, int64_array, uint8_array,
                   uint16_array, uint32_array, uint64_array, bool_array, string_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, ListType) {
  using tuple_type = std::tuple<std::vector<int64_t>>;

  auto expected_schema =
      std::make_shared<Schema>(FieldVector{field("column1", list(int64()), false)});
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(list(int64()), "[[1, 1, 2, 34], [2, -4]]");
  std::shared_ptr<Table> expected_table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> rows{tuple_type(std::vector<int64_t>{1, 1, 2, 34}),
                               tuple_type(std::vector<int64_t>{2, -4})};
  std::vector<std::string> names{"column1"};

  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));
  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, FixedSizeListType) {
  using tuple_type = std::tuple<std::array<int64_t, 4>>;

  auto expected_schema = std::make_shared<Schema>(
      FieldVector{field("column1", fixed_size_list(int64(), 4), false)});
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(fixed_size_list(int64(), 4), "[[1, 1, 2, 34], [2, -4, 1, 1]]");
  std::shared_ptr<Table> expected_table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> rows{tuple_type(std::array<int64_t, 4>{1, 1, 2, 34}),
                               tuple_type(std::array<int64_t, 4>{2, -4, 1, 1})};
  std::vector<std::string> names{"column1"};

  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));
  ASSERT_OK(table->ValidateFull());

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestTableFromTupleVector, ReferenceTuple) {
  std::vector<std::string> names{"column1", "column2", "column3", "column4", "column5",
                                 "column6", "column7", "column8", "column9", "column10"};
  std::vector<CustomType> rows{
      {-1, -2, -3, -4, 1, 2, 3, 4, true, std::string("Tests")},
      {-10, -20, -30, -40, 10, 20, 30, 40, false, std::string("Other")}};
  std::vector<decltype(rows[0].tie())> rng_rows{
      rows[0].tie(),
      rows[1].tie(),
  };
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rng_rows, names, &table));

  std::shared_ptr<Schema> expected_schema =
      schema({field("column1", int8(), false), field("column2", int16(), false),
              field("column3", int32(), false), field("column4", int64(), false),
              field("column5", uint8(), false), field("column6", uint16(), false),
              field("column7", uint32(), false), field("column8", uint64(), false),
              field("column9", boolean(), false), field("column10", utf8(), false)});

  // Construct expected arrays
  std::shared_ptr<Array> int8_array = ArrayFromJSON(int8(), "[-1, -10]");
  std::shared_ptr<Array> int16_array = ArrayFromJSON(int16(), "[-2, -20]");
  std::shared_ptr<Array> int32_array = ArrayFromJSON(int32(), "[-3, -30]");
  std::shared_ptr<Array> int64_array = ArrayFromJSON(int64(), "[-4, -40]");
  std::shared_ptr<Array> uint8_array = ArrayFromJSON(uint8(), "[1, 10]");
  std::shared_ptr<Array> uint16_array = ArrayFromJSON(uint16(), "[2, 20]");
  std::shared_ptr<Array> uint32_array = ArrayFromJSON(uint32(), "[3, 30]");
  std::shared_ptr<Array> uint64_array = ArrayFromJSON(uint64(), "[4, 40]");
  std::shared_ptr<Array> bool_array = ArrayFromJSON(boolean(), "[true, false]");
  std::shared_ptr<Array> string_array = ArrayFromJSON(utf8(), R"(["Tests", "Other"])");
  auto expected_table =
      Table::Make(expected_schema,
                  {int8_array, int16_array, int32_array, int64_array, uint8_array,
                   uint16_array, uint32_array, uint64_array, bool_array, string_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, NullableTypesWithBoostOptional) {
  std::vector<std::string> names{"column1", "column2", "column3", "column4", "column5",
                                 "column6", "column7", "column8", "column9", "column10"};
  using types_tuple = optional_types_tuple;
  std::vector<types_tuple> rows{
      types_tuple(-1, -2, -3, -4, 1, 2, 3, 4, true, std::string("Tests")),
      types_tuple(-10, -20, -30, -40, 10, 20, 30, 40, false, std::string("Other")),
      types_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
                  std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt),
  };
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));

  std::shared_ptr<Schema> expected_schema =
      schema({field("column1", int8(), true), field("column2", int16(), true),
              field("column3", int32(), true), field("column4", int64(), true),
              field("column5", uint8(), true), field("column6", uint16(), true),
              field("column7", uint32(), true), field("column8", uint64(), true),
              field("column9", boolean(), true), field("column10", utf8(), true)});

  // Construct expected arrays
  std::shared_ptr<Array> int8_array = ArrayFromJSON(int8(), "[-1, -10, null]");
  std::shared_ptr<Array> int16_array = ArrayFromJSON(int16(), "[-2, -20, null]");
  std::shared_ptr<Array> int32_array = ArrayFromJSON(int32(), "[-3, -30, null]");
  std::shared_ptr<Array> int64_array = ArrayFromJSON(int64(), "[-4, -40, null]");
  std::shared_ptr<Array> uint8_array = ArrayFromJSON(uint8(), "[1, 10, null]");
  std::shared_ptr<Array> uint16_array = ArrayFromJSON(uint16(), "[2, 20, null]");
  std::shared_ptr<Array> uint32_array = ArrayFromJSON(uint32(), "[3, 30, null]");
  std::shared_ptr<Array> uint64_array = ArrayFromJSON(uint64(), "[4, 40, null]");
  std::shared_ptr<Array> bool_array = ArrayFromJSON(boolean(), "[true, false, null]");
  std::shared_ptr<Array> string_array =
      ArrayFromJSON(utf8(), R"(["Tests", "Other", null])");
  auto expected_table =
      Table::Make(expected_schema,
                  {int8_array, int16_array, int32_array, int64_array, uint8_array,
                   uint16_array, uint32_array, uint64_array, bool_array, string_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, NullableTypesWithRawPointer) {
  std::vector<std::string> names{"column1", "column2", "column3", "column4", "column5",
                                 "column6", "column7", "column8", "column9", "column10"};
  std::vector<primitive_types_tuple> data_rows{
      primitive_types_tuple(-1, -2, -3, -4, 1, 2, 3, 4, true, std::string("Tests")),
      primitive_types_tuple(-10, -20, -30, -40, 10, 20, 30, 40, false,
                            std::string("Other")),
  };
  std::vector<raw_pointer_optional_types_tuple> pointer_rows;
  for (auto& row : data_rows) {
    pointer_rows.emplace_back(
        std::addressof(std::get<0>(row)), std::addressof(std::get<1>(row)),
        std::addressof(std::get<2>(row)), std::addressof(std::get<3>(row)),
        std::addressof(std::get<4>(row)), std::addressof(std::get<5>(row)),
        std::addressof(std::get<6>(row)), std::addressof(std::get<7>(row)),
        std::addressof(std::get<8>(row)), std::addressof(std::get<9>(row)));
  }
  pointer_rows.emplace_back(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
                            nullptr, nullptr, nullptr);
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), pointer_rows, names, &table));

  std::shared_ptr<Schema> expected_schema =
      schema({field("column1", int8(), true), field("column2", int16(), true),
              field("column3", int32(), true), field("column4", int64(), true),
              field("column5", uint8(), true), field("column6", uint16(), true),
              field("column7", uint32(), true), field("column8", uint64(), true),
              field("column9", boolean(), true), field("column10", utf8(), true)});

  // Construct expected arrays
  std::shared_ptr<Array> int8_array = ArrayFromJSON(int8(), "[-1, -10, null]");
  std::shared_ptr<Array> int16_array = ArrayFromJSON(int16(), "[-2, -20, null]");
  std::shared_ptr<Array> int32_array = ArrayFromJSON(int32(), "[-3, -30, null]");
  std::shared_ptr<Array> int64_array = ArrayFromJSON(int64(), "[-4, -40, null]");
  std::shared_ptr<Array> uint8_array = ArrayFromJSON(uint8(), "[1, 10, null]");
  std::shared_ptr<Array> uint16_array = ArrayFromJSON(uint16(), "[2, 20, null]");
  std::shared_ptr<Array> uint32_array = ArrayFromJSON(uint32(), "[3, 30, null]");
  std::shared_ptr<Array> uint64_array = ArrayFromJSON(uint64(), "[4, 40, null]");
  std::shared_ptr<Array> bool_array = ArrayFromJSON(boolean(), "[true, false, null]");
  std::shared_ptr<Array> string_array =
      ArrayFromJSON(utf8(), R"(["Tests", "Other", null])");
  auto expected_table =
      Table::Make(expected_schema,
                  {int8_array, int16_array, int32_array, int64_array, uint8_array,
                   uint16_array, uint32_array, uint64_array, bool_array, string_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, NullableTypesDoNotBreakUserSpecialization) {
  std::vector<std::string> names{"column1"};
  std::vector<std::tuple<CustomOptionalTypeMock>> rows(3);
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));

  std::shared_ptr<Schema> expected_schema = schema({field("column1", utf8(), true)});
  std::shared_ptr<Array> string_array =
      ArrayFromJSON(utf8(), R"([null, "mock yes", "mock no"])");
  auto expected_table = Table::Make(expected_schema, {string_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTableFromTupleVector, AppendingMultipleRows) {
  using row_type = std::tuple<std::vector<TestInt32Type>>;
  std::vector<std::string> names{"column1"};
  std::vector<row_type> rows = {
      row_type{{{1}, {2}, {3}}},    //
      row_type{{{10}, {20}, {30}}}  //
  };
  std::shared_ptr<Table> table;
  ASSERT_OK(TableFromTupleRange(default_memory_pool(), rows, names, &table));

  std::shared_ptr<Schema> expected_schema =
      schema({field("column1", list(int32()), false)});
  std::shared_ptr<Array> int_array =
      ArrayFromJSON(list(int32()), "[[1, 2, 3], [10, 20, 30]]");
  auto expected_table = Table::Make(expected_schema, {int_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestTupleVectorFromTable, PrimitiveTypes) {
  compute::ExecContext ctx;
  compute::CastOptions cast_options;

  std::vector<primitive_types_tuple> expected_rows{
      primitive_types_tuple(-1, -2, -3, -4, 1, 2, 3, 4, true, "Tests"),
      primitive_types_tuple(-10, -20, -30, -40, 10, 20, 30, 40, false, "Other")};

  std::shared_ptr<Schema> schema = std::shared_ptr<Schema>(
      new Schema({field("column1", int8(), false), field("column2", int16(), false),
                  field("column3", int32(), false), field("column4", int64(), false),
                  field("column5", uint8(), false), field("column6", uint16(), false),
                  field("column7", uint32(), false), field("column8", uint64(), false),
                  field("column9", boolean(), false), field("column10", utf8(), false)}));

  // Construct expected arrays
  auto int8_array = ArrayFromJSON(int8(), "[-1, -10]");
  auto int16_array = ArrayFromJSON(int16(), "[-2, -20]");
  auto int32_array = ArrayFromJSON(int32(), "[-3, -30]");
  auto int64_array = ArrayFromJSON(int64(), "[-4, -40]");
  auto uint8_array = ArrayFromJSON(uint8(), "[1, 10]");
  auto uint16_array = ArrayFromJSON(uint16(), "[2, 20]");
  auto uint32_array = ArrayFromJSON(uint32(), "[3, 30]");
  auto uint64_array = ArrayFromJSON(uint64(), "[4, 40]");
  auto bool_array = ArrayFromJSON(boolean(), "[true, false]");
  auto string_array = ArrayFromJSON(utf8(), R"(["Tests", "Other"])");
  auto table = Table::Make(
      schema, {int8_array, int16_array, int32_array, int64_array, uint8_array,
               uint16_array, uint32_array, uint64_array, bool_array, string_array});

  std::vector<primitive_types_tuple> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);

  // The number of rows must match
  std::vector<primitive_types_tuple> too_few_rows(1);
  ASSERT_RAISES(Invalid, TupleRangeFromTable(*table, cast_options, &ctx, &too_few_rows));

  // The number of columns must match
  ASSERT_OK_AND_ASSIGN(auto corrupt_table, table->RemoveColumn(0));
  ASSERT_RAISES(Invalid, TupleRangeFromTable(*corrupt_table, cast_options, &ctx, &rows));
}

TEST(TestTupleVectorFromTable, ListType) {
  using tuple_type = std::tuple<std::vector<int64_t>>;

  compute::ExecContext ctx;
  compute::CastOptions cast_options;
  auto expected_schema =
      std::make_shared<Schema>(FieldVector{field("column1", list(int64()), false)});
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(list(int64()), "[[1, 1, 2, 34], [2, -4]]");
  std::shared_ptr<Table> table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> expected_rows{tuple_type(std::vector<int64_t>{1, 1, 2, 34}),
                                        tuple_type(std::vector<int64_t>{2, -4})};

  std::vector<tuple_type> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);
}

TEST(TestTupleVectorFromTable, FixedSizeListType) {
  using tuple_type = std::tuple<std::array<int64_t, 4>>;

  compute::ExecContext ctx;
  compute::CastOptions cast_options;
  auto expected_schema = std::make_shared<Schema>(
      FieldVector{field("column1", fixed_size_list(int64(), 4), false)});
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(fixed_size_list(int64(), 4), "[[1, 1, 2, 34], [2, -4, 1, 1]]");
  std::shared_ptr<Table> table = Table::Make(expected_schema, {expected_array});
  ASSERT_OK(table->ValidateFull());

  std::vector<tuple_type> expected_rows{tuple_type(std::array<int64_t, 4>{1, 1, 2, 34}),
                                        tuple_type(std::array<int64_t, 4>{2, -4, 1, 1})};

  std::vector<tuple_type> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);
}

TEST(TestTupleVectorFromTable, CastingNeeded) {
  using tuple_type = std::tuple<std::vector<int64_t>>;

  compute::ExecContext ctx;
  compute::CastOptions cast_options;
  auto expected_schema =
      std::make_shared<Schema>(FieldVector{field("column1", list(int16()), false)});
  std::shared_ptr<Array> expected_array =
      ArrayFromJSON(list(int16()), "[[1, 1, 2, 34], [2, -4]]");
  std::shared_ptr<Table> table = Table::Make(expected_schema, {expected_array});

  std::vector<tuple_type> expected_rows{tuple_type(std::vector<int64_t>{1, 1, 2, 34}),
                                        tuple_type(std::vector<int64_t>{2, -4})};

  std::vector<tuple_type> rows(2);
  ASSERT_OK(TupleRangeFromTable(*table, cast_options, &ctx, &rows));
  ASSERT_EQ(rows, expected_rows);
}

TEST(STLMemoryPool, Base) {
  std::allocator<uint8_t> allocator;
  STLMemoryPool<std::allocator<uint8_t>> pool(allocator);

  uint8_t* data = nullptr;
  ASSERT_OK(pool.Allocate(100, &data));
  ASSERT_EQ(pool.max_memory(), 100);
  ASSERT_EQ(pool.bytes_allocated(), 100);
  ASSERT_NE(data, nullptr);

  ASSERT_OK(pool.Reallocate(100, 150, &data));
  ASSERT_EQ(pool.max_memory(), 150);
  ASSERT_EQ(pool.bytes_allocated(), 150);

  pool.Free(data, 150);

  ASSERT_EQ(pool.max_memory(), 150);
  ASSERT_EQ(pool.bytes_allocated(), 0);
}

TEST(allocator, MemoryTracking) {
  auto pool = default_memory_pool();
  allocator<uint64_t> alloc;
  uint64_t* data = alloc.allocate(100);

  ASSERT_EQ(100 * sizeof(uint64_t), pool->bytes_allocated());

  alloc.deallocate(data, 100);
  ASSERT_EQ(0, pool->bytes_allocated());
}

#if !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER) || defined(ARROW_JEMALLOC))

TEST(allocator, TestOOM) {
  allocator<uint8_t> alloc;
  size_t max_alloc = std::min<uint64_t>(std::numeric_limits<int64_t>::max(),
                                        std::numeric_limits<size_t>::max());
  ASSERT_THROW(alloc.allocate(max_alloc), std::bad_alloc);
}

TEST(stl_allocator, MaxMemory) {
  auto pool = MemoryPool::CreateDefault();

  allocator<uint8_t> alloc(pool.get());
  uint8_t* data = alloc.allocate(1000);
  uint8_t* data2 = alloc.allocate(1000);

  alloc.deallocate(data, 1000);
  alloc.deallocate(data2, 1000);

  ASSERT_EQ(2000, pool->max_memory());
}

#endif  // !(defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER)
        // || defined(ARROW_JEMALLOC))

}  // namespace stl
}  // namespace arrow
