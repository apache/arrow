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

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using testing::ElementsAre;

using internal::checked_cast;
using internal::checked_pointer_cast;

struct FieldPathTestCase {
  struct OutputValues {
    explicit OutputValues(std::vector<int> indices = {})
        : path(FieldPath(std::move(indices))) {}

    template <typename T>
    const auto& OutputAs() const {
      if constexpr (std::is_same_v<T, Field>) {
        return field;
      } else if constexpr (std::is_same_v<T, Array>) {
        return array;
      } else if constexpr (std::is_same_v<T, ArrayData>) {
        return array->data();
      } else if constexpr (std::is_same_v<T, ChunkedArray>) {
        return chunked_array;
      }
    }

    FieldPath path;
    std::shared_ptr<Field> field;
    std::shared_ptr<Array> array;
    std::shared_ptr<ChunkedArray> chunked_array;
  };

  static constexpr int kNumColumns = 2;
  static constexpr int kNumRows = 100;
  static constexpr int kRandomSeed = 0xbeef;

  // Input for the FieldPath::Get functions in multiple forms
  std::shared_ptr<Schema> schema;
  std::shared_ptr<DataType> type;
  std::shared_ptr<Array> array;
  std::shared_ptr<RecordBatch> record_batch;
  std::shared_ptr<ChunkedArray> chunked_array;
  std::shared_ptr<Table> table;

  template <typename T>
  const auto& InputAs() const {
    if constexpr (std::is_same_v<T, Schema>) {
      return schema;
    } else if constexpr (std::is_same_v<T, DataType>) {
      return type;
    } else if constexpr (std::is_same_v<T, Array>) {
      return array;
    } else if constexpr (std::is_same_v<T, ArrayData>) {
      return array->data();
    } else if constexpr (std::is_same_v<T, RecordBatch>) {
      return record_batch;
    } else if constexpr (std::is_same_v<T, ChunkedArray>) {
      return chunked_array;
    } else if constexpr (std::is_same_v<T, Table>) {
      return table;
    }
  }

  // Number of chunks for each column in the input Table
  const std::array<int, kNumColumns> num_column_chunks = {15, 20};
  // Number of chunks in the input ChunkedArray
  const int num_chunks = 15;

  // Expected outputs for each child;
  OutputValues v0{{0}}, v1{{1}};
  OutputValues v1_0{{1, 0}}, v1_1{{1, 1}};
  OutputValues v1_1_0{{1, 1, 0}}, v1_1_1{{1, 1, 1}};
  // Expected outputs for nested children with null flattening applied
  OutputValues v1_0_flat{{1, 0}}, v1_1_flat{{1, 1}};
  OutputValues v1_1_0_flat{{1, 1, 0}}, v1_1_1_flat{{1, 1, 1}};

  static const FieldPathTestCase* Instance() {
    static const auto maybe_instance = Make();
    return &maybe_instance.ValueOrDie();
  }

  static Result<FieldPathTestCase> Make() {
    // Generate test input based on a single schema. First by creating a StructArray,
    // then deriving the other input types (ChunkedArray, RecordBatch, Table, etc) from
    // it. We also compute the expected outputs for each child individually (for each
    // output type).
    FieldPathTestCase out;
    random::RandomArrayGenerator gen(kRandomSeed);

    // Define child fields and input schema

    // Intentionally duplicated names for the FieldRef tests
    out.v1_1_1.field = field("a", boolean());
    out.v1_1_0.field = field("a", float32());

    out.v1_1.field = field("b", struct_({out.v1_1_0.field, out.v1_1_1.field}));
    out.v1_0.field = field("a", int32());
    out.v1.field = field("b", struct_({out.v1_0.field, out.v1_1.field}));
    out.v0.field = field("a", utf8());
    out.schema = arrow::schema({out.v0.field, out.v1.field});
    out.type = struct_(out.schema->fields());

    // Create null bitmaps for the struct fields independent of its childrens'
    // bitmaps. For FieldPath::GetFlattened, parent/child bitmaps should be combined
    // - for FieldPath::Get, higher-level nulls are ignored.
    auto bitmap1_1 = gen.NullBitmap(kNumRows, 0.15);
    auto bitmap1 = gen.NullBitmap(kNumRows, 0.30);

    // Generate raw leaf arrays
    out.v1_1_1.array = gen.ArrayOf(out.v1_1_1.field->type(), kNumRows);
    out.v1_1_0.array = gen.ArrayOf(out.v1_1_0.field->type(), kNumRows);
    out.v1_0.array = gen.ArrayOf(out.v1_0.field->type(), kNumRows);
    out.v0.array = gen.ArrayOf(out.v0.field->type(), kNumRows);
    // Make struct fields from leaf arrays (we use the custom bitmaps here)
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1.array,
        StructArray::Make({out.v1_1_0.array, out.v1_1_1.array},
                          {out.v1_1_0.field, out.v1_1_1.field}, bitmap1_1));
    ARROW_ASSIGN_OR_RAISE(out.v1.array,
                          StructArray::Make({out.v1_0.array, out.v1_1.array},
                                            {out.v1_0.field, out.v1_1.field}, bitmap1));

    // Not used to create the test input, but pre-compute flattened versions of nested
    // arrays for comparisons in the GetFlattened tests.
    ARROW_ASSIGN_OR_RAISE(
        out.v1_0_flat.array,
        checked_pointer_cast<StructArray>(out.v1.array)->GetFlattenedField(0));
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1_flat.array,
        checked_pointer_cast<StructArray>(out.v1.array)->GetFlattenedField(1));
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1_0_flat.array,
        checked_pointer_cast<StructArray>(out.v1_1_flat.array)->GetFlattenedField(0));
    ARROW_ASSIGN_OR_RAISE(
        out.v1_1_1_flat.array,
        checked_pointer_cast<StructArray>(out.v1_1_flat.array)->GetFlattenedField(1));
    // Sanity check
    ARROW_CHECK(!out.v1_0_flat.array->Equals(out.v1_0.array));
    ARROW_CHECK(!out.v1_1_flat.array->Equals(out.v1_1.array));
    ARROW_CHECK(!out.v1_1_0_flat.array->Equals(out.v1_1_0.array));
    ARROW_CHECK(!out.v1_1_1_flat.array->Equals(out.v1_1_1.array));

    // Finalize the input Array
    ARROW_ASSIGN_OR_RAISE(out.array, StructArray::Make({out.v0.array, out.v1.array},
                                                       {out.v0.field, out.v1.field}));
    ARROW_RETURN_NOT_OK(out.array->ValidateFull());
    // Finalize the input RecordBatch
    ARROW_ASSIGN_OR_RAISE(out.record_batch, RecordBatch::FromStructArray(out.array));
    ARROW_RETURN_NOT_OK(out.record_batch->ValidateFull());
    // Finalize the input ChunkedArray
    out.chunked_array = SliceToChunkedArray(*out.array, out.num_chunks);
    ARROW_RETURN_NOT_OK(out.chunked_array->ValidateFull());

    // For each expected child array, create a chunked equivalent (we use a different
    // chunk layout for each top-level column to make the Table test more interesting)
    for (OutputValues* v :
         {&out.v0, &out.v1, &out.v1_0, &out.v1_1, &out.v1_1_0, &out.v1_1_1,
          &out.v1_0_flat, &out.v1_1_flat, &out.v1_1_0_flat, &out.v1_1_1_flat}) {
      v->chunked_array =
          SliceToChunkedArray(*v->array, out.num_column_chunks[v->path[0]]);
    }
    // Finalize the input Table
    out.table =
        Table::Make(out.schema, {out.v0.chunked_array, out.v1.chunked_array}, kNumRows);
    ARROW_RETURN_NOT_OK(out.table->ValidateFull());

    return std::move(out);
  }

 private:
  static std::shared_ptr<ChunkedArray> SliceToChunkedArray(const Array& array,
                                                           int num_chunks) {
    ARROW_CHECK(num_chunks > 0 && array.length() >= num_chunks);
    ArrayVector chunks;
    chunks.reserve(num_chunks);
    for (int64_t inc = array.length() / num_chunks, beg = 0,
                 end = inc + array.length() % num_chunks;
         end <= array.length(); beg = end, end += inc) {
      chunks.push_back(array.SliceSafe(beg, end - beg).ValueOrDie());
    }
    ARROW_CHECK_EQ(static_cast<int>(chunks.size()), num_chunks);
    return ChunkedArray::Make(std::move(chunks)).ValueOrDie();
  }
};

class FieldPathTestFixture : public ::testing::Test {
 public:
  FieldPathTestFixture() : case_(FieldPathTestCase::Instance()) {}

 protected:
  template <typename T>
  using OutputType = typename FieldRef::GetType<T>::element_type;

  template <typename I>
  void AssertOutputsEqual(const std::shared_ptr<Field>& expected,
                          const std::shared_ptr<Field>& actual) const {
    AssertFieldEqual(*expected, *actual);
  }
  template <typename I>
  void AssertOutputsEqual(const std::shared_ptr<Array>& expected,
                          const std::shared_ptr<Array>& actual) const {
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*expected, *actual);
  }
  template <typename I>
  void AssertOutputsEqual(const std::shared_ptr<ChunkedArray>& expected,
                          const std::shared_ptr<ChunkedArray>& actual) const {
    ASSERT_OK(actual->ValidateFull());
    // We only do this dance due to the way the test inputs/outputs are generated.
    // Basically, the "expected" output ChunkedArrays don't have an equal num_chunks since
    // they're reused to create the input Table (which has a distinct chunking per
    // column). However, if the input was the ChunkedArray, the returned outputs should
    // always have the same num_chunks as the input.
    if constexpr (std::is_same_v<I, ChunkedArray>) {
      EXPECT_EQ(case_->chunked_array->num_chunks(), actual->num_chunks());
    } else {
      EXPECT_EQ(expected->num_chunks(), actual->num_chunks());
    }
    AssertChunkedEquivalent(*expected, *actual);
  }

  const FieldPathTestCase* case_;
};

class TestFieldPath : public FieldPathTestFixture {
 protected:
  template <bool Flattened, typename T>
  static auto DoGet(const T& root, const FieldPath& path, MemoryPool* pool = nullptr) {
    if constexpr (Flattened) {
      return path.GetFlattened(root, pool);
    } else {
      return path.Get(root);
    }
  }

  template <typename I, bool Flattened = false>
  void TestGetWithInvalidIndex() const {
    const auto& input = case_->InputAs<I>();
    for (const auto& path :
         {FieldPath({2, 1, 0}), FieldPath({1, 2, 0}), FieldPath{1, 1, 2}}) {
      EXPECT_RAISES_WITH_MESSAGE_THAT(IndexError,
                                      ::testing::HasSubstr("index out of range"),
                                      DoGet<Flattened>(*input, path));
    }
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("empty indices cannot be traversed"),
        DoGet<Flattened>(*input, FieldPath()));
  }

  template <typename I, bool Flattened = false>
  void TestIndexErrorMessage() const {
    using O = OutputType<I>;
    auto result = DoGet<Flattened>(*case_->InputAs<I>(), FieldPath({1, 1, 2}));
    std::string substr = "index out of range. indices=[ 1 1 >2< ] ";
    if constexpr (std::is_same_v<O, Field>) {
      substr += "fields: { a: float, a: bool, }";
    } else {
      substr += "column types: { float, bool, }";
    }
    EXPECT_RAISES_WITH_MESSAGE_THAT(IndexError, ::testing::HasSubstr(substr), result);
  }

  template <typename I, bool Flattened = false>
  void TestGetWithNonStructArray() const {
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        NotImplemented, ::testing::HasSubstr("Get child data of non-struct array"),
        DoGet<Flattened>(*case_->v1_1_0.OutputAs<I>(), FieldPath({1, 1, 0})));
  }

  template <typename I, bool Flattened = false>
  void TestGet() const {
    using O = OutputType<I>;
    const auto& input = case_->InputAs<I>();
    ASSERT_OK_AND_ASSIGN(auto v0, DoGet<Flattened>(*input, FieldPath({0})));
    ASSERT_OK_AND_ASSIGN(auto v1, DoGet<Flattened>(*input, FieldPath({1})));
    ASSERT_OK_AND_ASSIGN(auto v1_0, DoGet<Flattened>(*input, FieldPath({1, 0})));
    ASSERT_OK_AND_ASSIGN(auto v1_1, DoGet<Flattened>(*input, FieldPath({1, 1})));
    ASSERT_OK_AND_ASSIGN(auto v1_1_0, DoGet<Flattened>(*input, FieldPath({1, 1, 0})));
    ASSERT_OK_AND_ASSIGN(auto v1_1_1, DoGet<Flattened>(*input, FieldPath({1, 1, 1})));

    AssertOutputsEqual<I>(case_->v0.OutputAs<O>(), v0);
    AssertOutputsEqual<I>(case_->v1.OutputAs<O>(), v1);
    if constexpr (Flattened) {
      AssertOutputsEqual<I>(case_->v1_0_flat.OutputAs<O>(), v1_0);
      AssertOutputsEqual<I>(case_->v1_1_flat.OutputAs<O>(), v1_1);
      AssertOutputsEqual<I>(case_->v1_1_0_flat.OutputAs<O>(), v1_1_0);
      AssertOutputsEqual<I>(case_->v1_1_1_flat.OutputAs<O>(), v1_1_1);
    } else {
      AssertOutputsEqual<I>(case_->v1_0.OutputAs<O>(), v1_0);
      AssertOutputsEqual<I>(case_->v1_1.OutputAs<O>(), v1_1);
      AssertOutputsEqual<I>(case_->v1_1_0.OutputAs<O>(), v1_1_0);
      AssertOutputsEqual<I>(case_->v1_1_1.OutputAs<O>(), v1_1_1);
    }
  }
};

class TestFieldRef : public FieldPathTestFixture {
 protected:
  template <bool Flattened, typename T>
  static auto DoGetOne(const T& root, const FieldRef& ref, MemoryPool* pool = nullptr) {
    if constexpr (Flattened) {
      return ref.GetOneFlattened(root, pool);
    } else {
      return ref.GetOne(root);
    }
  }
  template <bool Flattened, typename T>
  static auto DoGetOneOrNone(const T& root, const FieldRef& ref,
                             MemoryPool* pool = nullptr) {
    if constexpr (Flattened) {
      return ref.GetOneOrNoneFlattened(root, pool);
    } else {
      return ref.GetOneOrNone(root);
    }
  }
  template <bool Flattened, typename T>
  static auto DoGetAll(const T& root, const FieldRef& ref, MemoryPool* pool = nullptr) {
    if constexpr (Flattened) {
      return ref.GetAllFlattened(root, pool);
    } else {
      return ToResult(ref.GetAll(root));
    }
  }

  template <typename I, bool Flattened = false>
  void TestGet() const {
    using O = OutputType<I>;
    const auto& input = case_->InputAs<I>();
    ASSERT_OK_AND_ASSIGN(auto v0, DoGetOne<Flattened>(*input, FieldRef("a")));
    ASSERT_OK_AND_ASSIGN(auto v1, DoGetOne<Flattened>(*input, FieldRef("b")));
    ASSERT_OK_AND_ASSIGN(auto v1_0, DoGetOne<Flattened>(*input, FieldRef("b", "a")));
    ASSERT_OK_AND_ASSIGN(auto v1_1, DoGetOne<Flattened>(*input, FieldRef("b", "b")));
    ASSERT_OK_AND_ASSIGN(auto v1_1_0, DoGetOne<Flattened>(*input, FieldRef("b", "b", 0)));
    ASSERT_OK_AND_ASSIGN(auto v1_1_1, DoGetOne<Flattened>(*input, FieldRef("b", "b", 1)));

    AssertOutputsEqual<I>(case_->v0.OutputAs<O>(), v0);
    AssertOutputsEqual<I>(case_->v1.OutputAs<O>(), v1);
    if constexpr (Flattened) {
      AssertOutputsEqual<I>(case_->v1_0_flat.OutputAs<O>(), v1_0);
      AssertOutputsEqual<I>(case_->v1_1_flat.OutputAs<O>(), v1_1);
      AssertOutputsEqual<I>(case_->v1_1_0_flat.OutputAs<O>(), v1_1_0);
      AssertOutputsEqual<I>(case_->v1_1_1_flat.OutputAs<O>(), v1_1_1);
    } else {
      AssertOutputsEqual<I>(case_->v1_0.OutputAs<O>(), v1_0);
      AssertOutputsEqual<I>(case_->v1_1.OutputAs<O>(), v1_1);
      AssertOutputsEqual<I>(case_->v1_1_0.OutputAs<O>(), v1_1_0);
      AssertOutputsEqual<I>(case_->v1_1_1.OutputAs<O>(), v1_1_1);
    }

    // Cases where multiple matches are found
    EXPECT_OK_AND_ASSIGN(auto multiple_matches,
                         DoGetAll<Flattened>(*input, FieldRef("b", "b", "a")));
    EXPECT_EQ(multiple_matches.size(), 2);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Multiple matches for "),
        (DoGetOne<Flattened>(*input, FieldRef("b", "b", "a"))));
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("Multiple matches for "),
        (DoGetOneOrNone<Flattened>(*input, FieldRef("b", "b", "a"))));

    // Cases where no match is found
    EXPECT_OK_AND_ASSIGN(auto no_matches,
                         DoGetAll<Flattened>(*input, FieldRef("b", "b", "b")));
    EXPECT_EQ(no_matches.size(), 0);
    EXPECT_RAISES_WITH_MESSAGE_THAT(
        Invalid, ::testing::HasSubstr("No match for "),
        (DoGetOne<Flattened>(*input, FieldRef("b", "b", "b"))));
    ASSERT_OK_AND_EQ(nullptr,
                     (DoGetOneOrNone<Flattened>(*input, FieldRef("b", "b", "b"))));
  }
};

// ----------------------------------------------------------------------
// FieldPath

TEST_F(TestFieldPath, Basics) {
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  Schema s({f0, f1, f2, f3});

  // retrieving a field with single-element FieldPath is equivalent to Schema::field
  for (int index = 0; index < s.num_fields(); ++index) {
    ASSERT_OK_AND_EQ(s.field(index), FieldPath({index}).Get(s));
  }
}

TEST_F(TestFieldPath, GetFromEmptyChunked) {
  FieldVector fields = {
      field("i", int32()),
      field("s", struct_({field("b", boolean()), field("f", float32())}))};
  std::shared_ptr<ChunkedArray> child;

  // Empty ChunkedArray with no chunks
  ChunkedArray chunked_array({}, struct_(fields));
  ASSERT_OK(chunked_array.ValidateFull());
  ASSERT_EQ(chunked_array.num_chunks(), 0);
  ASSERT_OK_AND_ASSIGN(child, FieldPath({1, 1}).Get(chunked_array));
  AssertTypeEqual(float32(), child->type());
  ASSERT_EQ(child->length(), 0);

  // Empty Table with no column chunks
  ChunkedArrayVector table_columns;
  for (const auto& f : fields) {
    table_columns.push_back(std::make_shared<ChunkedArray>(ArrayVector{}, f->type()));
  }
  auto table = Table::Make(schema(fields), table_columns, 0);
  ASSERT_OK(table->ValidateFull());
  for (const auto& column : table->columns()) {
    ASSERT_EQ(column->num_chunks(), 0);
  }
  ASSERT_OK_AND_ASSIGN(child, FieldPath({1, 1}).Get(*table));
  AssertTypeEqual(float32(), child->type());
  ASSERT_EQ(child->length(), 0);
}

TEST_F(TestFieldPath, GetWithInvalidIndex) {
  TestGetWithInvalidIndex<Schema>();
  TestGetWithInvalidIndex<DataType>();
  TestGetWithInvalidIndex<Array>();
  TestGetWithInvalidIndex<ArrayData>();
  TestGetWithInvalidIndex<ChunkedArray>();
  TestGetWithInvalidIndex<RecordBatch>();
  TestGetWithInvalidIndex<Table>();
  // With flattening
  TestGetWithInvalidIndex<Array, true>();
  TestGetWithInvalidIndex<ArrayData, true>();
  TestGetWithInvalidIndex<ChunkedArray, true>();
  TestGetWithInvalidIndex<RecordBatch, true>();
  TestGetWithInvalidIndex<Table, true>();
}

TEST_F(TestFieldPath, IndexErrorMessage) {
  TestIndexErrorMessage<Schema>();
  TestIndexErrorMessage<DataType>();
  TestIndexErrorMessage<Array>();
  TestIndexErrorMessage<ArrayData>();
  TestIndexErrorMessage<ChunkedArray>();
  TestIndexErrorMessage<RecordBatch>();
  TestIndexErrorMessage<Table>();
}

TEST_F(TestFieldPath, GetWithNonStructArray) {
  TestGetWithNonStructArray<Array>();
  TestGetWithNonStructArray<ArrayData>();
  TestGetWithNonStructArray<ChunkedArray>();
  // With flattening
  TestGetWithNonStructArray<Array, true>();
  TestGetWithNonStructArray<ArrayData, true>();
  TestGetWithNonStructArray<ChunkedArray, true>();
}

TEST_F(TestFieldPath, GetFromSchema) { TestGet<Schema>(); }
TEST_F(TestFieldPath, GetFromDataType) { TestGet<DataType>(); }

TEST_F(TestFieldPath, GetFromArray) { TestGet<Array>(); }
TEST_F(TestFieldPath, GetFromChunkedArray) { TestGet<ChunkedArray>(); }
TEST_F(TestFieldPath, GetFromRecordBatch) { TestGet<RecordBatch>(); }
TEST_F(TestFieldPath, GetFromTable) { TestGet<Table>(); }

TEST_F(TestFieldPath, GetFlattenedFromArray) { TestGet<Array, true>(); }
TEST_F(TestFieldPath, GetFlattenedFromChunkedArray) { TestGet<ChunkedArray, true>(); }
TEST_F(TestFieldPath, GetFlattenedFromRecordBatch) { TestGet<RecordBatch, true>(); }
TEST_F(TestFieldPath, GetFlattenedFromTable) { TestGet<Table, true>(); }

// ----------------------------------------------------------------------
// FieldRef

TEST_F(TestFieldRef, Basics) {
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  Schema s({f0, f1, f2, f3});

  // lookup by index returns Indices{index}
  for (int index = 0; index < s.num_fields(); ++index) {
    EXPECT_THAT(FieldRef(index).FindAll(s), ElementsAre(FieldPath{index}));
  }
  // out of range index results in a failure to match
  EXPECT_THAT(FieldRef(s.num_fields() * 2).FindAll(s), ElementsAre());

  // lookup by name returns the Indices of both matching fields
  EXPECT_THAT(FieldRef("alpha").FindAll(s), ElementsAre(FieldPath{0}, FieldPath{2}));
  EXPECT_THAT(FieldRef("beta").FindAll(s), ElementsAre(FieldPath{1}, FieldPath{3}));
}

TEST_F(TestFieldRef, FindAllForTable) {
  constexpr int kNumRows = 100;
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  auto schema = arrow::schema({f0, f1, f2, f3});

  arrow::random::RandomArrayGenerator gen_{42};
  auto a0 = gen_.ArrayOf(int32(), kNumRows);
  auto a1 = gen_.ArrayOf(int32(), kNumRows);
  auto a2 = gen_.ArrayOf(int32(), kNumRows);
  auto a3 = gen_.ArrayOf(int32(), kNumRows);

  auto table_ptr = Table::Make(schema, {a0, a1, a2, a3});
  ASSERT_OK(table_ptr->ValidateFull());

  // lookup by index returns Indices{index}
  auto schema_num_fields = table_ptr->schema()->num_fields();
  for (int index = 0; index < schema_num_fields; ++index) {
    EXPECT_THAT(FieldRef(index).FindAll(*table_ptr), ElementsAre(FieldPath{index}));
  }
  // out of range index results in a failure to match
  EXPECT_THAT(FieldRef(schema_num_fields * 2).FindAll(*table_ptr), ElementsAre());

  //// lookup by name returns the Indices of both matching fields
  EXPECT_THAT(FieldRef("alpha").FindAll(*table_ptr),
              ElementsAre(FieldPath{0}, FieldPath{2}));
  EXPECT_THAT(FieldRef("beta").FindAll(*table_ptr),
              ElementsAre(FieldPath{1}, FieldPath{3}));
}

TEST_F(TestFieldRef, FindAllForRecordBatch) {
  constexpr int kNumRows = 100;
  auto f0 = field("alpha", int32());
  auto f1 = field("beta", int32());
  auto f2 = field("alpha", int32());
  auto f3 = field("beta", int32());
  auto schema = arrow::schema({f0, f1, f2, f3});

  arrow::random::RandomArrayGenerator gen_{42};
  auto a0 = gen_.ArrayOf(int32(), kNumRows);
  auto a1 = gen_.ArrayOf(int32(), kNumRows);
  auto a2 = gen_.ArrayOf(int32(), kNumRows);
  auto a3 = gen_.ArrayOf(int32(), kNumRows);

  auto record_batch_ptr = RecordBatch::Make(schema, kNumRows, {a0, a1, a2, a3});
  ASSERT_OK(record_batch_ptr->ValidateFull());

  // lookup by index returns Indices{index}
  auto schema_num_fields = record_batch_ptr->schema()->num_fields();
  for (int index = 0; index < schema_num_fields; ++index) {
    EXPECT_THAT(FieldRef(index).FindAll(*record_batch_ptr),
                ElementsAre(FieldPath{index}));
  }
  // out of range index results in a failure to match
  EXPECT_THAT(FieldRef(schema_num_fields * 2).FindAll(*record_batch_ptr), ElementsAre());

  //// lookup by name returns the Indices of both matching fields
  EXPECT_THAT(FieldRef("alpha").FindAll(*record_batch_ptr),
              ElementsAre(FieldPath{0}, FieldPath{2}));
  EXPECT_THAT(FieldRef("beta").FindAll(*record_batch_ptr),
              ElementsAre(FieldPath{1}, FieldPath{3}));
}

TEST_F(TestFieldRef, FromDotPath) {
  ASSERT_OK_AND_EQ(FieldRef("alpha"), FieldRef::FromDotPath(R"(.alpha)"));

  ASSERT_OK_AND_EQ(FieldRef("", ""), FieldRef::FromDotPath(R"(..)"));

  ASSERT_OK_AND_EQ(FieldRef(2), FieldRef::FromDotPath(R"([2])"));

  ASSERT_OK_AND_EQ(FieldRef("beta", 3), FieldRef::FromDotPath(R"(.beta[3])"));

  ASSERT_OK_AND_EQ(FieldRef(5, "gamma", "delta", 7),
                   FieldRef::FromDotPath(R"([5].gamma.delta[7])"));

  ASSERT_OK_AND_EQ(FieldRef("hello world"), FieldRef::FromDotPath(R"(.hello world)"));

  ASSERT_OK_AND_EQ(FieldRef(R"([y]\tho.\)"), FieldRef::FromDotPath(R"(.\[y\]\\tho\.\)"));

  ASSERT_OK_AND_EQ(FieldRef(), FieldRef::FromDotPath(R"()"));

  ASSERT_RAISES(Invalid, FieldRef::FromDotPath(R"(alpha)"));
  ASSERT_RAISES(Invalid, FieldRef::FromDotPath(R"([134234)"));
  ASSERT_RAISES(Invalid, FieldRef::FromDotPath(R"([1stuf])"));
}

TEST_F(TestFieldRef, DotPathRoundTrip) {
  auto check_roundtrip = [](const FieldRef& ref) {
    auto dot_path = ref.ToDotPath();
    ASSERT_OK_AND_EQ(ref, FieldRef::FromDotPath(dot_path));
  };

  check_roundtrip(FieldRef());
  check_roundtrip(FieldRef("foo"));
  check_roundtrip(FieldRef("foo", 1, "bar", 2, 3));
  check_roundtrip(FieldRef(1, 2, 3));
  check_roundtrip(FieldRef("foo", 1, FieldRef("bar", 2, 3), FieldRef()));
}

TEST_F(TestFieldRef, Nested) {
  auto f0 = field("alpha", int32());
  auto f1_0 = field("alpha", int32());
  auto f1 = field("beta", struct_({f1_0}));
  auto f2_0 = field("alpha", int32());
  auto f2_1_0 = field("alpha", int32());
  auto f2_1_1 = field("alpha", int32());
  auto f2_1 = field("gamma", struct_({f2_1_0, f2_1_1}));
  auto f2 = field("beta", struct_({f2_0, f2_1}));
  Schema s({f0, f1, f2});

  EXPECT_THAT(FieldRef("beta", "alpha").FindAll(s),
              ElementsAre(FieldPath{1, 0}, FieldPath{2, 0}));
  EXPECT_THAT(FieldRef("beta", "gamma", "alpha").FindAll(s),
              ElementsAre(FieldPath{2, 1, 0}, FieldPath{2, 1, 1}));
}

TEST_F(TestFieldRef, Flatten) {
  FieldRef ref;

  auto assert_name = [](const FieldRef& ref, const std::string& expected) {
    ASSERT_TRUE(ref.IsName());
    ASSERT_EQ(*ref.name(), expected);
  };

  auto assert_path = [](const FieldRef& ref, const std::vector<int>& expected) {
    ASSERT_TRUE(ref.IsFieldPath());
    ASSERT_EQ(ref.field_path()->indices(), expected);
  };

  auto assert_nested = [](const FieldRef& ref, const std::vector<FieldRef>& expected) {
    ASSERT_TRUE(ref.IsNested());
    ASSERT_EQ(*ref.nested_refs(), expected);
  };

  assert_path(FieldRef(), {});
  assert_path(FieldRef(1, 2, 3), {1, 2, 3});
  // If all leaves are field paths, they are fully flattened
  assert_path(FieldRef(1, FieldRef(2, 3)), {1, 2, 3});
  assert_path(FieldRef(1, FieldRef(2, 3), FieldRef(), FieldRef(FieldRef(4), FieldRef(5))),
              {1, 2, 3, 4, 5});
  assert_path(FieldRef(FieldRef(), FieldRef(FieldRef(), FieldRef())), {});

  assert_name(FieldRef("foo"), "foo");

  // Nested empty field refs are optimized away
  assert_nested(FieldRef("foo", 1, FieldRef(), FieldRef(FieldRef(), "bar")),
                {FieldRef("foo"), FieldRef(1), FieldRef("bar")});
  // For now, subsequences of indices are not concatenated
  assert_nested(FieldRef("foo", FieldRef("bar"), FieldRef(1, 2), FieldRef(3)),
                {FieldRef("foo"), FieldRef("bar"), FieldRef(1, 2), FieldRef(3)});
}

TEST_F(TestFieldRef, GetFromSchema) { TestGet<Schema>(); }
TEST_F(TestFieldRef, GetFromDataType) { TestGet<DataType>(); }

TEST_F(TestFieldRef, GetFromArray) { TestGet<Array>(); }
TEST_F(TestFieldRef, GetFromChunkedArray) { TestGet<ChunkedArray>(); }
TEST_F(TestFieldRef, GetFromRecordBatch) { TestGet<RecordBatch>(); }
TEST_F(TestFieldRef, GetFromTable) { TestGet<Table>(); }

TEST_F(TestFieldRef, GetFlattenedFromArray) { TestGet<Array, true>(); }
TEST_F(TestFieldRef, GetFlattenedFromChunkedArray) { TestGet<ChunkedArray, true>(); }
TEST_F(TestFieldRef, GetFlattenedFromRecordBatch) { TestGet<RecordBatch, true>(); }
TEST_F(TestFieldRef, GetFlattenedFromTable) { TestGet<Table, true>(); }

}  // namespace arrow
