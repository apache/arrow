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

#include "parquet/arrow/path_internal.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"
#include "parquet/properties.h"

using arrow::Array;
using arrow::ArrayFromJSON;
using arrow::AssertArraysEqual;
using arrow::ChunkedArray;
using arrow::DataType;
using arrow::field;
using arrow::int32;
using arrow::int64;
using arrow::list;
using arrow::MemoryPool;
using arrow::Result;
using arrow::Status;
using arrow::struct_;
using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;
using arrow::io::BufferOutputStream;
using arrow::io::BufferReader;

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::Eq;
using testing::NotNull;
using testing::SizeIs;

namespace parquet {
namespace arrow {

using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

using ParquetType = parquet::Type::type;
template <ParquetType T>
using ParquetTraits = parquet::type_traits<T>;

using LevelVector = std::vector<int16_t>;
// For readability
using DefLevels = LevelVector;
using RepLevels = LevelVector;
using Int32Vector = std::vector<int32_t>;
using Int64Vector = std::vector<int64_t>;

// A Parquet file builder that allows writing values one leaf column at a time
class FileBuilder {
 public:
  static Result<std::shared_ptr<FileBuilder>> Make(const NodePtr& group_node,
                                                   int num_columns) {
    auto self = std::make_shared<FileBuilder>();
    RETURN_NOT_OK(self->Open(group_node, num_columns));
    return self;
  }

  Result<std::shared_ptr<Buffer>> Finish() {
    DCHECK_EQ(column_index_, num_columns_);
    row_group_writer_->Close();
    file_writer_->Close();
    return stream_->Finish();
  }

  // Write a leaf (primitive) column
  template <ParquetType TYPE, typename C_TYPE = typename ParquetTraits<TYPE>::value_type>
  Status WriteColumn(const LevelVector& def_levels, const LevelVector& rep_levels,
                     const std::vector<C_TYPE>& values) {
    auto column_writer = row_group_writer_->NextColumn();
    auto column_descr = column_writer->descr();
    const int16_t max_def_level = column_descr->max_definition_level();
    const int16_t max_rep_level = column_descr->max_repetition_level();
    CheckTestedLevels(def_levels, max_def_level);
    CheckTestedLevels(rep_levels, max_rep_level);

    auto typed_writer =
        checked_cast<TypedColumnWriter<PhysicalType<TYPE>>*>(column_writer);

    const int64_t num_values =
        static_cast<int64_t>((max_def_level > 0)   ? def_levels.size()
                             : (max_rep_level > 0) ? rep_levels.size()
                                                   : values.size());
    const int64_t values_written = typed_writer->WriteBatch(
        num_values, LevelPointerOrNull(def_levels, max_def_level),
        LevelPointerOrNull(rep_levels, max_rep_level), values.data());
    DCHECK_EQ(values_written, static_cast<int64_t>(values.size()));  // Sanity check

    column_writer->Close();
    ++column_index_;
    return Status::OK();
  }

 protected:
  Status Open(const NodePtr& group_node, int num_columns) {
    ARROW_ASSIGN_OR_RAISE(stream_, BufferOutputStream::Create());
    file_writer_ =
        ParquetFileWriter::Open(stream_, checked_pointer_cast<GroupNode>(group_node));
    row_group_writer_ = file_writer_->AppendRowGroup();
    num_columns_ = num_columns;
    column_index_ = 0;
    return Status::OK();
  }

  void CheckTestedLevels(const LevelVector& levels, int16_t max_level) {
    // Tests are expected to exercise all possible levels in [0, max_level]
    if (!levels.empty()) {
      const int16_t max_seen_level = *std::max_element(levels.begin(), levels.end());
      DCHECK_EQ(max_seen_level, max_level);
    }
  }

  const int16_t* LevelPointerOrNull(const LevelVector& levels, int16_t max_level) {
    if (max_level > 0) {
      DCHECK_GT(levels.size(), 0);
      return levels.data();
    } else {
      DCHECK_EQ(levels.size(), 0);
      return nullptr;
    }
  }

  std::shared_ptr<BufferOutputStream> stream_;
  std::unique_ptr<ParquetFileWriter> file_writer_;
  RowGroupWriter* row_group_writer_;
  int num_columns_;
  int column_index_;
};

// A Parquet file tester that allows reading Arrow columns, corresponding to
// children of the top-level group node.
class FileTester {
 public:
  static Result<std::shared_ptr<FileTester>> Make(std::shared_ptr<Buffer> buffer,
                                                  MemoryPool* pool) {
    auto self = std::make_shared<FileTester>();
    RETURN_NOT_OK(self->Open(buffer, pool));
    return self;
  }

  Result<std::shared_ptr<Array>> ReadColumn(int column_index) {
    std::shared_ptr<ChunkedArray> column;
    RETURN_NOT_OK(file_reader_->ReadColumn(column_index, &column));
    return ::arrow::Concatenate(column->chunks(), pool_);
  }

  void CheckColumn(int column_index, const Array& expected) {
    ASSERT_OK_AND_ASSIGN(const auto actual, ReadColumn(column_index));
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(expected, *actual, /*verbose=*/true);
  }

 protected:
  Status Open(std::shared_ptr<Buffer> buffer, MemoryPool* pool) {
    pool_ = pool;
    return OpenFile(std::make_shared<BufferReader>(buffer), pool_, &file_reader_);
  }

  MemoryPool* pool_;
  std::unique_ptr<FileReader> file_reader_;
};

class TestReconstructColumn : public testing::Test {
 public:
  void SetUp() override { pool_ = ::arrow::default_memory_pool(); }

  // Write the next leaf (primitive) column
  template <ParquetType TYPE, typename C_TYPE = typename ParquetTraits<TYPE>::value_type>
  Status WriteColumn(const LevelVector& def_levels, const LevelVector& rep_levels,
                     const std::vector<C_TYPE>& values) {
    if (!builder_) {
      ARROW_ASSIGN_OR_RAISE(builder_,
                            FileBuilder::Make(group_node_, descriptor_->num_columns()));
    }
    return builder_->WriteColumn<TYPE, C_TYPE>(def_levels, rep_levels, values);
  }

  template <typename C_TYPE>
  Status WriteInt32Column(const LevelVector& def_levels, const LevelVector& rep_levels,
                          const std::vector<C_TYPE>& values) {
    return WriteColumn<ParquetType::INT32>(def_levels, rep_levels, values);
  }

  template <typename C_TYPE>
  Status WriteInt64Column(const LevelVector& def_levels, const LevelVector& rep_levels,
                          const std::vector<C_TYPE>& values) {
    return WriteColumn<ParquetType::INT64>(def_levels, rep_levels, values);
  }

  // Read a Arrow column and check its values
  void CheckColumn(int column_index, const Array& expected) {
    if (!tester_) {
      ASSERT_OK_AND_ASSIGN(auto buffer, builder_->Finish());
      ASSERT_OK_AND_ASSIGN(tester_, FileTester::Make(buffer, pool_));
    }
    tester_->CheckColumn(column_index, expected);
  }

  void CheckColumn(const Array& expected) { CheckColumn(/*column_index=*/0, expected); }

  // One-column shortcut
  template <ParquetType TYPE, typename C_TYPE = typename ParquetTraits<TYPE>::value_type>
  void AssertReconstruct(const Array& expected, const LevelVector& def_levels,
                         const LevelVector& rep_levels,
                         const std::vector<C_TYPE>& values) {
    ASSERT_OK((WriteColumn<TYPE, C_TYPE>(def_levels, rep_levels, values)));
    CheckColumn(/*column_index=*/0, expected);
  }

  ::arrow::Status MaybeSetParquetSchema(const NodePtr& column) {
    descriptor_.reset(new SchemaDescriptor());
    manifest_.reset(new SchemaManifest());
    group_node_ = GroupNode::Make("root", Repetition::REQUIRED, {column});
    descriptor_->Init(group_node_);
    return SchemaManifest::Make(descriptor_.get(),
                                std::shared_ptr<const ::arrow::KeyValueMetadata>(),
                                ArrowReaderProperties(), manifest_.get());
  }

  void SetParquetSchema(const NodePtr& column) {
    ASSERT_OK(MaybeSetParquetSchema(column));
  }

 protected:
  MemoryPool* pool_;
  NodePtr group_node_;
  std::unique_ptr<SchemaDescriptor> descriptor_;
  std::unique_ptr<SchemaManifest> manifest_;

  std::shared_ptr<FileBuilder> builder_;
  std::shared_ptr<FileTester> tester_;
};

static std::shared_ptr<DataType> OneFieldStruct(const std::string& name,
                                                std::shared_ptr<DataType> type,
                                                bool nullable = true) {
  return struct_({field(name, type, nullable)});
}

static std::shared_ptr<DataType> List(std::shared_ptr<DataType> type,
                                      bool nullable = true) {
  // TODO should field name "element" (Parquet convention for List nodes)
  // be changed to "item" (Arrow convention for List types)?
  return list(field("element", type, nullable));
}

//
// Primitive columns with no intermediate group node
//

TEST_F(TestReconstructColumn, PrimitiveOptional) {
  SetParquetSchema(
      PrimitiveNode::Make("node_name", Repetition::OPTIONAL, ParquetType::INT32));

  LevelVector def_levels = {1, 0, 1, 1};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(int32(), "[4, null, 5, 6]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, PrimitiveRequired) {
  SetParquetSchema(
      PrimitiveNode::Make("node_name", Repetition::REQUIRED, ParquetType::INT32));

  LevelVector def_levels = {};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(int32(), "[4, 5, 6]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, PrimitiveRepeated) {
  // Arrow schema: list(int32 not null) not null
  this->SetParquetSchema(
      PrimitiveNode::Make("node_name", Repetition::REPEATED, ParquetType::INT32));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(list(field("node_name", int32(), /*nullable=*/false)),
                                "[[], [4, 5], [6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// Struct encodings (one field each)
//

TEST_F(TestReconstructColumn, NestedRequiredRequired) {
  // Arrow schema: struct(a: int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32)}));

  LevelVector def_levels = {};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32(), false),
                                R"([{"a": 4}, {"a": 5}, {"a": 6}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedOptionalRequired) {
  // Arrow schema: struct(a: int32 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32)}));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32(), false),
                                R"([null, {"a": 4}, {"a": 5}, {"a": 6}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedRequiredOptional) {
  // Arrow schema: struct(a: int32) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32)}));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32()),
                                R"([{"a": null}, {"a": 4}, {"a": 5}, {"a": 6}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedOptionalOptional) {
  // Arrow schema: struct(a: int32)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32)}));

  LevelVector def_levels = {0, 1, 2, 2};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5};

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32()),
                                R"([null, {"a": null}, {"a": 4}, {"a": 5}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// Nested struct encodings (one field each)
//

TEST_F(TestReconstructColumn, NestedRequiredRequiredRequired) {
  // Arrow schema: struct(a: struct(b: int32 not null) not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "a", Repetition::REQUIRED,
          {PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT32)})}));

  LevelVector def_levels = {};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected =
      ArrayFromJSON(OneFieldStruct("a", OneFieldStruct("b", int32(), false), false),
                    R"([{"a": {"b": 4}},
                        {"a": {"b": 5}},
                        {"a": {"b": 6}}
                        ])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedRequiredOptionalRequired) {
  // Arrow schema: struct(a: struct(b: int32 not null)) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "a", Repetition::OPTIONAL,
          {PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT32)})}));

  LevelVector def_levels = {1, 0, 1, 1};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", OneFieldStruct("b", int32(), false)),
                                R"([{"a": {"b": 4}},
                                    {"a": null},
                                    {"a": {"b": 5}},
                                    {"a": {"b": 6}}
                                    ])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedOptionalRequiredOptional) {
  // Arrow schema: struct(a: struct(b: int32) not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "a", Repetition::REQUIRED,
          {PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT32)})}));

  LevelVector def_levels = {1, 2, 0, 2, 2};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", OneFieldStruct("b", int32()), false),
                                R"([{"a": {"b": null}},
                                    {"a": {"b": 4}},
                                    null,
                                    {"a": {"b": 5}},
                                    {"a": {"b": 6}}
                                    ])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedOptionalOptionalOptional) {
  // Arrow schema: struct(a: struct(b: int32) not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "a", Repetition::OPTIONAL,
          {PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT32)})}));

  LevelVector def_levels = {1, 2, 0, 3, 3, 3};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", OneFieldStruct("b", int32())),
                                R"([{"a": null},
                                    {"a": {"b": null}},
                                    null,
                                    {"a": {"b": 4}},
                                    {"a": {"b": 5}},
                                    {"a": {"b": 6}}
                                    ])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// Struct encodings (two fields)
//

TEST_F(TestReconstructColumn, NestedTwoFields1) {
  // Arrow schema: struct(a: int32 not null, b: int64 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)}));

  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  ASSERT_OK(WriteInt64Column(DefLevels{}, RepLevels{}, Int64Vector{7, 8, 9}));

  auto type = struct_(
      {field("a", int32(), /*nullable=*/false), field("b", int64(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, R"([{"a": 4, "b": 7},
                                          {"a": 5, "b": 8},
                                          {"a": 6, "b": 9}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields2) {
  // Arrow schema: struct(a: int32 not null, b: int64) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT64)}));

  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 1}, RepLevels{}, Int64Vector{7, 8}));

  auto type = struct_({field("a", int32(), /*nullable=*/false), field("b", int64())});
  auto expected = ArrayFromJSON(type, R"([{"a": 4, "b": null},
                                          {"a": 5, "b": 7},
                                          {"a": 6, "b": 8}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields3) {
  // Arrow schema: struct(a: int32 not null, b: int64 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)}));

  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 1}, RepLevels{}, Int32Vector{4, 5}));
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 1}, RepLevels{}, Int64Vector{7, 8}));

  auto type = struct_(
      {field("a", int32(), /*nullable=*/false), field("b", int64(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, R"([null,
                                         {"a": 4, "b": 7},
                                         {"a": 5, "b": 8}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields4) {
  // Arrow schema: struct(a: int32, b: int64 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)}));

  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2}, RepLevels{}, Int32Vector{4}));
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 1}, RepLevels{}, Int64Vector{7, 8}));

  auto type = struct_({field("a", int32()), field("b", int64(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, R"([null,
                                         {"a": null, "b": 7},
                                         {"a": 4, "b": 8}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields5) {
  // Arrow schema: struct(a: int32, b: int64)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT64)}));

  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2}, RepLevels{}, Int32Vector{4}));
  ASSERT_OK(WriteInt64Column(DefLevels{0, 2, 1}, RepLevels{}, Int64Vector{7}));

  auto type = struct_({field("a", int32()), field("b", int64())});
  auto expected = ArrayFromJSON(type, R"([null,
                                         {"a": null, "b": 7},
                                         {"a": 4, "b": null}])");

  CheckColumn(/*column_index=*/0, *expected);
}

//
// Nested struct encodings (two fields)
//

TEST_F(TestReconstructColumn, NestedNestedTwoFields1) {
  // Arrow schema: struct(a: struct(aa: int32 not null,
  //                                ab: int64 not null) not null,
  //                      b: int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
           "a", Repetition::REQUIRED,
           {PrimitiveNode::Make("aa", Repetition::REQUIRED, ParquetType::INT32),
            PrimitiveNode::Make("ab", Repetition::REQUIRED, ParquetType::INT64)}),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT32)}));

  // aa
  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  // ab
  ASSERT_OK(WriteInt64Column(DefLevels{}, RepLevels{}, Int64Vector{7, 8, 9}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{10, 11, 12}));

  auto type = struct_({field("a",
                             struct_({field("aa", int32(), /*nullable=*/false),
                                      field("ab", int64(), /*nullable=*/false)}),
                             /*nullable=*/false),
                       field("b", int32(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, R"([{"a": {"aa": 4, "ab": 7}, "b": 10},
                                          {"a": {"aa": 5, "ab": 8}, "b": 11},
                                          {"a": {"aa": 6, "ab": 9}, "b": 12}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedNestedTwoFields2) {
  // Arrow schema: struct(a: struct(aa: int32,
  //                                ab: int64 not null) not null,
  //                      b: int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
           "a", Repetition::REQUIRED,
           {PrimitiveNode::Make("aa", Repetition::OPTIONAL, ParquetType::INT32),
            PrimitiveNode::Make("ab", Repetition::REQUIRED, ParquetType::INT64)}),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT32)}));

  // aa
  ASSERT_OK(WriteInt32Column(DefLevels{1, 0, 1}, RepLevels{}, Int32Vector{4, 5}));
  // ab
  ASSERT_OK(WriteInt64Column(DefLevels{}, RepLevels{}, Int64Vector{7, 8, 9}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{10, 11, 12}));

  auto type = struct_(
      {field("a",
             struct_({field("aa", int32()), field("ab", int64(), /*nullable=*/false)}),
             /*nullable=*/false),
       field("b", int32(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, R"([{"a": {"aa": 4, "ab": 7}, "b": 10},
                                          {"a": {"aa": null, "ab": 8}, "b": 11},
                                          {"a": {"aa": 5, "ab": 9}, "b": 12}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedNestedTwoFields3) {
  // Arrow schema: struct(a: struct(aa: int32 not null,
  //                                ab: int64) not null,
  //                      b: int32) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
           "a", Repetition::REQUIRED,
           {PrimitiveNode::Make("aa", Repetition::REQUIRED, ParquetType::INT32),
            PrimitiveNode::Make("ab", Repetition::OPTIONAL, ParquetType::INT64)}),
       PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT32)}));

  // aa
  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  // ab
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 1}, RepLevels{}, Int64Vector{7, 8}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{1, 0, 1}, RepLevels{}, Int32Vector{10, 11}));

  auto type = struct_(
      {field("a",
             struct_({field("aa", int32(), /*nullable=*/false), field("ab", int64())}),
             /*nullable=*/false),
       field("b", int32())});
  auto expected = ArrayFromJSON(type, R"([{"a": {"aa": 4, "ab": null}, "b": 10},
                                          {"a": {"aa": 5, "ab": 7}, "b": null},
                                          {"a": {"aa": 6, "ab": 8}, "b": 11}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedNestedTwoFields4) {
  // Arrow schema: struct(a: struct(aa: int32 not null,
  //                                ab: int64),
  //                      b: int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
           "a", Repetition::OPTIONAL,
           {PrimitiveNode::Make("aa", Repetition::REQUIRED, ParquetType::INT32),
            PrimitiveNode::Make("ab", Repetition::OPTIONAL, ParquetType::INT64)}),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT32)}));

  // aa
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 1}, RepLevels{}, Int32Vector{4, 5}));
  // ab
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 2}, RepLevels{}, Int64Vector{7}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{}, RepLevels{}, Int32Vector{10, 11, 12}));

  auto type = struct_({field("a", struct_({field("aa", int32(), /*nullable=*/false),
                                           field("ab", int64())})),
                       field("b", int32(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, R"([{"a": null, "b": 10},
                                          {"a": {"aa": 4, "ab": null}, "b": 11},
                                          {"a": {"aa": 5, "ab": 7}, "b": 12}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedNestedTwoFields5) {
  // Arrow schema: struct(a: struct(aa: int32 not null,
  //                                ab: int64) not null,
  //                      b: int32)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
           "a", Repetition::REQUIRED,
           {PrimitiveNode::Make("aa", Repetition::REQUIRED, ParquetType::INT32),
            PrimitiveNode::Make("ab", Repetition::OPTIONAL, ParquetType::INT64)}),
       PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT32)}));

  // aa
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 1}, RepLevels{}, Int32Vector{4, 5}));
  // ab
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 2}, RepLevels{}, Int64Vector{7}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 2, 1}, RepLevels{}, Int32Vector{10}));

  auto type = struct_(
      {field("a",
             struct_({field("aa", int32(), /*nullable=*/false), field("ab", int64())}),
             /*nullable=*/false),
       field("b", int32())});
  auto expected = ArrayFromJSON(type, R"([null,
                                          {"a": {"aa": 4, "ab": null}, "b": 10},
                                          {"a": {"aa": 5, "ab": 7}, "b": null}])");

  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedNestedTwoFields6) {
  // Arrow schema: struct(a: struct(aa: int32 not null,
  //                                ab: int64),
  //                      b: int32)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
           "a", Repetition::OPTIONAL,
           {PrimitiveNode::Make("aa", Repetition::REQUIRED, ParquetType::INT32),
            PrimitiveNode::Make("ab", Repetition::OPTIONAL, ParquetType::INT64)}),
       PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT32)}));

  // aa
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2, 2}, RepLevels{}, Int32Vector{4, 5}));
  // ab
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 2, 3}, RepLevels{}, Int64Vector{7}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 2, 1, 2}, RepLevels{}, Int32Vector{10, 11}));

  auto type = struct_({field("a", struct_({field("aa", int32(), /*nullable=*/false),
                                           field("ab", int64())})),
                       field("b", int32())});
  auto expected = ArrayFromJSON(type, R"([null,
                                          {"a": null, "b": 10},
                                          {"a": {"aa": 4, "ab": null}, "b": null},
                                          {"a": {"aa": 5, "ab": 7}, "b": 11}])");

  CheckColumn(/*column_index=*/0, *expected);
}

//
// Three-level list encodings
//

TEST_F(TestReconstructColumn, ThreeLevelListRequiredRequired) {
  // Arrow schema: list(int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  // TODO should field name "element" (Parquet convention for List nodes)
  // be changed to "item" (Arrow convention for List types)?
  auto expected = ArrayFromJSON(List(int32(), /*nullable=*/false), "[[], [4, 5], [6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ThreeLevelListOptionalRequired) {
  // Arrow schema: list(int32 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected =
      ArrayFromJSON(List(int32(), /*nullable=*/false), "[null, [], [4, 5], [6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ThreeLevelListRequiredOptional) {
  // Arrow schema: list(int32) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::OPTIONAL, ParquetType::INT32)})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 1, 0, 1};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(List(int32()), "[[], [null, 4], [5, 6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ThreeLevelListOptionalOptional) {
  // Arrow schema: list(int32)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::OPTIONAL, ParquetType::INT32)})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 3, 3};
  LevelVector rep_levels = {0, 0, 0, 1, 0, 1};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(List(int32()), "[null, [], [null, 4], [5, 6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// Legacy list encodings
//

TEST_F(TestReconstructColumn, TwoLevelListRequired) {
  // Arrow schema: list(int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("element", Repetition::REPEATED, ParquetType::INT32)},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  // TODO should field name "element" (Parquet convention for List nodes)
  // be changed to "item" (Arrow convention for List types)?
  auto expected = ArrayFromJSON(List(int32(), /*nullable=*/false), "[[], [4, 5], [6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, TwoLevelListOptional) {
  // Arrow schema: list(int32 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("element", Repetition::REPEATED, ParquetType::INT32)},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected =
      ArrayFromJSON(List(int32(), /*nullable=*/false), "[null, [], [4, 5], [6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// List-in-struct
//

TEST_F(TestReconstructColumn, NestedList1) {
  // Arrow schema: struct(a: list(int32 not null) not null) not null
  SetParquetSchema(GroupNode::Make(
      "a", Repetition::REQUIRED,
      {GroupNode::Make(
          "p", Repetition::REQUIRED,
          {GroupNode::Make("list", Repetition::REPEATED,
                           {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                ParquetType::INT32)})},
          LogicalType::List())}));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = OneFieldStruct("p", List(int32(), /*nullable=*/false),
                             /*nullable=*/false);
  auto expected = ArrayFromJSON(type, R"([{"p": []},
                                          {"p": [4, 5]},
                                          {"p": [6]}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedList2) {
  // Arrow schema: struct(a: list(int32 not null) not null)
  SetParquetSchema(GroupNode::Make(
      "a", Repetition::OPTIONAL,
      {GroupNode::Make(
          "p", Repetition::REQUIRED,
          {GroupNode::Make("list", Repetition::REPEATED,
                           {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                ParquetType::INT32)})},
          LogicalType::List())}));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = OneFieldStruct("p", List(int32(), /*nullable=*/false),
                             /*nullable=*/false);
  auto expected = ArrayFromJSON(type, R"([null,
                                          {"p": []},
                                          {"p": [4, 5]},
                                          {"p": [6]}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedList3) {
  // Arrow schema: struct(a: list(int32 not null)) not null
  SetParquetSchema(GroupNode::Make(
      "a", Repetition::REQUIRED,  // column name (column a is a struct of)
      {GroupNode::Make(
          "p", Repetition::OPTIONAL,  // name in struct
          {GroupNode::Make("list", Repetition::REPEATED,
                           {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                ParquetType::INT32)})},
          LogicalType::List())}));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = OneFieldStruct("p", List(int32(), /*nullable=*/false));
  auto expected = ArrayFromJSON(type, R"([{"p": null},
                                          {"p": []},
                                          {"p": [4, 5]},
                                          {"p": [6]}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedList4) {
  // Arrow schema: struct(a: list(int32 not null))
  SetParquetSchema(GroupNode::Make(
      "a", Repetition::OPTIONAL,
      {GroupNode::Make(
          "p", Repetition::OPTIONAL,
          {GroupNode::Make("list", Repetition::REPEATED,
                           {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                ParquetType::INT32)})},
          LogicalType::List())}));

  LevelVector def_levels = {0, 1, 2, 3, 3, 3};
  LevelVector rep_levels = {0, 0, 0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = OneFieldStruct("p", List(int32(), /*nullable=*/false));
  auto expected = ArrayFromJSON(type, R"([null,
                                          {"p": null},
                                          {"p": []},
                                          {"p": [4, 5]},
                                          {"p": [6]}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedList5) {
  // Arrow schema: struct(a: list(int32) not null)
  SetParquetSchema(GroupNode::Make(
      "a", Repetition::OPTIONAL,
      {GroupNode::Make(
          "p", Repetition::REQUIRED,
          {GroupNode::Make("list", Repetition::REPEATED,
                           {PrimitiveNode::Make("element", Repetition::OPTIONAL,
                                                ParquetType::INT32)})},
          LogicalType::List())}));

  LevelVector def_levels = {0, 1, 3, 2, 3, 3};
  LevelVector rep_levels = {0, 0, 0, 1, 0, 1};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = OneFieldStruct("p", List(int32()), /*nullable=*/false);
  auto expected = ArrayFromJSON(type, R"([null,
                                          {"p": []},
                                          {"p": [4, null]},
                                          {"p": [5, 6]}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, NestedList6) {
  // Arrow schema: struct(a: list(int32))
  SetParquetSchema(GroupNode::Make(
      "a", Repetition::OPTIONAL,
      {GroupNode::Make(
          "p", Repetition::OPTIONAL,
          {GroupNode::Make("list", Repetition::REPEATED,
                           {PrimitiveNode::Make("element", Repetition::OPTIONAL,
                                                ParquetType::INT32)})},
          LogicalType::List())}));

  LevelVector def_levels = {0, 1, 2, 4, 3, 4, 4};
  LevelVector rep_levels = {0, 0, 0, 0, 1, 0, 1};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = OneFieldStruct("p", List(int32()));
  auto expected = ArrayFromJSON(type, R"([null,
                                          {"p": null},
                                          {"p": []},
                                          {"p": [4, null]},
                                          {"p": [5, 6]}])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// Struct-in-list
//

TEST_F(TestReconstructColumn, ListNested1) {
  // Arrow schema: list(struct(a: int32 not null) not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make("list", Repetition::REPEATED,
                       {GroupNode::Make("element", Repetition::REQUIRED,
                                        {PrimitiveNode::Make("a", Repetition::REQUIRED,
                                                             ParquetType::INT32)})})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 1, 1};
  LevelVector rep_levels = {0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(OneFieldStruct("a", int32(), /*nullable=*/false),
                   /*nullable=*/false);
  auto expected = ArrayFromJSON(type,
                                R"([[],
                                    [{"a": 4}, {"a": 5}],
                                    [{"a": 6}]])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListNested2) {
  // Arrow schema: list(struct(a: int32 not null) not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make("list", Repetition::REPEATED,
                       {GroupNode::Make("element", Repetition::REQUIRED,
                                        {PrimitiveNode::Make("a", Repetition::REQUIRED,
                                                             ParquetType::INT32)})})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 0, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(OneFieldStruct("a", int32(), /*nullable=*/false),
                   /*nullable=*/false);
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [{"a": 4}, {"a": 5}],
                                    [{"a": 6}]])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListNested3) {
  // Arrow schema: list(struct(a: int32 not null)) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make("list", Repetition::REPEATED,
                       {GroupNode::Make("element", Repetition::OPTIONAL,
                                        {PrimitiveNode::Make("a", Repetition::REQUIRED,
                                                             ParquetType::INT32)})})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 1, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(OneFieldStruct("a", int32(), /*nullable=*/false));
  auto expected = ArrayFromJSON(type,
                                R"([[],
                                    [null, {"a": 4}, {"a": 5}],
                                    [{"a": 6}]])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListNested4) {
  // Arrow schema: list(struct(a: int32 not null))
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make("list", Repetition::REPEATED,
                       {GroupNode::Make("element", Repetition::OPTIONAL,
                                        {PrimitiveNode::Make("a", Repetition::REQUIRED,
                                                             ParquetType::INT32)})})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 3, 3};
  LevelVector rep_levels = {0, 0, 0, 1, 1, 0};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(OneFieldStruct("a", int32(), /*nullable=*/false));
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [null, {"a": 4}, {"a": 5}],
                                    [{"a": 6}]])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListNested5) {
  // Arrow schema: list(struct(a: int32) not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make("list", Repetition::REPEATED,
                       {GroupNode::Make("element", Repetition::REQUIRED,
                                        {PrimitiveNode::Make("a", Repetition::OPTIONAL,
                                                             ParquetType::INT32)})})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 3, 3};
  LevelVector rep_levels = {0, 0, 0, 1, 0, 1};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(OneFieldStruct("a", int32()),
                   /*nullable=*/false);
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [{"a": null}, {"a": 4}],
                                    [{"a": 5}, {"a": 6}]])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListNested6) {
  // Arrow schema: list(struct(a: int32))
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make("list", Repetition::REPEATED,
                       {GroupNode::Make("element", Repetition::OPTIONAL,
                                        {PrimitiveNode::Make("a", Repetition::OPTIONAL,
                                                             ParquetType::INT32)})})},
      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 4, 4, 4};
  LevelVector rep_levels = {0, 0, 0, 1, 1, 0, 1};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(OneFieldStruct("a", int32()));
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [null, {"a": null}, {"a": 4}],
                                    [{"a": 5}, {"a": 6}]])");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

//
// Struct (two fields)-in-list
//

TEST_F(TestReconstructColumn, ListNestedTwoFields1) {
  // Arrow schema: list(struct(a: int32 not null,
  //                           b: int64 not null) not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {GroupNode::Make(
              "element", Repetition::REQUIRED,
              {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
               PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)})})},
      LogicalType::List()));

  // a
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 1, 1}, RepLevels{0, 0, 1, 0},
                             Int32Vector{4, 5, 6}));
  // b
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 1, 1}, RepLevels{0, 0, 1, 0},
                             Int64Vector{7, 8, 9}));

  auto type = List(struct_({field("a", int32(), /*nullable=*/false),
                            field("b", int64(), /*nullable=*/false)}),
                   /*nullable=*/false);
  auto expected = ArrayFromJSON(type,
                                R"([[],
                                    [{"a": 4, "b": 7}, {"a": 5, "b": 8}],
                                    [{"a": 6, "b": 9}]])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, ListNestedTwoFields2) {
  // Arrow schema: list(struct(a: int32,
  //                           b: int64 not null) not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {GroupNode::Make(
              "element", Repetition::REQUIRED,
              {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
               PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)})})},
      LogicalType::List()));

  // a
  ASSERT_OK(
      WriteInt32Column(DefLevels{0, 2, 1, 2}, RepLevels{0, 0, 1, 0}, Int32Vector{4, 5}));
  // b
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 1, 1}, RepLevels{0, 0, 1, 0},
                             Int64Vector{7, 8, 9}));

  auto type =
      List(struct_({field("a", int32()), field("b", int64(), /*nullable=*/false)}),
           /*nullable=*/false);
  auto expected = ArrayFromJSON(type,
                                R"([[],
                                    [{"a": 4, "b": 7}, {"a": null, "b": 8}],
                                    [{"a": 5, "b": 9}]])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, ListNestedTwoFields3) {
  // Arrow schema: list(struct(a: int32 not null,
  //                           b: int64 not null)) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {GroupNode::Make(
              "element", Repetition::OPTIONAL,
              {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
               PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)})})},
      LogicalType::List()));

  // a
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2, 2, 2}, RepLevels{0, 0, 1, 1, 0},
                             Int32Vector{4, 5, 6}));
  // b
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 2, 2, 2}, RepLevels{0, 0, 1, 1, 0},
                             Int64Vector{7, 8, 9}));

  auto type = List(struct_({field("a", int32(), /*nullable=*/false),
                            field("b", int64(), /*nullable=*/false)}));
  auto expected = ArrayFromJSON(type,
                                R"([[],
                                    [null, {"a": 4, "b": 7}, {"a": 5, "b": 8}],
                                    [{"a": 6, "b": 9}]])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, ListNestedTwoFields4) {
  // Arrow schema: list(struct(a: int32,
  //                           b: int64 not null) not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {GroupNode::Make(
              "element", Repetition::REQUIRED,
              {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
               PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)})})},
      LogicalType::List()));

  // a
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 3, 2, 3}, RepLevels{0, 0, 0, 1, 0},
                             Int32Vector{4, 5}));
  // b
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 2, 2, 2}, RepLevels{0, 0, 0, 1, 0},
                             Int64Vector{7, 8, 9}));

  auto type =
      List(struct_({field("a", int32()), field("b", int64(), /*nullable=*/false)}),
           /*nullable=*/false);
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [{"a": 4, "b": 7}, {"a": null, "b": 8}],
                                    [{"a": 5, "b": 9}]])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, ListNestedTwoFields5) {
  // Arrow schema: list(struct(a: int32,
  //                           b: int64 not null))
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {GroupNode::Make(
              "element", Repetition::OPTIONAL,
              {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
               PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)})})},
      LogicalType::List()));

  // a
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 4, 2, 3}, RepLevels{0, 0, 0, 1, 0},
                             Int32Vector{4}));
  // b
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 3, 2, 3}, RepLevels{0, 0, 0, 1, 0},
                             Int64Vector{7, 8}));

  auto type =
      List(struct_({field("a", int32()), field("b", int64(), /*nullable=*/false)}));
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [{"a": 4, "b": 7}, null],
                                    [{"a": null, "b": 8}]])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, ListNestedTwoFields6) {
  // Arrow schema: list(struct(a: int32,
  //                           b: int64))
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {GroupNode::Make(
              "element", Repetition::OPTIONAL,
              {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
               PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT64)})})},
      LogicalType::List()));

  // a
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 4, 2, 3}, RepLevels{0, 0, 0, 1, 0},
                             Int32Vector{4}));
  // b
  ASSERT_OK(WriteInt64Column(DefLevels{0, 1, 3, 2, 4}, RepLevels{0, 0, 0, 1, 0},
                             Int64Vector{7}));

  auto type = List(struct_({field("a", int32()), field("b", int64())}));
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    [],
                                    [{"a": 4, "b": null}, null],
                                    [{"a": null, "b": 7}]])");
  CheckColumn(/*column_index=*/0, *expected);
}

//
// List-in-struct (two fields)
//

TEST_F(TestReconstructColumn, NestedTwoFieldsList1) {
  // Arrow schema: struct(a: int64 not null,
  //                      b: list(int32 not null) not null
  //                     ) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT64),
       GroupNode::Make(
           "b", Repetition::REQUIRED,
           {GroupNode::Make("list", Repetition::REPEATED,
                            {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                 ParquetType::INT32)})},
           LogicalType::List())}));

  // a
  ASSERT_OK(WriteInt64Column(DefLevels{}, RepLevels{}, Int64Vector{4, 5, 6}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 1, 1}, RepLevels{0, 0, 1, 0},
                             Int32Vector{7, 8, 9}));

  auto type =
      struct_({field("a", int64(), /*nullable=*/false),
               field("b", List(int32(), /*nullable=*/false), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type,
                                R"([{"a": 4, "b": []},
                                    {"a": 5, "b": [7, 8]},
                                    {"a": 6, "b": [9]}])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFieldsList2) {
  // Arrow schema: struct(a: int64 not null,
  //                      b: list(int32 not null)
  //                     ) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT64),
       GroupNode::Make(
           "b", Repetition::OPTIONAL,
           {GroupNode::Make("list", Repetition::REPEATED,
                            {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                 ParquetType::INT32)})},
           LogicalType::List())}));

  // a
  ASSERT_OK(WriteInt64Column(DefLevels{}, RepLevels{}, Int64Vector{3, 4, 5, 6}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2, 2, 2}, RepLevels{0, 0, 0, 1, 0},
                             Int32Vector{7, 8, 9}));

  auto type = struct_({field("a", int64(), /*nullable=*/false),
                       field("b", List(int32(), /*nullable=*/false))});
  auto expected = ArrayFromJSON(type,
                                R"([{"a": 3, "b": null},
                                    {"a": 4, "b": []},
                                    {"a": 5, "b": [7, 8]},
                                    {"a": 6, "b": [9]}])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFieldsList3) {
  // Arrow schema: struct(a: int64,
  //                      b: list(int32 not null)
  //                     ) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT64),
       GroupNode::Make(
           "b", Repetition::OPTIONAL,
           {GroupNode::Make("list", Repetition::REPEATED,
                            {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                 ParquetType::INT32)})},
           LogicalType::List())}));

  // a
  ASSERT_OK(WriteInt64Column(DefLevels{1, 1, 0, 1}, RepLevels{}, Int64Vector{4, 5, 6}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2, 2, 2}, RepLevels{0, 0, 0, 1, 0},
                             Int32Vector{7, 8, 9}));

  auto type =
      struct_({field("a", int64()), field("b", List(int32(), /*nullable=*/false))});
  auto expected = ArrayFromJSON(type,
                                R"([{"a": 4, "b": null},
                                    {"a": 5, "b": []},
                                    {"a": null, "b": [7, 8]},
                                    {"a": 6, "b": [9]}])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFieldsList4) {
  // Arrow schema: struct(a: int64,
  //                      b: list(int32 not null)
  //                     )
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT64),
       GroupNode::Make(
           "b", Repetition::OPTIONAL,
           {GroupNode::Make("list", Repetition::REPEATED,
                            {PrimitiveNode::Make("element", Repetition::REQUIRED,
                                                 ParquetType::INT32)})},
           LogicalType::List())}));

  // a
  ASSERT_OK(
      WriteInt64Column(DefLevels{0, 2, 2, 1, 2}, RepLevels{}, Int64Vector{4, 5, 6}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2, 3, 3, 3}, RepLevels{0, 0, 0, 0, 1, 0},
                             Int32Vector{7, 8, 9}));

  auto type =
      struct_({field("a", int64()), field("b", List(int32(), /*nullable=*/false))});
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    {"a": 4, "b": null},
                                    {"a": 5, "b": []},
                                    {"a": null, "b": [7, 8]},
                                    {"a": 6, "b": [9]}])");
  CheckColumn(/*column_index=*/0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFieldsList5) {
  // Arrow schema: struct(a: int64, b: list(int32))
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT64),
       GroupNode::Make(
           "b", Repetition::OPTIONAL,
           {GroupNode::Make("list", Repetition::REPEATED,
                            {PrimitiveNode::Make("element", Repetition::OPTIONAL,
                                                 ParquetType::INT32)})},
           LogicalType::List())}));

  // a
  ASSERT_OK(
      WriteInt64Column(DefLevels{0, 2, 2, 1, 2}, RepLevels{}, Int64Vector{4, 5, 6}));
  // b
  ASSERT_OK(WriteInt32Column(DefLevels{0, 1, 2, 4, 3, 4}, RepLevels{0, 0, 0, 0, 1, 0},
                             Int32Vector{7, 8}));

  auto type = struct_({field("a", int64()), field("b", List(int32()))});
  auto expected = ArrayFromJSON(type,
                                R"([null,
                                    {"a": 4, "b": null},
                                    {"a": 5, "b": []},
                                    {"a": null, "b": [7, null]},
                                    {"a": 6, "b": [8]}])");
  CheckColumn(/*column_index=*/0, *expected);
}

//
// List-in-list
//

TEST_F(TestReconstructColumn, ListList1) {
  // Arrow schema: list(list(int32 not null) not null) not null
  auto inner_list = GroupNode::Make(
      "element", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})},
      LogicalType::List());
  SetParquetSchema(
      GroupNode::Make("parent", Repetition::REQUIRED,
                      {GroupNode::Make("list", Repetition::REPEATED, {inner_list})},
                      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 2, 2};
  LevelVector rep_levels = {0, 0, 1, 0, 2};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(List(int32(), /*nullable=*/false), /*nullable=*/false);
  auto expected = ArrayFromJSON(type, "[[], [[], [4]], [[5, 6]]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListList2) {
  // Arrow schema: list(list(int32 not null) not null)
  auto inner_list = GroupNode::Make(
      "element", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})},
      LogicalType::List());
  SetParquetSchema(
      GroupNode::Make("parent", Repetition::OPTIONAL,
                      {GroupNode::Make("list", Repetition::REPEATED, {inner_list})},
                      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 3, 3};
  LevelVector rep_levels = {0, 0, 0, 1, 0, 2};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(List(int32(), /*nullable=*/false), /*nullable=*/false);
  auto expected = ArrayFromJSON(type, "[null, [], [[], [4]], [[5, 6]]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListList3) {
  // Arrow schema: list(list(int32 not null)) not null
  auto inner_list = GroupNode::Make(
      "element", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})},
      LogicalType::List());
  SetParquetSchema(
      GroupNode::Make("parent", Repetition::REQUIRED,
                      {GroupNode::Make("list", Repetition::REPEATED, {inner_list})},
                      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 3, 3};
  LevelVector rep_levels = {0, 0, 1, 0, 1, 2};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(List(int32(), /*nullable=*/false));
  auto expected = ArrayFromJSON(type, "[[], [null, []], [[4], [5, 6]]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListList4) {
  // Arrow schema: list(list(int32 not null))
  auto inner_list = GroupNode::Make(
      "element", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::REQUIRED, ParquetType::INT32)})},
      LogicalType::List());
  SetParquetSchema(
      GroupNode::Make("parent", Repetition::OPTIONAL,
                      {GroupNode::Make("list", Repetition::REPEATED, {inner_list})},
                      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 4, 4, 4};
  LevelVector rep_levels = {0, 0, 0, 1, 1, 0, 2};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(List(int32(), /*nullable=*/false));
  auto expected = ArrayFromJSON(type, "[null, [], [null, [], [4]], [[5, 6]]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListList5) {
  // Arrow schema: list(list(int32) not null)
  auto inner_list = GroupNode::Make(
      "element", Repetition::REQUIRED,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::OPTIONAL, ParquetType::INT32)})},
      LogicalType::List());
  SetParquetSchema(
      GroupNode::Make("parent", Repetition::OPTIONAL,
                      {GroupNode::Make("list", Repetition::REPEATED, {inner_list})},
                      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 4, 4, 3, 4};
  LevelVector rep_levels = {0, 0, 0, 1, 0, 1, 2};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(List(int32()), /*nullable=*/false);
  auto expected = ArrayFromJSON(type, "[null, [], [[], [4]], [[5], [null, 6]]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

TEST_F(TestReconstructColumn, ListList6) {
  // Arrow schema: list(list(int32))
  auto inner_list = GroupNode::Make(
      "element", Repetition::OPTIONAL,
      {GroupNode::Make(
          "list", Repetition::REPEATED,
          {PrimitiveNode::Make("element", Repetition::OPTIONAL, ParquetType::INT32)})},
      LogicalType::List());
  SetParquetSchema(
      GroupNode::Make("parent", Repetition::OPTIONAL,
                      {GroupNode::Make("list", Repetition::REPEATED, {inner_list})},
                      LogicalType::List()));

  LevelVector def_levels = {0, 1, 2, 3, 4, 5, 5, 5};
  LevelVector rep_levels = {0, 0, 0, 1, 1, 2, 0, 2};
  std::vector<int32_t> values = {4, 5, 6};

  auto type = List(List(int32()));
  auto expected = ArrayFromJSON(type, "[null, [], [null, [], [null, 4]], [[5, 6]]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

// TODO legacy-list-in-struct etc.?

}  // namespace arrow
}  // namespace parquet
