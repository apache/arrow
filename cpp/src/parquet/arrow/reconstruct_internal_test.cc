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

    auto typed_writer =
        checked_cast<TypedColumnWriter<PhysicalType<TYPE>>*>(column_writer);

    const int64_t num_values = static_cast<int64_t>(
        (max_def_level > 0) ? def_levels.size()
                            : (max_rep_level > 0) ? rep_levels.size() : values.size());
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

  // Read a Arrow column and check its values
  void CheckColumn(int column_index, const Array& expected) {
    if (!tester_) {
      ASSERT_OK_AND_ASSIGN(auto buffer, builder_->Finish());
      ASSERT_OK_AND_ASSIGN(tester_, FileTester::Make(buffer, pool_));
    }
    tester_->CheckColumn(column_index, expected);
  }

  void CheckColumn(const Array& expected) { CheckColumn(0, expected); }

  // One-column shortcut
  template <ParquetType TYPE, typename C_TYPE = typename ParquetTraits<TYPE>::value_type>
  void AssertReconstruct(const Array& expected, const LevelVector& def_levels,
                         const LevelVector& rep_levels,
                         const std::vector<C_TYPE>& values) {
    ASSERT_OK((WriteColumn<TYPE, C_TYPE>(def_levels, rep_levels, values)));
    CheckColumn(0, expected);
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

static std::shared_ptr<DataType> OneFieldStruct(const std::string& name,
                                                std::shared_ptr<DataType> type,
                                                bool nullable = true) {
  return struct_({field(name, type, nullable)});
}

TEST_F(TestReconstructColumn, NestedRequiredRequired) {
  // Arrow schema: struct(a: int32 not null) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32)}));

  LevelVector def_levels = {};
  LevelVector rep_levels = {};
  std::vector<int32_t> values = {4, 5, 6};

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32(), false), "[[4], [5], [6]]");
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

  auto expected =
      ArrayFromJSON(OneFieldStruct("a", int32(), false), "[null, [4], [5], [6]]");
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

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32()), "[[null], [4], [5], [6]]");
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

  auto expected = ArrayFromJSON(OneFieldStruct("a", int32()), "[null, [null], [4], [5]]");
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
                    "[[[4]], [[5]], [[6]]]");
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
                                "[[[4]], [[null]], [[5]], [[6]]]");
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
                                "[[[null]], [[4]], null, [[5]], [[6]]]");
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
                                "[[null], [[null]], null, [[4]], [[5]], [[6]]]");
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

  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  ASSERT_OK(
      WriteColumn<ParquetType::INT64>(DefLevels{}, RepLevels{}, Int64Vector{7, 8, 9}));

  auto type = struct_(
      {field("a", int32(), /*nullable=*/false), field("b", int64(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, "[[4, 7], [5, 8], [6, 9]]");

  CheckColumn(0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields2) {
  // Arrow schema: struct(a: int32 not null, b: int64) not null
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::REQUIRED,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::OPTIONAL, ParquetType::INT64)}));

  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  ASSERT_OK(WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int64Vector{7, 8}));

  auto type = struct_({field("a", int32(), /*nullable=*/false), field("b", int64())});
  auto expected = ArrayFromJSON(type, "[[4, null], [5, 7], [6, 8]]");

  CheckColumn(0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields3) {
  // Arrow schema: struct(a: int32 not null, b: int64 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::REQUIRED, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)}));

  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int32Vector{4, 5}));
  ASSERT_OK(WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int64Vector{7, 8}));

  auto type = struct_(
      {field("a", int32(), /*nullable=*/false), field("b", int64(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, "[null, [4, 7], [5, 8]]");

  CheckColumn(0, *expected);
}

TEST_F(TestReconstructColumn, NestedTwoFields4) {
  // Arrow schema: struct(a: int32, b: int64 not null)
  SetParquetSchema(GroupNode::Make(
      "parent", Repetition::OPTIONAL,
      {PrimitiveNode::Make("a", Repetition::OPTIONAL, ParquetType::INT32),
       PrimitiveNode::Make("b", Repetition::REQUIRED, ParquetType::INT64)}));

  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{0, 1, 2}, RepLevels{}, Int32Vector{4}));
  ASSERT_OK(WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int64Vector{7, 8}));

  auto type = struct_({field("a", int32()), field("b", int64(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, "[null, [null, 7], [4, 8]]");

  CheckColumn(0, *expected);
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
  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  // ab
  ASSERT_OK(
      WriteColumn<ParquetType::INT64>(DefLevels{}, RepLevels{}, Int64Vector{7, 8, 9}));
  // b
  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{10, 11, 12}));

  auto type = struct_({field("a",
                             struct_({field("aa", int32(), /*nullable=*/false),
                                      field("ab", int64(), /*nullable=*/false)}),
                             /*nullable=*/false),
                       field("b", int32(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, "[[[4, 7], 10], [[5, 8], 11], [[6, 9], 12]]");

  CheckColumn(0, *expected);
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
  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{1, 0, 1}, RepLevels{},
                                            Int32Vector{4, 5}));
  // ab
  ASSERT_OK(
      WriteColumn<ParquetType::INT64>(DefLevels{}, RepLevels{}, Int64Vector{7, 8, 9}));
  // b
  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{10, 11, 12}));

  auto type = struct_(
      {field("a",
             struct_({field("aa", int32()), field("ab", int64(), /*nullable=*/false)}),
             /*nullable=*/false),
       field("b", int32(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, "[[[4, 7], 10], [[null, 8], 11], [[5, 9], 12]]");

  CheckColumn(0, *expected);
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
  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{4, 5, 6}));
  // ab
  ASSERT_OK(WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int64Vector{7, 8}));
  // b
  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{1, 0, 1}, RepLevels{},
                                            Int32Vector{10, 11}));

  auto type = struct_(
      {field("a",
             struct_({field("aa", int32(), /*nullable=*/false), field("ab", int64())}),
             /*nullable=*/false),
       field("b", int32())});
  auto expected = ArrayFromJSON(type, "[[[4, null], 10], [[5, 7], null], [[6, 8], 11]]");

  CheckColumn(0, *expected);
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
  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int32Vector{4, 5}));
  // ab
  ASSERT_OK(
      WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 2}, RepLevels{}, Int64Vector{7}));
  // b
  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{}, RepLevels{}, Int32Vector{10, 11, 12}));

  auto type = struct_({field("a", struct_({field("aa", int32(), /*nullable=*/false),
                                           field("ab", int64())})),
                       field("b", int32(), /*nullable=*/false)});
  auto expected = ArrayFromJSON(type, "[[null, 10], [[4, null], 11], [[5, 7], 12]]");

  CheckColumn(0, *expected);
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
  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{0, 1, 1}, RepLevels{},
                                            Int32Vector{4, 5}));
  // ab
  ASSERT_OK(
      WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 2}, RepLevels{}, Int64Vector{7}));
  // b
  ASSERT_OK(
      WriteColumn<ParquetType::INT32>(DefLevels{0, 2, 1}, RepLevels{}, Int32Vector{10}));

  auto type = struct_(
      {field("a",
             struct_({field("aa", int32(), /*nullable=*/false), field("ab", int64())}),
             /*nullable=*/false),
       field("b", int32())});
  auto expected = ArrayFromJSON(type, "[null, [[4, null], 10], [[5, 7], null]]");

  CheckColumn(0, *expected);
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
  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{0, 1, 2, 2}, RepLevels{},
                                            Int32Vector{4, 5}));
  // ab
  ASSERT_OK(WriteColumn<ParquetType::INT64>(DefLevels{0, 1, 2, 3}, RepLevels{},
                                            Int64Vector{7}));
  // b
  ASSERT_OK(WriteColumn<ParquetType::INT32>(DefLevels{0, 2, 1, 2}, RepLevels{},
                                            Int32Vector{10, 11}));

  auto type = struct_({field("a", struct_({field("aa", int32(), /*nullable=*/false),
                                           field("ab", int64())})),
                       field("b", int32())});
  auto expected =
      ArrayFromJSON(type, "[null, [null, 10], [[4, null], null], [[5, 7], 11]]");

  CheckColumn(0, *expected);
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
  auto expected = ArrayFromJSON(list(field("element", int32(), /*nullable=*/false)),
                                "[[], [4, 5], [6]]");
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

  auto expected = ArrayFromJSON(list(field("element", int32(), /*nullable=*/false)),
                                "[null, [], [4, 5], [6]]");
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

  auto expected =
      ArrayFromJSON(list(field("element", int32())), "[[], [null, 4], [5, 6]]");
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

  auto expected =
      ArrayFromJSON(list(field("element", int32())), "[null, [], [null, 4], [5, 6]]");
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
  auto expected = ArrayFromJSON(list(field("element", int32(), /*nullable=*/false)),
                                "[[], [4, 5], [6]]");
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

  auto expected = ArrayFromJSON(list(field("element", int32(), /*nullable=*/false)),
                                "[null, [], [4, 5], [6]]");
  AssertReconstruct<ParquetType::INT32>(*expected, def_levels, rep_levels, values);
}

// TODO list-in-list, list-in-struct, struct-in-list...

}  // namespace arrow
}  // namespace parquet
