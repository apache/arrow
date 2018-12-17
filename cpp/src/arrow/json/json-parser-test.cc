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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>

#include "arrow/json/options.h"
#include "arrow/json/parser.h"
#include "arrow/status.h"
#include "arrow/test-util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace json {

template <typename ItemType>
class TypedListBuilder : public ListBuilder {
 public:
  using ItemBuilder = typename TypeTraits<ItemType>::BuilderType;

  using ListBuilder::ListBuilder;

  template <typename ItemBuilderVisitor>
  Status Append(ItemBuilderVisitor&& visitor) {
    ARROW_RETURN_NOT_OK(ListBuilder::Append());
    return visitor(static_cast<ItemBuilder*>(value_builder()));
  }
};

template <typename ItemType>
Status MakeTypedListBuilder(std::unique_ptr<TypedListBuilder<ItemType>>* builder) {
  using ItemBuilder = typename TypedListBuilder<ItemType>::ItemBuilder;
  auto pool = default_memory_pool();
  std::unique_ptr<ArrayBuilder> item_builder;
  ARROW_RETURN_NOT_OK(
      MakeBuilder(pool, TypeTraits<ItemType>::type_singleton(), &item_builder));
  builder->reset(new TypedListBuilder<ItemType>(pool, std::move(item_builder)));
  return Status::OK();
}

char scalars_only_src[] = R"(
    { "hello": 3.5, "world": false, "yo": "thing" }
    { "hello": 3.2, "world": null }
    { "hello": 3.4, "world": null, "yo": "\u5fcd" }
    { "hello": 0.0, "world": true, "yo": null }
  )";

char nested_src[] = R"(
    { "hello": 3.5, "world": false, "yo": "thing", "arr": [1, 2, 3], "nuf": {} }
    { "hello": 3.2, "world": null, "arr": [2], "nuf": null }
    { "hello": 3.4, "world": null, "yo": "\u5fcd", "arr": [], "nuf": { "ps": 78 } }
    { "hello": 0.0, "world": true, "yo": null, "arr": null, "nuf": { "ps": 90 } }
  )";

std::shared_ptr<Array> GetColumn(const RecordBatch& batch, const std::string& name) {
  return batch.column(batch.schema()->GetFieldIndex(name));
}

TEST(BlockParserWithSchema, Basics) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("hello", float64()), field("world", boolean()), field("yo", utf8())});
  BlockParser parser(options);
  uint32_t out_size;
  std::string src(scalars_only_src);
  ASSERT_OK(parser.Parse(src.data(), static_cast<uint32_t>(src.size()), &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::shared_ptr<Array> hello, world, yo;
  ArrayFromVector<DoubleType>({3.5, 3.2, 3.4, 0.0}, &hello);
  ArrayFromVector<BooleanType, bool>({1, 0, 0, 1}, {0, 1, 0, 1}, &world);
  ArrayFromVector<StringType, std::string>({1, 0, 1, 0},
                                           {"thing", "", "\xe5\xbf\x8d", ""}, &yo);

  AssertArraysEqual(*hello, *GetColumn(*parsed, "hello"));
  AssertArraysEqual(*world, *GetColumn(*parsed, "world"));
  AssertArraysEqual(*yo, *GetColumn(*parsed, "yo"));
}

TEST(BlockParserWithSchema, Empty) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema =
      schema({field("hello", float64()), field("world", boolean()), field("yo", utf8())});
  BlockParser parser(options);
  uint32_t out_size;
  ASSERT_OK(parser.Parse("", 0, &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::shared_ptr<Array> hello, world, yo;
  ArrayFromVector<DoubleType>({}, &hello);
  ArrayFromVector<BooleanType, bool>({}, &world);
  ArrayFromVector<StringType, std::string>({}, &yo);

  AssertArraysEqual(*hello, *GetColumn(*parsed, "hello"));
  AssertArraysEqual(*world, *GetColumn(*parsed, "world"));
  AssertArraysEqual(*yo, *GetColumn(*parsed, "yo"));
}

TEST(BlockParserWithSchema, SkipFieldsOutsideSchema) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("hello", float64()), field("yo", utf8())});
  BlockParser parser(options);
  uint32_t out_size;
  std::string src(scalars_only_src);
  ASSERT_OK(parser.Parse(src.data(), static_cast<uint32_t>(src.size()), &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::shared_ptr<Array> hello, yo;
  ArrayFromVector<DoubleType>({3.5, 3.2, 3.4, 0.0}, &hello);
  ArrayFromVector<StringType, std::string>({1, 0, 1, 0},
                                           {"thing", "", "\xe5\xbf\x8d", ""}, &yo);

  AssertArraysEqual(*hello, *GetColumn(*parsed, "hello"));
  AssertArraysEqual(*yo, *GetColumn(*parsed, "yo"));
}

TEST(BlockParserWithSchema, FailOnInconvertible) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("a", int32())});
  BlockParser parser(options);
  uint32_t out_size;
  char a_changes_type[] = "{\"a\":0}\n{\"a\":true}";
  ASSERT_RAISES(Invalid, parser.Parse(a_changes_type, sizeof(a_changes_type), &out_size));
}

TEST(BlockParserWithSchema, Nested) {
  auto options = ParseOptions::Defaults();
  auto arr_type = list(int32());
  auto nuf_type = struct_({field("ps", int32())});
  options.explicit_schema =
      schema({field("yo", utf8()), field("arr", arr_type), field("nuf", nuf_type)});
  BlockParser parser(options);
  uint32_t out_size;
  std::string src(nested_src);
  ASSERT_OK(parser.Parse(src.data(), static_cast<uint32_t>(src.size()), &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::unique_ptr<TypedListBuilder<Int32Type>> arr_builder;
  ASSERT_OK(MakeTypedListBuilder(&arr_builder));
  ASSERT_OK(arr_builder->Append([](Int32Builder* b) {
    return b->AppendValues({1, 2, 3});
  }));
  ASSERT_OK(arr_builder->Append([](Int32Builder* b) { return b->AppendValues({2}); }));
  ASSERT_OK(arr_builder->Append([](Int32Builder* b) { return b->AppendValues({}); }));
  ASSERT_OK(arr_builder->AppendNull());
  std::shared_ptr<Array> arr_expected;
  ASSERT_OK(arr_builder->Finish(&arr_expected));

  auto nuf_ps_builder = std::make_shared<Int32Builder>();
  StructBuilder nuf_builder(nuf_type, default_memory_pool(), {nuf_ps_builder});
  ASSERT_OK(nuf_builder.Append());
  ASSERT_OK(nuf_ps_builder->AppendNull());
  ASSERT_OK(nuf_builder.AppendNull());
  ASSERT_OK(nuf_ps_builder->AppendNull());
  ASSERT_OK(nuf_builder.Append());
  ASSERT_OK(nuf_ps_builder->Append(78));
  ASSERT_OK(nuf_builder.Append());
  ASSERT_OK(nuf_ps_builder->Append(90));
  std::shared_ptr<Array> nuf_expected;
  ASSERT_OK(nuf_builder.Finish(&nuf_expected));

  std::shared_ptr<Array> yo;
  ArrayFromVector<StringType, std::string>({1, 0, 1, 0},
                                           {"thing", "", "\xe5\xbf\x8d", ""}, &yo);

  AssertArraysEqual(*yo, *GetColumn(*parsed, "yo"));
  AssertArraysEqual(*arr_expected, *GetColumn(*parsed, "arr"));
  AssertArraysEqual(*nuf_expected, *GetColumn(*parsed, "nuf"));
}

TEST(BlockParserWithSchema, FailOnIncompleteJson) {
  auto options = ParseOptions::Defaults();
  options.explicit_schema = schema({field("a", int32())});
  BlockParser parser(options);
  uint32_t out_size;
  char incomplete[] = "{\"a\":0, \"b\"";
  ASSERT_RAISES(Invalid, parser.Parse(incomplete, sizeof(incomplete), &out_size));
}

TEST(BlockParser, Basics) {
  auto options = ParseOptions::Defaults();
  BlockParser parser(options);
  uint32_t out_size;
  std::string src(scalars_only_src);
  ASSERT_OK(parser.Parse(src.data(), static_cast<uint32_t>(src.size()), &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::shared_ptr<Array> hello, world, yo;
  ArrayFromVector<DoubleType>({3.5, 3.2, 3.4, 0.0}, &hello);
  ArrayFromVector<BooleanType, bool>({1, 0, 0, 1}, {0, 1, 0, 1}, &world);
  ArrayFromVector<StringType, std::string>({1, 0, 1, 0},
                                           {"thing", "", "\xe5\xbf\x8d", ""}, &yo);

  AssertArraysEqual(*hello, *GetColumn(*parsed, "hello"));
  AssertArraysEqual(*world, *GetColumn(*parsed, "world"));
  AssertArraysEqual(*yo, *GetColumn(*parsed, "yo"));
}

TEST(BlockParser, FailOnInconvertible) {
  auto options = ParseOptions::Defaults();
  BlockParser parser(options);
  uint32_t out_size;
  char a_changes_type[] = "{\"a\":0}\n{\"a\":true}";
  ASSERT_RAISES(Invalid, parser.Parse(a_changes_type, sizeof(a_changes_type), &out_size));
}

TEST(BlockParser, Nested) {
  auto options = ParseOptions::Defaults();
  auto nuf_type = struct_({field("ps", int64())});
  BlockParser parser(options);
  uint32_t out_size;
  std::string src(nested_src);
  ASSERT_OK(parser.Parse(src.data(), static_cast<uint32_t>(src.size()), &out_size));
  std::shared_ptr<RecordBatch> parsed;
  ASSERT_OK(parser.Finish(&parsed));

  std::unique_ptr<TypedListBuilder<Int64Type>> arr_builder;
  ASSERT_OK(MakeTypedListBuilder(&arr_builder));
  ASSERT_OK(arr_builder->Append([](Int64Builder* b) {
    return b->AppendValues({1, 2, 3});
  }));
  ASSERT_OK(arr_builder->Append([](Int64Builder* b) { return b->AppendValues({2}); }));
  ASSERT_OK(arr_builder->Append([](Int64Builder* b) { return b->AppendValues({}); }));
  ASSERT_OK(arr_builder->AppendNull());
  std::shared_ptr<Array> arr_expected;
  ASSERT_OK(arr_builder->Finish(&arr_expected));

  auto nuf_ps_builder = std::make_shared<Int64Builder>();
  StructBuilder nuf_builder(nuf_type, default_memory_pool(), {nuf_ps_builder});
  ASSERT_OK(nuf_builder.Append());
  ASSERT_OK(nuf_ps_builder->AppendNull());
  ASSERT_OK(nuf_builder.AppendNull());
  ASSERT_OK(nuf_ps_builder->AppendNull());
  ASSERT_OK(nuf_builder.Append());
  ASSERT_OK(nuf_ps_builder->Append(78));
  ASSERT_OK(nuf_builder.Append());
  ASSERT_OK(nuf_ps_builder->Append(90));
  std::shared_ptr<Array> nuf_expected;
  ASSERT_OK(nuf_builder.Finish(&nuf_expected));

  std::shared_ptr<Array> yo;
  ArrayFromVector<StringType, std::string>({1, 0, 1, 0},
                                           {"thing", "", "\xe5\xbf\x8d", ""}, &yo);

  AssertArraysEqual(*yo, *GetColumn(*parsed, "yo"));
  AssertArraysEqual(*arr_expected, *GetColumn(*parsed, "arr"));
  AssertArraysEqual(*nuf_expected, *GetColumn(*parsed, "nuf"));
}

}  // namespace json
}  // namespace arrow
