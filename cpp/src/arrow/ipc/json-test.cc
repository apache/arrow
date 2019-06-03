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
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/json-integration.h"
#include "arrow/ipc/json-internal.h"
#include "arrow/ipc/test-common.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace ipc {
namespace internal {
namespace json {

using namespace ::arrow::ipc::test;  // NOLINT

void TestSchemaRoundTrip(const Schema& schema) {
  rj::StringBuffer sb;
  rj::Writer<rj::StringBuffer> writer(sb);

  DictionaryMemo out_memo;

  writer.StartObject();
  ASSERT_OK(WriteSchema(schema, &out_memo, &writer));
  writer.EndObject();

  std::string json_schema = sb.GetString();

  rj::Document d;
  // Pass explicit size to avoid ASAN issues with
  // SIMD loads in RapidJson.
  d.Parse(json_schema.data(), json_schema.size());

  DictionaryMemo in_memo;
  std::shared_ptr<Schema> out;
  if (!ReadSchema(d, default_memory_pool(), &in_memo, &out).ok()) {
    FAIL() << "Unable to read JSON schema: " << json_schema;
  }

  if (!schema.Equals(*out)) {
    FAIL() << "In schema: " << schema.ToString() << "\nOut schema: " << out->ToString();
  }
}

void TestArrayRoundTrip(const Array& array) {
  static std::string name = "dummy";

  rj::StringBuffer sb;
  rj::Writer<rj::StringBuffer> writer(sb);

  ASSERT_OK(WriteArray(name, array, &writer));

  std::string array_as_json = sb.GetString();

  rj::Document d;
  // Pass explicit size to avoid ASAN issues with
  // SIMD loads in RapidJson.
  d.Parse(array_as_json.data(), array_as_json.size());

  if (d.HasParseError()) {
    FAIL() << "JSON parsing failed";
  }

  DictionaryMemo out_memo;

  std::shared_ptr<Array> out;
  ASSERT_OK(ReadArray(default_memory_pool(), d, ::arrow::field(name, array.type()),
                      &out_memo, &out));

  // std::cout << array_as_json << std::endl;
  CompareArraysDetailed(0, *out, array);
}

template <typename T, typename ValueType>
void CheckPrimitive(const std::shared_ptr<DataType>& type,
                    const std::vector<bool>& is_valid,
                    const std::vector<ValueType>& values) {
  MemoryPool* pool = default_memory_pool();
  typename TypeTraits<T>::BuilderType builder(pool);

  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ASSERT_OK(builder.Append(values[i]));
    } else {
      ASSERT_OK(builder.AppendNull());
    }
  }

  std::shared_ptr<Array> array;
  ASSERT_OK(builder.Finish(&array));
  TestArrayRoundTrip(*array.get());
}

TEST(TestJsonSchemaWriter, FlatTypes) {
  // TODO
  // field("f14", date32())
  std::vector<std::shared_ptr<Field>> fields = {
      field("f0", int8()),
      field("f1", int16(), false),
      field("f2", int32()),
      field("f3", int64(), false),
      field("f4", uint8()),
      field("f5", uint16()),
      field("f6", uint32()),
      field("f7", uint64()),
      field("f8", float32()),
      field("f9", float64()),
      field("f10", utf8()),
      field("f11", binary()),
      field("f12", list(int32())),
      field("f13", struct_({field("s1", int32()), field("s2", utf8())})),
      field("f15", date64()),
      field("f16", timestamp(TimeUnit::NANO)),
      field("f17", time64(TimeUnit::MICRO)),
      field("f18", union_({field("u1", int8()), field("u2", time32(TimeUnit::MILLI))},
                          {0, 1}, UnionMode::DENSE))};

  Schema schema(fields);
  TestSchemaRoundTrip(schema);
}

template <typename T>
void PrimitiveTypesCheckOne() {
  using c_type = typename T::c_type;

  std::vector<bool> is_valid = {true, false, true, true, true, false, true, true};
  std::vector<c_type> values = {0, 1, 2, 3, 4, 5, 6, 7};
  CheckPrimitive<T, c_type>(std::make_shared<T>(), is_valid, values);
}

TEST(TestJsonArrayWriter, PrimitiveTypes) {
  PrimitiveTypesCheckOne<Int8Type>();
  PrimitiveTypesCheckOne<Int16Type>();
  PrimitiveTypesCheckOne<Int32Type>();
  PrimitiveTypesCheckOne<Int64Type>();
  PrimitiveTypesCheckOne<UInt8Type>();
  PrimitiveTypesCheckOne<UInt16Type>();
  PrimitiveTypesCheckOne<UInt32Type>();
  PrimitiveTypesCheckOne<UInt64Type>();
  PrimitiveTypesCheckOne<FloatType>();
  PrimitiveTypesCheckOne<DoubleType>();

  std::vector<bool> is_valid = {true, false, true, true, true, false, true, true};
  std::vector<std::string> values = {"foo", "bar", "", "baz", "qux", "foo", "a", "1"};

  CheckPrimitive<StringType, std::string>(utf8(), is_valid, values);
  CheckPrimitive<BinaryType, std::string>(binary(), is_valid, values);
}

TEST(TestJsonArrayWriter, NestedTypes) {
  auto value_type = int32();

  std::vector<bool> values_is_valid = {true, false, true, true, false, true, true};

  std::vector<int32_t> values = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> values_array;
  ArrayFromVector<Int32Type, int32_t>(values_is_valid, values, &values_array);

  std::vector<int16_t> i16_values = {0, 1, 2, 3, 4, 5, 6};
  std::shared_ptr<Array> i16_values_array;
  ArrayFromVector<Int16Type, int16_t>(values_is_valid, i16_values, &i16_values_array);

  // List
  std::vector<bool> list_is_valid = {true, false, true, true, true};
  std::vector<int32_t> offsets = {0, 0, 0, 1, 4, 7};

  std::shared_ptr<Buffer> list_bitmap;
  ASSERT_OK(GetBitmapFromVector(list_is_valid, &list_bitmap));
  std::shared_ptr<Buffer> offsets_buffer = Buffer::Wrap(offsets);

  ListArray list_array(list(value_type), 5, offsets_buffer, values_array, list_bitmap, 1);

  TestArrayRoundTrip(list_array);

  // FixedSizeList
  FixedSizeListArray fixed_size_list_array(fixed_size_list(value_type, 2), 3,
                                           values_array->Slice(1), list_bitmap, 1);

  TestArrayRoundTrip(fixed_size_list_array);

  // Struct
  std::vector<bool> struct_is_valid = {true, false, true, true, true, false, true};
  std::shared_ptr<Buffer> struct_bitmap;
  ASSERT_OK(GetBitmapFromVector(struct_is_valid, &struct_bitmap));

  auto struct_type =
      struct_({field("f1", int32()), field("f2", int32()), field("f3", int32())});

  std::vector<std::shared_ptr<Array>> fields = {values_array, values_array, values_array};
  StructArray struct_array(struct_type, static_cast<int>(struct_is_valid.size()), fields,
                           struct_bitmap, 2);
  TestArrayRoundTrip(struct_array);
}

TEST(TestJsonArrayWriter, Unions) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeUnion(&batch));

  for (int i = 0; i < batch->num_columns(); ++i) {
    std::shared_ptr<Array> col = batch->column(i);
    TestArrayRoundTrip(*col.get());
  }
}

// Data generation for test case below
void MakeBatchArrays(const std::shared_ptr<Schema>& schema, const int num_rows,
                     std::vector<std::shared_ptr<Array>>* arrays) {
  const float null_prob = 0.25f;
  random::RandomArrayGenerator rand(0x564a3bf0);

  *arrays = {rand.Boolean(num_rows, 0.75, null_prob),
             rand.Int8(num_rows, 0, 100, null_prob),
             rand.Int32(num_rows, -1000, 1000, null_prob),
             rand.UInt64(num_rows, 0, 1UL << 16, null_prob)};

  static const int kBufferSize = 10;
  static uint8_t buffer[kBufferSize];
  static uint32_t seed = 0;
  StringBuilder string_builder;
  for (int i = 0; i < num_rows; ++i) {
    random_ascii(kBufferSize, seed++, buffer);
    ASSERT_OK(string_builder.Append(buffer, kBufferSize));
  }
  std::shared_ptr<Array> v3;
  ASSERT_OK(string_builder.Finish(&v3));

  arrays->emplace_back(v3);
}

TEST(TestJsonFileReadWrite, BasicRoundTrip) {
  auto v1_type = boolean();
  auto v2_type = int8();
  auto v3_type = int32();
  auto v4_type = uint64();
  auto v5_type = utf8();

  auto schema =
      ::arrow::schema({field("f1", v1_type), field("f2", v2_type), field("f3", v3_type),
                       field("f4", v4_type), field("f5", v5_type)});

  std::unique_ptr<JsonWriter> writer;
  ASSERT_OK(JsonWriter::Open(schema, &writer));

  const int nbatches = 3;
  std::vector<std::shared_ptr<RecordBatch>> batches;
  for (int i = 0; i < nbatches; ++i) {
    int num_rows = 5 + i * 5;
    std::vector<std::shared_ptr<Array>> arrays;

    MakeBatchArrays(schema, num_rows, &arrays);
    auto batch = RecordBatch::Make(schema, num_rows, arrays);
    batches.push_back(batch);
    ASSERT_OK(writer->WriteRecordBatch(*batch));
  }

  std::string result;
  ASSERT_OK(writer->Finish(&result));

  std::unique_ptr<JsonReader> reader;

  auto buffer = std::make_shared<Buffer>(result);

  ASSERT_OK(JsonReader::Open(buffer, &reader));
  ASSERT_TRUE(reader->schema()->Equals(*schema));

  ASSERT_EQ(nbatches, reader->num_record_batches());

  for (int i = 0; i < nbatches; ++i) {
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(reader->ReadRecordBatch(i, &batch));
    ASSERT_BATCHES_EQUAL(*batch, *batches[i]);
  }
}

TEST(TestJsonFileReadWrite, MinimalFormatExample) {
  static const char* example = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "foo",
        "type": {"name": "int", "isSigned": true, "bitWidth": 32},
        "nullable": true, "children": [],
        "typeLayout": {
          "vectors": [
            {"type": "VALIDITY", "typeBitWidth": 1},
            {"type": "DATA", "typeBitWidth": 32}
          ]
        }
      },
      {
        "name": "bar",
        "type": {"name": "floatingpoint", "precision": "DOUBLE"},
        "nullable": true, "children": [],
        "typeLayout": {
          "vectors": [
            {"type": "VALIDITY", "typeBitWidth": 1},
            {"type": "DATA", "typeBitWidth": 64}
          ]
        }
      }
    ]
  },
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "foo",
          "count": 5,
          "DATA": [1, 2, 3, 4, 5],
          "VALIDITY": [1, 0, 1, 1, 1]
        },
        {
          "name": "bar",
          "count": 5,
          "DATA": [1.0, 2.0, 3.0, 4.0, 5.0],
          "VALIDITY": [1, 0, 0, 1, 1]
        }
      ]
    }
  ]
}
)example";

  auto buffer = Buffer::Wrap(example, strlen(example));

  std::unique_ptr<JsonReader> reader;
  ASSERT_OK(JsonReader::Open(buffer, &reader));

  Schema ex_schema({field("foo", int32()), field("bar", float64())});

  ASSERT_TRUE(reader->schema()->Equals(ex_schema));
  ASSERT_EQ(1, reader->num_record_batches());

  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(reader->ReadRecordBatch(0, &batch));

  std::vector<bool> foo_valid = {true, false, true, true, true};
  std::vector<int32_t> foo_values = {1, 2, 3, 4, 5};
  std::shared_ptr<Array> foo;
  ArrayFromVector<Int32Type, int32_t>(foo_valid, foo_values, &foo);
  ASSERT_TRUE(batch->column(0)->Equals(foo));

  std::vector<bool> bar_valid = {true, false, false, true, true};
  std::vector<double> bar_values = {1, 2, 3, 4, 5};
  std::shared_ptr<Array> bar;
  ArrayFromVector<DoubleType, double>(bar_valid, bar_values, &bar);
  ASSERT_TRUE(batch->column(1)->Equals(bar));
}

#define BATCH_CASES()                                                              \
  ::testing::Values(&MakeIntRecordBatch, &MakeListRecordBatch,                     \
                    &MakeFixedSizeListRecordBatch, &MakeNonNullRecordBatch,        \
                    &MakeZeroLengthRecordBatch, &MakeDeeplyNestedList,             \
                    &MakeStringTypesRecordBatchWithNulls, &MakeStruct, &MakeUnion, \
                    &MakeDates, &MakeTimestamps, &MakeTimes, &MakeFWBinary,        \
                    &MakeDecimal, &MakeDictionary, &MakeIntervals);

class TestJsonRoundTrip : public ::testing::TestWithParam<MakeRecordBatch*> {
 public:
  void SetUp() {}
  void TearDown() {}
};

void CheckRoundtrip(const RecordBatch& batch) {
  TestSchemaRoundTrip(*batch.schema());

  std::unique_ptr<JsonWriter> writer;
  ASSERT_OK(JsonWriter::Open(batch.schema(), &writer));
  ASSERT_OK(writer->WriteRecordBatch(batch));

  std::string result;
  ASSERT_OK(writer->Finish(&result));

  auto buffer = std::make_shared<Buffer>(result);

  std::unique_ptr<JsonReader> reader;
  ASSERT_OK(JsonReader::Open(buffer, &reader));

  std::shared_ptr<RecordBatch> result_batch;
  ASSERT_OK(reader->ReadRecordBatch(0, &result_batch));

  CompareBatch(batch, *result_batch);
}

TEST_P(TestJsonRoundTrip, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  CheckRoundtrip(*batch);
}

INSTANTIATE_TEST_CASE_P(TestJsonRoundTrip, TestJsonRoundTrip, BATCH_CASES());

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow
