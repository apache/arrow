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

#include "gtest/gtest.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/writer.h"
#include "arrow/json/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace json {

class TestWriteJSON : public ::testing::Test {
 protected:
  template <typename Data>
  Result<std::string> ToJSONString(const Data& data, const WriteOptions& options) {
    std::shared_ptr<io::BufferOutputStream> out;
    ARROW_ASSIGN_OR_RAISE(out, io::BufferOutputStream::Create());

    RETURN_NOT_OK(WriteJSON(data, options, out.get()));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, out->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size());
  }

  Result<std::string> ToJSONStringUsingWriter(const Table& data,
                                              const WriteOptions& options) {
    std::shared_ptr<io::BufferOutputStream> out;
    ARROW_ASSIGN_OR_RAISE(out, io::BufferOutputStream::Create());
    ARROW_ASSIGN_OR_RAISE(auto writer, MakeJSONWriter(out, data.schema(), options));
    TableBatchReader reader(data);
    reader.set_chunksize(1);
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader.ReadNext(&batch));
    while (batch != nullptr) {
      RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
      RETURN_NOT_OK(reader.ReadNext(&batch));
    }
    RETURN_NOT_OK(writer->Close());
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> buffer, out->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()), buffer->size());
  }
};

TEST_F(TestWriteJSON, EmptyBatch) {
  auto schema = ::arrow::schema({field("a", int64()), field("b", utf8())});
  auto batch = RecordBatchFromJSON(schema, "[]");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json, "");
}

TEST_F(TestWriteJSON, SimpleTypes) {
  auto schema = ::arrow::schema({
      field("int64", int64()),
      field("double", float64()),
      field("string", utf8()),
      field("bool", boolean()),
      field("date32", date32()),
      field("timestamp", timestamp(TimeUnit::SECOND)),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"int64": 42, "double": 3.14, "string": "hello", "bool": true, "date32": 172800000, "timestamp": 172800000},
    {"int64": -1, "double": 2.71, "string": "world", "bool": false, "date32": 259200000, "timestamp": 259200000}
  ])");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(
      json,
      R"({"int64":42,"double":3.14,"string":"hello","bool":true,"date32":172800000,"timestamp":172800000}
{"int64":-1,"double":2.71,"string":"world","bool":false,"date32":259200000,"timestamp":259200000}
)");
}

TEST_F(TestWriteJSON, NullValues) {
  auto schema = ::arrow::schema({
      field("a", int64()),
      field("b", utf8()),
      field("c", boolean()),
      field("d", null()),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": 1, "b": "x", "c": true, "d": null},
    {"a": null, "b": "y", "c": null, "d": null},
    {"a": 3, "b": "z", "c": false, "d": null}
  ])");

  WriteOptions options;
  options.emit_null = true;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"a":1,"b":"x","c":true,"d":null}
{"a":null,"b":"y","c":null,"d":null}
{"a":3,"b":"z","c":false,"d":null}
)");
}

TEST_F(TestWriteJSON, SkipNullValues) {
  auto schema = ::arrow::schema({
      field("a", int64()),
      field("b", utf8()),
      field("c", boolean()),
      field("d", null()),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": 1, "b": "x", "c": true, "d": null},
    {"a": null, "b": "y", "c": null, "d": null},
    {"a": 3, "b": null, "c": false, "d": null}
  ])");

  WriteOptions options;
  options.emit_null = false;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"a":1,"b":"x","c":true}
{"b":"y"}
{"a":3,"c":false}
)");
}

TEST_F(TestWriteJSON, NestedStruct) {
  auto schema = ::arrow::schema({
      field("id", int64()),
      field("data", struct_({
                        field("name", utf8()),
                        field("value", int32()),
                    })),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"id": 1, "data": {"name": "foo", "value": 42}},
    {"id": 2, "data": {"name": "bar", "value": 100}}
  ])");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"id":1,"data":{"name":"foo","value":42}}
{"id":2,"data":{"name":"bar","value":100}}
)");
}

TEST_F(TestWriteJSON, ListType) {
  auto schema = ::arrow::schema({
      field("id", int64()),
      field("values", list(int32())),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"id": 1, "values": [1, 2, 3]},
    {"id": 2, "values": [4, 5]},
    {"id": 3, "values": []}
  ])");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"id":1,"values":[1,2,3]}
{"id":2,"values":[4,5]}
{"id":3,"values":[]}
)");
}

TEST_F(TestWriteJSON, NestedListAndStruct) {
  auto schema = ::arrow::schema({
      field("id", int64()),
      field("items", list(struct_({
                         field("key", utf8()),
                         field("value", int64()),
                     }))),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"id": 1, "items": [{"key": "a", "value": 1}, {"key": "b", "value": 2}]},
    {"id": 2, "items": []}
  ])");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"id":1,"items":[{"key":"a","value":1},{"key":"b","value":2}]}
{"id":2,"items":[]}
)");
}

TEST_F(TestWriteJSON, FromTable) {
  auto schema = ::arrow::schema({
      field("a", int64()),
      field("b", utf8()),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": 1, "b": "x"},
    {"a": 2, "b": "y"}
  ])");

  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches({batch}));

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*table, options));
  EXPECT_EQ(json,
            R"({"a":1,"b":"x"}
{"a":2,"b":"y"}
)");
}

TEST_F(TestWriteJSON, FromRecordBatchReader) {
  auto schema = ::arrow::schema({
      field("a", int64()),
      field("b", utf8()),
  });

  auto batch1 = RecordBatchFromJSON(schema, R"([
    {"a": 1, "b": "x"}
  ])");
  auto batch2 = RecordBatchFromJSON(schema, R"([
    {"a": 2, "b": "y"}
  ])");

  ASSERT_OK_AND_ASSIGN(auto reader, RecordBatchReader::Make({batch1, batch2}));

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(reader, options));
  EXPECT_EQ(json,
            R"({"a":1,"b":"x"}
{"a":2,"b":"y"}
)");
}

TEST_F(TestWriteJSON, BatchSize) {
  auto schema = ::arrow::schema({
      field("a", int64()),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}
  ])");

  WriteOptions options;
  options.batch_size = 2;  // Process 2 rows at a time
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"a":1}
{"a":2}
{"a":3}
{"a":4}
{"a":5}
)");
}

TEST_F(TestWriteJSON, UsingWriter) {
  auto schema = ::arrow::schema({
      field("a", int64()),
      field("b", utf8()),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": 1, "b": "x"},
    {"a": 2, "b": "y"}
  ])");

  ASSERT_OK_AND_ASSIGN(auto table, Table::FromRecordBatches({batch}));

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONStringUsingWriter(*table, options));
  EXPECT_EQ(json,
            R"({"a":1,"b":"x"}
{"a":2,"b":"y"}
)");
}

TEST_F(TestWriteJSON, LargeString) {
  auto schema = ::arrow::schema({
      field("a", large_utf8()),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": "hello"},
    {"a": "world"}
  ])");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"a":"hello"}
{"a":"world"}
)");
}

TEST_F(TestWriteJSON, FixedSizeList) {
  auto schema = ::arrow::schema({
      field("a", fixed_size_list(int32(), 3)),
  });

  auto batch = RecordBatchFromJSON(schema, R"([
    {"a": [1, 2, 3]},
    {"a": [4, 5, 6]}
  ])");

  WriteOptions options;
  ASSERT_OK_AND_ASSIGN(std::string json, ToJSONString(*batch, options));
  EXPECT_EQ(json,
            R"({"a":[1,2,3]}
{"a":[4,5,6]}
)");
}

TEST_F(TestWriteJSON, OptionsValidation) {
  WriteOptions options;
  options.batch_size = 0;
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("batch_size must be at least 1"), options.Validate());

  options.batch_size = 1024;
  ASSERT_OK(options.Validate());
}

}  // namespace json
}  // namespace arrow
