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
#include <fstream>  // IWYU pragma: keep
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/io/file.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/json_integration.h"
#include "arrow/testing/json_internal.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/io_util.h"

DEFINE_string(arrow, "", "Arrow file name");
DEFINE_string(json, "", "JSON file name");
DEFINE_string(
    mode, "VALIDATE",
    "Mode of integration testing tool (ARROW_TO_JSON, JSON_TO_ARROW, VALIDATE)");
DEFINE_bool(integration, false, "Run in integration test mode");
DEFINE_bool(verbose, true, "Verbose output");
DEFINE_bool(
    validate_decimals, true,
    "Validate that decimal values are in range for the given precision (ARROW-13558: "
    "'golden' test data from previous versions may have out-of-range decimal values)");
DEFINE_bool(validate_date64, true,
            "Validate that values for DATE64 represent whole numbers of days");
DEFINE_bool(validate_times, true,
            "Validate that values for TIME32 and TIME64 are within their valid ranges");

namespace arrow {

using internal::TemporaryDir;
using ipc::DictionaryFieldMapper;
using ipc::DictionaryMemo;
using ipc::IpcWriteOptions;
using ipc::MetadataVersion;

namespace testing {

using namespace ::arrow::ipc::test;  // NOLINT

// Convert JSON file to IPC binary format
static Status ConvertJsonToArrow(const std::string& json_path,
                                 const std::string& arrow_path) {
  ARROW_ASSIGN_OR_RAISE(auto in_file, io::ReadableFile::Open(json_path));
  ARROW_ASSIGN_OR_RAISE(auto out_file, io::FileOutputStream::Open(arrow_path));

  ARROW_ASSIGN_OR_RAISE(int64_t file_size, in_file->GetSize());
  ARROW_ASSIGN_OR_RAISE(auto json_buffer, in_file->Read(file_size));

  std::unique_ptr<IntegrationJsonReader> reader;
  RETURN_NOT_OK(IntegrationJsonReader::Open(json_buffer, &reader));

  if (FLAGS_verbose) {
    std::cout << "Found schema:\n"
              << reader->schema()->ToString(/* show_metadata = */ true) << std::endl;
  }

  ARROW_ASSIGN_OR_RAISE(auto writer, ipc::MakeFileWriter(out_file, reader->schema(),
                                                         IpcWriteOptions::Defaults()));
  for (int i = 0; i < reader->num_record_batches(); ++i) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader->ReadRecordBatch(i, &batch));
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  return writer->Close();
}

// Convert IPC binary format to JSON
static Status ConvertArrowToJson(const std::string& arrow_path,
                                 const std::string& json_path) {
  ARROW_ASSIGN_OR_RAISE(auto in_file, io::ReadableFile::Open(arrow_path));
  ARROW_ASSIGN_OR_RAISE(auto out_file, io::FileOutputStream::Open(json_path));

  std::shared_ptr<ipc::RecordBatchFileReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, ipc::RecordBatchFileReader::Open(in_file.get()));

  if (FLAGS_verbose) {
    std::cout << "Found schema:\n" << reader->schema()->ToString() << std::endl;
  }

  std::unique_ptr<IntegrationJsonWriter> writer;
  RETURN_NOT_OK(IntegrationJsonWriter::Open(reader->schema(), &writer));

  for (int i = 0; i < reader->num_record_batches(); ++i) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> batch, reader->ReadRecordBatch(i));
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }

  std::string result;
  RETURN_NOT_OK(writer->Finish(&result));
  return out_file->Write(result.c_str(), static_cast<int64_t>(result.size()));
}

// Validate the batch, accounting for the -validate_decimals , -validate_date64, and
// -validate_times flags
static Status ValidateFull(const RecordBatch& batch) {
  if (FLAGS_validate_decimals && FLAGS_validate_date64 && FLAGS_validate_times) {
    return batch.ValidateFull();
  }
  // Decimal, date64, or times32/64 validation disabled, so individually validate columns
  RETURN_NOT_OK(batch.Validate());
  for (const auto& column : batch.columns()) {
    auto type_id = column->type()->id();
    if (!FLAGS_validate_decimals && is_decimal(type_id)) {
      continue;
    }
    if (!FLAGS_validate_date64 && type_id == Type::DATE64) {
      continue;
    }
    if (!FLAGS_validate_times && (type_id == Type::TIME32 || type_id == Type::TIME64)) {
      continue;
    }
    RETURN_NOT_OK(column->ValidateFull());
  }
  return Status::OK();
}

static Status ValidateArrowVsJson(const std::string& arrow_path,
                                  const std::string& json_path) {
  // Construct JSON reader
  ARROW_ASSIGN_OR_RAISE(auto json_file, io::ReadableFile::Open(json_path));

  ARROW_ASSIGN_OR_RAISE(int64_t file_size, json_file->GetSize());
  ARROW_ASSIGN_OR_RAISE(auto json_buffer, json_file->Read(file_size));

  std::unique_ptr<IntegrationJsonReader> json_reader;
  RETURN_NOT_OK(IntegrationJsonReader::Open(json_buffer, &json_reader));

  // Construct Arrow reader
  ARROW_ASSIGN_OR_RAISE(auto arrow_file, io::ReadableFile::Open(arrow_path));

  std::shared_ptr<ipc::RecordBatchFileReader> arrow_reader;
  ARROW_ASSIGN_OR_RAISE(arrow_reader, ipc::RecordBatchFileReader::Open(arrow_file.get()));

  auto json_schema = json_reader->schema();
  auto arrow_schema = arrow_reader->schema();

  if (!json_schema->Equals(*arrow_schema)) {
    std::stringstream ss;
    ss << "JSON schema: \n"
       << json_schema->ToString(/* show_metadata = */ true) << "\n\n"
       << "Arrow schema: \n"
       << arrow_schema->ToString(/* show_metadata = */ true) << "\n";

    if (FLAGS_verbose) {
      std::cout << ss.str() << std::endl;
    }
    return Status::Invalid("Schemas did not match");
  }

  const int json_nbatches = json_reader->num_record_batches();
  const int arrow_nbatches = arrow_reader->num_record_batches();

  if (json_nbatches != arrow_nbatches) {
    return Status::Invalid("Different number of record batches: ", json_nbatches,
                           " (JSON) vs ", arrow_nbatches, " (Arrow)");
  }

  std::shared_ptr<RecordBatch> arrow_batch;
  std::shared_ptr<RecordBatch> json_batch;
  for (int i = 0; i < json_nbatches; ++i) {
    RETURN_NOT_OK(json_reader->ReadRecordBatch(i, &json_batch));
    ARROW_ASSIGN_OR_RAISE(arrow_batch, arrow_reader->ReadRecordBatch(i));
    Status valid_st = ValidateFull(*json_batch);
    if (!valid_st.ok()) {
      return Status::Invalid("JSON record batch ", i, " did not validate:\n",
                             valid_st.ToString());
    }
    valid_st = ValidateFull(*arrow_batch);
    if (!valid_st.ok()) {
      return Status::Invalid("Arrow record batch ", i, " did not validate:\n",
                             valid_st.ToString());
    }

    if (!json_batch->ApproxEquals(*arrow_batch)) {
      std::stringstream ss;
      ss << "Record batch " << i << " did not match";

      ss << "\nJSON:\n";
      RETURN_NOT_OK(PrettyPrint(*json_batch, 0, &ss));

      ss << "\nArrow:\n";
      RETURN_NOT_OK(PrettyPrint(*arrow_batch, 0, &ss));
      return Status::Invalid(ss.str());
    }
  }

  return Status::OK();
}

Status RunCommand(const std::string& json_path, const std::string& arrow_path,
                  const std::string& command) {
  // Make sure the required extension types are registered, as they will be
  // referenced in test data.
  ExtensionTypeGuard ext_guard({uuid(), dict_extension_type()});

  if (json_path == "") {
    return Status::Invalid("Must specify json file name");
  }

  if (arrow_path == "") {
    return Status::Invalid("Must specify arrow file name");
  }

  auto file_exists = [](const char* path) { return std::ifstream(path).good(); };

  if (command == "ARROW_TO_JSON") {
    if (!file_exists(arrow_path.c_str())) {
      return Status::Invalid("Input file does not exist");
    }

    return ConvertArrowToJson(arrow_path, json_path);
  } else if (command == "JSON_TO_ARROW") {
    if (!file_exists(json_path.c_str())) {
      return Status::Invalid("Input file does not exist");
    }

    return ConvertJsonToArrow(json_path, arrow_path);
  } else if (command == "VALIDATE") {
    if (!file_exists(json_path.c_str())) {
      return Status::Invalid("JSON file does not exist");
    }

    if (!file_exists(arrow_path.c_str())) {
      return Status::Invalid("Arrow file does not exist");
    }

    return ValidateArrowVsJson(arrow_path, json_path);
  } else {
    return Status::Invalid("Unknown command: ", command);
  }
}

class TestJSONIntegration : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("json-integration-test-"));
  }

  std::string mkstemp() {
    std::stringstream ss;
    ss << temp_dir_->path().ToString();
    ss << "file" << ntemp_++;
    return ss.str();
  }

  Status WriteJson(const char* data, const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto out_file, io::FileOutputStream::Open(path));
    return out_file->Write(data, static_cast<int64_t>(strlen(data)));
  }

  void TearDown() { temp_dir_.reset(); }

 protected:
  std::unique_ptr<TemporaryDir> temp_dir_;
  int ntemp_ = 1;
};

static const char* JSON_EXAMPLE = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "foo",
        "type": {"name": "int", "isSigned": true, "bitWidth": 64},
        "nullable": true, "children": []
      },
      {
        "name": "bar",
        "type": {"name": "floatingpoint", "precision": "DOUBLE"},
        "nullable": true, "children": []
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
          "DATA": ["1", "2", "3", "4", "5"],
          "VALIDITY": [1, 0, 1, 1, 1]
        },
        {
          "name": "bar",
          "count": 5,
          "DATA": [1.0, 2.0, 3.0, 4.0, 5.0],
          "VALIDITY": [1, 0, 0, 1, 1]
        }
      ]
    },
    {
      "count": 4,
      "columns": [
        {
          "name": "foo",
          "count": 4,
          "DATA": ["-1", "0", "9223372036854775807", "-9223372036854775808"],
          "VALIDITY": [1, 0, 1, 1]
        },
        {
          "name": "bar",
          "count": 4,
          "DATA": [1.0, 2.0, 3.0, 4.0],
          "VALIDITY": [1, 0, 0, 1]
        }
      ]
    }
  ]
}
)example";

static const char* JSON_EXAMPLE2 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "foo",
        "type": {"name": "int", "isSigned": true, "bitWidth": 32},
        "nullable": true, "children": [],
        "metadata": [
          {"key": "converted_from_time32", "value": "true"}
        ]
      }
    ],
    "metadata": [
      {"key": "schema_custom_0", "value": "eh"}
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
        }
      ]
    }
  ]
}
)example";

TEST_F(TestJSONIntegration, ConvertAndValidate) {
  std::string json_path = this->mkstemp();
  std::string arrow_path = this->mkstemp();

  ASSERT_OK(WriteJson(JSON_EXAMPLE, json_path));

  ASSERT_OK(RunCommand(json_path, arrow_path, "JSON_TO_ARROW"));
  ASSERT_OK(RunCommand(json_path, arrow_path, "VALIDATE"));

  // Convert and overwrite
  ASSERT_OK(RunCommand(json_path, arrow_path, "ARROW_TO_JSON"));

  // Convert back to arrow, and validate
  ASSERT_OK(RunCommand(json_path, arrow_path, "JSON_TO_ARROW"));
  ASSERT_OK(RunCommand(json_path, arrow_path, "VALIDATE"));
}

TEST_F(TestJSONIntegration, ErrorStates) {
  std::string json_path = this->mkstemp();
  std::string json_path2 = this->mkstemp();
  std::string arrow_path = this->mkstemp();

  ASSERT_OK(WriteJson(JSON_EXAMPLE, json_path));
  ASSERT_OK(WriteJson(JSON_EXAMPLE2, json_path2));

  ASSERT_OK(ConvertJsonToArrow(json_path, arrow_path));
  ASSERT_RAISES(Invalid, ValidateArrowVsJson(arrow_path, json_path2));

  ASSERT_RAISES(IOError, ValidateArrowVsJson("does_not_exist-1234", json_path2));
  ASSERT_RAISES(IOError, ValidateArrowVsJson(arrow_path, "does_not_exist-1234"));

  ASSERT_RAISES(Invalid, RunCommand("", arrow_path, "VALIDATE"));
  ASSERT_RAISES(Invalid, RunCommand(json_path, "", "VALIDATE"));
}

// A batch with primitive types
static const char* json_example1 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "foo",
        "type": {"name": "int", "isSigned": true, "bitWidth": 32},
        "nullable": true, "children": []
      },
      {
        "name": "bar",
        "type": {"name": "floatingpoint", "precision": "DOUBLE"},
        "nullable": true, "children": []
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

// A batch with extension types
static const char* json_example2 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "uuids",
        "type" : {
           "name" : "fixedsizebinary",
           "byteWidth" : 16
        },
        "nullable": true,
        "children" : [],
        "metadata" : [
           {"key": "ARROW:extension:name", "value": "uuid"},
           {"key": "ARROW:extension:metadata", "value": "uuid-serialized"}
        ]
      },
      {
        "name": "things",
        "type" : {
           "name" : "null"
        },
        "nullable": true,
        "children" : [],
        "metadata" : [
           {"key": "ARROW:extension:name", "value": "!does not exist!"},
           {"key": "ARROW:extension:metadata", "value": ""},
           {"key": "ARROW:integration:allow_unregistered_extension", "value": "true"}
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 2,
      "columns": [
        {
          "name": "uuids",
          "count": 2,
          "DATA": ["30313233343536373839616263646566",
                   "00000000000000000000000000000000"],
          "VALIDITY": [1, 0]
        },
        {
          "name": "things",
          "count": 2
        }
      ]
    }
  ]
}
)example";

// A batch with dict-extension types
static const char* json_example3 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "dict-extensions",
        "type" : {
           "name" : "utf8"
        },
        "nullable": true,
        "children" : [],
        "dictionary": {
          "id": 0,
          "indexType": {
            "name": "int",
            "isSigned": true,
            "bitWidth": 8
          },
          "isOrdered": false
        },
        "metadata" : [
           {"key": "ARROW:extension:name", "value": "dict-extension"},
           {"key": "ARROW:extension:metadata", "value": "dict-extension-serialized"}
        ]
      }
    ]
  },
  "dictionaries": [
    {
      "id": 0,
      "data": {
        "count": 3,
        "columns": [
          {
            "name": "DICT0",
            "count": 3,
            "VALIDITY": [
              1,
              1,
              1
            ],
            "OFFSET": [
              0,
              3,
              6,
              10
            ],
            "DATA": [
              "foo",
              "bar",
              "quux"
            ]
          }
        ]
      }
    }
  ],
  "batches": [
    {
      "count": 5,
      "columns": [
        {
          "name": "dict-extensions",
          "count": 5,
          "DATA": [2, 0, 1, 1, 2],
          "VALIDITY": [1, 1, 0, 1, 1]
        }
      ]
    }
  ]
}
)example";

// A batch with a map type with non-canonical field names
static const char* json_example4 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "maps",
        "type": {
          "name": "map",
          "keysSorted": false
        },
        "nullable": true,
        "children": [
          {
            "name": "some_entries",
            "type": {
              "name": "struct"
            },
            "nullable": false,
            "children": [
              {
                "name": "some_key",
                "type": {
                  "name": "int",
                  "isSigned": true,
                  "bitWidth": 16
                },
                "nullable": false,
                "children": []
              },
              {
                "name": "some_value",
                "type": {
                  "name": "int",
                  "isSigned": true,
                  "bitWidth": 32
                },
                "nullable": true,
                "children": []
              }
            ]
          }
        ]
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "map_other_names",
          "count": 3,
          "VALIDITY": [1, 0, 1],
          "OFFSET": [0, 3, 3, 5],
          "children": [
            {
              "name": "some_entries",
              "count": 5,
              "VALIDITY": [1, 1, 1, 1, 1],
              "children": [
                {
                  "name": "some_key",
                  "count": 5,
                  "VALIDITY": [1, 1, 1, 1, 1],
                  "DATA": [11, 22, 33, 44, 55]
                },
                {
                  "name": "some_value",
                  "count": 5,
                  "VALIDITY": [1, 1, 0, 1, 1],
                  "DATA": [111, 222, 0, 444, 555]
                }
              ]
            }
          ]
        }
      ]
    }
  ]
}
)example";

// An empty struct type, with "children" member in batches
static const char* json_example5 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "empty_struct",
        "nullable": true,
        "type": {
          "name": "struct"
        },
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 3,
      "columns": [
        {
          "name": "empty_struct",
          "count": 3,
          "VALIDITY": [1, 0, 1],
          "children": []
        }
      ]
    }
  ]
}
)example";

// An empty struct type, without "children" member in batches
static const char* json_example6 = R"example(
{
  "schema": {
    "fields": [
      {
        "name": "empty_struct",
        "nullable": true,
        "type": {
          "name": "struct"
        },
        "children": []
      }
    ]
  },
  "batches": [
    {
      "count": 2,
      "columns": [
        {
          "name": "empty_struct",
          "count": 2,
          "VALIDITY": [1, 0]
        }
      ]
    }
  ]
}
)example";

void TestSchemaRoundTrip(const Schema& schema) {
  rj::StringBuffer sb;
  rj::Writer<rj::StringBuffer> writer(sb);

  DictionaryFieldMapper mapper(schema);

  writer.StartObject();
  ASSERT_OK(json::WriteSchema(schema, mapper, &writer));
  writer.EndObject();

  std::string json_schema = sb.GetString();

  rj::Document d;
  // Pass explicit size to avoid ASAN issues with
  // SIMD loads in RapidJson.
  d.Parse(json_schema.data(), json_schema.size());

  DictionaryMemo in_memo;
  std::shared_ptr<Schema> out;
  const auto status = json::ReadSchema(d, default_memory_pool(), &in_memo, &out);
  if (!status.ok()) {
    FAIL() << "Unable to read JSON schema: " << json_schema << "\nStatus: " << status;
  }

  if (!schema.Equals(*out)) {
    FAIL() << "In schema: " << schema.ToString() << "\nOut schema: " << out->ToString();
  }
}

void TestArrayRoundTrip(const Array& array) {
  static std::string name = "dummy";

  rj::StringBuffer sb;
  rj::Writer<rj::StringBuffer> writer(sb);

  ASSERT_OK(json::WriteArray(name, array, &writer));

  std::string array_as_json = sb.GetString();

  rj::Document d;
  // Pass explicit size to avoid ASAN issues with
  // SIMD loads in RapidJson.
  d.Parse(array_as_json.data(), array_as_json.size());

  if (d.HasParseError()) {
    FAIL() << "JSON parsing failed";
  }

  std::shared_ptr<Array> out;
  ASSERT_OK(json::ReadArray(default_memory_pool(), d, ::arrow::field(name, array.type()),
                            &out));

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
  TestArrayRoundTrip(*array);
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
      field("f18",
            dense_union({field("u1", int8()), field("u2", time32(TimeUnit::MILLI))},
                        {0, 1})),
      field("f19", large_list(uint8())),
      field("f20", null()),
      field("f21", run_end_encoded(int16(), utf8())),
      field("f22", run_end_encoded(int32(), utf8())),
      field("f23", run_end_encoded(int64(), utf8())),
  };

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

TEST(TestJsonArrayWriter, NullType) {
  auto arr = std::make_shared<NullArray>(10);
  TestArrayRoundTrip(*arr);
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

  auto values_array = ArrayFromJSON(int32(), "[0, null, 2, 3, null, 5, 6]");
  auto i16_values_array = ArrayFromJSON(int32(), "[0, null, 2, 3, null, 5, 6]");

  // List
  std::vector<bool> list_is_valid = {true, false, true, true, true};
  std::shared_ptr<Buffer> list_bitmap;
  ASSERT_OK(GetBitmapFromVector(list_is_valid, &list_bitmap));
  std::vector<int32_t> offsets = {0, 0, 0, 1, 4, 7};
  std::shared_ptr<Buffer> offsets_buffer = Buffer::Wrap(offsets);
  {
    ListArray list_array(list(value_type), 5, offsets_buffer, values_array, list_bitmap,
                         1);
    TestArrayRoundTrip(list_array);
  }

  // LargeList
  std::vector<int64_t> large_offsets = {0, 0, 0, 1, 4, 7};
  std::shared_ptr<Buffer> large_offsets_buffer = Buffer::Wrap(large_offsets);
  {
    LargeListArray list_array(large_list(value_type), 5, large_offsets_buffer,
                              values_array, list_bitmap, 1);
    TestArrayRoundTrip(list_array);
  }

  // Map
  auto map_type = map(utf8(), int32());
  auto keys_array = ArrayFromJSON(utf8(), R"(["a", "b", "c", "d", "a", "b", "c"])");

  MapArray map_array(map_type, 5, offsets_buffer, keys_array, values_array, list_bitmap,
                     1);

  TestArrayRoundTrip(map_array);

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

  // Run-End Encoded Type
  auto run_ends = ArrayFromJSON(int32(), "[100, 200, 300, 400, 500, 600, 700]");
  ASSERT_OK_AND_ASSIGN(auto ree_array,
                       RunEndEncodedArray::Make(700, run_ends, i16_values_array));
  TestArrayRoundTrip(*ree_array);
  auto sliced_ree_array = ree_array->Slice(150, 300);
  TestArrayRoundTrip(*sliced_ree_array);
}

TEST(TestJsonArrayWriter, Unions) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK(MakeUnion(&batch));

  for (int i = 0; i < batch->num_columns(); ++i) {
    TestArrayRoundTrip(*batch->column(i));
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

  std::unique_ptr<IntegrationJsonWriter> writer;
  ASSERT_OK(IntegrationJsonWriter::Open(schema, &writer));

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

  std::unique_ptr<IntegrationJsonReader> reader;

  auto buffer = std::make_shared<Buffer>(result);

  ASSERT_OK(IntegrationJsonReader::Open(buffer, &reader));
  ASSERT_TRUE(reader->schema()->Equals(*schema));

  ASSERT_EQ(nbatches, reader->num_record_batches());

  for (int i = 0; i < nbatches; ++i) {
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(reader->ReadRecordBatch(i, &batch));
    ASSERT_BATCHES_EQUAL(*batch, *batches[i]);
  }
}

static void ReadOneBatchJson(const char* json, const Schema& expected_schema,
                             std::shared_ptr<RecordBatch>* out) {
  auto buffer = Buffer::Wrap(json, strlen(json));

  std::unique_ptr<IntegrationJsonReader> reader;
  ASSERT_OK(IntegrationJsonReader::Open(buffer, &reader));

  AssertSchemaEqual(*reader->schema(), expected_schema, /*check_metadata=*/true);
  ASSERT_EQ(1, reader->num_record_batches());

  ASSERT_OK(reader->ReadRecordBatch(0, out));
}

TEST(TestJsonFileReadWrite, JsonExample1) {
  Schema ex_schema({field("foo", int32()), field("bar", float64())});

  std::shared_ptr<RecordBatch> batch;
  ReadOneBatchJson(json_example1, ex_schema, &batch);

  auto foo = ArrayFromJSON(int32(), "[1, null, 3, 4, 5]");
  ASSERT_TRUE(batch->column(0)->Equals(foo));

  auto bar = ArrayFromJSON(float64(), "[1, null, null, 4, 5]");
  ASSERT_TRUE(batch->column(1)->Equals(bar));
}

TEST(TestJsonFileReadWrite, JsonExample2) {
  // Example 2: two extension types (one registered, one unregistered)
  auto uuid_type = uuid();
  auto buffer = Buffer::Wrap(json_example2, strlen(json_example2));

  std::unique_ptr<IntegrationJsonReader> reader;
  {
    ExtensionTypeGuard ext_guard(uuid_type);

    ASSERT_OK(IntegrationJsonReader::Open(buffer, &reader));
    // The second field is an unregistered extension and will be read as
    // its underlying storage.
    Schema ex_schema({field("uuids", uuid_type), field("things", null())});

    AssertSchemaEqual(ex_schema, *reader->schema());
    ASSERT_EQ(1, reader->num_record_batches());

    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(reader->ReadRecordBatch(0, &batch));

    auto storage_array =
        ArrayFromJSON(fixed_size_binary(16), R"(["0123456789abcdef", null])");
    AssertArraysEqual(*batch->column(0), UuidArray(uuid_type, storage_array));

    AssertArraysEqual(*batch->column(1), NullArray(2));
  }

  // Should fail now that the Uuid extension is unregistered
  ASSERT_RAISES(KeyError, IntegrationJsonReader::Open(buffer, &reader));
}

TEST(TestJsonFileReadWrite, JsonExample3) {
  // Example 3: An extension type with a dictionary storage type
  auto dict_ext_type = std::make_shared<DictExtensionType>();
  ExtensionTypeGuard ext_guard(dict_ext_type);
  Schema ex_schema({field("dict-extensions", dict_ext_type)});

  std::shared_ptr<RecordBatch> batch;
  ReadOneBatchJson(json_example3, ex_schema, &batch);
  auto storage_array = std::make_shared<DictionaryArray>(
      dict_ext_type->storage_type(), ArrayFromJSON(int8(), "[2, 0, null, 1, 2]"),
      ArrayFromJSON(utf8(), R"(["foo", "bar", "quux"])"));
  AssertArraysEqual(*batch->column(0), ExtensionArray(dict_ext_type, storage_array),
                    /*verbose=*/true);
}

TEST(TestJsonFileReadWrite, JsonExample4) {
  // Example 4: A map type with non-canonical field names
  ASSERT_OK_AND_ASSIGN(auto map_type,
                       MapType::Make(field("some_entries",
                                           struct_({field("some_key", int16(), false),
                                                    field("some_value", int32())}),
                                           false)));
  Schema ex_schema({field("maps", map_type)});

  std::shared_ptr<RecordBatch> batch;
  ReadOneBatchJson(json_example4, ex_schema, &batch);

  auto expected_array = ArrayFromJSON(
      map(int16(), int32()),
      R"([[[11, 111], [22, 222], [33, null]], null, [[44, 444], [55, 555]]])");
  AssertArraysEqual(*batch->column(0), *expected_array);
}

TEST(TestJsonFileReadWrite, JsonExample5) {
  // Example 5: An empty struct
  auto struct_type = struct_(FieldVector{});
  Schema ex_schema({field("empty_struct", struct_type)});

  std::shared_ptr<RecordBatch> batch;
  ReadOneBatchJson(json_example5, ex_schema, &batch);

  auto expected_array = ArrayFromJSON(struct_type, "[{}, null, {}]");
  AssertArraysEqual(*batch->column(0), *expected_array);
}

TEST(TestJsonFileReadWrite, JsonExample6) {
  // Example 6: An empty struct
  auto struct_type = struct_(FieldVector{});
  Schema ex_schema({field("empty_struct", struct_type)});

  std::shared_ptr<RecordBatch> batch;
  ReadOneBatchJson(json_example6, ex_schema, &batch);

  auto expected_array = ArrayFromJSON(struct_type, "[{}, null]");
  AssertArraysEqual(*batch->column(0), *expected_array);
}

class TestJsonRoundTrip : public ::testing::TestWithParam<MakeRecordBatch*> {
 public:
  void SetUp() {}
  void TearDown() {}
};

void CheckRoundtrip(const RecordBatch& batch) {
  ExtensionTypeGuard guard({uuid(), dict_extension_type(), complex128()});

  TestSchemaRoundTrip(*batch.schema());

  std::unique_ptr<IntegrationJsonWriter> writer;
  ASSERT_OK(IntegrationJsonWriter::Open(batch.schema(), &writer));
  ASSERT_OK(writer->WriteRecordBatch(batch));

  std::string result;
  ASSERT_OK(writer->Finish(&result));

  auto buffer = std::make_shared<Buffer>(result);

  std::unique_ptr<IntegrationJsonReader> reader;
  ASSERT_OK(IntegrationJsonReader::Open(buffer, &reader));

  std::shared_ptr<RecordBatch> result_batch;
  ASSERT_OK(reader->ReadRecordBatch(0, &result_batch));

  // take care of float rounding error in the text representation
  ApproxCompareBatch(batch, *result_batch);
}

TEST_P(TestJsonRoundTrip, RoundTrip) {
  std::shared_ptr<RecordBatch> batch;
  ASSERT_OK((*GetParam())(&batch));  // NOLINT clang-tidy gtest issue

  CheckRoundtrip(*batch);
}

const std::vector<ipc::test::MakeRecordBatch*> kBatchCases = {
    &MakeIntRecordBatch,
    &MakeListRecordBatch,
    &MakeFixedSizeListRecordBatch,
    &MakeNonNullRecordBatch,
    &MakeZeroLengthRecordBatch,
    &MakeDeeplyNestedList,
    &MakeStringTypesRecordBatchWithNulls,
    &MakeStruct,
    &MakeUnion,
    &MakeDictionary,
    &MakeNestedDictionary,
    &MakeMap,
    &MakeMapOfDictionary,
    &MakeDates,
    &MakeTimestamps,
    &MakeTimes,
    &MakeFWBinary,
    &MakeNull,
    &MakeDecimal,
    &MakeBooleanBatch,
    &MakeFloatBatch,
    &MakeIntervals,
    &MakeUuid,
    &MakeComplex128,
    &MakeDictExtension};

INSTANTIATE_TEST_SUITE_P(TestJsonRoundTrip, TestJsonRoundTrip,
                         ::testing::ValuesIn(kBatchCases));

}  // namespace testing
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int ret = 0;

  if (FLAGS_integration) {
    arrow::Status result =
        arrow::testing::RunCommand(FLAGS_json, FLAGS_arrow, FLAGS_mode);
    if (!result.ok()) {
      std::cout << "Error message: " << result.ToString() << std::endl;
      ret = 1;
    }
  } else {
    ::testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  gflags::ShutDownCommandLineFlags();
  return ret;
}
