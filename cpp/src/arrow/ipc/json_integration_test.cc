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

#include "arrow/io/file.h"
#include "arrow/ipc/json_integration.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/io_util.h"

DEFINE_string(arrow, "", "Arrow file name");
DEFINE_string(json, "", "JSON file name");
DEFINE_string(
    mode, "VALIDATE",
    "Mode of integration testing tool (ARROW_TO_JSON, JSON_TO_ARROW, VALIDATE)");
DEFINE_bool(integration, false, "Run in integration test mode");
DEFINE_bool(verbose, true, "Verbose output");

namespace arrow {

class Buffer;
using internal::TemporaryDir;

namespace ipc {

bool file_exists(const char* path) {
  std::ifstream handle(path);
  return handle.good();
}

// Convert JSON file to IPC binary format
static Status ConvertJsonToArrow(const std::string& json_path,
                                 const std::string& arrow_path) {
  ARROW_ASSIGN_OR_RAISE(auto in_file, io::ReadableFile::Open(json_path));
  ARROW_ASSIGN_OR_RAISE(auto out_file, io::FileOutputStream::Open(arrow_path));

  ARROW_ASSIGN_OR_RAISE(int64_t file_size, in_file->GetSize());
  ARROW_ASSIGN_OR_RAISE(auto json_buffer, in_file->Read(file_size));

  std::unique_ptr<internal::json::JsonReader> reader;
  RETURN_NOT_OK(internal::json::JsonReader::Open(json_buffer, &reader));

  if (FLAGS_verbose) {
    std::cout << "Found schema:\n" << reader->schema()->ToString() << std::endl;
  }

  std::shared_ptr<RecordBatchWriter> writer;
  RETURN_NOT_OK(RecordBatchFileWriter::Open(out_file.get(), reader->schema(), &writer));

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

  std::shared_ptr<RecordBatchFileReader> reader;
  RETURN_NOT_OK(RecordBatchFileReader::Open(in_file.get(), &reader));

  if (FLAGS_verbose) {
    std::cout << "Found schema:\n" << reader->schema()->ToString() << std::endl;
  }

  std::unique_ptr<internal::json::JsonWriter> writer;
  RETURN_NOT_OK(internal::json::JsonWriter::Open(reader->schema(), &writer));

  for (int i = 0; i < reader->num_record_batches(); ++i) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader->ReadRecordBatch(i, &batch));
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }

  std::string result;
  RETURN_NOT_OK(writer->Finish(&result));
  return out_file->Write(result.c_str(), static_cast<int64_t>(result.size()));
}

static Status ValidateArrowVsJson(const std::string& arrow_path,
                                  const std::string& json_path) {
  // Construct JSON reader
  ARROW_ASSIGN_OR_RAISE(auto json_file, io::ReadableFile::Open(json_path));

  ARROW_ASSIGN_OR_RAISE(int64_t file_size, json_file->GetSize());
  ARROW_ASSIGN_OR_RAISE(auto json_buffer, json_file->Read(file_size));

  std::unique_ptr<internal::json::JsonReader> json_reader;
  RETURN_NOT_OK(internal::json::JsonReader::Open(json_buffer, &json_reader));

  // Construct Arrow reader
  ARROW_ASSIGN_OR_RAISE(auto arrow_file, io::ReadableFile::Open(arrow_path));

  std::shared_ptr<RecordBatchFileReader> arrow_reader;
  RETURN_NOT_OK(RecordBatchFileReader::Open(arrow_file.get(), &arrow_reader));

  auto json_schema = json_reader->schema();
  auto arrow_schema = arrow_reader->schema();

  if (!json_schema->Equals(*arrow_schema)) {
    std::stringstream ss;
    ss << "JSON schema: \n"
       << json_schema->ToString() << "\n"
       << "Arrow schema: \n"
       << arrow_schema->ToString();

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
    RETURN_NOT_OK(arrow_reader->ReadRecordBatch(i, &arrow_batch));
    Status valid_st = json_batch->ValidateFull();
    if (!valid_st.ok()) {
      return Status::Invalid("JSON record batch ", i, " did not validate:\n",
                             valid_st.ToString());
    }
    valid_st = arrow_batch->ValidateFull();
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
  if (json_path == "") {
    return Status::Invalid("Must specify json file name");
  }

  if (arrow_path == "") {
    return Status::Invalid("Must specify arrow file name");
  }

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
    },
    {
      "count": 4,
      "columns": [
        {
          "name": "foo",
          "count": 4,
          "DATA": [1, 2, 3, 4],
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
        "typeLayout": {
          "vectors": [
            {"type": "VALIDITY", "typeBitWidth": 1},
            {"type": "DATA", "typeBitWidth": 32}
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

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  int ret = 0;

  if (FLAGS_integration) {
    arrow::Status result = arrow::ipc::RunCommand(FLAGS_json, FLAGS_arrow, FLAGS_mode);
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
