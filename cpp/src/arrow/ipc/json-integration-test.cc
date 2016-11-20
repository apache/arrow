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
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "arrow/array.h"
#include "arrow/io/file.h"
#include "arrow/ipc/file.h"
#include "arrow/ipc/json.h"
#include "arrow/table.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

DEFINE_string(arrow, "", "Arrow file name");
DEFINE_string(json, "", "JSON file name");
DEFINE_string(mode, "VALIDATE",
    "Mode of integration testing tool (ARROW_TO_JSON, JSON_TO_ARROW, VALIDATE)");
DEFINE_bool(unittest, false, "Run integration test self unit tests");

namespace arrow {

bool file_exists(const char* path) {
  std::ifstream handle(path);
  return handle.good();
}

// Convert JSON file to IPC binary format
static Status ConvertJsonToArrow(
    const std::string& json_path, const std::string& arrow_path) {
  std::shared_ptr<io::ReadableFile> in_file;
  std::shared_ptr<io::FileOutputStream> out_file;

  RETURN_NOT_OK(io::ReadableFile::Open(json_path, &in_file));
  RETURN_NOT_OK(io::FileOutputStream::Open(arrow_path, &out_file));

  int64_t file_size = 0;
  RETURN_NOT_OK(in_file->GetSize(&file_size));

  std::shared_ptr<Buffer> json_buffer;
  RETURN_NOT_OK(in_file->Read(file_size, &json_buffer));

  std::unique_ptr<ipc::JsonReader> reader;
  RETURN_NOT_OK(ipc::JsonReader::Open(json_buffer, &reader));

  std::cout << "Found schema: " << reader->schema()->ToString() << std::endl;

  std::shared_ptr<ipc::FileWriter> writer;
  RETURN_NOT_OK(ipc::FileWriter::Open(out_file.get(), reader->schema(), &writer));

  for (int i = 0; i < reader->num_record_batches(); ++i) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader->GetRecordBatch(i, &batch));
    RETURN_NOT_OK(writer->WriteRecordBatch(batch->columns(), batch->num_rows()));
  }
  return writer->Close();
}

// Convert IPC binary format to JSON
static Status ConvertArrowToJson(
    const std::string& arrow_path, const std::string& json_path) {
  std::shared_ptr<io::ReadableFile> in_file;
  std::shared_ptr<io::FileOutputStream> out_file;

  RETURN_NOT_OK(io::ReadableFile::Open(arrow_path, &in_file));
  RETURN_NOT_OK(io::FileOutputStream::Open(json_path, &out_file));

  std::shared_ptr<ipc::FileReader> reader;
  RETURN_NOT_OK(ipc::FileReader::Open(in_file, &reader));

  std::cout << "Found schema: " << reader->schema()->ToString() << std::endl;

  std::unique_ptr<ipc::JsonWriter> writer;
  RETURN_NOT_OK(ipc::JsonWriter::Open(reader->schema(), &writer));

  for (int i = 0; i < reader->num_record_batches(); ++i) {
    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader->GetRecordBatch(i, &batch));
    RETURN_NOT_OK(writer->WriteRecordBatch(batch->columns(), batch->num_rows()));
  }

  std::string result;
  RETURN_NOT_OK(writer->Finish(&result));
  return out_file->Write(reinterpret_cast<const uint8_t*>(result.c_str()),
      static_cast<int64_t>(result.size()));
}

static Status ValidateArrowVsJson(
    const std::string& arrow_path, const std::string& json_path) {
  // Construct JSON reader
  std::shared_ptr<io::ReadableFile> json_file;
  RETURN_NOT_OK(io::ReadableFile::Open(json_path, &json_file));

  int64_t file_size = 0;
  RETURN_NOT_OK(json_file->GetSize(&file_size));

  std::shared_ptr<Buffer> json_buffer;
  RETURN_NOT_OK(json_file->Read(file_size, &json_buffer));

  std::unique_ptr<ipc::JsonReader> json_reader;
  RETURN_NOT_OK(ipc::JsonReader::Open(json_buffer, &json_reader));

  // Construct Arrow reader
  std::shared_ptr<io::ReadableFile> arrow_file;
  RETURN_NOT_OK(io::ReadableFile::Open(arrow_path, &arrow_file));

  std::shared_ptr<ipc::FileReader> arrow_reader;
  RETURN_NOT_OK(ipc::FileReader::Open(arrow_file, &arrow_reader));

  auto json_schema = json_reader->schema();
  auto arrow_schema = arrow_reader->schema();

  if (!json_schema->Equals(arrow_schema)) {
    std::stringstream ss;
    ss << "JSON schema: \n"
       << json_schema->ToString() << "\n"
       << "Arrow schema: \n"
       << arrow_schema->ToString();

    std::cout << ss.str() << std::endl;
    return Status::Invalid("Schemas did not match");
  }

  const int json_nbatches = json_reader->num_record_batches();
  const int arrow_nbatches = arrow_reader->num_record_batches();

  if (json_nbatches != arrow_nbatches) {
    std::stringstream ss;
    ss << "Different number of record batches: " << json_nbatches << " (JSON) vs "
       << arrow_nbatches << " (Arrow)";
    return Status::Invalid(ss.str());
  }

  std::shared_ptr<RecordBatch> arrow_batch;
  std::shared_ptr<RecordBatch> json_batch;
  for (int i = 0; i < json_nbatches; ++i) {
    RETURN_NOT_OK(json_reader->GetRecordBatch(i, &json_batch));
    RETURN_NOT_OK(arrow_reader->GetRecordBatch(i, &arrow_batch));

    if (!json_batch->Equals(*arrow_batch.get())) {
      std::stringstream ss;
      ss << "Record batch " << i << " did not match";
      return Status::Invalid(ss.str());
    }
  }

  return Status::OK();
}

Status RunCommand(const std::string& json_path, const std::string& arrow_path,
    const std::string& command) {
  if (json_path == "") { return Status::Invalid("Must specify json file name"); }

  if (arrow_path == "") { return Status::Invalid("Must specify arrow file name"); }

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
    std::stringstream ss;
    ss << "Unknown command: " << command;
    return Status::Invalid(ss.str());
  }
}

}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_unittest) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
  }

  arrow::Status result = arrow::RunCommand(FLAGS_json, FLAGS_arrow, FLAGS_mode);
  if (!result.ok()) {
    std::cout << "Error message: " << result.ToString() << std::endl;
    return 1;
  }

  return 0;
}
