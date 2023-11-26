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

// A command line executable that generates a bunch of valid IPC files
// containing example record batches.  Those are used as fuzzing seeds
// to make fuzzing more efficient.

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "arrow/io/file.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/testing/extension_type.h"
#include "arrow/util/io_util.h"
#include "arrow/util/key_value_metadata.h"

namespace arrow {
namespace ipc {

using ::arrow::internal::CreateDir;
using ::arrow::internal::PlatformFilename;
using internal::json::ArrayFromJSON;

Result<std::shared_ptr<RecordBatch>> MakeExtensionBatch() {
  auto array = ExampleUuid();
  auto md = key_value_metadata({"key1", "key2"}, {"value1", ""});
  auto schema = ::arrow::schema({field("f0", array->type())}, md);
  return RecordBatch::Make(schema, array->length(), {array});
}

Result<std::shared_ptr<RecordBatch>> MakeMapBatch() {
  std::shared_ptr<Array> array;
  const char* json_input = R"(
[
    [[0, 1], [1, 1], [2, 2], [3, 3], [4, 5], [5, 8]],
    null,
    [[0, null], [1, null], [2, 0], [3, 1], [4, null], [5, 2]],
    []
  ]
)";
  ARROW_ASSIGN_OR_RAISE(array, ArrayFromJSON(map(int16(), int32()), json_input));
  auto schema = ::arrow::schema({field("f0", array->type())});
  return RecordBatch::Make(schema, array->length(), {array});
}

Result<std::vector<std::shared_ptr<RecordBatch>>> Batches() {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  std::shared_ptr<RecordBatch> batch;
  std::shared_ptr<Array> array;

  RETURN_NOT_OK(test::MakeNullRecordBatch(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeListRecordBatch(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeListViewRecordBatch(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeDictionary(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeTimestamps(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeFWBinary(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeStruct(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeUnion(&batch));
  batches.push_back(batch);
  RETURN_NOT_OK(test::MakeFixedSizeListRecordBatch(&batch));
  batches.push_back(batch);
  ARROW_ASSIGN_OR_RAISE(batch, MakeExtensionBatch());
  batches.push_back(batch);
  ARROW_ASSIGN_OR_RAISE(batch, MakeMapBatch());
  batches.push_back(batch);

  return batches;
}

Result<std::shared_ptr<Buffer>> SerializeRecordBatch(
    const std::shared_ptr<RecordBatch>& batch, bool is_stream_format) {
  ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create(1024));
  std::shared_ptr<RecordBatchWriter> writer;
  if (is_stream_format) {
    ARROW_ASSIGN_OR_RAISE(writer, MakeStreamWriter(sink, batch->schema()));
  } else {
    ARROW_ASSIGN_OR_RAISE(writer, MakeFileWriter(sink, batch->schema()));
  }
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

Status DoMain(bool is_stream_format, const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(out_dir));
  RETURN_NOT_OK(CreateDir(dir_fn));

  int sample_num = 1;
  auto sample_name = [&]() -> std::string {
    return "batch-" + std::to_string(sample_num++);
  };

  ARROW_ASSIGN_OR_RAISE(auto batches, Batches());

  for (const auto& batch : batches) {
    RETURN_NOT_OK(batch->ValidateFull());
    ARROW_ASSIGN_OR_RAISE(auto buf, SerializeRecordBatch(batch, is_stream_format));
    ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_name()));
    std::cerr << sample_fn.ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
    RETURN_NOT_OK(file->Write(buf));
    RETURN_NOT_OK(file->Close());
  }
  return Status::OK();
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: arrow-ipc-generate-fuzz-corpus "
            << "[-stream|-file] <output directory>" << std::endl;
  std::exit(2);
}

int Main(int argc, char** argv) {
  if (argc != 3) {
    Usage();
  }
  auto opt = std::string(argv[1]);
  if (opt != "-stream" && opt != "-file") {
    Usage();
  }
  auto out_dir = std::string(argv[2]);

  Status st = DoMain(opt == "-stream", out_dir);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) { return arrow::ipc::Main(argc, argv); }
