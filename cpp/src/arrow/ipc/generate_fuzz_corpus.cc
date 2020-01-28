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

using ::arrow::internal::PlatformFilename;
using internal::json::ArrayFromJSON;

Result<PlatformFilename> PrepareDirectory(const std::string& dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(dir));
  RETURN_NOT_OK(::arrow::internal::CreateDir(dir_fn));
  return std::move(dir_fn);
}

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
  RETURN_NOT_OK(ArrayFromJSON(map(int16(), int32()), json_input, &array));
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

Result<std::shared_ptr<Buffer>> MakeSerializedBuffer(
    std::function<Status(const std::shared_ptr<io::BufferOutputStream>&)> fn) {
  ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create(1024));
  RETURN_NOT_OK(fn(sink));
  return sink->Finish();
}

Result<std::shared_ptr<Buffer>> SerializeRecordBatch(
    const std::shared_ptr<RecordBatch>& batch, bool is_stream_format) {
  return MakeSerializedBuffer(
      [&](const std::shared_ptr<io::BufferOutputStream>& sink) {
        std::shared_ptr<RecordBatchWriter> writer;
        if (is_stream_format) {
          ARROW_ASSIGN_OR_RAISE(writer, MakeStreamWriter(sink, batch->schema()));
        } else {
          ARROW_ASSIGN_OR_RAISE(writer, MakeFileWriter(sink, batch->schema()));
        }
        RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
        return writer->Close();
      });
}

Status GenerateRecordBatches(bool is_stream_format, const PlatformFilename& dir_fn) {
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

Result<std::shared_ptr<Buffer>> SerializeTensor(
    const std::shared_ptr<Tensor>& tensor, bool is_stream_format) {
  return MakeSerializedBuffer(
      [&](const std::shared_ptr<io::BufferOutputStream>& sink) -> Status {
        int32_t metadata_length;
        int64_t body_length;
        // TODO(mrkn): FileWriter for tensors
        return ipc::WriteTensor(*tensor, sink.get(), &metadata_length, &body_length);
      });
}

Result<std::vector<std::shared_ptr<Tensor>>> Tensors() {
  std::vector<std::shared_ptr<Tensor>> tensors;
  std::shared_ptr<Tensor> tensor;
  std::vector<int64_t> shape = {5, 3, 7};
  std::shared_ptr<DataType> types[] = {int8(),  int16(),  int32(),  int64(),
                                       uint8(), uint16(), uint32(), uint64()};
  for (auto type : types) {
    RETURN_NOT_OK(test::MakeTensor(type, shape, true, &tensor));
    tensors.push_back(tensor);
    RETURN_NOT_OK(test::MakeTensor(type, shape, false, &tensor));
    tensors.push_back(tensor);
  }
  return tensors;
}

Status GenerateTensors(bool is_stream_format, const PlatformFilename& dir_fn) {
  if (!is_stream_format) {
    return Status::NotImplemented("Tensor now does not support serialization to a file");
  }

  int sample_num = 1;
  auto sample_name = [&]() -> std::string {
    return "tensor-" + std::to_string(sample_num++);
  };

  ARROW_ASSIGN_OR_RAISE(auto tensors, Tensors());

  for (const auto& tensor : tensors) {
    ARROW_ASSIGN_OR_RAISE(auto buf, SerializeTensor(tensor, is_stream_format));
    ARROW_ASSIGN_OR_RAISE(auto sample_fn, dir_fn.Join(sample_name()));
    std::cerr << sample_fn.ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(sample_fn.ToString()));
    RETURN_NOT_OK(file->Write(buf));
    RETURN_NOT_OK(file->Close());
  }
  return Status::OK();
}

Status DoMain(bool is_stream_format, const std::string& type,
              const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PrepareDirectory(out_dir));

  if (type == "record_batch") {
    return GenerateRecordBatches(is_stream_format, dir_fn);
  } else if (type == "tensor") {
    return GenerateTensors(is_stream_format, dir_fn);
  }

  return Status::Invalid("Unknown type: " + type);
}

ARROW_NORETURN void Usage() {
  std::cerr << "Usage: arrow-ipc-generate-fuzz-corpus "
            << "[-stream|-file] [record_batch|tensor] "
            << "<output directory>" << std::endl;
  std::exit(2);
}

int Main(int argc, char** argv) {
  if (argc != 4) {
    Usage();
  }

  auto opt = std::string(argv[1]);
  if (opt != "-stream" && opt != "-file") {
    Usage();
  }

  auto type = std::string(argv[2]);
  if (type != "record_batch" && type != "tensor") {
    Usage();
  }

  if (opt == "-file" && type == "tensor") {
    std::cerr << "file output for " << type << " is currently unsupported" << std::endl;
    Usage();
  }

  auto out_dir = std::string(argv[3]);

  Status st = DoMain(opt == "-stream", type, out_dir);
  if (!st.ok()) {
    std::cerr << st.ToString() << std::endl;
    return 1;
  }
  return 0;
}

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) { return arrow::ipc::Main(argc, argv); }
