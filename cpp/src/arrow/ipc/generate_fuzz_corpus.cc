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
#include "arrow/ipc/test_common.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/io_util.h"

namespace arrow {
namespace ipc {

using ::arrow::internal::CreateDir;
using ::arrow::internal::PlatformFilename;

Result<std::vector<std::shared_ptr<RecordBatch>>> Batches() {
  std::vector<std::shared_ptr<RecordBatch>> batches;
  std::shared_ptr<RecordBatch> batch;
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
  return batches;
}

template <typename RecordBatchWriterClass>
Result<std::shared_ptr<Buffer>> SerializeRecordBatch(
    const std::shared_ptr<RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto sink, io::BufferOutputStream::Create(1024));
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        RecordBatchWriterClass::Open(sink.get(), batch->schema()));
  RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  RETURN_NOT_OK(writer->Close());
  return sink->Finish();
}

Status DoMain(bool is_stream_format, const std::string& out_dir) {
  ARROW_ASSIGN_OR_RAISE(auto dir_fn, PlatformFilename::FromString(out_dir));
  RETURN_NOT_OK(CreateDir(dir_fn));

  auto serialize_func = is_stream_format ? SerializeRecordBatch<RecordBatchStreamWriter>
                                         : SerializeRecordBatch<RecordBatchFileWriter>;

  ARROW_ASSIGN_OR_RAISE(auto batches, Batches());
  int batch_num = 1;
  for (const auto& batch : batches) {
    RETURN_NOT_OK(batch->ValidateFull());
    ARROW_ASSIGN_OR_RAISE(auto buf, serialize_func(batch));
    auto name = "batch-" + std::to_string(batch_num++);

    ARROW_ASSIGN_OR_RAISE(auto batch_fn, dir_fn.Join(name));
    std::cerr << batch_fn.ToString() << std::endl;
    ARROW_ASSIGN_OR_RAISE(auto file, io::FileOutputStream::Open(batch_fn.ToString()));
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
