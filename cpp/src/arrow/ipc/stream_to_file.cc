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

#include <iostream>
#include <memory>
#include <string>

#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"

#include "arrow/util/io_util.h"

namespace arrow {
namespace ipc {

// Converts a stream from stdin to a file written to standard out.
// A typical usage would be:
// $ <program that produces streaming output> | stream-to-file > file.arrow
Status ConvertToFile() {
  io::StdinStream input;
  io::StdoutStream sink;

  ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchStreamReader::Open(&input));
  ARROW_ASSIGN_OR_RAISE(
      auto writer, NewFileWriter(&sink, reader->schema(), IpcWriteOptions::Defaults()));
  std::shared_ptr<RecordBatch> batch;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(batch, reader->Next());
    if (batch == nullptr) break;
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  return writer->Close();
}

}  // namespace ipc
}  // namespace arrow

int main(int argc, char** argv) {
  arrow::Status status = arrow::ipc::ConvertToFile();
  if (!status.ok()) {
    std::cerr << "Could not convert to file: " << status.ToString() << std::endl;
    return 1;
  }
  return 0;
}
