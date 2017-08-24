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
#include "arrow/io/file.h"
#include "arrow/ipc/reader.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"

#include "arrow/util/io-util.h"

namespace arrow {
namespace ipc {

// Converts a stream from stdin to a file written to standard out.
// A typical usage would be:
// $ <program that produces streaming output> | stream-to-file > file.arrow
Status ConvertToFile() {
  io::StdinStream input;
  std::shared_ptr<RecordBatchReader> reader;
  RETURN_NOT_OK(RecordBatchStreamReader::Open(&input, &reader));

  io::StdoutStream sink;
  std::shared_ptr<RecordBatchWriter> writer;
  RETURN_NOT_OK(RecordBatchFileWriter::Open(&sink, reader->schema(), &writer));

  std::shared_ptr<RecordBatch> batch;
  while (true) {
    RETURN_NOT_OK(reader->ReadNextRecordBatch(&batch));
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
