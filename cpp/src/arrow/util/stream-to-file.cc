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
#include "arrow/ipc/file.h"
#include "arrow/ipc/stream.h"
#include "arrow/status.h"

#include "arrow/util/io-util.h"

using namespace arrow;
using namespace arrow::io;
using namespace arrow::ipc;
using namespace std;

// Converts a stream from stdin to a file written to standard out.
// A typical usage would be:
// $ <program that produces streaming output> | stream-to-file > file.arrow
Status ConvertToFile() {
  shared_ptr<InputStream> input(new StdinStream);
  shared_ptr<StreamReader> reader;
  RETURN_NOT_OK(StreamReader::Open(input, &reader));

  StdoutStream sink;
  shared_ptr<FileWriter> writer;
  RETURN_NOT_OK(FileWriter::Open(&sink, reader->schema(), &writer));

  shared_ptr<RecordBatch> batch;
  while (true) {
    RETURN_NOT_OK(reader->GetNextRecordBatch(&batch));
    if (batch == nullptr) break;
    RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  }
  return writer->Close();
}

int main(int argc, char** argv) {
  Status status = ConvertToFile();
  if (!status.ok()) {
    cerr << "Could not convert to file: " << status.ToString() << endl;
    return 1;
  }
  return 0;
}
