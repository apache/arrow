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

// Reads a file on the file system and prints to stdout the stream version of it.
Status ConvertToStream(const char* path) {
  shared_ptr<ReadableFile> in_file;
  shared_ptr<ipc::FileReader> reader;

  RETURN_NOT_OK(ReadableFile::Open(path, &in_file));
  RETURN_NOT_OK(FileReader::Open(in_file, &reader));

  StdoutStream sink;
  shared_ptr<StreamWriter> writer;
  RETURN_NOT_OK(StreamWriter::Open(&sink, reader->schema(), &writer));
  for (int i = 0; i < reader->num_record_batches(); ++i) {
    shared_ptr<RecordBatch> chunk;
    RETURN_NOT_OK(reader->GetRecordBatch(i, &chunk));
    RETURN_NOT_OK(writer->WriteRecordBatch(*chunk));
  }
  return writer->Close();
}

int main(int argc, char** argv) {
  if (argc != 2) {
    cerr << "Usage: file-to-stream <input arrow file>" << endl;
    return 1;
  }
  Status status = ConvertToStream(argv[1]);
  if (!status.ok()) {
    cerr << "Could not convert to stream: " << status.ToString() << endl;
    return 1;
  }
  return 0;
}
