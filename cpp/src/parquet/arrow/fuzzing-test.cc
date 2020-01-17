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

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>

#include <memory>

#include "parquet/arrow/reader.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  arrow::Status status;

  auto buffer = std::make_shared<arrow::Buffer>(data, size);
  arrow::io::BufferReader buffer_reader(buffer);

  std::unique_ptr<parquet::arrow::FileReader> reader;
  status = parquet::arrow::OpenFile(std::make_shared<arrow::io::BufferReader>(buffer),
                                    ::arrow::default_memory_pool(), &reader);
  if (!status.ok()) {
    return 0;
  }

  int64_t num_rows_returned = 0;
  status = reader->ScanContents({}, 1, &num_rows_returned);
  if (!status.ok()) {
    return 0;
  }

  return 0;
}
