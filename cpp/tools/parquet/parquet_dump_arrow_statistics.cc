// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <cstdlib>
#include <iostream>

namespace {
arrow::Status PrintArrowStatistics(const char* path) {
  ARROW_ASSIGN_OR_RAISE(
      auto input, arrow::io::MemoryMappedFile::Open(path, arrow::io::FileMode::READ));
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        parquet::arrow::OpenFile(input, arrow::default_memory_pool()));
  ARROW_ASSIGN_OR_RAISE(auto record_batch_reader, reader->GetRecordBatchReader());
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto record_batch, record_batch_reader->Next());
    if (!record_batch) {
      break;
    }
    ARROW_ASSIGN_OR_RAISE(auto statistics_array, record_batch->MakeStatisticsArray());
    std::cout << statistics_array->ToString() << std::endl;
  }
  return arrow::Status::OK();
}
};  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " PARQUET_PATH" << std::endl;
    std::cerr << " e.g.: " << argv[0] << " sample.parquet" << std::endl;
    return EXIT_FAILURE;
  }

  auto status = PrintArrowStatistics(argv[1]);
  if (status.ok()) {
    return EXIT_SUCCESS;
  } else {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
}
