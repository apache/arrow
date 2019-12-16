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

#include "parquet/api/reader.h"
#include "parquet/api/schema.h"

int main(int argc, char** argv) {
  bool help_flag = false;
  std::string filename;

  for (int i = 1; i < argc; i++) {
    if (!std::strcmp(argv[i], "-?") || !std::strcmp(argv[i], "-h") ||
        !std::strcmp(argv[i], "--help")) {
      help_flag = true;
    } else {
      filename = argv[i];
    }
  }

  if (argc != 2 || help_flag) {
    std::cerr << "Usage: parquet-dump-schema [-h] [--help]"
              << " <filename>" << std::endl;
    return -1;
  }

  try {
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename);
    PrintSchema(reader->metadata()->schema()->schema_root().get(), std::cout);
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

  return 0;
}
