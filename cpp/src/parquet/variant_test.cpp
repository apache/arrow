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

#include <gtest/gtest.h>

#include <string>

#include "parquet/test_util.h"
#include "parquet/variant.h"

#include <arrow/filesystem/localfs.h>
#include <arrow/testing/gtest_util.h>

namespace parquet::variant {

TEST(ParquetVariant, MetadataBase) {
  std::string test_file = {"primitive_boolean_true.metadata"};
  auto file_system = std::make_shared<::arrow::fs::LocalFileSystem>();
  {
    std::string dir_string(parquet::test::get_variant_dir());
    std::string path = dir_string + "/" + test_file;
    ASSERT_OK_AND_ASSIGN(auto file, file_system->OpenInputFile(path));
    ASSERT_OK_AND_ASSIGN(auto file_size, file->GetSize());
    ASSERT_OK_AND_ASSIGN(auto buf, file->Read(file_size));

    VariantMetadata metadata;
    metadata.metadata = std::string_view{*buf};
    std::cout << "file_size:" << buf->size() << std::endl;
  }
}

}  // namespace parquet::variant