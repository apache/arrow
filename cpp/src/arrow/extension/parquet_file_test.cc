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

#include "arrow/extension/parquet_file.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow::extension {

TEST(FileType, InvalidStorage) {
  // FILE storage must be a non-empty struct.
  ASSERT_NOT_OK(FileExtensionType::Make(nullptr));
  ASSERT_NOT_OK(FileExtensionType::Make(utf8()));
  ASSERT_NOT_OK(FileExtensionType::Make(struct_({})));

  // Every FILE storage field must be nullable, including when other fields are valid.
  ASSERT_NOT_OK(FileExtensionType::Make(
      struct_({field("uri", utf8()), field("inline", binary(), /*nullable=*/false)})));

  // FILE storage fields must use one of the six recognized names.
  ASSERT_NOT_OK(FileExtensionType::Make(
      struct_({field("uri", utf8()), field("unknown", binary())})));

  // The uri, content_type, and checksum fields must use a string storage family.
  ASSERT_NOT_OK(FileExtensionType::Make(struct_({field("uri", binary())})));
  ASSERT_NOT_OK(
      FileExtensionType::Make(struct_({field("content_type", large_binary())})));
  ASSERT_NOT_OK(FileExtensionType::Make(struct_({field("checksum", binary_view())})));

  // The offset and size fields must use INT64 storage.
  ASSERT_NOT_OK(FileExtensionType::Make(struct_({field("offset", int32())})));
  ASSERT_NOT_OK(FileExtensionType::Make(struct_({field("size", int32())})));

  // The inline field must use a binary storage family.
  ASSERT_NOT_OK(FileExtensionType::Make(struct_({field("inline", utf8())})));

  // FILE storage field names must be unique.
  ASSERT_NOT_OK(
      FileExtensionType::Make(struct_({field("uri", utf8()), field("uri", utf8())})));
}

}  // namespace arrow::extension
