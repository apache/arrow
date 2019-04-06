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

#include "arrow/builder.h"
#include "arrow/io/file.h"
#include "arrow/json/reader.h"
#include "arrow/json/test-common.h"
#include "arrow/table.h"
#include "arrow/testing/util.h"
#include "arrow/util/stl.h"
#include "arrow/util/thread-pool.h"

namespace arrow {
namespace json {

using util::string_view;

using internal::checked_cast;
using internal::GetCpuThreadPool;
using internal::TaskGroup;

TEST(SerialTableReader, Basics) {
  auto read_options = ReadOptions::Defaults();
  read_options.use_threads = false;
  auto parse_options = ParseOptions::Defaults();
  std::shared_ptr<io::InputStream> input;
  ASSERT_OK(MakeStream("{}\n{}\n", &input));
  std::shared_ptr<TableReader> reader;
  ASSERT_OK(TableReader::Make(default_memory_pool(), input, read_options, parse_options,
                              &reader));

  std::shared_ptr<Table> table;
  ASSERT_OK(reader->Read(&table));
}

}  // namespace json
}  // namespace arrow
