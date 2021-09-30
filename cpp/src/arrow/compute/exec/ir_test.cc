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

#include "arrow/compute/exec/ir_consumer.h"

#include <unistd.h>
#include <fstream>

#include "arrow/io/file.h"
#include "arrow/testing/matchers.h"
#include "arrow/util/io_util.h"
#include "arrow/util/string_view.h"

#include "generated/Plan_generated.h"

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::Eq;
using testing::HasSubstr;
using testing::Optional;
using testing::UnorderedElementsAreArray;

namespace ir = org::apache::arrow::computeir::flatbuf;
namespace flatbuf = org::apache::arrow::flatbuf;

namespace arrow {
namespace compute {

std::shared_ptr<Buffer> FlatbufferFromJSON(util::string_view json,
                                           std::string schema_path) {
  static std::unique_ptr<arrow::internal::TemporaryDir> dir;
  if (!dir) {
    dir = *arrow::internal::TemporaryDir::Make("ir_json_");
    chdir(dir->path().ToString().c_str());
  }

  std::ofstream{"ir.json"} << json;

  std::string cmd = "flatc --binary " + schema_path + " ir.json";
  if (int err = std::system(cmd.c_str())) {
    std::cerr << cmd << " failed with error code: " << err;
    std::abort();
  }

  auto bin = *io::MemoryMappedFile::Open("ir.bin", io::FileMode::READ);
  return *bin->Read(*bin->GetSize());
}

TEST(FromJSON, Basic) {
  auto buf = FlatbufferFromJSON(R"({
    type: {
      type_type: "Int",
      type: { bitWidth: 64, is_signed: true }
    },
    impl_type: "Int64Literal",
    impl: { value: 42 }
  })",
                                "~/arrow/experimental/computeir/Literal.fbs");

  ASSERT_THAT(ConvertRoot<ir::Literal>(*buf), ResultWith(DataEq<int64_t>(42)));
}

}  // namespace compute
}  // namespace arrow
