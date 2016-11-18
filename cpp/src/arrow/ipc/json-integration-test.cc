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

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "gflags/gflags.h"

#include "arrow/array.h"
#include "arrow/ipc/json.h"
#include "arrow/table.h"
#include "arrow/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/types/primitive.h"
#include "arrow/types/string.h"
#include "arrow/types/struct.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

DEFINE_string(infile, "", "Input file name");
DEFINE_string(outfile, "", "Output file name");
DEFINE_string(mode, "VALIDATE",
    "Mode of integration testing tool (ARROW_TO_JSON, JSON_TO_ARROW, VALIDATE)");

namespace arrow {

}  // namespace arrow

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
}
